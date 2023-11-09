import os
import json
from datetime import datetime
import numpy as np

import pandas as pd

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"

def geojson_data_to_parquet(json_file_path: str, parquet_file_path:str):
    with open(json_file_path, "r") as f:
        json_data = json.load(f)
        df = pd.json_normalize(json_data["features"])


        renaming = {'properties.mag': 'properties_magnitude',
            'properties.place': 'properties_place',
            'properties.time': 'properties_time',
            'properties.updated': 'properties_updated',
            'properties.felt': 'properties_felt_count',
            'properties.alert': 'properties_alert',
            'properties.status': 'properties_status',
            'properties.tsunami': 'properties_tsunami',
            'properties.sig': 'properties_significance',
            'properties.nst': 'properties_seismic_station_count',
            'properties.type': 'properties_type',
            'properties.title': 'properties_title'}

        new_names = {}
        create_empty = []
        for k,v in renaming.items():
            if k in df.columns:
                new_names[k] = v
            else:
                create_empty.append(v)

        force_types = {
            "properties.mag": "float",
            'properties.place': 'string',
            'properties.time': 'datetime64[ms]',
            'properties.updated': 'datetime64[ms]',
            'properties.felt': 'Int64',
            'properties.alert': 'string',
            'properties.status': 'string',
            'properties.tsunami': 'Int64',
            'properties.sig': 'Int64',
            'properties.nst': 'Int64',
            'properties.type': 'string',
            'properties.title': 'string'
        }

        for new_column in create_empty:
            df[new_column] = None
            if new_column in force_types.keys():
                if new_column == "properties.mag":
                    df[new_column] = 0.0
                elif new_column == "properties.felt":
                    df[new_column] = 0
                else:
                    df[new_column].astype(force_types[new_column])

        df.rename(columns=new_names, inplace=True)
        df["properties_felt_count"] = df["properties_felt_count"].astype("Int64")
        df["properties_seismic_station_count"] = df["properties_seismic_station_count"].astype("Int64")
        df["properties_time"] = df["properties_time"].astype('datetime64[ms]')
        df['properties_updated'] = df['properties_updated'].astype('datetime64[ms]')
        df["properties_alert"] = df["properties_alert"].astype("string")
        df["properties_place"] = df["properties_place"].astype("string")
        df["properties_status"] = df["properties_status"].astype("string")
        df["properties_type"] = df["properties_type"].astype("string")

        if "geometry.coordinates" in df.columns:
            df = pd.concat([df, pd.DataFrame(df["geometry.coordinates"].tolist(), columns=["longitude", "latitude", "elevation"])], axis=1)
        else:
            df['longitude'] = df.get('longitude', None)
            df["longitude"] = df["longitude"].astype("Int64")
            df['latitude'] = df.get('latitude', None)
            df["latitude"] = df["latitude"].astype("Int64")
            df['elevation'] = df.get('elevation', None)
            df["elevation"] = df["elevation"].astype("Int64")

        if "id" not in df.columns:
            df["id"] = None
            df["id"] = df["id"].astype("string")

        df[['id', 'properties_magnitude', 'properties_place',
            'properties_time', 'properties_updated',
            'properties_felt_count', 'properties_alert',
            'properties_status', 'properties_tsunami',
            'properties_significance', 'properties_seismic_station_count',
            'properties_type', 'properties_title', 'longitude', 'latitude', 'elevation']] \
            .to_parquet(parquet_file_path, index=False)


with DAG(
    "load_november",
    default_args={"depends_on_past": False},
    start_date=datetime(2023, 11, 2),
    schedule_interval="@once",
    catchup=True
) as dag:
    date_str = "{{ yesterday_ds }}"

    earthquake_file_name = f"{FILE_PREFIX}_{date_str}"

    local_bronze_path = f"{AIRFLOW_HOME}/data/bronze"
    local_silver_path = f"{AIRFLOW_HOME}/data/silver"

    earthquake_json_file_path = f"{local_bronze_path}/{earthquake_file_name}.json"
    earthquake_parquet_file_path = f"{local_silver_path}/{earthquake_file_name}.parquet"

    gcp_conn_id = os.environ["GCP_CONNECTION_ID"]

    wait_for_extract_task = ExternalTaskSensor(
        task_id="extract_sensor",
        external_dag_id="extract_november",
        external_task_id="upload_local_earthquake_file_to_gcs",
        timeout=600,
        allowed_states=["success"],
        poke_interval=10,
    )

    create_silver_folder_task = BashOperator(task_id="create_silver_folder", bash_command=f"mkdir -p {local_silver_path}")

    download_geojson_data_task = GCSToLocalFilesystemOperator(
        task_id="download_geojson_data",
        object_name=f"usgs_data/{earthquake_json_file_path.split('/')[-1]}",
        bucket=os.environ["BRONZE_BUCKET_NAME"],
        filename=earthquake_json_file_path,
        gcp_conn_id=gcp_conn_id
    )

    geojson_data_to_parquet_task = PythonOperator(
        task_id="geojson_data_to_parquet",
        python_callable=geojson_data_to_parquet,
        op_kwargs=dict(json_file_path=earthquake_json_file_path, parquet_file_path=earthquake_parquet_file_path),
    )

    upload_local_earthquake_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_earthquake_file_to_gcs",
        src=earthquake_parquet_file_path,
        dst="usgs_data/",
        bucket=os.environ["SILVER_BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id
    )

    wait_for_extract_task >> create_silver_folder_task

    create_silver_folder_task >> download_geojson_data_task
    download_geojson_data_task >> geojson_data_to_parquet_task
    geojson_data_to_parquet_task >> upload_local_earthquake_file_to_gcs_task
