import os
import json
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"

def split_coordinates(row: pd.Series):
    coordinates = row['geometry.coordinates']
    row['longitude'] = coordinates[0]
    row['latitude'] = coordinates[1]
    row['elevation'] = coordinates[2]
    return row

def geojson_data_to_parquet(json_file_path: str, parquet_file_path:str):
    with open(json_file_path, "r") as f:
        df = pd.json_normalize(json.load(f)["features"])
        renaming = {'properties.mag': 'properties_magnitude',
            'properties.place':  'properties_place',
            'properties.time':  'properties_time',
            'properties.updated':  'properties_updated',
            'properties.felt':  'properties_felt_count',
            'properties.alert':  'properties_alert',
            'properties.status':  'properties_status',
            'properties.tsunami':  'properties_tsunami',
            'properties.sig':  'properties_significance',
            'properties.nst':  'properties_seismic_station_count',
            'properties.type':  'properties_type',
            'properties.title':  'properties_title'}
        df.rename(columns=renaming, inplace=True)
        df.apply(split_coordinates, axis='columns')\
            .to_parquet(parquet_file_path, index=False)

with DAG(
    "load",
    default_args={"depends_on_past": False},
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True
) as dag:
    date_str = "{{ yesterday_ds }}"

    file_name = f"{FILE_PREFIX}_{date_str}"
    local_bronze_path = f"{AIRFLOW_HOME}/data/bronze"
    local_silver_path = f"{AIRFLOW_HOME}/data/silver"
    json_file_path = f"{local_bronze_path}/{file_name}.json"
    parquet_file_path = f"{local_silver_path}/{file_name}.parquet"

    gcp_conn_id = os.environ["GCP_CONNECTION_ID"]

    wait_for_extract_task = ExternalTaskSensor(
        task_id="extract_sensor",
        external_dag_id='extract',
        external_task_id='upload_local_file_to_gcs',
        timeout=600,
        allowed_states=['success'],
        poke_interval=10
    )

    create_silver_folder_task = BashOperator(
        task_id="create_silver_folder",
        bash_command=f"mkdir -p {local_silver_path}"
    )

    download_geojson_data_task = GCSToLocalFilesystemOperator(
        task_id="download_geojson_data",
        object_name=f"usgs_data/{json_file_path.split('/')[-1]}",
        bucket=os.environ["BRONZE_BUCKET_NAME"],
        filename=json_file_path,
        gcp_conn_id=gcp_conn_id
    )

    geojson_data_to_parquet_task = PythonOperator(
        task_id="geojson_data_to_parquet",
        python_callable=geojson_data_to_parquet,
        op_kwargs=dict(
            json_file_path=json_file_path,
            parquet_file_path=parquet_file_path
        )
    )

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        src=parquet_file_path,
        dst=f"usgs_data/",
        bucket=os.environ["SILVER_BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id
    )

    wait_for_extract_task >> create_silver_folder_task
    create_silver_folder_task >> download_geojson_data_task
    download_geojson_data_task >> geojson_data_to_parquet_task
    geojson_data_to_parquet_task >> upload_local_file_to_gcs_task
