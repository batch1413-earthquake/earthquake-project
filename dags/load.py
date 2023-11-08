import os
import json
from datetime import datetime

import pandas as pd
import geopandas as gpd


from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"


def geojson_data_to_parquet(json_file_path: str, parquet_file_path: str):
    with open(json_file_path, "r") as f:
        pd.json_normalize(json.load(f)["features"]).to_parquet(parquet_file_path, index=False)


# def merge_countries_and_details_save_locally(
#     countries_geojson_bronze_file_path: str,
#     countries_details_bronze_file_path: str,
#     countries_geojson_silver_file_path: str,
# ):
#     gdf = gpd.read_file(countries_geojson_bronze_file_path)
#     df_detail = pd.read_csv(countries_details_bronze_file_path)
#     gdf["ISO_A3"] = gdf["ISO_A3"].astype(str)
#     df_detail['ISO_A3'] = df_detail['alpha-3'].astype(str)
#     new_df = gdf.join(df_detail.set_index("ISO_A3"), on="ISO_A3")
#     new_df["COUNTRY_NAME"] = new_df["ADMIN"]
#     export = new_df[["COUNTRY_NAME", "ISO_A3", "geometry", "region", "sub-region"]]
#     export.to_file(countries_geojson_silver_file_path, driver="GeoJSON")


with DAG(
    "load", default_args={"depends_on_past": False}, start_date=datetime(2023, 1, 1), end_date=datetime(2024, 1, 1), schedule_interval="@monthly", catchup=True
) as dag:
    date_str = "{{ yesterday_ds }}"

    earthquake_file_name = f"{FILE_PREFIX}_{date_str}"
    countries_geojson_file_name = "countries.geojson"
    countries_details_file_name = "countries_detail.csv"

    local_bronze_path = f"{AIRFLOW_HOME}/data/bronze"
    local_silver_path = f"{AIRFLOW_HOME}/data/silver"

    earthquake_json_file_path = f"{local_bronze_path}/{earthquake_file_name}.json"
    earthquake_parquet_file_path = f"{local_silver_path}/{earthquake_file_name}.parquet"

    gcp_conn_id = os.environ["GCP_CONNECTION_ID"]

    wait_for_extract_task = ExternalTaskSensor(
        task_id="extract_sensor",
        external_dag_id="extract",
        external_task_id="upload_local_earthquake_file_to_gcs",
        timeout=600,
        allowed_states=["success"],
        poke_interval=10,
    )

    create_silver_folder_task = BashOperator(task_id="create_silver_folder", bash_command=f"mkdir -p {local_silver_path}")

    # earthquake flow

    download_geojson_data_task = GCSToLocalFilesystemOperator(
        task_id="download_geojson_data",
        object_name=f"bronze/usgs_data/{earthquake_json_file_path.split('/')[-1]}",
        bucket=os.environ["BUCKET_NAME"],
        filename=earthquake_json_file_path,
        gcp_conn_id=gcp_conn_id,
    )

    geojson_data_to_parquet_task = PythonOperator(
        task_id="geojson_data_to_parquet",
        python_callable=geojson_data_to_parquet,
        op_kwargs=dict(json_file_path=earthquake_json_file_path, parquet_file_path=earthquake_parquet_file_path),
    )

    upload_local_earthquake_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_earthquake_file_to_gcs",
        src=earthquake_parquet_file_path,
        dst=f"silver/usgs_data/",
        bucket=os.environ["BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id,
    )

    wait_for_extract_task >> create_silver_folder_task

    # # earthquake flow orchestration
    create_silver_folder_task >> download_geojson_data_task
    download_geojson_data_task >> geojson_data_to_parquet_task
    geojson_data_to_parquet_task >> upload_local_earthquake_file_to_gcs_task
