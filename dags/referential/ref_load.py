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


def merge_countries_and_details_save_locally(
    countries_geojson_bronze_file_path: str,
    countries_details_bronze_file_path: str,
    countries_geojson_silver_file_path: str,
):
    gdf = gpd.read_file(countries_geojson_bronze_file_path)
    df_detail = pd.read_csv(countries_details_bronze_file_path)
    gdf["ISO_A3"] = gdf["ISO_A3"].astype(str)
    df_detail['ISO_A3'] = df_detail['alpha-3'].astype(str)
    new_df = gdf.join(df_detail.set_index("ISO_A3"), on="ISO_A3")
    new_df["COUNTRY_NAME"] = new_df["ADMIN"]
    export = new_df[["COUNTRY_NAME", "ISO_A3", "geometry", "region", "sub-region"]]
    export.to_file(countries_geojson_silver_file_path, driver="GeoJSON")


with DAG(
    "ref_load",
    start_date=datetime.now(),
    schedule_interval="@once",
    catchup=False,
) as dag:
    date_str = "{{ yesterday_ds }}"

    earthquake_file_name = f"{FILE_PREFIX}_{date_str}"
    countries_geojson_file_name = "countries.geojson"
    countries_details_file_name = "countries_detail.csv"

    local_bronze_path = f"{AIRFLOW_HOME}/data/bronze"
    local_silver_path = f"{AIRFLOW_HOME}/data/silver"

    earthquake_json_file_path = f"{local_bronze_path}/{earthquake_file_name}.json"
    earthquake_parquet_file_path = f"{local_silver_path}/{earthquake_file_name}.parquet"

    countries_geojson_bronze_file_path = f"{local_bronze_path}/countries.geojson"
    countries_details_bronze_file_path = f"{local_bronze_path}/countries_detail.csv"
    countries_geojson_silver_file_path = f"{local_silver_path}/referential_{countries_geojson_file_name}"

    gcp_conn_id = os.environ["GCP_CONNECTION_ID"]

    wait_for_extract_ref_task = ExternalTaskSensor(
        task_id="extract_sensor_ref",
        external_dag_id="extract_ref",
        external_task_id="upload_local_earthquake_file_to_gcs",
        timeout=600,
        allowed_states=["success"],
        poke_interval=10,
    )

    create_silver_folder_task = BashOperator(task_id="create_silver_folder", bash_command=f"mkdir -p {local_silver_path}")


    # referential flow

    merge_countries_and_details_save_locally_task = PythonOperator(
        task_id="merge_countries_and_details_save_locally",
        python_callable=merge_countries_and_details_save_locally,
        op_kwargs=dict(
            countries_geojson_bronze_file_path=countries_geojson_bronze_file_path,
            countries_details_bronze_file_path=countries_details_bronze_file_path,
            countries_geojson_silver_file_path=countries_geojson_silver_file_path,
        ),
    )

    upload_local_referential_countries_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_referential_countries_file_to_gcs",
        src=countries_geojson_silver_file_path,
        dst=f"silver/referential/",
        bucket=os.environ["BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id,
    )


    wait_for_extract_ref_task >> create_silver_folder_task


    create_silver_folder_task >> merge_countries_and_details_save_locally_task >> upload_local_referential_countries_file_to_gcs_task
