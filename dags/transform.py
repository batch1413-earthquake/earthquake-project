import os
from datetime import datetime

import pandas as pd

from airflow import DAG

from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.operators.python import BranchPythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"

def should_upload_to_big_query(parquet_file_path):
    if len(pd.read_parquet(parquet_file_path)) > 0:
        return "load_to_bigquery"
    else:
        return "do_not_need"


with DAG(
    "transform",
    default_args={"depends_on_past": True},
    start_date=datetime(1949, 1, 1),
    end_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True
) as dag:

    date_str = "{{ yesterday_ds }}"

    earthquake_file_name = f"{FILE_PREFIX}_{date_str}"

    local_silver_path = f"{AIRFLOW_HOME}/data/silver"

    earthquake_parquet_file_path = f"{local_silver_path}/{earthquake_file_name}.parquet"

    wait_for_load_task = ExternalTaskSensor(
        task_id="load_sensor",
        external_dag_id='load',
        external_task_id='upload_local_earthquake_file_to_gcs',
        timeout=600,
        allowed_states=['success'],
        poke_interval=10
    )

    should_upload_to_big_query_task = BranchPythonOperator(
        task_id='should_upload_to_big_query',
        python_callable=should_upload_to_big_query,
        op_kwargs={"parquet_file_path": earthquake_parquet_file_path}
    )

    do_not_need_task = EmptyOperator(task_id="do_not_need")


    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=os.environ['SILVER_BUCKET_NAME'],
        source_objects=f"usgs_data/{earthquake_file_name}.parquet",
        source_format='parquet',
        destination_project_dataset_table="batch1413-earthquake.gold_earthquake_dataset.earthquakes",
        gcp_conn_id="google_cloud_connection",
        write_disposition="WRITE_APPEND",
        max_bad_records=1,
        ignore_unknown_values=True,
    )

    wait_for_load_task >> should_upload_to_big_query_task >> [load_to_bigquery_task, do_not_need_task]
