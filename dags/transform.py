import os
from datetime import datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"

with DAG(
    "create_big_query_dataset",
    schedule_interval='@once',
    start_date= datetime.now(),
) as dag:

    date_str = "{{ yesterday_ds }}"

    file_name = f"{FILE_PREFIX}_{date_str}"
    local_silver_path = f"{AIRFLOW_HOME}/data/silver"
    parquet_file_path = f"{local_silver_path}/{file_name}.parquet"
