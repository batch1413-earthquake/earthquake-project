import os
from datetime import datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"

with DAG(
    "transform_1990_2007",
    default_args={"depends_on_past": True},
    start_date=datetime(1990, 1, 1),
    end_date=datetime(2007, 12, 1),
    schedule_interval="@monthly",
    catchup=True
) as dag:

    date_str = "{{ yesterday_ds }}"

    file_name = f"{FILE_PREFIX}_{date_str}"

    wait_for_load_task = ExternalTaskSensor(
        task_id="load_sensor",
        external_dag_id='load_1990_2007',
        external_task_id='upload_local_earthquake_file_to_gcs',
        timeout=600,
        allowed_states=['success'],
        poke_interval=10
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=os.environ['SILVER_BUCKET_NAME'],
        source_objects=f"usgs_data/{file_name}.parquet",
        source_format='parquet',
        destination_project_dataset_table="batch1413-earthquake.gold_earthquake_dataset.earthquakes",
        gcp_conn_id="google_cloud_connection",
        write_disposition="WRITE_APPEND",
        max_bad_records=1,
        ignore_unknown_values=True,
    )

    wait_for_load_task >> load_to_bigquery_task
