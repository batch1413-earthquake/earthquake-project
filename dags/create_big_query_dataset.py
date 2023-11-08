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
GCP_CONNECTION_ID = os.getenv('GCP_CONNECTION_ID')


with DAG(
    "create_big_query_dataset",
    schedule_interval='@once',
    start_date= datetime(2023, 1, 1),
) as dag:


    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id=f"{GCP_CONNECTION_ID}",
        dataset_id="gold_earthquake_dataset"
    )

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        gcp_conn_id=f"{GCP_CONNECTION_ID}",
        dataset_id="gold_earthquake_dataset",
        table_id="earthquakes",
        schema_fields=[{'name': 'type', 'type': 'STRING'},
            {'name': 'id', 'type': 'STRING'},
            {'name': 'properties_magnitude', 'type': 'FLOAT'},
            {'name': 'properties_place', 'type': 'STRING'},
            {'name': 'properties_time', 'type': 'DATETIME'},
            {'name': 'properties_updated', 'type': 'DATETIME'},
            {'name': 'properties_felt_count', 'type': 'INTEGER'},
            {'name': 'properties_alert', 'type': 'STRING'},
            {'name': 'properties_status', 'type': 'STRING'},
            {'name': 'properties_tsunami', 'type': 'BOOLEAN'},
            {'name': 'properties_significance', 'type': 'INTEGER'},
            {'name': 'properties_seismic_station_count', 'type': 'INTEGER'},
            {'name': 'properties_type', 'type': 'STRING'},
            {'name': 'properties_title', 'type': 'STRING'},
            {'name': 'longitude', 'type': 'FLOAT'},
            {'name': 'latitude', 'type': 'FLOAT'},
            {'name': 'elevation', 'type': 'FLOAT'}]
    )

    create_dataset_task >> create_table_task
