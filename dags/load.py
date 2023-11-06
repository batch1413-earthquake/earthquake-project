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

with DAG(
    "load",
    default_args={"depends_on_past": True},
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 12, 31),
    schedule_interval="@monthly",
) as dag:

    date = "{{ ds[:7] }}"
    filtered_data_file = f"{AIRFLOW_HOME}/data/silver/yellow_tripdata_{date}.csv"
    bucket_name = 'de_airflow_taxi_silver_jlsrvr'
    gcp_conn_id = 'google_cloud_connection'
    dataset_id = 'de_airflow_taxi_gold'
    project_id = 'skilful-bloom-401510'
    table_id = "trips"


    wait_for_transform_task = ExternalTaskSensor(
        task_id="transform_sensor",
        external_dag_id='transform',
        poke_interval=10,
        timeout=600,
        allowed_states=['success']
    )

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        src=filtered_data_file,
        dst="data/",
        bucket=bucket_name,
        gcp_conn_id=gcp_conn_id
    )

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=dataset_id,
        project_id=project_id,
        gcp_conn_id=gcp_conn_id,
        exists_ok=True,
    )

    schema_fields=[{"name": "date", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"}]
    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=dataset_id,
        project_id=project_id,
        gcp_conn_id=gcp_conn_id,
        table_id=table_id,
        schema_fields= schema_fields,
    )

    remove_existing_data_task = BigQueryInsertJobOperator(
        task_id="remove_existing_data",
        gcp_conn_id=gcp_conn_id,
        configuration={
            'query': {
                'query': open(f"{AIRFLOW_HOME}/dags/delete_all_for_date.sql", 'r').read().format(**locals()),
                'useLegacySql': False,
            }
        },
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        gcp_conn_id=gcp_conn_id,
        bucket=bucket_name,
        source_objects=f"data/yellow_tripdata_{date}.csv",
        destination_project_dataset_table= f"{project_id}.{dataset_id}.{table_id}",
        schema_fields=schema_fields,
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1
    )

    wait_for_transform_task >> upload_local_file_to_gcs_task >> create_dataset_task >> create_table_task >> remove_existing_data_task >> load_to_bigquery_task
