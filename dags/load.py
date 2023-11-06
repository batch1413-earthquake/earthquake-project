import os
from datetime import datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

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
    wait_for_transform_task = ExternalTaskSensor(task_id="transform_sensor", external_dag_id="transform", timeout=600, poke_interval=10, mode="poke")

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        src=filtered_data_file,
        dst=f"yellow_tripdata_{date}.csv",
        bucket="de_airflow_taxi_silver_maxime_delobello",
        gcp_conn_id="google_cloud_connection",
    )

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        project_id="lewagon-bootcamp-401510",
        dataset_id="de_airflow_taxi_gold",
        location="EU",
        gcp_conn_id="google_cloud_connection",
        exists_ok=True,
    )

    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        table_id="trips",
        schema_fields=[
            {"name": "date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        project_id="lewagon-bootcamp-401510",
        dataset_id="de_airflow_taxi_gold",
        location="EU",
        gcp_conn_id="google_cloud_connection",
        exists_ok=True,
    )

    remove_existing_data_task = BigQueryInsertJobOperator(
        task_id="remove_existing_data",
        configuration={"query": {"query": f"DELETE FROM `lewagon-bootcamp-401510.de_airflow_taxi_gold.trips` WHERE date ='{date}'", "use_legacy_sql": False}},
        project_id="lewagon-bootcamp-401510",
        location="EU",
        gcp_conn_id="google_cloud_connection",
    )

    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        gcp_conn_id="google_cloud_connection",
        bucket="de_airflow_taxi_silver_maxime_delobello",
        source_objects=f"yellow_tripdata_{date}.csv",
        destination_project_dataset_table="de_airflow_taxi_gold.trips",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
    )

    wait_for_transform_task >> upload_local_file_to_gcs_task >> create_dataset_task >> create_table_task >> remove_existing_data_task >> load_to_bigquery_task
