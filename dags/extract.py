import os
import requests
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"
API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?"

def extract_geojson_data(date_str: str, json_file_path: str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    query_params = {
        'starttime': date.replace(day=1),
        'endtime': date,
        'format': 'geojson',
    }

    try:
        response = requests.get(API_URL, params=query_params)

        json_data = {}
        if response.status_code in [200, 204]:
            json_data = response.json()
        else:
            json_data = {"error": f"Request failed with status code: {response.status_code}"}

    except requests.exceptions.RequestException as e:
        json_data = {"error": f"Request error: {e}"}

    with open(json_file_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f)

with DAG(
    "extract",
    default_args={"depends_on_past": False},
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True
) as dag:
    date_str = "{{ yesterday_ds }}"
    json_file_path = f"{AIRFLOW_HOME}/data/{FILE_PREFIX}_{date_str}.json"
    gcp_conn_id = os.environ["GCP_CONNECTION_ID"]

    extract_geojson_data_task = PythonOperator(
        task_id="extract_geojson_data",
        python_callable=extract_geojson_data,
        op_kwargs=dict(
            date_str=date_str,
            json_file_path=json_file_path
        )
    )

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        src=json_file_path,
        dst="bronze/",
        bucket=os.environ["BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id
    )

    extract_geojson_data_task >> upload_local_file_to_gcs_task
