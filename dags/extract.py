import os
import requests
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
FILE_PREFIX = "geojson_data"
API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?"

COUNTRIES_URL_GEOJSON = "https://pkgstore.datahub.io/core/geo-countries/countries/archive/23f420f929e0e09c39d916b8aaa166fb/countries.geojson"
COUNTRIES_DETAIL_GEOJSON = "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv"


def call_api(url, query_params):
    try:
        response = requests.get(url, params=query_params)

        json_data = {}
        if response.status_code in [200, 204]:
            json_data = response.json()
        else:
            json_data = {"error": f"Request failed with status code: {response.status_code}"}

    except requests.exceptions.RequestException as e:
        json_data = {"error": f"Request error: {e}"}

    return json_data


def save_file_locally(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def extract_geojson_data(date_str: str, json_file_path: str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    query_params = {
        "starttime": date.replace(day=1),
        "endtime": date,
        "format": "geojson",
    }

    data = call_api(API_URL, query_params)
    save_file_locally(json_file_path, data)


def extract_geojson_countries(json_file_path):
    data = call_api(COUNTRIES_URL_GEOJSON, {})
    save_file_locally(json_file_path, data)


def extract_detail_countries(json_file_path):
    data = call_api(COUNTRIES_DETAIL_GEOJSON, {})
    save_file_locally(json_file_path, data)


with DAG(
    "extract",
    default_args={"depends_on_past": False},
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True,
) as dag:
    date_str = "{{ yesterday_ds }}"
    earthquake_json_file_path = f"{AIRFLOW_HOME}/data/{FILE_PREFIX}_{date_str}.json"
    countries_json_file_path = f"{AIRFLOW_HOME}/data/countries.geojson"
    countries_detail_json_file_path = f"{AIRFLOW_HOME}/data/countries_detail.csv"

    gcp_conn_id = os.environ["GCP_CONNECTION_ID"]

    # create folder if not exists
    create_bronze_folder_task = BashOperator(
        task_id="create_bronze_folder",
        bash_command=f"mkdir -p {AIRFLOW_HOME}/data/bronze/",
    )

    extract_geojson_data_task = PythonOperator(
        task_id="extract_geojson_data", python_callable=extract_geojson_data, op_kwargs=dict(date_str=date_str, json_file_path=earthquake_json_file_path)
    )

    extract_country_geojson_task = PythonOperator(
        task_id="extract_country_geojson", python_callable=extract_geojson_countries, op_kwargs=dict(json_file_path=countries_json_file_path)
    )

    extract_country_detail_task = PythonOperator(
        task_id="extract_country_detail", python_callable=extract_detail_countries, op_kwargs=dict(json_file_path=countries_detail_json_file_path)
    )

    upload_local_earthquake_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_earthquake_file_to_gcs",
        src=earthquake_json_file_path,
        dst="bronze/usgs_data/",
        bucket=os.environ["BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id,
    )

    upload_local_country_geojson_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_country_geojson_file_to_gcs",
        src=countries_json_file_path,
        dst="bronze/referential/",
        bucket=os.environ["BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id,
    )

    upload_local_country_detail_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_country_detail_file_to_gcs",
        src=countries_detail_json_file_path,
        dst="bronze/referential/",
        bucket=os.environ["BUCKET_NAME"],
        gcp_conn_id=gcp_conn_id,
    )

    create_bronze_folder_task >> extract_geojson_data_task >> upload_local_earthquake_file_to_gcs_task

    create_bronze_folder_task >> extract_country_geojson_task >> upload_local_country_geojson_file_to_gcs_task

    create_bronze_folder_task >> extract_country_detail_task >> upload_local_country_detail_file_to_gcs_task
