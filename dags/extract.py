import os
import requests
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
logging.basicConfig()

def extract_geojson_data(date, **kwargs):

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query?"

    date = datetime.strptime(date, "%Y-%m-%d")
    yesterday = date - relativedelta(days=1)
    # Define the query parameters
    query_params = {
        'starttime': yesterday.replace(day=1),
        'endtime': yesterday,
        'orderby': 'time',
        'format': 'geojson',
        'nodata': '404'
    }

    logging.info(f"CCCCCCCCCCC{query_params}")
    logging.info(f"DDDDDDDD{kwargs}")


    try:
        # Send a GET request with the specified query parameters
        response = requests.get(url, params=query_params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the content of the response
            return response.text
        else:
            return f"Request failed with status code: {response.status_code}"

    except requests.exceptions.RequestException as e:
        return f"Request error: {e}"


def transform_geojson_to_dataframe(geojson: str):
    logging.info(geojson)


with DAG(
    "extract",
    default_args={"depends_on_past": True},
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True
) as dag:

    extract_geojson_data_task = PythonOperator(
        task_id="extract_geojson_data",
        python_callable=extract_geojson_data,
        op_kwargs=dict(
            date="{{ ds }}",
            execution_date = "{{ execution_date }}",
            data_interval_start="{{data_interval_start}}",
            data_interval_end="{{data_interval_end}}"
        )
    )

    extract_geojson_data_task
