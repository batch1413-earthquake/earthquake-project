import os
import requests
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import relativedelta

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
logging.basicConfig()

def print_execution_date(date):

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

    try:
        # Send a GET request with the specified query parameters
        response = requests.get(url, params=query_params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Print the content of the response
            print(response.text)
        else:
            print(f"Request failed with status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")


with DAG(
    "extract",
    default_args={"depends_on_past": True},
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 1, 1),
    schedule_interval="@monthly",
    catchup=True
) as dag:

    date = "{{ execution_date }}"


    print_execution_date_task = PythonOperator(
        task_id="print_execution_date",
        python_callable=print_execution_date,
        op_kwargs=dict(date="{{ ds }}")
    )

    print_execution_date_task
