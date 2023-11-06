import os
from datetime import datetime

from airflow import DAG

# $IMPORT_BEGIN
# noreorder
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator

import logging

from airflow.utils.trigger_rule import TriggerRule


AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")


def is_month_odd(date: str) -> str:
    """
    date: formatted as YYYY-MM.
    Returns "filter_expensive_trips" if the month date is even, "filter_long_trips" otherwise.
    """
    return "filter_expensive_trips" if int(date[-2:]) % 2 == 0 else "filter_long_trips"


def prepare_data(bronze_file: str, date: str):
    """
    - Converts data from `bronze_file` to DataFrame  using pandas
    - Adds a new column named 'date' that stores the current month (should be formatted as YYYY-MM)
    - Keeps only the ["date", "trip_distance" and "total_amount"] columns, in that order
    - Returns the DataFrame
    """
    logging.info("///////////////////")
    logging.info(date)
    logging.info(bronze_file)

    df = pd.read_parquet(bronze_file)
    df["date"] = date
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
    return df[["date", "trip_distance", "total_amount"]]


def filter_long_trips(bronze_file: str, silver_file: str, date: str, distance: int) -> None:
    """
    - Calls prepare_data to get a cleaned DataFrame
    - Keep only rows for which the trip_distance's value is greater than `distance`
    - Saves the DataFrame to `silver_file` without keeping the DataFrame indexes
    """
    df = prepare_data(bronze_file, date)
    filteredDf = df[df["trip_distance"] > distance]
    filteredDf.to_csv(silver_file, index=False)


def filter_expensive_trips(bronze_file: str, silver_file: str, date: str, amount: int) -> None:
    """
    - Calls prepare_data to get a cleaned DataFrame
    - Keep only rows for which the total_amount's value is greater than `amount`
    - Saves the DataFrame to `silver_file` without keeping the DataFrame indexes
    """
    df = prepare_data(bronze_file, date)
    filteredDf = df[df["total_amount"] > amount]
    filteredDf.to_csv(silver_file, index=False)


# 2021, 12, 31
with DAG(
    "transform",
    default_args={"depends_on_past": True},
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 12, 1),
    schedule_interval="@monthly",
    catchup=True,
) as dag:
    date = "{{ ds[:7] }}"

    bronze_file = f"{AIRFLOW_HOME}/data/bronze/yellow_tripdata_{date}.parquet"
    silver_file = f"{AIRFLOW_HOME}/data/silver/yellow_tripdata_{date}.csv"

    wait_for_extract = ExternalTaskSensor(
        task_id="extract_sensor", external_dag_id="extract", external_task_id="curl_trip_data", timeout=600, poke_interval=10, mode="poke"
    )

    is_month_odd_task = BranchPythonOperator(task_id="is_month_odd", python_callable=is_month_odd, op_kwargs={"date": date})

    filter_long_trips_task = PythonOperator(
        task_id="filter_long_trips",
        python_callable=filter_long_trips,
        op_kwargs={"bronze_file": bronze_file, "silver_file": silver_file, "distance": 150, "date": date},
    )

    filter_expensive_trips_task = PythonOperator(
        task_id="filter_expensive_trips",
        python_callable=filter_expensive_trips,
        op_kwargs={"bronze_file": bronze_file, "silver_file": silver_file, "amount": 500, "date": date},
    )

    end_task = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS)

    wait_for_extract >> is_month_odd_task >> [filter_long_trips_task, filter_expensive_trips_task] >> end_task
