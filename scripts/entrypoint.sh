#!/usr/bin/env bash

airflow db upgrade

airflow users create -r Admin -u ${AIRFLOW_USER} -p ${AIRFLOW_USER_PASSWORD} -e ${AIRFLOW_USER_EMAIL} -f admin -l airflow

airflow webserver
