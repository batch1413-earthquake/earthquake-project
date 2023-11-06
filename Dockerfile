FROM python:3.8.14-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1

ENV AIRFLOW_HOME=/app/airflow

# Install curl and mysql that will be needed for our tests
RUN apt-get update && apt-get install -y curl \
    gcc default-mysql-server default-libmysqlclient-dev

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN chmod +x scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main
