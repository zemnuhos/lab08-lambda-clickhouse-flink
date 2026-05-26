FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.19 \
    requests==2.32.3 \
    pendulum==3.0.0 \
    dbt-core==1.8.7 \
    dbt-clickhouse==1.8.3 \
    astronomer-cosmos==1.10.2

ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False \
    AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
