from datetime import datetime

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import (
    ProfileConfig,
    ProjectConfig,
)

from cosmos.profiles.clickhouse.user_pass import (
    ClickhouseUserPasswordProfileMapping,
)

CLICKHOUSE_CONN_ID = "clickhouse_conn"

profile_config = ProfileConfig(
    profile_name="lab08",
    target_name="dev",
    profile_mapping=ClickhouseUserPasswordProfileMapping(
        conn_id=CLICKHOUSE_CONN_ID,
        profile_args={
            "schema": "analytics",
            "driver": "http",
            "secure": False,
        },
    ),
)

project_config = ProjectConfig(
    "/opt/airflow/dbt",
)


def execute_clickhouse_query(query: str):
    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)

    response = requests.post(
        f"http://{conn.host}:{conn.port or 8123}/",
        params={"query": query},
        auth=(conn.login, conn.password),
        timeout=300,
    )

    response.raise_for_status()

    return response.text


with DAG(
    dag_id="lab08_marts",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=[
        "lab08",
        "marts",
        "dbt",
        "clickhouse",
    ],
) as dag:

    dbt_transformations = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
    )

    @task(task_id="cleanup_rt_tables")
    def cleanup_rt_tables():
        conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)
        database = conn.schema or "analytics"

        tables_query = f"""
        SELECT name
        FROM system.tables
        WHERE database = '{database}'
          AND startsWith(name, 'rt_')
          AND engine NOT IN ('View', 'MaterializedView')
        """

        tables = [
            table.strip()
            for table in execute_clickhouse_query(tables_query).splitlines()
            if table.strip()
        ]

        for table in tables:
            execute_clickhouse_query(
                f"TRUNCATE TABLE IF EXISTS `{database}`.`{table}`"
            )

    dbt_transformations >> cleanup_rt_tables()
