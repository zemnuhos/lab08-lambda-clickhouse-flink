import json
import logging
import os
from datetime import datetime
from typing import List

import boto3
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from functools import lru_cache

from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError


CLICKHOUSE_CONN_ID = os.getenv(
    "AIRFLOW_CONN_CLICKHOUSE_ID",
    "clickhouse_conn",
)

S3_BUCKET = os.getenv("S3_BUCKET")

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT_URL",
    "https://storage.yandexcloud.net",
)

S3_REGION = os.getenv(
    "S3_REGION",
    "ru-central1",
)

S3_AUTH_MODE = os.getenv(
    "S3_AUTH_MODE",
    "auto",
).lower()

S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")

TRANSACTION_BATCH_SIZE = 200
CANCELLATION_BATCH_SIZE = 50
EXCHANGE_RATE_BATCH_SIZE = 50

LOGGER = logging.getLogger(__name__)


def _create_public_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        config=Config(signature_version=UNSIGNED),
    )


def _create_private_s3_client():
    if not S3_ACCESS_KEY_ID or not S3_SECRET_ACCESS_KEY:
        raise ValueError(
            "S3 bucket is private, but S3_ACCESS_KEY_ID or "
            "S3_SECRET_ACCESS_KEY is not set in .env"
        )

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


def _can_list_bucket(s3_client) -> bool:
    try:
        s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            MaxKeys=1,
        )

        return True

    except ClientError as e:
        error_code = e.response.get(
            "Error",
            {},
        ).get("Code")

        if error_code in (
            "403",
            "AccessDenied",
            "Unauthorized",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
        ):
            return False

        raise


@lru_cache(maxsize=1)
def get_s3_client():
    """
    Создание S3-клиента.

    Поддерживает три режима:
    - public: доступ к открытому бакету без ключей;
    - private: доступ к закрытому бакету через access_key/secret_key;
    - auto: сначала проверяем публичный доступ, затем fallback на private.
    """

    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is not set in .env")

    if S3_AUTH_MODE == "public":
        LOGGER.info(
            "Using public unsigned S3 access for bucket: %s",
            S3_BUCKET,
        )

        return _create_public_s3_client()

    if S3_AUTH_MODE == "private":
        LOGGER.info(
            "Using private signed S3 access for bucket: %s",
            S3_BUCKET,
        )

        return _create_private_s3_client()

    if S3_AUTH_MODE != "auto":
        raise ValueError(
            "Invalid S3_AUTH_MODE. Expected one of: auto, public, private"
        )

    public_client = _create_public_s3_client()

    if _can_list_bucket(public_client):
        LOGGER.info(
            "Bucket %s is public. Using unsigned S3 access.",
            S3_BUCKET,
        )

        return public_client

    LOGGER.info(
        "Bucket %s is not available via public access. "
        "Trying signed S3 access.",
        S3_BUCKET,
    )

    return _create_private_s3_client()


def get_clickhouse_connection():
    """
    Get ClickHouse connection from Airflow connection.
    """

    conn = BaseHook.get_connection(CLICKHOUSE_CONN_ID)

    return {
        "host": conn.host,
        "port": conn.port or 8123,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password,
    }


def execute_clickhouse_insert(
    table_name: str,
    rows: List[dict],
):
    """
    Insert rows into ClickHouse using JSONEachRow.
    """

    if not rows:
        LOGGER.info(
            "No rows to insert into %s",
            table_name,
        )
        return

    ch = get_clickhouse_connection()

    query = f"""
    INSERT INTO {ch['database']}.{table_name}
    FORMAT JSONEachRow
    """

    payload = "\n".join(
        json.dumps(row)
        for row in rows
    )

    response = requests.post(
        f"http://{ch['host']}:{ch['port']}/",
        params={"query": query},
        data=payload.encode(),
        auth=(ch["user"], ch["password"]),
        timeout=300,
    )

    if not response.ok:
        LOGGER.error(
            "ClickHouse insert error:\n%s",
            response.text,
        )

        raise Exception(response.text)

    LOGGER.info(
        "Inserted %s rows into %s",
        len(rows),
        table_name,
    )


def chunk_list(items, chunk_size):
    """
    Split list into batches.
    """

    for i in range(
        0,
        len(items),
        chunk_size,
    ):
        yield items[i:i + chunk_size]


def is_file_already_loaded(source_file: str) -> bool:
    """
    Check load_log to avoid reloading files.
    """

    ch = get_clickhouse_connection()

    query = f"""
    SELECT count()
    FROM {ch['database']}.load_log
    WHERE source_file = '{source_file}'
      AND status = 'success'
    """

    response = requests.post(
        f"http://{ch['host']}:{ch['port']}/",
        params={"query": query},
        auth=(ch["user"], ch["password"]),
        timeout=60,
    )

    response.raise_for_status()

    return response.text.strip() != "0"


def write_load_log(
    source_type: str,
    source_file: str,
    file_size: int,
    loaded_rows: int,
    status: str,
    error_message: str = "",
):
    """
    Write ingestion result to load_log.
    """

    ch = get_clickhouse_connection()

    row = {
        "source_type": source_type,
        "source_file": source_file,
        "file_size": file_size,
        "loaded_rows": loaded_rows,
        "status": status,
        "error_message": error_message,
        "started_at": datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "finished_at": datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }

    query = f"""
    INSERT INTO {ch['database']}.load_log
    FORMAT JSONEachRow
    """

    response = requests.post(
        f"http://{ch['host']}:{ch['port']}/",
        params={"query": query},
        data=json.dumps(row).encode(),
        auth=(ch["user"], ch["password"]),
        timeout=60,
    )

    response.raise_for_status()


def load_jsonl_from_s3(
    key: str,
    table_name: str,
    source_type: str,
):
    """
    Load single JSONL file from S3 to ClickHouse.
    """

    if is_file_already_loaded(key):

        LOGGER.info(
            "File already loaded: %s",
            key,
        )

        return

    s3 = get_s3_client()

    LOGGER.info(
        "Loading file: %s",
        key,
    )

    response = s3.get_object(
        Bucket=S3_BUCKET,
        Key=key,
    )

    file_size = response["ContentLength"]

    rows = []

    try:

        for idx, line in enumerate(
            response["Body"].iter_lines()
        ):

            if not line:
                continue

            try:

                row = json.loads(line)

                row["source_file"] = key
                row["line_number"] = idx + 1

                rows.append(row)

            except Exception as row_error:

                LOGGER.exception(
                    "Row parsing error in %s line %s: %s",
                    key,
                    idx + 1,
                    row_error,
                )

        execute_clickhouse_insert(
            table_name,
            rows,
        )

        write_load_log(
            source_type=source_type,
            source_file=key,
            file_size=file_size,
            loaded_rows=len(rows),
            status="success",
        )

    except Exception as e:

        LOGGER.exception(
            "File ingestion failed: %s",
            key,
        )

        write_load_log(
            source_type=source_type,
            source_file=key,
            file_size=file_size,
            loaded_rows=0,
            status="failed",
            error_message=str(e),
        )

        raise


@dag(
    dag_id="lab08_raw_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=20,
    default_args={
        "owner": "airflow",
        "retries": 2,
    },
    tags=[
        "lab08",
        "raw",
        "s3",
        "clickhouse",
    ],
)
def lab08_raw_ingestion():

    @task
    def load_reference_data():

        reference_files = {
            "reference/users.jsonl":
                "raw_users",

            "reference/promo_codes.jsonl":
                "raw_promo_codes",

            "reference/test_users.jsonl":
                "raw_test_users",
        }

        for key, table_name in (
            reference_files.items()
        ):

            load_jsonl_from_s3(
                key=key,
                table_name=table_name,
                source_type="reference",
            )

    @task
    def discover_transaction_files():

        s3 = get_s3_client()

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        files = []

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
        ):

            for obj in page.get(
                "Contents",
                [],
            ):

                key = obj["Key"]

                if key.endswith(
                    "transactions.jsonl"
                ):

                    if not is_file_already_loaded(
                        key
                    ):
                        files.append(key)

        LOGGER.info(
            "Found %s new transaction files",
            len(files),
        )

        return list(
            chunk_list(
                files,
                TRANSACTION_BATCH_SIZE,
            )
        )

    @task
    def discover_cancellation_files():

        s3 = get_s3_client()

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        files = []

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix="cancellations/",
        ):

            for obj in page.get(
                "Contents",
                [],
            ):

                key = obj["Key"]

                if key.endswith(
                    "cancellations.jsonl"
                ):

                    if not is_file_already_loaded(
                        key
                    ):
                        files.append(key)

        LOGGER.info(
            "Found %s new cancellation files",
            len(files),
        )

        return list(
            chunk_list(
                files,
                CANCELLATION_BATCH_SIZE,
            )
        )

    @task
    def discover_exchange_rate_files():

        s3 = get_s3_client()

        paginator = s3.get_paginator(
            "list_objects_v2"
        )

        files = []

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix="exchange_rates/",
        ):

            for obj in page.get(
                "Contents",
                [],
            ):

                key = obj["Key"]

                if key.endswith(
                    "rates.jsonl"
                ):

                    if not is_file_already_loaded(
                        key
                    ):
                        files.append(key)

        LOGGER.info(
            "Found %s new exchange rate files",
            len(files),
        )

        return list(
            chunk_list(
                files,
                EXCHANGE_RATE_BATCH_SIZE,
            )
        )

    @task
    def load_transactions(keys):

        for key in keys:

            load_jsonl_from_s3(
                key=key,
                table_name="raw_transactions",
                source_type="transactions",
            )

    @task
    def load_cancellations(keys):

        for key in keys:

            load_jsonl_from_s3(
                key=key,
                table_name="raw_cancellations",
                source_type="cancellations",
            )

    @task
    def load_exchange_rates(keys):

        for key in keys:

            load_jsonl_from_s3(
                key=key,
                table_name="raw_exchange_rates",
                source_type="exchange_rates",
            )

    refs = load_reference_data()

    transaction_batches = (
        discover_transaction_files()
    )

    cancellation_batches = (
        discover_cancellation_files()
    )

    exchange_rate_batches = (
        discover_exchange_rate_files()
    )

    load_transactions.expand(
        keys=transaction_batches
    )

    load_cancellations.expand(
        keys=cancellation_batches
    )

    load_exchange_rates.expand(
        keys=exchange_rate_batches
    )

    refs


dag = lab08_raw_ingestion()