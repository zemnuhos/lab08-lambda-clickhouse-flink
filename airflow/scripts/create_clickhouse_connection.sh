#!/usr/bin/env bash
set -e

airflow connections delete "${AIRFLOW_CONN_CLICKHOUSE_ID}" || true

airflow connections add "${AIRFLOW_CONN_CLICKHOUSE_ID}" \
  --conn-type "http" \
  --conn-host "${CLICKHOUSE_HOST}" \
  --conn-port "${CLICKHOUSE_HTTP_PORT}" \
  --conn-schema "${CLICKHOUSE_DB}" \
  --conn-login "${CLICKHOUSE_USER}" \
  --conn-password "${CLICKHOUSE_PASSWORD}"