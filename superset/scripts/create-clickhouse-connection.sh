#!/usr/bin/env bash
set -euo pipefail

: "${CLICKHOUSE_HOST:=clickhouse}"
: "${CLICKHOUSE_HTTP_PORT:=8123}"
: "${CLICKHOUSE_DB:=default}"
: "${CLICKHOUSE_USER:=default}"
: "${CLICKHOUSE_PASSWORD:=}"
: "${SUPERSET_ADMIN_USER:=admin}"
: "${SUPERSET_CLICKHOUSE_DATABASE_NAME:=ClickHouse}"

echo "Creating/updating Superset database connection: ${SUPERSET_CLICKHOUSE_DATABASE_NAME}"

cat > /tmp/clickhouse_database.yaml <<EOF
databases:
  - database_name: ${SUPERSET_CLICKHOUSE_DATABASE_NAME}
    sqlalchemy_uri: clickhousedb://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@${CLICKHOUSE_HOST}:${CLICKHOUSE_HTTP_PORT}/${CLICKHOUSE_DB}
    expose_in_sqllab: true
    allow_ctas: false
    allow_cvas: false
    allow_dml: false
EOF

superset import-datasources   -p /tmp/clickhouse_database.yaml   -u "${SUPERSET_ADMIN_USER}"

echo "ClickHouse connection is ready."
