#!/usr/bin/env bash
set -euo pipefail

: "${SUPERSET_ADMIN_USER:=admin}"
: "${SUPERSET_ADMIN_PASSWORD:=admin}"
: "${SUPERSET_ADMIN_FIRSTNAME:=Superset}"
: "${SUPERSET_ADMIN_LASTNAME:=Admin}"
: "${SUPERSET_ADMIN_EMAIL:=admin@example.com}"

echo "Running Superset DB migrations..."
superset db upgrade

echo "Creating Superset admin user if it does not exist..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER}" \
  --firstname "${SUPERSET_ADMIN_FIRSTNAME}" \
  --lastname "${SUPERSET_ADMIN_LASTNAME}" \
  --email "${SUPERSET_ADMIN_EMAIL}" \
  --password "${SUPERSET_ADMIN_PASSWORD}" || true

echo "Initializing Superset roles and permissions..."
superset init

echo "Creating ClickHouse connection..."
/app/scripts/create-clickhouse-connection.sh

echo "Importing Superset assets..."
/app/scripts/import-superset-assets.sh

echo "Superset initialization completed."
