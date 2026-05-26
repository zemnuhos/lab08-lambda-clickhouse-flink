#!/usr/bin/env bash
set -euo pipefail

: "${SUPERSET_ADMIN_USER:=admin}"
: "${SUPERSET_ASSETS_DIR:=/app/superset_assets}"

if [ ! -d "${SUPERSET_ASSETS_DIR}" ]; then
  echo "Superset assets directory does not exist: ${SUPERSET_ASSETS_DIR}"
  exit 0
fi

shopt -s nullglob
dashboard_archives=("${SUPERSET_ASSETS_DIR}"/*.zip)

if [ ${#dashboard_archives[@]} -eq 0 ]; then
  echo "No Superset dashboard ZIP files found in ${SUPERSET_ASSETS_DIR}. Skipping dashboard import."
  exit 0
fi

for dashboard_zip in "${dashboard_archives[@]}"; do
  echo "Importing Superset dashboard bundle: ${dashboard_zip}"

  superset import-dashboards     -p "${dashboard_zip}"     -u "${SUPERSET_ADMIN_USER}"
done

echo "Superset dashboard import completed."
