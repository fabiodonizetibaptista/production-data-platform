#!/usr/bin/env bash

# Este script é executado como arquivo, então aqui o fail-fast é desejável.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

required_vars=(
  DATABRICKS_HOST
  DATABRICKS_HTTP_PATH
  DATABRICKS_TOKEN
)

for var_name in "${required_vars[@]}"; do
  if [[ -z "${!var_name:-}" ]]; then
    echo "ERRO: variável ${var_name} não está disponível."
    exit 1
  fi
done

if [[ $# -lt 1 ]]; then
  echo "Uso:"
  echo "  ./scripts/dbt_databricks.sh debug"
  echo "  ./scripts/dbt_databricks.sh build"
  echo "  ./scripts/dbt_databricks.sh show --inline \"SELECT ...\""
  exit 1
fi

DBT_COMMAND="$1"
shift

docker run --rm \
  --network host \
  --user "$(id -u):$(id -g)" \
  -e HOME=/tmp \
  -e DATABRICKS_HOST \
  -e DATABRICKS_HTTP_PATH \
  -e DATABRICKS_TOKEN \
  -e DATABRICKS_CATALOG="${DATABRICKS_CATALOG:-workspace}" \
  -e DATABRICKS_SILVER_SCHEMA="${DATABRICKS_SILVER_SCHEMA:-nyc_taxi_silver}" \
  -e DATABRICKS_GOLD_SCHEMA="${DATABRICKS_GOLD_SCHEMA:-nyc_taxi_gold}" \
  -v "${ROOT_DIR}:/workspace" \
  -w /workspace/dbt \
  production-data-platform-dbt \
  "${DBT_COMMAND}" \
  --profiles-dir /workspace/dbt/profiles/databricks \
  --target databricks \
  "$@"
