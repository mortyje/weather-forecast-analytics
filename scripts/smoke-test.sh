#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
compose=(docker compose -f docker-compose.yaml)
scheduler=("${compose[@]}" exec -T airflow-scheduler)
# Match the DAG bundle import path used by the Airflow DAG processor.
airflow=("${compose[@]}" exec -T -e PYTHONPATH=/opt/airflow/dags airflow-scheduler airflow)
dbt_args=(--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt)

echo '==> Checking Docker Compose'
"${compose[@]}" config --quiet

echo '==> Checking Python ingestion'
docker build --target test -t weather-ingestion-tests ./ingestion
docker run --rm weather-ingestion-tests

echo '==> Validating Grafana dashboards'
for dashboard in grafana/dashboards/*.json; do
    python3 -m json.tool "$dashboard" >/dev/null
    echo "Valid: $dashboard"
done

echo '==> Checking Airflow DAGs'
"${airflow[@]}" dags list-import-errors --local --output json |
    python3 -c '
import json
import sys

errors = json.load(sys.stdin)
if errors:
    print(json.dumps(errors, indent=2), file=sys.stderr)
    sys.exit("Airflow DAG import errors")
'
"${airflow[@]}" dags list --local --output json |
    python3 -c '
import json
import sys

found = {dag["dag_id"] for dag in json.load(sys.stdin)}
required = {
    "weather_forecast_collection",
    "weather_observations_collection",
    "weather_dwh_transform",
}
if missing := required - found:
    sys.exit("Missing Airflow DAGs: " + ", ".join(sorted(missing)))
print("Airflow DAGs: " + ", ".join(sorted(found)))
'

echo '==> Parsing dbt project'
"${scheduler[@]}" dbt parse "${dbt_args[@]}"

echo '==> Running dbt tests'
"${scheduler[@]}" dbt test "${dbt_args[@]}"

echo '==> Checking DWH data and Grafana reader permissions'
"${compose[@]}" exec -T postgres \
    sh -c 'psql -X -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' \
    < scripts/check-data.sql

echo '==> Checking Grafana API'
grafana_address=$("${compose[@]}" port grafana 3000)
curl --fail --silent --show-error --retry 6 --retry-connrefused --retry-max-time 30 \
    --max-time 5 "http://$grafana_address/api/health" |
    python3 -c '
import json
import sys

health = json.load(sys.stdin)
if health["database"] != "ok":
    sys.exit(f"Grafana database health check failed: {health}")
print("Grafana database: ok")
'

echo '==> All smoke tests passed'
