#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "$0")/.."
compose=(docker compose -f docker-compose.yaml)
dbt_args=(--project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt)

if [[ ! -f .env ]]; then
    echo 'ERROR: Create .env from .env.example before running bootstrap.' >&2
    exit 1
fi

echo '==> Validating Docker Compose'
"${compose[@]}" config --quiet

echo '==> Starting services and running database, Kafka and Airflow initialization'
"${compose[@]}" up -d --build spark airflow-scheduler airflow-dag-processor grafana

echo '==> Running dbt models'
"${compose[@]}" exec -T airflow-scheduler dbt run "${dbt_args[@]}"

echo '==> Configuring Grafana database permissions'
"${compose[@]}" --profile setup run --rm -T grafana-db-setup

echo '==> Running dbt tests'
"${compose[@]}" exec -T airflow-scheduler dbt test "${dbt_args[@]}"

echo '==> Initial setup completed'
