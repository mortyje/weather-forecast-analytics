from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


with DAG(
    dag_id="weather_forecast_collection",
    description="Collect weather forecasts from Open-Meteo",
    start_date=datetime(2026, 11, 20, tzinfo=UTC),
    schedule="0 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "weather"],
) as dag:
    collect_forecasts = BashOperator(
        task_id="collect_forecasts",
        bash_command=(
            "weather-ingestion "
            "--locations /opt/airflow/ingestion/config/locations.json "
            "--output kafka"
        ),
        execution_timeout=timedelta(minutes=10),
        retries=0,
        do_xcom_push=False,
    )