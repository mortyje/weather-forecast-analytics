from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

LOCATIONS_PATH = "/opt/airflow/ingestion/config/locations.json"

with DAG(
    dag_id="weather_observations_collection",
    description="Collect hourly observations from Meteostat",
    start_date=datetime(2026, 11, 20, tzinfo=UTC),
    schedule="15 7 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["ingestion", "weather", "meteostat"],
) as dag:
    ensure_station_assignments = BashOperator(
        task_id="ensure_station_assignments",
        bash_command=f"weather-stations --locations {LOCATIONS_PATH} --assign",
        execution_timeout=timedelta(minutes=30),
        do_xcom_push=False,
    )

    collect_observations = BashOperator(
        task_id="collect_observations",
        bash_command=f"weather-observations --locations {LOCATIONS_PATH} --output kafka",
        execution_timeout=timedelta(minutes=30),
        do_xcom_push=False,
    )

    ensure_station_assignments >> collect_observations
