from datetime import UTC, datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from weather_airflow.raw_readiness import capture_kafka_ranges, raw_ranges_are_loaded

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_ARGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

with DAG(
    dag_id="weather_dwh_transform",
    description="Transform weather data from Raw to Core DWH",
    start_date=datetime(2026, 11, 20, tzinfo=UTC),
    schedule="30 */3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "dbt"],
) as dag:
    capture_offsets = PythonOperator(
        task_id="capture_kafka_offsets",
        python_callable=capture_kafka_ranges,
    )

    wait_for_raw = PythonSensor(
        task_id="wait_for_raw",
        python_callable=raw_ranges_are_loaded,
        op_kwargs={"kafka_ranges": capture_offsets.output},
        poke_interval=30,
        timeout=1800,
        mode="reschedule",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {DBT_ARGS}",
        do_xcom_push=False,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_ARGS}",
        do_xcom_push=False,
    )

    capture_offsets >> wait_for_raw >> dbt_run >> dbt_test
