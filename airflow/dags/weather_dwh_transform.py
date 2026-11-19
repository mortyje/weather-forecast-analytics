from datetime import UTC, datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="weather_dwh_transform",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["dwh", "dbt"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt run "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        do_xcom_push=False,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"dbt test "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROJECT_DIR}"
        ),
        do_xcom_push=False,
    )

    dbt_run >> dbt_test
