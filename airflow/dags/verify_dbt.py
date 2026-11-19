from datetime import UTC, datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="verify_dbt_environment",
    start_date=datetime(2026, 11, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    tags=["setup"],
) as dag:
    check_dbt_version = BashOperator(
        task_id="check_dbt_version",
        bash_command="dbt --version",
    )
