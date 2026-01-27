from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="dbt_transform_gold",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["transformation", "dbt", "gold"],
) as dag:
    dbt_run_test = BashOperator(
        task_id="dbt_run_and_test",
        bash_command=(
            "cd /opt/project/transformation/dbt/air_traffic_analytics "
            "&& dbt run && dbt test"
        ),
    )

    dbt_run_test
