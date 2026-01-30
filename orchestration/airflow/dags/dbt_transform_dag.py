from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from pendulum import datetime
import logging


def on_failure_callback(context):
    dag_id = context["dag"].dag_id if context.get("dag") else "unknown_dag"
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    log_url = ti.log_url if ti else "no_log_url"
    exception = context.get("exception")
    logging.error(
        f"[ALERT] Failure: dag={dag_id} task={task_id} run={run_id} "
        f"exception={exception} log_url={log_url}"
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,  # stronger default for prod
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["philipwemgah990@gmail.com"],
}

DBT_PROJECT_DIR = "/opt/project/transformation/dbt/air_traffic_analytics"
# <-- make sure this is the one you created
DBT_PROFILES_DIR = "/home/airflow/.dbt"


with DAG(
    dag_id="dbt_transform_gold",
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # triggered by orchestrator/ingestion
    catchup=False,
    tags=["dbt", "gold", "transformation"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=10),
        sla=timedelta(minutes=10),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=45),
        sla=timedelta(minutes=45),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} "
            f"&& dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
        execution_timeout=timedelta(minutes=20),
        sla=timedelta(minutes=20),
    )

    dbt_deps >> dbt_run >> dbt_test
