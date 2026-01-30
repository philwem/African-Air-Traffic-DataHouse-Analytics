from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
from pendulum import datetime
import logging


def on_failure_callback(context):
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
    task_id = context.get("task_instance").task_id if context.get(
        "task_instance") else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    logging.error(f"[ALERT] Failure: dag={dag_id} task={task_id} run={run_id}")


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["philipwemgah990@gmail.com"],
}

with DAG(
    dag_id="adsb_ingestion_bronze",
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["adsb", "bronze", "ingestion"],
) as dag:

    fetch_opensky_raw = BashOperator(
        task_id="fetch_opensky_raw",
        bash_command="python /opt/project/ingestion/adsb/fetch_opensky_raw.py",
    )

    fetch_adsbdb_aircraft = BashOperator(
        task_id="fetch_adsbdb_aircraft",
        bash_command="python /opt/project/ingestion/metadata/fetch_adsbdb_aircraft.py",
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command="python /opt/project/ingestion/build_silver.py",
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transform_gold",
        trigger_dag_id="dbt_transform_gold",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    [fetch_opensky_raw, fetch_adsbdb_aircraft] >> build_silver >> trigger_dbt
