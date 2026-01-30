from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
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
    dag_id="air_traffic_pipeline",
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["adsb", "dbt", "lakehouse"],
) as dag:

    # 1) Bronze ingestion (reuse your existing ingestion DAG tasks style)
    # If you already have scripts for these tasks, call them here.
    # Otherwise keep them as-is in your existing ingestion DAG and we can use a TriggerDagRunOperator.
    fetch_opensky_raw = BashOperator(
        task_id="fetch_opensky_raw",
        bash_command="python /opt/project/ingestion/adsb/fetch_opensky_raw.py",
    )

    fetch_adsbdb_aircraft = BashOperator(
        task_id="fetch_adsbdb_aircraft",
        bash_command="python /opt/project/ingestion/metadata/fetch_adsbdb_aircraft.py",
    )

    fetch_adsbdb_airlines = BashOperator(
        task_id="fetch_adsbdb_airlines",
        bash_command="python /opt/project/ingestion/metadata/fetch_adsbdb_airlines.py",
    )

    # 2) Build Silver
    build_silver = BashOperator(
        task_id="build_silver",
        bash_command="/opt/airflow/scripts/run_build_silver.sh",
    )

    # 3) dbt pipeline (staging -> intermediate -> marts -> tests)
    dbt_pipeline = BashOperator(
        task_id="dbt_pipeline",
        bash_command="/opt/airflow/scripts/run_dbt_pipeline.sh",
    )

    # Optional: final marker task
    pipeline_done = BashOperator(
        task_id="pipeline_done",
        bash_command='echo "✅ Air traffic pipeline completed successfully"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependencies (THIS is the wiring)
    [fetch_opensky_raw, fetch_adsbdb_aircraft,
        fetch_adsbdb_airlines] >> build_silver >> dbt_pipeline >> pipeline_done
