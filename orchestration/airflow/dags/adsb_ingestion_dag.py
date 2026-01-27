from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="adsb_ingestion_bronze",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["ingestion", "bronze", "adsb"],
) as dag:
    fetch_opensky = BashOperator(
        task_id="fetch_opensky_raw",
        bash_command="python /opt/project/ingestion/adsb/fetch_opensky_raw.py",
    )

    fetch_aircraft = BashOperator(
        task_id="fetch_adsbdb_aircraft",
        bash_command="python /opt/project/ingestion/metadata/fetch_adsbdb_aircraft.py",
    )

    fetch_opensky >> fetch_aircraft
