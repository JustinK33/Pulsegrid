from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="pulsegrid_event_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pulsegrid", "kafka", "postgres"],
) as dag:
    init_postgres_schema = BashOperator(
        task_id="init_postgres_schema",
        bash_command="python -m pulsegrid.init_db",
    )

    publish_raw_events = BashOperator(
        task_id="publish_raw_events",
        bash_command="python -m pulsegrid.streaming.producer --input /opt/airflow/data/raw/events.csv",
    )

    consume_events_to_postgres = BashOperator(
        task_id="consume_events_to_postgres",
        bash_command=(
            "python -m pulsegrid.streaming.consumer "
            "--max-records 1000 "
            "--idle-timeout 30 "
            "--batch-size 100"
        ),
    )

    init_postgres_schema >> publish_raw_events >> consume_events_to_postgres
