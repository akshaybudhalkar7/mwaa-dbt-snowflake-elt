"""Smoke-test DAG: proves MWAA can parse and run a task."""

import sys
from datetime import datetime

# Airflow 3: DAG authoring imports come from the Task SDK.
# Airflow 2 equivalent: from airflow.decorators import dag, task
from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2026, 9, 1), catchup=False, tags=["smoke"])
def hello_mwaa():
    @task
    def hello():
        print(f"Hello from MWAA - Python {sys.version}")

    hello()


hello_mwaa()
