# Practice DAG: the smallest possible TaskFlow DAG (Airflow 3 version).
# Everything at the top level of this file (imports, code outside tasks) runs on EVERY parse
# (~every 30s by the DAG processor), so keep it light - no API calls or DB queries up here.

import sys
from datetime import datetime

# Airflow 3: DAG authoring imports come from the Task SDK.
# Airflow 2 equivalent: from airflow.decorators import dag, task
from airflow.sdk import dag, task

@dag(
    schedule=None,                     # no automatic runs; only runs when triggered manually
                                       # (daily would be "@daily" or a cron like "0 2 * * *")
    start_date=datetime(2026, 9, 1),   # first date the DAG may run for; always a FIXED date,
                                       # never datetime.now() (it changes on every parse)
    catchup = False,                   # don't create runs for past intervals since start_date
                                       # (Airflow 3 default is already False; Airflow 2 default was True)
    tags = ["smoke"]                   # label for filtering in the Airflow UI
)

# The function name becomes the DAG ID shown in the UI: "dag_101"
def dag_101():
    # @task turns this function into a task; its name becomes the task ID: "hey".
    # The body runs on a Celery worker (not the scheduler) when the task executes.
    @task
    def hey():
        # print() output lands in the task log (stored in CloudWatch on MWAA)
        print(f"Hello from MWAA - Python {sys.version}")

    # Calling the task here does NOT run it - it ADDS it to the DAG.
    # With two tasks, b(a()) means "b runs after a" and a's return value goes to b via XCom.
    hey()

# Calling the DAG function REGISTERS the DAG. Without this line the DAG processor
# parses the file but finds no DAG, so it never shows up in the UI.
dag_101()
