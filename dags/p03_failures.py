import time
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, task


def notify_failure(context):
    ti = context["ti"]
    print(f"ALERT: {ti.dag_id}.{ti.task_id} failed on try {ti.try_number}: {context.get('exception')}")


default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    params={"scenario": Param("flaky_then_ok", enum=["flaky_then_ok", "hard_failure", "timeout"])},
    tags=["practice"],
)
def p03_failures():
    @task(execution_timeout=timedelta(seconds=30))
    def call_flaky_api(params=None, ti=None):
        scenario = params["scenario"]
        print(f"scenario={scenario}, try_number={ti.try_number}")

        if scenario == "flaky_then_ok" and ti.try_number < 3:
            raise ConnectionError(f"API timed out on try {ti.try_number}")

        if scenario == "hard_failure":
            raise AirflowFailException("Schema changed in source - retrying will not help")

        if scenario == "timeout":
            time.sleep(60)

        return "ok"

    @task(trigger_rule="one_failed", retries=0)
    def alert_and_fail_run():
        print("Upstream failed -> page on-call")
        raise AirflowFailException("Failing the DAG run so monitoring sees it")

    @task(trigger_rule="all_done")
    def cleanup():
        print("Always runs: delete temp files, release locks")

    api_result = call_flaky_api()
    api_result >> [alert_and_fail_run(), cleanup()]


p03_failures()
