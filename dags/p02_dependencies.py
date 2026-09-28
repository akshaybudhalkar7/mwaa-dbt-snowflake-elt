import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, task_group


@dag(
schedule=None,
start_date = pendulum.datetime(2026, 9, 1, tz='UTC'),
catchup=False,
tags= ['practice'],
)


def p02_dependencies():


    start = EmptyOperator(task_id='start')


    @task
    def extract() -> list[dict]:
        return [
            {"policy_id": 1, "premium": 1200.0},
            {"policy_id": 2, "premium": 800.0}
        ]


    @task
    def transform(policies: list[dict]) -> dict:
        total = sum(p["premium"] for p in policies)
        return {"policy_count": len(policies), "Total_Premium": total}


    @task.branch
    def choose_load_path(summary: dict) -> str:
        if summary["Total_Premium"] > 1000:
            return "big_batch_load"
        else:
            return "small_batch_load"


    big_batch_load = EmptyOperator(task_id="big_batch_load")
    small_batch_load = EmptyOperator(task_id="small_batch_load")


    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")


    @task_group
    def quality_check(summary: dict):
        @task
        def check_not_empty(s: dict):
            assert s["policy_count"] > 0, "no policies extracted"


        @task
        def check_positive_premium(s: dict):
            assert s["Total_Premium"] > 0, "total premium must be positive"


        check_not_empty(summary)
        check_positive_premium(summary)


    end = EmptyOperator(task_id="end")


    policies  = extract()
    summary = transform(policies)
    start >> policies >> choose_load_path(summary) >> [big_batch_load, small_batch_load] >> join >> quality_check(summary) >> end


p02_dependencies()