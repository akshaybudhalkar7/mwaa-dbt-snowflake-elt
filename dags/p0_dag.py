import json
import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


DATA_LAKE_BUCKET = "mwaa-dbt-elt-data-lake-656559336744-us-east-1"

@dag(
    schedule = "@daily",
    start_date = pendulum.datetime(2026, 9, 26, tz='UTC'),
    catchup = True,
    max_active_runs = 1,
    tags = ["practice"]
)

def p01_dag():
    @task.bash
    def show_dates():
        return(
        'echo "ds = {{ ds }}"; '
        'echo "logical_date = {{ logical_date }}"; '
        'echo "interval = {{ data_interval_start }} -> {{ data_interval_end }}"; '
        'echo "run_id = {{ run_id }}"'
        )

    @task
    def write_partition(ds=None, run_id=None):
        key = f"practice/p01/dt={ds}/orders.json"
        rows = [{"order_id": f"{ds}-{i}", "amount": i * 10} for i in range(3)]

        S3Hook().load_string(
            json.dumps(rows),
            key=key,
            bucket_name=DATA_LAKE_BUCKET,
            replace=True,
        )

        print(f'wrote s3://{DATA_LAKE_BUCKET}/{key} (run {run_id})')

    
    dates = show_dates()
    partition  = write_partition()

    dates >> partition

p01_dag()
    