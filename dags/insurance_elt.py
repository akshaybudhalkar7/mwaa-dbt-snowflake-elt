"""Insurance ELT: policy admin extract -> S3 (Parquet) -> Snowflake -> dbt.

Step 2 (this version): extract only. Load and dbt tasks are added in the next steps.
"""

import io
from datetime import timedelta

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

from insurance.config import DATA_LAKE_BUCKET, RAW_PREFIX


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 25, tz="UTC"),
    catchup=True,  # backfill a few days so dbt has history (SCD2 changes, claim updates)
    max_active_runs=1,  # business dates load in order: day N's claim updates need day N-1
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["insurance", "elt"],
)
def insurance_elt():
    @task(multiple_outputs=False)  # explicit: "-> dict" would otherwise turn it on silently
    def extract_to_s3(ds=None) -> dict:
        # Imported inside the task: heavy libraries stay out of DAG parsing (runs every ~30s)
        import pyarrow as pa
        import pyarrow.parquet as pq

        from insurance.generator import generate_day

        s3 = S3Hook()
        row_counts = {}
        for entity, rows in generate_day(ds).items():
            buffer = io.BytesIO()
            pq.write_table(pa.Table.from_pylist(rows), buffer)

            # Partition by business date + overwrite -> re-running a day never duplicates files
            key = f"{RAW_PREFIX}/{entity}/dt={ds}/{entity}.parquet"
            s3.load_bytes(buffer.getvalue(), key=key, bucket_name=DATA_LAKE_BUCKET, replace=True)
            row_counts[entity] = len(rows)
            print(f"wrote {len(rows)} rows -> s3://{DATA_LAKE_BUCKET}/{key}")

        return row_counts  # small XCom: counts only, never the data itself

    extract_to_s3()


insurance_elt()
