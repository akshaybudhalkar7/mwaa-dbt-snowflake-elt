import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task

DATA_LAKE_BUCKET = "mwaa-dbt-elt-data-lake-656559336744-us-east-1"

@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["practice"],
)

def p04_dynamic_mapping():
    @task
    def list_files() -> list[str]:
        keys = S3Hook().list_keys(bucket_name=DATA_LAKE_BUCKET, prefix="raw/")
        files = [k for k in keys if k.endswith(".parquet")]
        print(f"found {len(files)} parquet files")
        return files

    
    @task(max_active_tis_per_dag=2)
    def process_file(key: str, bucket: str) -> int:
        size = S3Hook().get_key(key, bucket_name=bucket).content_length
        entity = key.split("/")[1]
        print(f"[{entity}] {key}: {size} bytes")
        return size

    @task
    def summarize(sizes: list[int]):
        sizes = list(sizes)
        print(f"{len(sizes)} files, {sum(sizes)} bytes in total")

    sizes = process_file.partial(bucket=DATA_LAKE_BUCKET).expand(key=list_files())
    summarize(sizes)

    files = list_files()
    size = process_file.partial(bucket=DATA_LAKE_BUCKET).expand(key=files)
    done = summarize(size)

    files >> size >> done 



p04_dynamic_mapping()
