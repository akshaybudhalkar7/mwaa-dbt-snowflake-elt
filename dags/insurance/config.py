"""Shared settings for the insurance ELT pipeline."""

DATA_LAKE_BUCKET = "mwaa-dbt-elt-data-lake-656559336744-us-east-1"
RAW_PREFIX = "raw"

ENTITIES = ("policyholders", "policies", "claims")

# Snowflake: MWAA signs in as this SERVICE user via Workload Identity Federation (AWS),
# i.e. with the MWAA execution role - no password or key anywhere.
SNOWFLAKE_ACCOUNT = "XJVCLEX-NNC43628"
SNOWFLAKE_USER = "INS_MWAA_SVC"

# Python of the venv created by mwaa/startup.sh (dbt-snowflake -> includes the Snowflake connector)
DBT_VENV_PYTHON = "/usr/local/airflow/dbt_venv/bin/python"

# The "source system" starts here: policyholder history is rebuilt from this date,
# so any business date always produces exactly the same data (idempotent re-runs).
EPOCH = "2026-09-01"

# ---- v2: watermark-driven API ingest
EXTRACTOR_FUNCTION = "mwaa-dbt-elt-extractor"
RAW_API_PREFIX = "raw/api"