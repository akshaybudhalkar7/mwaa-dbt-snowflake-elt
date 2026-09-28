"""Shared settings for the insurance ELT pipeline."""

DATA_LAKE_BUCKET = "mwaa-dbt-elt-data-lake-656559336744-us-east-1"
RAW_PREFIX = "raw"

# The "source system" starts here: policyholder history is rebuilt from this date,
# so any business date always produces exactly the same data (idempotent re-runs).
EPOCH = "2026-09-01"
