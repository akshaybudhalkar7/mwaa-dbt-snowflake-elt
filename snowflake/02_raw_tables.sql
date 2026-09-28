-- =====================================================================
-- 02_raw_tables.sql - raw landing tables (run once in Snowsight)
-- Created AS INS_LOADER so the loader owns them (can DELETE + COPY INTO).
-- dbt (INS_TRANSFORMER) can read them via the FUTURE grant from 01_setup.sql.
-- =====================================================================
USE ROLE INS_LOADER;
USE WAREHOUSE INS_WH;
USE SCHEMA INS_RAW.POLICY_ADMIN;

-- Same shape for every entity: the source row as-is + load metadata.
--   RECORD        full Parquet row as VARIANT -> new source columns never break the load
--   BUSINESS_DATE which daily extract (ds) the row came from -> delete + reload one day
--   SOURCE_FILE   S3 file the row came from -> lineage / debugging
--   LOADED_AT     when Snowflake loaded it -> freshness checks in dbt
CREATE TABLE IF NOT EXISTS POLICYHOLDERS (
  RECORD        VARIANT,
  BUSINESS_DATE DATE,
  SOURCE_FILE   STRING,
  LOADED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS POLICIES (
  RECORD        VARIANT,
  BUSINESS_DATE DATE,
  SOURCE_FILE   STRING,
  LOADED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CLAIMS (
  RECORD        VARIANT,
  BUSINESS_DATE DATE,
  SOURCE_FILE   STRING,
  LOADED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

SHOW TABLES IN SCHEMA INS_RAW.POLICY_ADMIN;
