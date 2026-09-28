-- =====================================================================
-- 03_snowpipes.sql - auto-ingest pipes: S3 file lands -> loaded in ~1 minute
-- Run once in Snowsight AFTER 02_raw_tables.sql.
-- Then: SHOW PIPES -> copy "notification_channel" (an SQS ARN) -> CDK S3 event notification.
-- =====================================================================

-- The loader role will own the pipes (it already owns the raw tables)
USE ROLE SECURITYADMIN;
GRANT CREATE PIPE ON SCHEMA INS_RAW.POLICY_ADMIN TO ROLE INS_LOADER;

USE ROLE INS_LOADER;
USE SCHEMA INS_RAW.POLICY_ADMIN;

-- One pipe per entity. Each pipe = a saved COPY INTO that runs whenever a new file arrives.
--   $1                 the whole Parquet row as VARIANT
--   BUSINESS_DATE      parsed from the path: <entity>/dt=YYYY-MM-DD/<file>.parquet
--   METADATA$FILENAME  the S3 file path (lineage)
-- AUTO_INGEST = TRUE -> triggered by S3 event notifications (via Snowflake's SQS queue)
CREATE PIPE IF NOT EXISTS PIPE_POLICYHOLDERS AUTO_INGEST = TRUE AS
COPY INTO POLICYHOLDERS (RECORD, BUSINESS_DATE, SOURCE_FILE)
FROM (
  SELECT $1,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)::DATE,
         METADATA$FILENAME
  FROM @S3_RAW_STAGE/policyholders/
);

CREATE PIPE IF NOT EXISTS PIPE_POLICIES AUTO_INGEST = TRUE AS
COPY INTO POLICIES (RECORD, BUSINESS_DATE, SOURCE_FILE)
FROM (
  SELECT $1,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)::DATE,
         METADATA$FILENAME
  FROM @S3_RAW_STAGE/policies/
);

CREATE PIPE IF NOT EXISTS PIPE_CLAIMS AUTO_INGEST = TRUE AS
COPY INTO CLAIMS (RECORD, BUSINESS_DATE, SOURCE_FILE)
FROM (
  SELECT $1,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)::DATE,
         METADATA$FILENAME
  FROM @S3_RAW_STAGE/claims/
);

-- All pipes in an account/region share ONE SQS queue -> copy "notification_channel"
SHOW PIPES IN SCHEMA INS_RAW.POLICY_ADMIN;

-- ---------------------------------------------------------------------
-- Useful later (debugging):
--   SELECT SYSTEM$PIPE_STATUS('INS_RAW.POLICY_ADMIN.PIPE_CLAIMS');
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--       TABLE_NAME => 'CLAIMS', START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())));
--   Files that landed BEFORE the S3 notification existed: ALTER PIPE PIPE_CLAIMS REFRESH;
-- ---------------------------------------------------------------------
