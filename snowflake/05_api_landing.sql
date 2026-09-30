-- =====================================================================
-- 05_api_landing.sql - landing tables for the API extract (v2), loaded by Snowpipe
-- Files:     s3://<lake>/raw/api/<entity>/dt=YYYY-MM-DD/run=<run_id>/attempt=<ts>/part-NNNNN.json.gz (NDJSON)
-- Manifests: s3://<lake>/raw/api/_manifests/...  -> outside every pipe path, never loaded
-- Lives under raw/ -> reuses INS_S3_INT, the S3 read role and the SQS notification as-is.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. SCHEMA + FILE FORMAT + STAGE - SYSADMIN owns objects
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE SCHEMA IF NOT EXISTS INS_RAW.API COMMENT = 'Source system: policy admin REST API (JSON, v2)';

CREATE FILE FORMAT IF NOT EXISTS INS_RAW.API.FF_NDJSON
  TYPE              = JSON
  STRIP_OUTER_ARRAY = FALSE   -- NDJSON: one object per line, no [ ... ] wrapper
  COMPRESSION       = AUTO;   -- .json and .json.gz both work

CREATE STAGE IF NOT EXISTS INS_RAW.API.S3_API_STAGE
  STORAGE_INTEGRATION = INS_S3_INT
  URL                 = 's3://mwaa-dbt-elt-data-lake-656559336744-us-east-1/raw/api/'
  FILE_FORMAT         = INS_RAW.API.FF_NDJSON;


-- ---------------------------------------------------------------------
-- 2. GRANTS
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

-- LOADER: owns the landing tables + pipes, reads the stage
GRANT USAGE, CREATE TABLE, CREATE PIPE ON SCHEMA INS_RAW.API TO ROLE INS_LOADER;
GRANT USAGE ON STAGE       INS_RAW.API.S3_API_STAGE          TO ROLE INS_LOADER;
GRANT USAGE ON FILE FORMAT INS_RAW.API.FF_NDJSON             TO ROLE INS_LOADER;

-- TRANSFORMER (dbt): read the landing tables, incl. ones created later
GRANT USAGE  ON SCHEMA INS_RAW.API                  TO ROLE INS_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA INS_RAW.API TO ROLE INS_TRANSFORMER;

-- Verify: empty result = OK (no files yet). "Access denied" = integration/role problem.
LIST @INS_RAW.API.S3_API_STAGE;


-- ---------------------------------------------------------------------
-- 3. LANDING TABLES - owned by INS_LOADER (it will own the pipes too)
-- ---------------------------------------------------------------------
USE ROLE INS_LOADER;
USE WAREHOUSE INS_WH;
USE SCHEMA INS_RAW.API;

-- Same shape as v1 + RUN_ID and FILE_ROW:
--   RECORD        one API object as-is (VARIANT) -> new source fields never break the load
--   BUSINESS_DATE parsed from dt=  in the path
--   RUN_ID        parsed from run= in the path  -> reconcile + watermark are PER RUN
--   SOURCE_FILE   METADATA$FILENAME             -> lineage
--   FILE_ROW      METADATA$FILE_ROW_NUMBER      -> exact line in the file (debugging bad rows)
--   LOADED_AT     load time                     -> freshness + "latest copy wins" dedup
CREATE TABLE IF NOT EXISTS POLICYHOLDERS (
  RECORD        VARIANT,
  BUSINESS_DATE DATE,
  RUN_ID        STRING,
  SOURCE_FILE   STRING,
  FILE_ROW      NUMBER,
  LOADED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- LIKE copies the column definitions (incl. the DEFAULT), no data
CREATE TABLE IF NOT EXISTS POLICIES LIKE POLICYHOLDERS;
CREATE TABLE IF NOT EXISTS CLAIMS   LIKE POLICYHOLDERS;

SHOW TABLES IN SCHEMA INS_RAW.API;   -- 3 tables, owner INS_LOADER


-- ---------------------------------------------------------------------
-- 4. PIPES - auto-ingest: file lands in S3 -> loaded in ~1 minute
-- ---------------------------------------------------------------------
USE ROLE INS_LOADER;
USE SCHEMA INS_RAW.API;

-- Path inside the stage: <entity>/dt=YYYY-MM-DD/run=<run_id>/attempt=<ts>/part-NNNNN.json.gz
--   SPLIT_PART(SPLIT_PART(path, 'dt=',  2), '/', 1)  -> YYYY-MM-DD
--   SPLIT_PART(SPLIT_PART(path, 'run=', 2), '/', 1)  -> run_id
-- PATTERN: load ONLY data parts -> a stray file (.tmp, _SUCCESS, a manifest) is never a row
CREATE PIPE IF NOT EXISTS PIPE_API_POLICYHOLDERS AUTO_INGEST = TRUE AS
COPY INTO POLICYHOLDERS (RECORD, BUSINESS_DATE, RUN_ID, SOURCE_FILE, FILE_ROW)
FROM (
  SELECT $1,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)::DATE,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'run=', 2), '/', 1),
         METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER
  FROM @S3_API_STAGE/policyholders/
)
PATTERN = '.*part-[0-9]+[.]json([.]gz)?';

CREATE PIPE IF NOT EXISTS PIPE_API_POLICIES AUTO_INGEST = TRUE AS
COPY INTO POLICIES (RECORD, BUSINESS_DATE, RUN_ID, SOURCE_FILE, FILE_ROW)
FROM (
  SELECT $1,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)::DATE,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'run=', 2), '/', 1),
         METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER
  FROM @S3_API_STAGE/policies/
)
PATTERN = '.*part-[0-9]+[.]json([.]gz)?';

CREATE PIPE IF NOT EXISTS PIPE_API_CLAIMS AUTO_INGEST = TRUE AS
COPY INTO CLAIMS (RECORD, BUSINESS_DATE, RUN_ID, SOURCE_FILE, FILE_ROW)
FROM (
  SELECT $1,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'dt=', 2), '/', 1)::DATE,
         SPLIT_PART(SPLIT_PART(METADATA$FILENAME, 'run=', 2), '/', 1),
         METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER
  FROM @S3_API_STAGE/claims/
)
PATTERN = '.*part-[0-9]+[.]json([.]gz)?';

-- notification_channel must be the SAME SQS ARN as the v1 pipes (one queue per account/region)
-- -> the existing S3 notification (prefix raw/) already covers raw/api/
SHOW PIPES IN SCHEMA INS_RAW.API;
SHOW PIPES IN SCHEMA INS_RAW.POLICY_ADMIN;

-- ---------------------------------------------------------------------
-- Debugging:
--   SELECT SYSTEM$PIPE_STATUS('INS_RAW.API.PIPE_API_CLAIMS');
--   SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--       TABLE_NAME => 'INS_RAW.API.CLAIMS', START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())));
-- ---------------------------------------------------------------------
