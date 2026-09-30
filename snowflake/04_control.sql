-- =====================================================================
-- 04_control.sql - control tables for watermark-driven API ingestion
-- Run in Snowsight, top to bottom, one section at a time.
--   WATERMARKS   : where each entity's last SUCCESSFUL load stopped (1 row per entity)
--   EXTRACT_RUNS : audit log, 1 row per run per entity (SUCCEEDED / FAILED)
-- Lambda only READS watermarks. Only Airflow advances them, after dbt build succeeds.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. SCHEMA - SYSADMIN owns it, INS_LOADER owns the tables inside (same pattern as 01_setup)
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;
CREATE SCHEMA IF NOT EXISTS INS_RAW.CONTROL COMMENT = 'Pipeline state: watermarks + run audit';

USE ROLE SECURITYADMIN;
GRANT USAGE, CREATE TABLE ON SCHEMA INS_RAW.CONTROL TO ROLE INS_LOADER;


-- ---------------------------------------------------------------------
-- 2. WATERMARKS - one row per (source, entity)
-- ---------------------------------------------------------------------
USE ROLE INS_LOADER;
USE WAREHOUSE INS_WH;
USE SCHEMA INS_RAW.CONTROL;

CREATE TABLE IF NOT EXISTS WATERMARKS (
  SOURCE_NAME    STRING        NOT NULL,   -- which source system, e.g. 'policy_admin_api'
  ENTITY         STRING        NOT NULL,   -- policyholders / policies / claims
  HIGH_WATERMARK TIMESTAMP_NTZ NOT NULL,   -- max source updated_at that is safely in gold
  LAST_RUN_ID    STRING,                   -- the run that last moved it (lineage)
  UPDATED_AT     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  -- Documentation only: Snowflake does NOT enforce PK/UNIQUE (only NOT NULL)
  CONSTRAINT PK_WATERMARKS PRIMARY KEY (SOURCE_NAME, ENTITY)
);


-- ---------------------------------------------------------------------
-- 3. EXTRACT_RUNS - audit log, one row per (run, entity). Append-only.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EXTRACT_RUNS (
  RUN_ID             STRING        NOT NULL,  -- Airflow dag run id (same across task retries)
  SOURCE_NAME        STRING        NOT NULL,
  ENTITY             STRING        NOT NULL,
  BUSINESS_DATE      DATE          NOT NULL,  -- dt= partition the files were written to
  WINDOW_START       TIMESTAMP_NTZ,           -- old watermark - buffer
  WINDOW_END         TIMESTAMP_NTZ,           -- run's interval end
  ROW_COUNT_MANIFEST NUMBER,                  -- rows Lambda says it wrote
  ROW_COUNT_LOADED   NUMBER,                  -- rows Snowpipe actually loaded
  MAX_UPDATED_AT     TIMESTAMP_NTZ,           -- max updated_at loaded -> the new watermark
  STATUS             STRING        NOT NULL,  -- SUCCEEDED / FAILED
  ERROR_MESSAGE      STRING,
  STARTED_AT         TIMESTAMP_LTZ,
  FINISHED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT PK_EXTRACT_RUNS PRIMARY KEY (RUN_ID, ENTITY)
);


-- ---------------------------------------------------------------------
-- 4. SEED - one watermark per entity, starting at "the beginning of time"
--    MERGE, not INSERT: re-running this script must NEVER reset a live watermark
-- ---------------------------------------------------------------------
MERGE INTO WATERMARKS t
USING (
  SELECT 'policy_admin_api' AS SOURCE_NAME, COLUMN1 AS ENTITY
  FROM VALUES ('policyholders'), ('policies'), ('claims')
) s
ON t.SOURCE_NAME = s.SOURCE_NAME AND t.ENTITY = s.ENTITY
WHEN NOT MATCHED THEN
  INSERT (SOURCE_NAME, ENTITY, HIGH_WATERMARK, LAST_RUN_ID)
  VALUES (s.SOURCE_NAME, s.ENTITY, '1970-01-01 00:00:00'::TIMESTAMP_NTZ, 'seed');

SELECT * FROM WATERMARKS ORDER BY ENTITY;


-- ---------------------------------------------------------------------
-- 5. EXTRACTOR ROLE + LAMBDA SERVICE USER - read the watermark, nothing else
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS INS_EXTRACTOR COMMENT = 'Extractor Lambda: reads watermarks only';
GRANT ROLE INS_EXTRACTOR TO ROLE SYSADMIN;

GRANT USAGE  ON WAREHOUSE INS_WH                     TO ROLE INS_EXTRACTOR;
GRANT USAGE  ON DATABASE  INS_RAW                    TO ROLE INS_EXTRACTOR;
GRANT USAGE  ON SCHEMA    INS_RAW.CONTROL            TO ROLE INS_EXTRACTOR;
GRANT SELECT ON TABLE     INS_RAW.CONTROL.WATERMARKS TO ROLE INS_EXTRACTOR;

-- The IAM role doesn't exist yet (CDK creates it in Phase 4 with this FIXED name).
-- Snowflake doesn't check the ARN now; login works once the role exists.
USE ROLE USERADMIN;
CREATE USER IF NOT EXISTS INS_LAMBDA_SVC
  TYPE              = SERVICE
  WORKLOAD_IDENTITY = (TYPE = AWS  ARN = 'arn:aws:iam::656559336744:role/mwaa-dbt-elt-extractor')
  DEFAULT_ROLE      = INS_EXTRACTOR
  DEFAULT_WAREHOUSE = INS_WH
  COMMENT           = 'Extractor Lambda via AWS workload identity';

USE ROLE SECURITYADMIN;
GRANT ROLE INS_EXTRACTOR TO USER INS_LAMBDA_SVC;

-- dbt (TRANSFORMER): read pipeline state -> freshness / audit models later
GRANT USAGE  ON SCHEMA INS_RAW.CONTROL               TO ROLE INS_TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA INS_RAW.CONTROL TO ROLE INS_TRANSFORMER;


-- ---------------------------------------------------------------------
-- 6. VERIFY
-- ---------------------------------------------------------------------
SHOW TABLES IN SCHEMA INS_RAW.CONTROL;  -- owner must be INS_LOADER
SHOW GRANTS TO ROLE INS_EXTRACTOR;      -- expect: USAGE wh/db/schema + SELECT on WATERMARKS only
DESC USER INS_LAMBDA_SVC;               -- expect: TYPE=SERVICE, workload identity set
