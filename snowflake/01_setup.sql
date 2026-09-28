-- =====================================================================
-- 01_setup.sql - Snowflake foundation for the insurance ELT pipeline
-- Run in Snowsight, top to bottom, one section at a time.
-- Everything is prefixed INS_ so it never collides with other projects.
-- Section 6 uses the "ExecutionRoleArn" output of the Elt-Mwaa CloudFormation stack.
-- If the MWAA stack is ever recreated, that ARN changes -> ALTER USER with the new one.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. ROLES - SECURITYADMIN manages roles and grants
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

-- One role per job = least privilege (a leaked credential can only do its own job)
CREATE ROLE IF NOT EXISTS INS_LOADER      COMMENT = 'Loads files from S3 into INS_RAW';
CREATE ROLE IF NOT EXISTS INS_TRANSFORMER COMMENT = 'dbt: reads INS_RAW, builds INS_ANALYTICS';
CREATE ROLE IF NOT EXISTS INS_REPORTER    COMMENT = 'Analysts / BI: read-only on analytics';

-- Custom roles roll up to SYSADMIN, so admins can see and manage what these roles create
GRANT ROLE INS_LOADER      TO ROLE SYSADMIN;
GRANT ROLE INS_TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE INS_REPORTER    TO ROLE SYSADMIN;


-- ---------------------------------------------------------------------
-- 2. COMPUTE + STORAGE OBJECTS - SYSADMIN owns objects
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS INS_WH
  WAREHOUSE_SIZE      = 'XSMALL'   -- 1 credit/hour, only while running
  AUTO_SUSPEND        = 60         -- suspend after 60s idle -> no idle cost
  AUTO_RESUME         = TRUE       -- wake up automatically on the next query
  INITIALLY_SUSPENDED = TRUE
  COMMENT             = 'Loads + dbt for the insurance pipeline';

CREATE DATABASE IF NOT EXISTS INS_RAW       COMMENT = 'Raw data, loaded as-is from S3';
CREATE SCHEMA   IF NOT EXISTS INS_RAW.POLICY_ADMIN COMMENT = 'Source system: policy admin';
CREATE DATABASE IF NOT EXISTS INS_ANALYTICS COMMENT = 'dbt models: staging, intermediate, marts';


-- ---------------------------------------------------------------------
-- 3. STORAGE INTEGRATION - Snowflake reads S3 through an IAM role (no AWS keys)
--    Only ACCOUNTADMIN (or CREATE INTEGRATION privilege) can create it.
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- NEVER use CREATE OR REPLACE here: it generates a NEW external ID and breaks the AWS trust
CREATE STORAGE INTEGRATION IF NOT EXISTS INS_S3_INT
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::656559336744:role/mwaa-dbt-elt-snowflake-s3-read'
  STORAGE_ALLOWED_LOCATIONS = ('s3://mwaa-dbt-elt-data-lake-656559336744-us-east-1/raw/');

-- Copy these two values -> they go into the AWS role's trust policy (CDK context):
--   STORAGE_AWS_IAM_USER_ARN   (the IAM user Snowflake uses in its own AWS account)
--   STORAGE_AWS_EXTERNAL_ID    (shared secret that prevents the "confused deputy" problem)
DESC INTEGRATION INS_S3_INT;

GRANT USAGE ON INTEGRATION INS_S3_INT TO ROLE SYSADMIN;

-- Cost guardrail: notify at 80%, suspend the warehouse at 100% of 10 credits/month
CREATE RESOURCE MONITOR IF NOT EXISTS INS_RM
  WITH CREDIT_QUOTA = 10
  FREQUENCY = MONTHLY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY
           ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE INS_WH SET RESOURCE_MONITOR = INS_RM;


-- ---------------------------------------------------------------------
-- 4. FILE FORMAT + EXTERNAL STAGE - where COPY INTO reads from
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE FILE FORMAT IF NOT EXISTS INS_RAW.POLICY_ADMIN.FF_PARQUET
  TYPE = PARQUET;

-- A stage is a named pointer to an S3 location + how to access it (the integration)
CREATE STAGE IF NOT EXISTS INS_RAW.POLICY_ADMIN.S3_RAW_STAGE
  STORAGE_INTEGRATION = INS_S3_INT
  URL                 = 's3://mwaa-dbt-elt-data-lake-656559336744-us-east-1/raw/'
  FILE_FORMAT         = INS_RAW.POLICY_ADMIN.FF_PARQUET;


-- ---------------------------------------------------------------------
-- 5. GRANTS - SECURITYADMIN has MANAGE GRANTS (needed for FUTURE grants)
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

-- Everyone who runs queries needs the warehouse
GRANT USAGE ON WAREHOUSE INS_WH TO ROLE INS_LOADER;
GRANT USAGE ON WAREHOUSE INS_WH TO ROLE INS_TRANSFORMER;
GRANT USAGE ON WAREHOUSE INS_WH TO ROLE INS_REPORTER;

-- LOADER: create and write raw tables, read the stage
GRANT USAGE ON DATABASE INS_RAW                               TO ROLE INS_LOADER;
GRANT USAGE, CREATE TABLE ON SCHEMA INS_RAW.POLICY_ADMIN      TO ROLE INS_LOADER;
GRANT USAGE ON STAGE INS_RAW.POLICY_ADMIN.S3_RAW_STAGE        TO ROLE INS_LOADER;
GRANT USAGE ON FILE FORMAT INS_RAW.POLICY_ADMIN.FF_PARQUET    TO ROLE INS_LOADER;

-- TRANSFORMER (dbt): read raw (incl. tables created later), build anything in analytics
GRANT USAGE ON DATABASE INS_RAW                                     TO ROLE INS_TRANSFORMER;
GRANT USAGE ON SCHEMA INS_RAW.POLICY_ADMIN                          TO ROLE INS_TRANSFORMER;
GRANT SELECT ON ALL TABLES    IN SCHEMA INS_RAW.POLICY_ADMIN        TO ROLE INS_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA INS_RAW.POLICY_ADMIN        TO ROLE INS_TRANSFORMER;
GRANT USAGE, CREATE SCHEMA ON DATABASE INS_ANALYTICS                TO ROLE INS_TRANSFORMER;

-- REPORTER: read-only on everything dbt builds (FUTURE = objects that don't exist yet)
GRANT USAGE ON DATABASE INS_ANALYTICS                       TO ROLE INS_REPORTER;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE INS_ANALYTICS     TO ROLE INS_REPORTER;
GRANT SELECT ON FUTURE TABLES IN DATABASE INS_ANALYTICS     TO ROLE INS_REPORTER;
GRANT SELECT ON FUTURE VIEWS  IN DATABASE INS_ANALYTICS     TO ROLE INS_REPORTER;


-- ---------------------------------------------------------------------
-- 6. SERVICE USER FOR MWAA - Workload Identity Federation (no password, no key)
--    MWAA workers sign AWS requests with the execution role; Snowflake verifies them.
-- ---------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE USER IF NOT EXISTS INS_MWAA_SVC
  TYPE              = SERVICE   -- machine user: no password/UI login, no MFA prompts
  WORKLOAD_IDENTITY = (TYPE = AWS  ARN = 'arn:aws:iam::656559336744:role/Elt-Mwaa-ExecutionRole605A040B-y4hLwUDGoo73')
  DEFAULT_ROLE      = INS_TRANSFORMER
  DEFAULT_WAREHOUSE = INS_WH
  COMMENT           = 'MWAA workers via AWS workload identity';

USE ROLE SECURITYADMIN;
GRANT ROLE INS_LOADER      TO USER INS_MWAA_SVC;
GRANT ROLE INS_TRANSFORMER TO USER INS_MWAA_SVC;


-- ---------------------------------------------------------------------
-- 7. VERIFY
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;
SHOW GRANTS TO ROLE INS_TRANSFORMER;
DESC USER INS_MWAA_SVC;

-- Works only AFTER the AWS trust policy has Snowflake's user ARN + external ID.
-- Empty result = success (no files yet). An "access denied" error = trust not set up.
LIST @INS_RAW.POLICY_ADMIN.S3_RAW_STAGE;
