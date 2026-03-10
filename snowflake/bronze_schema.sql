-- ============================================================
-- BRONZE SCHEMA — Raw layer
-- Exact copy of data from Adzuna API, no transformations
-- Never modify data in this layer
-- ============================================================

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS JOB_INTELLIGENCE;

CREATE SCHEMA IF NOT EXISTS JOB_INTELLIGENCE.BRONZE;
CREATE SCHEMA IF NOT EXISTS JOB_INTELLIGENCE.SILVER;
CREATE SCHEMA IF NOT EXISTS JOB_INTELLIGENCE.GOLD;

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Raw job postings table
-- job_id is VARCHAR to match Adzuna API string format and avoid type mismatch on deduplication
CREATE OR REPLACE TABLE JOB_INTELLIGENCE.BRONZE.RAW_JOB_POSTINGS (
    COMPANY_NAME_SEARCHED   VARCHAR,        -- company name we searched for in Adzuna
    JOB_ID                  VARCHAR,        -- unique job identifier from Adzuna (kept as string)
    JOB_TITLE               VARCHAR,        -- raw job title
    COMPANY                 VARCHAR,        -- company name as returned by Adzuna
    LOCATION                VARCHAR,        -- full location string e.g. "Greater Vancouver, British Columbia"
    DESCRIPTION             VARCHAR,        -- truncated job description (max 500 chars from Adzuna)
    SALARY_MIN              FLOAT,          -- minimum salary if provided (often null)
    SALARY_MAX              FLOAT,          -- maximum salary if provided (often null)
    CONTRACT_TYPE           VARCHAR,        -- e.g. permanent, contract (often null)
    CONTRACT_TIME           VARCHAR,        -- e.g. full_time, part_time
    CATEGORY                VARCHAR,        -- Adzuna job category e.g. "IT Jobs", "Engineering Jobs"
    CREATED                 TIMESTAMP,      -- when the job was posted
    REDIRECT_URL            VARCHAR,        -- link to full job posting
    EXTRACTED_AT            TIMESTAMP       -- when our pipeline pulled this record
);