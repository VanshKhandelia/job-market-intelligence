-- ============================================================
-- GOLD SCHEMA — Business ready layer
-- Aggregated, analytics-ready tables
-- Single source of truth for Tableau dashboards
-- ============================================================

-- Company Tech Mix
-- One row per company + capability domain + month
-- Tracks what each company is investing in over time
CREATE OR REPLACE TABLE JOB_INTELLIGENCE.GOLD.COMPANY_TECH_MIX (
    COMPANY_CLEAN           VARCHAR,        -- standardized company name (lowercase, no Inc/Ltd etc)
    CAPABILITY_DOMAIN       VARCHAR,        -- data_ai | engineering | cloud_infra | security | networking | gtm | product | support_services
    YEAR_MONTH              VARCHAR,        -- e.g. "2026-02" — enables trend analysis over time
    JOB_COUNT               INTEGER,        -- number of jobs for this company + domain + month
    TOTAL_TECH_JOBS         INTEGER,        -- total tech jobs for this company in this month
    TECH_SHARE              FLOAT,          -- job_count / total_tech_jobs — % of company focus in this domain
    CAPABILITY_RANK         INTEGER,        -- rank of this domain within the company (1 = most jobs)
    DOMINANT_CAPABILITY     BOOLEAN,        -- true if this is the company's #1 capability domain
    TECH_INTENSITY_RANK     INTEGER         -- rank of company by total tech hiring volume this month
);

-- ============================================================
-- USEFUL QUERIES
-- ============================================================

-- What is each company's primary focus this month?
-- SELECT COMPANY_CLEAN, CAPABILITY_DOMAIN, JOB_COUNT, TECH_SHARE
-- FROM GOLD.COMPANY_TECH_MIX
-- WHERE DOMINANT_CAPABILITY = TRUE
-- AND YEAR_MONTH = '2026-03'
-- ORDER BY JOB_COUNT DESC;

-- How has a specific company's focus shifted over time?
-- SELECT YEAR_MONTH, CAPABILITY_DOMAIN, JOB_COUNT, TECH_SHARE
-- FROM GOLD.COMPANY_TECH_MIX
-- WHERE COMPANY_CLEAN = 'fortinet'
-- ORDER BY YEAR_MONTH, CAPABILITY_RANK;

-- Which capability domain is most in demand across all companies?
-- SELECT CAPABILITY_DOMAIN, SUM(JOB_COUNT) AS TOTAL_JOBS
-- FROM GOLD.COMPANY_TECH_MIX
-- WHERE YEAR_MONTH = '2026-03'
-- GROUP BY CAPABILITY_DOMAIN
-- ORDER BY TOTAL_JOBS DESC;

-- Which companies are hiring most aggressively in data_ai?
-- SELECT COMPANY_CLEAN, JOB_COUNT, TECH_SHARE
-- FROM GOLD.COMPANY_TECH_MIX
-- WHERE CAPABILITY_DOMAIN = 'data_ai'
-- AND YEAR_MONTH = '2026-03'
-- ORDER BY JOB_COUNT DESC;