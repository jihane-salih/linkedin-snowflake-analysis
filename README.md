# 🧊 Lab — Analyse LinkedIn avec Snowflake + Streamlit

---

## 🎯 Objectif

Ce lab a pour objectif de construire un pipeline complet de données avec Snowflake (Bronze → Silver → Gold) et de créer un dashboard interactif avec Streamlit afin d’analyser les tendances du marché de l’emploi LinkedIn.

Nous allons :
- Charger des données depuis S3 (CSV + JSON)
- Construire une architecture de données en couches
- Nettoyer et transformer les données
- Créer des KPIs métiers
- Visualiser les résultats avec Streamlit

-- =========================
-- DATABASE + SCHEMAS
-- =========================

-- Create database
CREATE DATABASE IF NOT EXISTS LINKEDIN;
USE DATABASE LINKEDIN;

-- Create schemas (Bronze / Silver / Gold)
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Use Bronze schema
USE SCHEMA BRONZE;

-- =========================
-- STAGE + FILE FORMATS
-- =========================

-- Create external stage (S3 bucket)
CREATE OR REPLACE STAGE linkedin_stage
URL = 's3://snowflake-lab-bucket/';

-- CSV file format definition
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON file format definition
CREATE OR REPLACE FILE FORMAT json_format
TYPE = JSON;

-- =========================
-- BRONZE TABLES (RAW DATA)
-- =========================

-- Set context
USE SCHEMA BRONZE;

-- =========================
-- JOB_POSTINGS TABLE
-- =========================

-- Create raw job postings table
CREATE OR REPLACE TABLE JOB_POSTINGS (
    job_id STRING,
    company_name STRING,
    title STRING,
    description STRING,
    max_salary STRING,
    min_salary STRING,
    med_salary STRING,
    pay_period STRING,
    formatted_work_type STRING,
    location STRING,
    applies STRING,
    views STRING,
    formatted_experience_level STRING,
    work_type STRING,
    currency STRING,
    compensation_type STRING,
    original_listed_time STRING,
    expiry STRING,
    closed_time STRING,
    remote_allowed STRING,
    job_posting_url STRING,
    application_url STRING,
    application_type STRING,
    posting_domain STRING,
    sponsored STRING
);

-- Load CSV data into JOB_POSTINGS
COPY INTO JOB_POSTINGS
FROM @linkedin_stage/job_postings.csv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- Check data
SELECT * FROM JOB_POSTINGS;

-- =========================
-- COMPANIES TABLE
-- =========================

-- Create raw companies table (JSON)
CREATE OR REPLACE TABLE COMPANIES (data VARIANT);

-- Load JSON data
COPY INTO COMPANIES
FROM @linkedin_stage/companies.json
FILE_FORMAT = json_format;

-- Check data
SELECT * FROM COMPANIES;

-- =========================
-- JOB INDUSTRIES RAW
-- =========================

-- Create raw job industries table
CREATE OR REPLACE TABLE JOB_INDUSTRIES_RAW (data VARIANT);

-- Load JSON data
COPY INTO JOB_INDUSTRIES_RAW
FROM @linkedin_stage/job_industries.json
FILE_FORMAT = json_format;

-- Check data
SELECT * FROM JOB_INDUSTRIES_RAW;

-- =========================
-- BENEFITS TABLE
-- =========================

-- Create benefits table
CREATE OR REPLACE TABLE bronze_benefits (
job_id STRING,
inferred STRING,
type STRING
);

-- Load benefits data
COPY INTO bronze_benefits
FROM @linkedin_stage/benefits.csv
FILE_FORMAT = csv_format;

-- Check data
SELECT * FROM bronze_benefits;

-- =========================
-- EMPLOYEE COUNTS TABLE
-- =========================

-- Create employee counts table
CREATE OR REPLACE TABLE bronze_employee_counts (
company_id STRING,
employee_count STRING,
follower_count STRING,
time_recorded STRING
);

-- Load employee data
COPY INTO bronze_employee_counts
FROM @linkedin_stage/employee_counts.csv
FILE_FORMAT = csv_format;

-- Check data
SELECT * FROM bronze_employee_counts;

-- =========================
-- JOB SKILLS TABLE
-- =========================

-- Create job skills table
CREATE OR REPLACE TABLE bronze_job_skills (
job_id STRING,
skill_abr STRING
);

-- Load skills data
COPY INTO bronze_job_skills
FROM @linkedin_stage/job_skills.csv
FILE_FORMAT = csv_format;

-- Check data
SELECT * FROM bronze_job_skills;

-- =========================
-- COMPANY SPECIALITIES
-- =========================

-- Create table (JSON raw)
CREATE OR REPLACE TABLE linkedin.bronze.company_specialities (
    data variant
);

-- Load data
COPY INTO linkedin.bronze.company_specialities
FROM @linkedin.bronze.linkedin_stage/company_specialities.json
FILE_FORMAT = (type = 'json');

-- Check data
SELECT * FROM linkedin.bronze.company_specialities;

-- =========================
-- COMPANY INDUSTRIES
-- =========================

-- Create table (JSON raw)
CREATE OR REPLACE TABLE linkedin.bronze.company_industries (
    data variant
);

-- Load data
COPY INTO linkedin.bronze.company_industries
FROM @linkedin.bronze.linkedin_stage/company_industries.json
FILE_FORMAT = (type = 'json');

-- Check data
SELECT * FROM linkedin.bronze.company_industries;

-- =========================
-- SILVER LAYER (CLEAN DATA)
-- =========================

-- Create companies clean table
CREATE OR REPLACE TABLE SILVER.COMPANIES_CLEAN AS
SELECT
    TRIM(TO_VARCHAR(f.value:"company_id")) AS company_id,
    TRIM(f.value:"name") AS company_name,

    -- numeric company size
    f.value:"company_size"::NUMBER AS company_size,

    -- company size label
    CASE
        WHEN f.value:"company_size"::NUMBER BETWEEN 0 AND 1 THEN 'Startup (1-10)'
        WHEN f.value:"company_size"::NUMBER = 2 THEN 'Small (11-50)'
        WHEN f.value:"company_size"::NUMBER = 3 THEN 'Small-Mid (51-200)'
        WHEN f.value:"company_size"::NUMBER = 4 THEN 'Mid (201-500)'
        WHEN f.value:"company_size"::NUMBER = 5 THEN 'Large (501-1000)'
        WHEN f.value:"company_size"::NUMBER = 6 THEN 'Enterprise (1001-5000)'
        WHEN f.value:"company_size"::NUMBER >= 7 THEN 'Mega Enterprise (5000+)'
        ELSE 'Not Classified'
    END AS company_size_label

FROM BRONZE.COMPANIES,
LATERAL FLATTEN(input => data) f;

-- Check table
SELECT * FROM SILVER.COMPANIES_CLEAN;


-- =========================
-- JOB POSTINGS CLEAN
-- =========================

CREATE OR REPLACE TABLE SILVER.JOB_POSTINGS AS
SELECT
    job_id,
    TRIM(title) AS title,
    TRIM(company_name) AS company_id,
    location,
    formatted_work_type,
    formatted_experience_level,

    -- clean salary
    TRY_TO_NUMBER(REGEXP_REPLACE(max_salary, '[^0-9]', '')) AS max_salary,

    -- yearly salary conversion
    CASE
        WHEN LOWER(pay_period) = 'hourly'
            THEN TRY_TO_NUMBER(REGEXP_REPLACE(max_salary, '[^0-9]', '')) * 2080
        WHEN LOWER(pay_period) = 'monthly'
            THEN TRY_TO_NUMBER(REGEXP_REPLACE(max_salary, '[^0-9]', '')) * 12
        ELSE TRY_TO_NUMBER(REGEXP_REPLACE(max_salary, '[^0-9]', ''))
    END AS max_salary_yearly

FROM BRONZE.JOB_POSTINGS;

-- Check table
SELECT * FROM SILVER.JOB_POSTINGS;


-- =========================
-- JOB INDUSTRIES CLEAN
-- =========================

CREATE OR REPLACE TABLE SILVER.JOB_INDUSTRIES AS
SELECT
    f.value:"job_id"::STRING AS job_id,
    f.value:"industry_id"::STRING AS industry_id
FROM BRONZE.JOB_INDUSTRIES_RAW,
LATERAL FLATTEN(input => data) f;

-- Check table
SELECT * FROM SILVER.JOB_INDUSTRIES;


-- =========================
-- JOB + COMPANY JOIN
-- =========================

CREATE OR REPLACE TABLE SILVER.JOB_ENRICHED AS
SELECT
    jp.*,
    c.company_name
FROM SILVER.JOB_POSTINGS jp
LEFT JOIN SILVER.COMPANIES_CLEAN c
    ON jp.company_id = c.company_id;

-- Check table
SELECT * FROM SILVER.JOB_ENRICHED;


-- =========================
-- GOLD LAYER (ANALYTICS)
-- =========================

-- Create industry dimension table
CREATE OR REPLACE TABLE GOLD.DIM_INDUSTRY AS
SELECT DISTINCT
    industry_id,

    CASE industry_id
        WHEN '1' THEN 'Technology & IT'
        WHEN '2' THEN 'Financial Services'
        WHEN '3' THEN 'Healthcare'
        WHEN '4' THEN 'Education'
        WHEN '5' THEN 'Retail'
        WHEN '6' THEN 'Manufacturing'
        WHEN '7' THEN 'Telecommunications'
        WHEN '8' THEN 'Consulting'
        WHEN '9' THEN 'Marketing & Advertising'
        WHEN '10' THEN 'Transport & Logistics'
        WHEN '11' THEN 'Real Estate'
        WHEN '12' THEN 'Energy & Utilities'
        WHEN '13' THEN 'Human Resources'
        WHEN '14' THEN 'Media & Entertainment'
        WHEN '15' THEN 'Government'
        ELSE 'NOT CLASSIFIED'
    END AS industry_name
FROM SILVER.JOB_INDUSTRIES;


-- Map jobs to industries
CREATE OR REPLACE TABLE GOLD.JOB_INDUSTRY AS
SELECT
    ji.job_id,
    di.industry_name
FROM SILVER.JOB_INDUSTRIES ji
LEFT JOIN GOLD.DIM_INDUSTRY di
    ON ji.industry_id = di.industry_id;


-- =========================
-- TOP JOB TITLES PER INDUSTRY
-- =========================

CREATE OR REPLACE TABLE GOLD.TOP_JOBS_BY_INDUSTRY AS
SELECT *
FROM (
    SELECT
        ji.industry_name,
        je.title,
        COUNT(*) AS total_jobs,

        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_name
            ORDER BY COUNT(*) DESC
        ) AS rn

    FROM SILVER.JOB_ENRICHED je
    JOIN GOLD.JOB_INDUSTRY ji
        ON je.job_id = ji.job_id
    GROUP BY ji.industry_name, je.title
)
WHERE rn <= 10;


-- =========================
-- TOP SALARIES PER INDUSTRY
-- =========================

CREATE OR REPLACE TABLE GOLD.TOP_SALARIES_BY_INDUSTRY AS
SELECT *
FROM (
    SELECT
        ji.industry_name,
        je.title,
        je.max_salary_yearly,

        ROW_NUMBER() OVER (
            PARTITION BY ji.industry_name
            ORDER BY je.max_salary_yearly DESC NULLS LAST
        ) AS rn

    FROM SILVER.JOB_ENRICHED je
    JOIN GOLD.JOB_INDUSTRY ji
        ON je.job_id = ji.job_id
    WHERE je.max_salary_yearly IS NOT NULL
)
WHERE rn <= 10;


-- =========================
-- JOBS BY INDUSTRY
-- =========================

CREATE OR REPLACE TABLE GOLD.INDUSTRY_DISTRIBUTION AS
SELECT
    industry_name,
    COUNT(*) AS total_jobs
FROM GOLD.JOB_INDUSTRY
GROUP BY industry_name;


-- =========================
-- COMPANY SIZE DISTRIBUTION
-- =========================

CREATE OR REPLACE TABLE GOLD.COMPANY_SIZE AS
SELECT
    company_size_label,
    COUNT(*) AS total_offerts
FROM SILVER.COMPANIES_CLEAN
GROUP BY company_size_label;


-- =========================
-- WORK TYPE DISTRIBUTION
-- =========================

CREATE OR REPLACE TABLE GOLD.WORK_TYPE AS
SELECT
    formatted_work_type,
    COUNT(*) AS total_jobs
FROM SILVER.JOB_ENRICHED
GROUP BY formatted_work_type;