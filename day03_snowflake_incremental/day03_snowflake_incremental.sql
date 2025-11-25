-- =========================================================
-- Day 3 - Data Modeling + Snowflake Setup + Incremental Load
-- 15-Day Multi-Cloud Data Engineering Mastery
-- Depends on: HEALTHCARE_RAW, FACT_ADMISSION, DIM tables from Day 1
-- =========================================================


-- =========================================================
-- 0. Context Setup
-- =========================================================
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS HEALTHCARE_DEMO_DB;
USE DATABASE HEALTHCARE_DEMO_DB;

CREATE SCHEMA IF NOT EXISTS IOT_HEALTHCARE;
USE SCHEMA IOT_HEALTHCARE;


-- =========================================================
-- 1. LOAD_PROCESS_WATERMARK Control Table
-- =========================================================
CREATE OR REPLACE TABLE LOAD_PROCESS_WATERMARK (
    SOURCE_SYSTEM              STRING,
    TABLE_NAME                 STRING,
    LAST_SUCCESSFUL_WATERMARK  TIMESTAMP_NTZ
);

-- Seed an initial watermark for the healthcare admissions pipeline
INSERT INTO LOAD_PROCESS_WATERMARK (
    SOURCE_SYSTEM,
    TABLE_NAME,
    LAST_SUCCESSFUL_WATERMARK
)
VALUES (
    'HEALTHCARE_PIPELINE',
    'HEALTHCARE_RAW',
    '1900-01-01 00:00:00'
);


-- =========================================================
-- 2. Incremental Fact Table (FACT_ADMISSION_INC)
--    This will hold incrementally loaded admissions.
-- =========================================================

-- Create an empty incremental fact table with the same structure as FACT_ADMISSION
CREATE OR REPLACE TABLE FACT_ADMISSION_INC AS
SELECT *
FROM FACT_ADMISSION
WHERE 1 = 0;


-- =========================================================
-- 3. Incremental Load Logic
--    Load only records newer than the last watermark.
-- =========================================================

-- Insert incremental records into FACT_ADMISSION_INC
INSERT INTO FACT_ADMISSION_INC (
    PATIENT_NAME,
    DOCTOR_NAME,
    HOSPITAL_NAME,
    MEDICATION,
    ADMISSION_DATE,
    DISCHARGE_DATE,
    ADMISSION_TYPE,
    ROOM_NUMBER,
    BILLING_AMOUNT
)
SELECT
    NAME AS PATIENT_NAME,
    DOCTOR AS DOCTOR_NAME,
    HOSPITAL AS HOSPITAL_NAME,
    MEDICATION,
    "Date of Admission"    AS ADMISSION_DATE,
    "Discharge Date"       AS DISCHARGE_DATE,
    "Admission Type"       AS ADMISSION_TYPE,
    "Room Number"          AS ROOM_NUMBER,
    "Billing Amount"       AS BILLING_AMOUNT
FROM HEALTHCARE_RAW
WHERE "Date of Admission" > (
    SELECT LAST_SUCCESSFUL_WATERMARK
    FROM LOAD_PROCESS_WATERMARK
    WHERE SOURCE_SYSTEM = 'HEALTHCARE_PIPELINE'
      AND TABLE_NAME    = 'HEALTHCARE_RAW'
);


-- =========================================================
-- 4. Update Watermark After Successful Load
--    Set the watermark to the max Date of Admission loaded.
-- =========================================================
UPDATE LOAD_PROCESS_WATERMARK
SET LAST_SUCCESSFUL_WATERMARK = (
    SELECT MAX("Date of Admission")
    FROM HEALTHCARE_RAW
)
WHERE SOURCE_SYSTEM = 'HEALTHCARE_PIPELINE'
  AND TABLE_NAME    = 'HEALTHCARE_RAW';


-- =========================================================
-- 5. Validation Queries
-- =========================================================

-- Check number of rows loaded incrementally
SELECT COUNT(*) AS ROW_COUNT_INCREMENTAL
FROM FACT_ADMISSION_INC;

-- Check min/max admission dates loaded
SELECT 
    MIN(ADMISSION_DATE) AS MIN_ADMISSION_DATE,
    MAX(ADMISSION_DATE) AS MAX_ADMISSION_DATE
FROM FACT_ADMISSION_INC;

-- View current watermark
SELECT *
FROM LOAD_PROCESS_WATERMARK
WHERE SOURCE_SYSTEM = 'HEALTHCARE_PIPELINE'
  AND TABLE_NAME    = 'HEALTHCARE_RAW';
