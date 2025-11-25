-- =========================================================
-- Day 2 - Advanced SQL & Query Optimization
-- 15-Day Multi-Cloud Data Engineering Mastery
-- Tables used: FACT_ADMISSION, DIM_PATIENT, DIM_DOCTOR, DIM_HOSPITAL
-- =========================================================


-- =========================================================
-- 1. Rolling Average Billing Per Patient (Window Function)
-- =========================================================
SELECT 
    PATIENT_NAME,
    ADMISSION_DATE,
    BILLING_AMOUNT,
    AVG(BILLING_AMOUNT) OVER (
        PARTITION BY PATIENT_NAME 
        ORDER BY ADMISSION_DATE
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS ROLLING_AVG_BILLING
FROM FACT_ADMISSION
ORDER BY PATIENT_NAME, ADMISSION_DATE;


-- =========================================================
-- 2. Top 5 Highest Billing Cases (Global Ranking)
-- =========================================================
SELECT *
FROM (
    SELECT
        PATIENT_NAME,
        HOSPITAL_NAME,
        BILLING_AMOUNT,
        RANK() OVER (ORDER BY BILLING_AMOUNT DESC) AS BILLING_RANK
    FROM FACT_ADMISSION
) t
WHERE BILLING_RANK <= 5
ORDER BY BILLING_RANK, BILLING_AMOUNT DESC;


-- =========================================================
-- 3. 2nd Highest Billing Per Hospital (Interview Classic)
-- =========================================================
SELECT *
FROM (
    SELECT
        HOSPITAL_NAME,
        PATIENT_NAME,
        BILLING_AMOUNT,
        DENSE_RANK() OVER (
            PARTITION BY HOSPITAL_NAME
            ORDER BY BILLING_AMOUNT DESC
        ) AS BILL_RANK
    FROM FACT_ADMISSION
) t
WHERE BILL_RANK = 2
ORDER BY HOSPITAL_NAME, BILLING_AMOUNT DESC;


-- =========================================================
-- 4. Hospital-Level Billing Aggregation (OLAP Style)
-- =========================================================
SELECT 
    HOSPITAL_NAME,
    AVG(BILLING_AMOUNT) AS AVG_BILL,
    SUM(BILLING_AMOUNT) AS TOTAL_BILLING,
    COUNT(*) AS TOTAL_ADMISSIONS
FROM FACT_ADMISSION
GROUP BY HOSPITAL_NAME
ORDER BY TOTAL_BILLING DESC;


-- =========================================================
-- 5. OLTP-style Row-Level Lookup (Comparison)
-- =========================================================
-- Example of a transactional-style query (OLTP pattern)
SELECT *
FROM FACT_ADMISSION
WHERE PATIENT_NAME = 'John Doe'
ORDER BY ADMISSION_DATE DESC;


-- =========================================================
-- 6. Patient - Doctor - Hospital Analytics View (Join Practice)
-- =========================================================
SELECT 
    f.PATIENT_NAME,
    p.AGE,
    p.GENDER,
    d.DOCTOR_NAME,
    h.HOSPITAL_NAME,
    h.INSURANCE_PROVIDER,
    f.ADMISSION_DATE,
    f.DISCHARGE_DATE,
    f.ADMISSION_TYPE,
    f.ROOM_NUMBER,
    f.BILLING_AMOUNT
FROM FACT_ADMISSION f
JOIN DIM_PATIENT p 
    ON f.PATIENT_NAME = p.PATIENT_NAME
JOIN DIM_DOCTOR d 
    ON f.DOCTOR_NAME = d.DOCTOR_NAME
JOIN DIM_HOSPITAL h 
    ON f.HOSPITAL_NAME = h.HOSPITAL_NAME
ORDER BY f.ADMISSION_DATE DESC;


-- =========================================================
-- 7. Using EXPLAIN to Inspect Query Plan
-- =========================================================
EXPLAIN
SELECT 
    HOSPITAL_NAME,
    AVG(BILLING_AMOUNT) AS AVG_BILL
FROM FACT_ADMISSION
GROUP BY HOSPITAL_NAME;


-- =========================================================
-- 8. Optional: Running Total Billing Per Hospital
-- =========================================================
SELECT
    HOSPITAL_NAME,
    ADMISSION_DATE,
    BILLING_AMOUNT,
    SUM(BILLING_AMOUNT) OVER (
        PARTITION BY HOSPITAL_NAME
        ORDER BY ADMISSION_DATE
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS RUNNING_TOTAL_BILLING
FROM FACT_ADMISSION
ORDER BY HOSPITAL_NAME, ADMISSION_DATE;
