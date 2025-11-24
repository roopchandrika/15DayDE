🏥 Healthcare Data Engineering — Day 1
SQL Foundations + Dimensional Modeling + Snowflake Pipeline

This repository contains all my Day 1 work for the 15-Day Multi-Cloud Data Engineering Mastery Series.
Today's focus was on SQL fundamentals and data modeling using a healthcare dataset.

📌 1. Concepts Covered
🔹 SQL Fundamentals

Joins (Inner, Left, Right)

CTEs (Common Table Expressions)

Subqueries

Aggregations (SUM, AVG, COUNT)

Window Functions (ROW_NUMBER, RANK)

🔹 Dimensional Modeling

Differences between Fact & Dimension tables

Designing a Star Schema

Understanding the grain of a fact table

Building dimensions for:

Patient

Doctor

Hospital

Medication

Creating a Fact table for admissions

🔹 SCD Type 2 (Concept)

Used for maintaining history of changes in dimension tables.
Instead of overwriting older records, we keep:

effective_date

end_date

is_current flag

This creates a full audit trail of how an entity changes over time.

📂 2. Folder Structure
/sql/
    healthcare_dim_fact_creation.sql
    practice_queries.sql

README.md

🛠 3. Steps I Completed Today

Uploaded healthcare dataset into Snowflake stage

Created a RAW table (HEALTHCARE_RAW)

Designed Star Schema

Built all Dimensions (Patient, Doctor, Hospital, Medication)

Created Fact table (FACT_ADMISSION)

Wrote practice SQL covering joins, CTEs, subqueries, and aggregates

Tested data quality with exploratory queries

📊 4. Star Schema Overview
                 DIM_HOSPITAL
                        |
DIM_PATIENT —— FACT_ADMISSION —— DIM_DOCTOR
                        |
                 DIM_MEDICATION

🧪 5. What This Analysis Can Be Used For

Hospital occupancy forecasting

Patient condition tracking

Billing analytics & insurance analysis

Doctor workload analysis

Healthcare operational dashboards

Real-world BI projects (Power BI / Tableau)

Foundations for building an end-to-end ETL pipeline

🔗 6. GitHub Scripts Included

All SQL scripts are placed inside the /sql folder.

healthcare_dim_fact_creation.sql → Creates DIM + FACT tables

practice_queries.sql → All Day-1 practice queries

📖 6. SCD Type 2 — Creative Explanation

“If data were memories, SCD Type 2 would be the photo album that keeps every moment — not just the newest one.”

Every patient update becomes a new snapshot with start and end timestamps.
This ensures analytics can answer questions like:

"What was the patient’s insurance at the time of admission?"

"How did medical condition history evolve?"

🔥 7. Next Steps

Day 2 → Advanced SQL + Query Optimization + Windowing Functions
More pipelines, more modeling, more hands-on!
