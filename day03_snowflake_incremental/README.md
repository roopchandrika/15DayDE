# Day 3 — Data Modeling + Snowflake Setup  
### 15-Day Multi-Cloud Data Engineering Mastery Series

Today I focused on **Snowflake-specific data modeling and incremental loading**, moving from simple table creation to patterns that look much closer to real-world production pipelines.

The main goals were:
- Understand how Snowflake stores and optimizes data (micro-partitions)
- Design and prepare for Slowly Changing Dimensions (SCD Type 2)
- Implement an **incremental load** for healthcare admissions using a **watermark table**

---

## 📌 1. Topics Learned

### 🔹 Schema Design in Analytics Systems
- Difference between **Star Schema** and **Snowflake Schema**
- When to use **denormalized dimensions** for reporting
- Using **documentation-level constraints** (PK, FK) in Snowflake to clarify relationships

### 🔹 SCD Type 2 (Slowly Changing Dimensions)
- Concept of tracking historical changes in dimension tables
- Using:
  - Surrogate keys
  - `EFFECTIVE_START_DATE`
  - `EFFECTIVE_END_DATE`
  - `IS_CURRENT` flags
- Why SCD2 is critical when business wants **“as of date”** reporting

### 🔹 Snowflake Architecture & Objects
- **Database** → e.g., `HEALTHCARE_DEMO_DB`
- **Schema** → e.g., `IOT_HEALTHCARE`
- **Warehouse** → compute engine for queries and loads
- **Tables & Stages** → data storage and file ingestion
- Role of context commands:
  - `USE ROLE`, `USE WAREHOUSE`, `USE DATABASE`, `USE SCHEMA`

### 🔹 Micro-Partitions & Performance
- How Snowflake automatically stores data in **micro-partitions**
- Each micro-partition contains:
  - A subset of table rows
  - Column-level stats (min, max, etc.)
- Query performance benefits:
  - **Partition pruning** → skipping micro-partitions that don’t match filter criteria
  - Less data scanned → lower cost + faster queries
- Why good filter columns (like `ADMISSION_DATE`) matter for analytics

### 🔹 Incremental Load with LOAD_PROCESS_WATERMARK
- Avoiding full reloads on each pipeline run
- Creating a `LOAD_PROCESS_WATERMARK` table with:
  - `SOURCE_SYSTEM`
  - `TABLE_NAME`
  - `LAST_SUCCESSFUL_WATERMARK`
- Incremental pattern:
  1. Read the last watermark
  2. Load only rows newer than the watermark
  3. Insert into target fact table
  4. Update the watermark to the max processed timestamp

---

## 📌 2. What I Implemented Today

- Validated and used Snowflake context (role, warehouse, DB, schema)
- Created a **`LOAD_PROCESS_WATERMARK`** control table
- Seeded initial watermark value for the healthcare pipeline
- Created an incremental target table: `FACT_ADMISSION_INC`
- Implemented **incremental load** from `HEALTHCARE_RAW` → `FACT_ADMISSION_INC` using `Date of Admission` as the watermark column
- Updated the watermark after successful load
- Verified that:
  - First run loads all data
  - Subsequent runs only capture new rows

All SQL code for today is stored in:
📂 `/day03_snowflake_incremental/day03_snowflake_incremental.sql`

---

## 💡 Interview Focus

### 🧩 Micro-Partitions
I can now explain how Snowflake uses micro-partitions and metadata to:
- Skip irrelevant data blocks
- Speed up queries based on filters
- Optimize analytical workloads with minimal tuning

### 🧩 Incremental Load Design
I can confidently describe a **watermark-based incremental load**:
- Why it’s needed
- How the control table works
- How it scales better than full reloads
- How it fits into daily scheduled pipelines

---

## ✨ Quote of the Day

> **“A scalable pipeline isn’t built in one big load — it’s built one smart increment at a time.”**

---
