# Day 5 — PySpark Optimization & Delta Lake  
### 15-Day Multi-Cloud Data Engineering Mastery Series

Today’s focus was on taking PySpark beyond the basics by learning how to optimize distributed workloads and how to implement Delta Lake for reliable, ACID-compliant data pipelines. This step is crucial for building production-grade data engineering solutions.

---

## 📌 1. Topics Learned

### 🔹 PySpark Optimization Concepts
- **Partitioning** for balanced task distribution  
- **Repartition vs Coalesce** and when to use each  
- **Caching / Persisting** to reuse DataFrames efficiently  
- **Skew handling** using repartitioning and salting techniques  
- Impact of shuffles and how to minimize them

### 🔹 Delta Lake Fundamentals
- **What makes Delta different from Parquet**  
- **ACID transactions** using the Delta transaction log  
- **Schema enforcement** to avoid bad data  
- **Schema evolution** to handle column changes safely  
- **Time travel** for versioned querying  
- MERGE operations for **upserts** and **slowly changing data**

---

## 📌 2. What I Implemented Today

- Cleaned and sanitized column names to make them Delta-safe  
- Repartitioned and cached DataFrames for better performance  
- Converted the healthcare dataset from Parquet → Delta Lake  
- Registered a Delta table in the metastore  
- Simulated new/updated data for admissions  
- Performed a **Delta MERGE** (upsert) operation  
- Checked Delta table history and versions

---

## ✨ Quote of the Day

> **"In data engineering, speed is good but consistency and correctness are everything."**
