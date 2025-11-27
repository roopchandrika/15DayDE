# 📘 **Day 4 — PySpark DataFrame Basics**

### *15-Day Multi-Cloud Data Engineering Mastery Series*

Today I focused on understanding **PySpark DataFrames**, the core abstraction used in distributed data processing. This included learning how Spark executes transformations, how to optimize joins, and how Spark’s architecture supports large-scale workloads. I worked hands-on with the healthcare dataset to clean, transform, and analyze data using PySpark.

---

## 📌 **1. Topics Learned**

### 🔹 Transformations vs Actions

* **Transformations** (`select`, `filter`, `withColumn`, `join`)

  * Lazy evaluated
  * Build an execution plan (DAG)
* **Actions** (`show`, `count`, `collect`, `write`)

  * Trigger the execution
  * Cause Spark to run the DAG

Understanding this distinction is essential for performance optimization and DAG debugging.

---

### 🔹 UDFs vs Built-In Functions

* UDFs allow custom logic but:

  * Are slower
  * Break Catalyst optimization
  * Should be avoided unless necessary
* Prefer built-in Spark functions (`lower`, `date_format`, `substring`, etc.)

---

### 🔹 Joins & Aggregations

* Types: inner, left, right, full, semi, anti
* Joins often cause a **shuffle**, which is expensive
* Aggregations (`groupBy`, `agg`) used for summarization
* Ensuring join keys have matching data types improves performance

---

### 🔹 Spark Architecture

* **Driver:** orchestrates computation, creates the DAG
* **Executors:** run tasks, store cached data
* **Cluster Manager:** manages resources (Databricks, YARN, K8s)
* Distributed processing enables high parallelism and scalability

---

## 📌 **2. What I Implemented Today**

* Initiated a SparkSession inside Databricks
* Read the healthcare dataset into a DataFrame
* Cleaned null values and standardized date fields
* Created new derived columns (year, month, etc.)
* Performed DataFrame joins using patient data
* Executed aggregations such as total billing per hospital
* Wrote cleaned data back in Parquet format (optional step)

---

## 📌 **3. Interview Focus Topics**

### 🧩 How to Optimize PySpark Joins

* Repartition on join keys
* Avoid data skew
* Ensure matching data types
* Use **broadcast joins** for small dimension tables
* Reduce shuffle by optimizing partition sizes
* Cache DataFrames reused multiple times

### 🧩 Broadcast Join Explanation

A broadcast join distributes the entire small table to each executor.

* Removes shuffle
* Fastest join pattern when one table fits in memory
* Used widely for dimension lookup tables

---

## ✨ **Quote of the Day**

> **“In distributed systems, small optimizations in logic create huge optimizations in performance.”**

