# 📘 **Day 2 — Advanced SQL & Query Optimization**

### *15-Day Multi-Cloud Data Engineering Mastery Series*

Today focused on deepening my SQL fundamentals with analytics-driven SQL patterns and performance tuning techniques. I used the same healthcare admissions dataset from Day 1 to perform analytical queries, ranking, and optimization.

---

## 📌 **1. Topics Learned**

### 🔹 Advanced SQL (Analytical Functions)

* Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`)
* Frame clauses (`ROWS BETWEEN …`)
* Time-series analytics (rolling averages, trend analysis)
* Running totals and partitioned analytics

### 🔹 Query Optimization Concepts

* Using `EXPLAIN` to understand execution plans
* Predicate pushdown
* Avoiding unnecessary subqueries
* Understanding Snowflake’s micro-partition pruning
* Efficient filtering and JOIN strategies

### 🔹 OLTP vs OLAP Workloads

* OLTP → transactional, row-level, real-time writes
* OLAP → analytical, aggregated reads, dashboards
* Why Snowflake is optimized for OLAP
* Common query patterns for both systems

---

## 📌 **2. What I Implemented Today**

* Calculated rolling averages for patient billing
* Ranked highest-cost admissions
* Found second-highest billing per hospital (interview classic)
* Performed hospital-level cost analytics
* Used `EXPLAIN` to understand Snowflake query behavior
* Compared OLTP-style vs OLAP-style queries
* Documented all query scripts in a separate SQL file

---

## 💡 **Quote of the Day**

**“Analytical SQL turns data into patterns; optimization turns patterns into performance.”**
