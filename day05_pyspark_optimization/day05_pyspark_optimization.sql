# ============================================================
# Day 5 - PySpark Optimization & Delta Lake
# 15-Day Multi-Cloud Data Engineering Mastery
# Includes: Column Cleaning, Partitioning, Caching, Delta MERGE
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, rand, lit,
    sum as spark_sum, broadcast
)
import re

# ============================================================
# 1. Spark Session Initialization
# ============================================================
spark = SparkSession.builder \
    .appName("Healthcare_PySpark_Day5") \
    .getOrCreate()


# ============================================================
# 2. Read Healthcare Dataset
# ============================================================
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/FileStore/tables/healthcare_raw.csv")

df.printSchema()
df.show(5)


# ============================================================
# 3. Sanitize Column Names (CRITICAL for Delta!)
# ============================================================
def sanitize(col_name):
    """
    Replace any character that is NOT a-z, A-Z, 0-9, or underscore
    with an underscore.
    Ideal for Delta, Parquet, and clean Spark pipelines.
    """
    return re.sub('[^a-zA-Z0-9_]', '_', col_name)

for col_name in df.columns:
    df = df.withColumnRenamed(col_name, sanitize(col_name))

print("Cleaned Columns:", df.columns)


# ============================================================
# 4. Standardize Date Fields
# ============================================================
df_clean = df.withColumn(
    "Date_of_Admission",
    to_date(col("Date_of_Admission"), "yyyy-MM-dd")
).withColumn(
    "Discharge_Date",
    to_date(col("Discharge_Date"), "yyyy-MM-dd")
)


# ============================================================
# 5. Feature Engineering (Year, Month)
# ============================================================
df_clean = df_clean.withColumn("Admission_Year", year("Date_of_Admission")) \
                   .withColumn("Admission_Month", month("Date_of_Admission"))


# ============================================================
# 6. Partition & Cache (Performance Optimization)
# ============================================================
df_partitioned = df_clean.repartition(8, "Hospital")
df_partitioned.cache()
df_partitioned.count()   # triggers caching


# ============================================================
# 7. Write Data as Delta Table
# ============================================================
delta_path = "/mnt/healthcare_delta/day5_admissions"

df_partitioned.write.format("delta") \
    .mode("overwrite") \
    .save(delta_path)


# Register Delta Table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS healthcare_admissions_delta
    USING DELTA
    LOCATION '{delta_path}'
""")

print("Delta Table Created at:", delta_path)


# ============================================================
# 8. Create Updates DataFrame (Simulated New Admissions)
# ============================================================
updates_data = [
    ("John Doe", "City Hospital", 5000.0),
    ("Jane Smith", "General Hospital", 7500.0)
]

update_columns = ["Name", "Hospital", "Billing_Amount"]

df_updates = spark.createDataFrame(updates_data, update_columns)
df_updates.show()


# ============================================================
# 9. Delta MERGE (Upsert)
# ============================================================
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, delta_path)

(
    delta_table.alias("t")
    .merge(
        df_updates.alias("s"),
        "t.Name = s.Name AND t.Hospital = s.Hospital"
    )
    .whenMatchedUpdate(set={
        "Billing_Amount": "s.Billing_Amount"
    })
    .whenNotMatchedInsert(values={
        "Name": "s.Name",
        "Hospital": "s.Hospital",
        "Billing_Amount": "s.Billing_Amount"
    })
    .execute()
)


# ============================================================
# 10. Verify MERGE Result + Table History
# ============================================================
spark.read.format("delta").load(delta_path).show()

# If in Databricks, show Delta version history:
spark.sql("DESCRIBE HISTORY healthcare_admissions_delta").show()


# ============================================================
# End of Day 5 Script
# ============================================================
