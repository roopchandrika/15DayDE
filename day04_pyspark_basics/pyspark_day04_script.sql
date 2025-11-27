# ============================================================
# Day 4 - PySpark DataFrame Basics
# 15-Day Multi-Cloud Data Engineering Mastery
# Healthcare Dataset Transformation
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, month, sum as spark_sum, broadcast
)

# ============================================================
# 1. Spark Session Initialization
# ============================================================
spark = SparkSession.builder \
    .appName("Healthcare_PySpark_Day4") \
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
# 3. Clean Null Values
# ============================================================
df_clean = df.fillna({
    "Medical Condition": "Unknown",
    "Doctor": "Not Assigned",
    "Billing Amount": 0
})


# ============================================================
# 4. Standardize Date Formats
# ============================================================
df_clean = df_clean.withColumn(
    "Admission_Date",
    to_date(col("Date of Admission"), "yyyy-MM-dd")
).withColumn(
    "Discharge_Date",
    to_date(col("Discharge Date"), "yyyy-MM-dd")
)


# ============================================================
# 5. Feature Engineering (Year, Month)
# ============================================================
df_clean = df_clean.withColumn("Admission_Year", year("Admission_Date")) \
                   .withColumn("Admission_Month", month("Admission_Date"))


# ============================================================
# 6. Create Patient Dimension (Unique Patient Info)
# ============================================================
dim_patient = df_clean.select(
    col("Name").alias("Patient_Name"),
    "Age",
    "Gender"
).distinct()


# ============================================================
# 7. Join Operation Example
# ============================================================
joined_df = df_clean.join(
    broadcast(dim_patient),
    df_clean["Name"] == dim_patient["Patient_Name"],
    "inner"
)

joined_df.show(5)


# ============================================================
# 8. Aggregation Example (Billing by Hospital)
# ============================================================
df_billing = df_clean.groupBy("Hospital") \
    .agg(
        spark_sum("Billing Amount").alias("Total_Billing")
    )

df_billing.show()


# ============================================================
# 9. (Optional) Write Cleaned Data to Parquet
# ============================================================
df_clean.write \
    .mode("overwrite") \
    .parquet("/mnt/healthcare_clean/day4_clean_data")


# ============================================================
# End of Day 4 Script
# ============================================================
