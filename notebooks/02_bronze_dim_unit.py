# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS UBER_PIPELINE.UBER_RAW.LANDING_DIM_UNIT;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DateType

dim_unit_schema = StructType([
    StructField("unit_code", StringType(), True),
    StructField("unit_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("cost_center_responsible", StringType(), True),
    StructField("snapshot_date", DateType(), True),
])

df_dim_unit_bronze = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(dim_unit_schema)
    .load("/Volumes/uber_pipeline/uber_raw/landing_dim_unit/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df_dim_unit_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("uber_pipeline.uber_raw.bronze_dim_unit_snapshots")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unit_code, cost_center_responsible, snapshot_date
# MAGIC FROM uber_pipeline.uber_raw.bronze_dim_unit_snapshots
# MAGIC WHERE unit_code = '01001'
# MAGIC ORDER BY snapshot_date