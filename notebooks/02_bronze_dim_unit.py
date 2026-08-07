# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import StructType, StructField, StringType, DateType

spark.sql("USE CATALOG uber_pipeline")

dim_unit_schema = StructType([
    StructField("unit_code", StringType(), True),
    StructField("unit_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("cost_center", StringType(), True),
    StructField("snapshot_date", DateType(), True),
])

df_dim_unit_bronze = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(dim_unit_schema)
    .load("/Volumes/uber_pipeline/bronze/landing_dim_unit/")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

table_exists = spark.catalog.tableExists("bronze.dim_unit_snapshots")

if table_exists:
    already_loaded = spark.table("bronze.dim_unit_snapshots").select("unit_code", "snapshot_date")
    df_dim_unit_new = df_dim_unit_bronze.join(
        already_loaded,
        on=["unit_code", "snapshot_date"],
        how="left_anti",
    )
else:
    df_dim_unit_new = df_dim_unit_bronze

(
    df_dim_unit_new.write
    .format("delta")
    .mode("append")
    .saveAsTable("bronze.dim_unit_snapshots")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unit_code, cost_center, snapshot_date
# MAGIC FROM bronze.dim_unit_snapshots
# MAGIC WHERE unit_code = '01001'
# MAGIC ORDER BY snapshot_date
