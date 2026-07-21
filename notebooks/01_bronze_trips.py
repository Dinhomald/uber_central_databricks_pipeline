# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS UBER_PIPELINE;
# MAGIC CREATE SCHEMA IF NOT EXISTS UBER_PIPELINE.UBER_RAW;
# MAGIC CREATE VOLUME IF NOT EXISTS UBER_PIPELINE.UBER_RAW.LANDING;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

bronze_schema_location = "/Volumes/uber_pipeline/uber_raw/landing/_autoloader_schema/daily_trips"
bronze_checkpoint_location = "/Volumes/uber_pipeline/uber_raw/landing/_checkpoint/daily_trips"

df_bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", bronze_schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "unit_code STRING")  # corrige só essa coluna, resto continua inferido
    .option("header", "true")
    .load("/Volumes/uber_pipeline/uber_raw/landing/raw")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint_location)
    .trigger(availableNow=True)
    .table("uber_pipeline.uber_raw.bronze_daily_trips")
)