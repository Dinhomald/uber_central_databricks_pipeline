# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

spark.sql("USE CATALOG uber_pipeline")

bronze_schema_location = "/Volumes/uber_pipeline/bronze/landing/_autoloader_schema/daily_trips"
bronze_checkpoint_location = "/Volumes/uber_pipeline/bronze/landing/_checkpoint_v2/daily_trips"

df_bronze = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", bronze_schema_location)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "unit_code STRING")  # corrige só essa coluna, resto continua inferido
    .option("header", "true")
    .load("/Volumes/uber_pipeline/bronze/landing/raw")
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source_file", col("_metadata.file_path"))
)

(
    df_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint_location)
    .trigger(availableNow=True)
    .table("bronze.daily_trips")
)
