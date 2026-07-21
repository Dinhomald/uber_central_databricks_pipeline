# Databricks notebook source
from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window

df_bronze = spark.table("uber_pipeline.uber_raw.bronze_daily_trips")

window_dedup = Window.partitionBy("trip_uuid").orderBy(col("_ingestion_timestamp").asc())

df_silver = (
    df_bronze
    .withColumn("_dedup_rank", row_number().over(window_dedup))
    .filter(col("_dedup_rank") == 1)
    .drop("_dedup_rank")
    .withColumn("event_date", to_date(col("pickup_datetime")))
)

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .saveAsTable("uber_pipeline.uber_raw.silver_daily_trips")
)