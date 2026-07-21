# Databricks notebook source
spark.sql("USE CATALOG uber_pipeline")

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

df_trips = spark.table("silver.daily_trips")
df_dim_unit = spark.table("gold.dim_unit")

df_gold_fact = (
    df_trips.alias("f")
    .join(
        df_dim_unit.alias("d"),
        on=(
            (col("f.unit_code") == col("d.unit_code")) &
            (col("f.event_date") >= col("d.valid_from")) &
            (col("f.event_date") <= coalesce(col("d.valid_to"), lit("9999-12-31").cast("date")))
        ),
        how="left",
    )
    .select(
        col("f.trip_uuid"),
        col("f.event_date"),
        col("f.pickup_datetime"),
        col("f.dropoff_datetime"),
        col("f.employee_id"),
        col("f.unit_code"),
        col("f.fare_value"),
        col("f.distance_km"),
        col("d.sk_unit"),
        col("d.cost_center"),
        col("d.unit_name"),
        col("d.region"),
    )
)

(
    df_gold_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .saveAsTable("gold.fact_trips")
)