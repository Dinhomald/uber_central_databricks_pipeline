# Databricks notebook source
# MAGIC %md
# MAGIC ## 03_silver_trips
# MAGIC Incremental via Structured Streaming (readStream + foreachBatch), trigger availableNow.
# MAGIC Dedup por trip_uuid dentro de cada microbatch (mantem a ingestao mais recente).
# MAGIC MERGE contra silver.daily_trips com whenNotMatchedInsertAll — silver e insert-only,
# MAGIC nunca atualiza uma trip ja existente, so acrescenta as que ainda nao foram vistas.

# COMMAND ----------

spark.sql("USE CATALOG uber_pipeline")

# COMMAND ----------

from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window
from delta.tables import DeltaTable

CHECKPOINT_LOCATION = "/Volumes/uber_pipeline/control/checkpoints/silver_daily_trips"

# COMMAND ----------

def upsert_to_silver(microbatch_df, batch_id):
    # Dedup dentro do microbatch: o mesmo trip_uuid pode aparecer mais de uma vez no
    # mesmo lote (arquivo de origem com linha duplicada, ou dois arquivos processados
    # no mesmo microbatch). Mantem so a ingestao mais recente por trip_uuid.
    window_dedup = Window.partitionBy("trip_uuid").orderBy(col("_ingestion_timestamp").desc())

    deduped_df = (
        microbatch_df
        .withColumn("_dedup_rank", row_number().over(window_dedup))
        .filter(col("_dedup_rank") == 1)
        .drop("_dedup_rank")
        # corte de dia (23:45-23:59): event_date deriva do pickup_datetime real da corrida,
        # nao do nome/data do arquivo de origem — resolve a anomalia de fronteira de dia
        # sem precisar de logica adicional.
        .withColumn("event_date", to_date(col("pickup_datetime")))
    )

    if spark.catalog.tableExists("silver.daily_trips"):
        silver_table = DeltaTable.forName(spark, "silver.daily_trips")
        (
            silver_table.alias("target")
            .merge(deduped_df.alias("source"), "target.trip_uuid = source.trip_uuid")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # Primeira execucao: schema de silver.daily_trips herda o schema inferido de
        # bronze.daily_trips (mesma logica do Auto Loader na Bronze — a fonte e dona
        # do contrato, nao o notebook). DDL fixo nao entra no 00_architecture por isso.
        (
            deduped_df.write
            .format("delta")
            .mode("append")
            .partitionBy("event_date")
            .saveAsTable("silver.daily_trips")
        )

# COMMAND ----------

df_bronze_stream = spark.readStream.table("bronze.daily_trips")

(
    df_bronze_stream.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM uber_pipeline.gold.fact_trips;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM gold.fact_trips WHERE sk_unit IS NULL;