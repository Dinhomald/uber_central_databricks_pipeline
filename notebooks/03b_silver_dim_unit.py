# Databricks notebook source
# MAGIC %md
# MAGIC ## 03b_silver_dim_unit
# MAGIC Camada de limpeza da dimensao de unidades, para nao pular direto de Bronze pra Gold.
# MAGIC Valida nulos nos campos obrigatorios (descarta e loga, nao falha o notebook), dedup por
# MAGIC (unit_code, snapshot_date) mantendo a ingestao mais recente. Escrita: overwrite completo —
# MAGIC decisao deliberada, nao preguica: dataset de referencia pequeno (~15 linhas hoje); se crescer
# MAGIC muito, revisitar para incremental.

# COMMAND ----------

spark.sql("USE CATALOG uber_pipeline")

# COMMAND ----------

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

REQUIRED_COLUMNS = ["unit_code", "unit_name", "region", "cost_center", "snapshot_date"]

# COMMAND ----------

df_bronze_dim_unit = spark.table("bronze.dim_unit_snapshots")

null_filter = None
for column_name in REQUIRED_COLUMNS:
    condition = col(column_name).isNull()
    null_filter = condition if null_filter is None else (null_filter | condition)

df_invalid = df_bronze_dim_unit.filter(null_filter)
invalid_count = df_invalid.count()
if invalid_count > 0:
    print(f"Descartadas {invalid_count} linha(s) com campo obrigatorio nulo.")

df_valid = df_bronze_dim_unit.filter(~null_filter)

# COMMAND ----------

# Dedup por (unit_code, snapshot_date): mantem so a ingestao mais recente, caso o mesmo
# snapshot tenha sido carregado mais de uma vez na Bronze.
window_dedup = Window.partitionBy("unit_code", "snapshot_date").orderBy(col("_ingestion_timestamp").desc())

df_silver_dim_unit = (
    df_valid
    .withColumn("_dedup_rank", row_number().over(window_dedup))
    .filter(col("_dedup_rank") == 1)
    .drop("_dedup_rank")
    .select(*REQUIRED_COLUMNS, "_ingestion_timestamp")
)

# COMMAND ----------

(
    df_silver_dim_unit.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.dim_unit")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unit_code, cost_center, snapshot_date
# MAGIC FROM silver.dim_unit
# MAGIC ORDER BY unit_code, snapshot_date
