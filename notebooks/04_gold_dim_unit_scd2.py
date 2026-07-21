# Databricks notebook source
spark.sql("DROP TABLE IF EXISTS uber_pipeline.uber_raw.gold_dim_unit")

spark.sql("""
CREATE TABLE uber_pipeline.uber_raw.gold_dim_unit (
    sk_unit STRING,
    unit_code STRING,
    unit_name STRING,
    region STRING,
    cost_center_responsible STRING,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
) USING DELTA
""")

# COMMAND ----------

from pyspark.sql.functions import col, lit, sha2, concat_ws, date_sub
from delta.tables import DeltaTable

TRACKED_COLUMNS = ["unit_name", "region", "cost_center_responsible"]

def apply_scd2_snapshot(snapshot_date: str):
    source_df = (
        spark.table("uber_pipeline.uber_raw.bronze_dim_unit_snapshots")
        .filter(col("snapshot_date") == snapshot_date)
        .select("unit_code", "unit_name", "region", "cost_center_responsible")
    )

    current_df = spark.table("uber_pipeline.uber_raw.gold_dim_unit").filter(col("is_current"))

    joined = source_df.alias("src").join(current_df.alias("cur"), on="unit_code", how="left")

    is_new = col("cur.unit_code").isNull()
    is_changed = (
        (col("src.unit_name") != col("cur.unit_name")) |
        (col("src.region") != col("cur.region")) |
        (col("src.cost_center_responsible") != col("cur.cost_center_responsible"))
    )

    rows_to_version = joined.filter(is_new | is_changed)

    # 1. Expira a versão vigente das unidades que mudaram (não toca nas que são 100% novas)
    rows_to_expire = (
        rows_to_version.filter(~is_new)
        .select(col("cur.sk_unit").alias("sk_unit"))
    )

    if rows_to_expire.count() > 0:
        gold_table = DeltaTable.forName(spark, "uber_pipeline.uber_raw.gold_dim_unit")
        (
            gold_table.alias("target")
            .merge(rows_to_expire.alias("source"), "target.sk_unit = source.sk_unit")
            .whenMatchedUpdate(set={
                "valid_to": date_sub(lit(snapshot_date).cast("date"), 1),
                "is_current": lit(False),
            })
            .execute()
        )

    # 2. Insere a nova versão vigente (unidades novas + unidades que mudaram)
    new_versions = (
        rows_to_version
        .select(
            col("src.unit_code").alias("unit_code"),
            col("src.unit_name").alias("unit_name"),
            col("src.region").alias("region"),
            col("src.cost_center_responsible").alias("cost_center_responsible"),
            lit(snapshot_date).cast("date").alias("valid_from"),
            lit(None).cast("date").alias("valid_to"),
            lit(True).alias("is_current"),
        )
        .withColumn(
            "sk_unit",
            sha2(concat_ws("||", col("unit_code"), col("valid_from").cast("string")), 256)
        )
    )

    new_versions.write.format("delta").mode("append").saveAsTable("uber_pipeline.uber_raw.gold_dim_unit")


# Processa os snapshots em ordem cronológica - ordem importa, não é opcional
for snapshot_date in ["2026-04-01", "2026-05-01", "2026-05-31"]:
    apply_scd2_snapshot(snapshot_date)