# Databricks notebook source
# MAGIC %md
# MAGIC ## 04_gold_dim_unit_scd2
# MAGIC SCD Type 2 incremental para gold.dim_unit.
# MAGIC DDL da tabela e responsabilidade exclusiva do 00_architecture.py — este notebook so faz MERGE/INSERT.
# MAGIC Idempotencia via control.processed_snapshots (ler a mesma snapshot_date duas vezes = no-op),
# MAGIC com content_hash como segunda camada de guarda contra reprocessamento acidental.

# COMMAND ----------

spark.sql("USE CATALOG uber_pipeline")

# COMMAND ----------

from pyspark.sql.functions import col, lit, sha2, concat_ws, date_sub
from delta.tables import DeltaTable

SOURCE_TABLE = "silver.dim_unit"
BUSINESS_COLUMNS = ["unit_name", "region", "cost_center"]  # atributos versionados pelo SCD2

# COMMAND ----------

# Descobre quais snapshot_date ainda nao foram processados por este pipeline.
# left_anti contra control.processed_snapshots: se nao ha nada novo, encerra o notebook aqui,
# sem tocar em gold.dim_unit.
snapshot_dates_bronze = (
    spark.table(SOURCE_TABLE)
    .select("snapshot_date")
    .distinct()
)

already_processed = (
    spark.table("control.processed_snapshots")
    .filter(col("source_table") == SOURCE_TABLE)
    .select("snapshot_date")
)

pending_snapshots = (
    snapshot_dates_bronze
    .join(already_processed, on="snapshot_date", how="left_anti")
    .orderBy("snapshot_date")  # ordem cronologica importa, nao e opcional
    .collect()
)

if not pending_snapshots:
    dbutils.notebook.exit("Nenhum snapshot novo em bronze.dim_unit_snapshots. Nada a processar.")

pending_dates = [row["snapshot_date"] for row in pending_snapshots]
print(f"Snapshots pendentes: {pending_dates}")

# COMMAND ----------

def apply_scd2_snapshot(snapshot_date):
    source_df = (
        spark.table(SOURCE_TABLE)
        .filter(col("snapshot_date") == snapshot_date)
        .select("unit_code", *BUSINESS_COLUMNS)
        .withColumn("content_hash", sha2(concat_ws("||", *BUSINESS_COLUMNS), 256))
    )

    current_df = (
        spark.table("gold.dim_unit")
        .filter(col("is_current"))
        .select(
            col("sk_unit").alias("cur_sk_unit"),
            col("unit_code").alias("cur_unit_code"),
            col("content_hash").alias("cur_content_hash"),
        )
    )

    # Nomes de coluna totalmente distintos dos dois lados do join: nenhuma coluna
    # compartilha nome entre source_df e current_df, entao nao ha fusao/deduplicacao
    # de coluna pelo join nem dependencia de qualificacao por alias (col("x.y")).
    joined = source_df.join(
        current_df,
        on=source_df["unit_code"] == current_df["cur_unit_code"],
        how="left",
    )

    is_new = col("cur_unit_code").isNull()
    # guarda contra reprocessamento acidental do mesmo snapshot: so versiona se o
    # conteudo de negocio realmente mudou, nao a cada execucao
    is_changed = (~is_new) & (col("content_hash") != col("cur_content_hash"))

    rows_to_version = joined.filter(is_new | is_changed)

    if rows_to_version.limit(1).count() == 0:
        print(f"{snapshot_date}: nenhuma unidade nova ou alterada.")
        return

    # 1. Expira a versao vigente das unidades que mudaram (nao toca nas que sao 100% novas)
    rows_to_expire = (
        rows_to_version.filter(is_changed)
        .select(col("cur_sk_unit").alias("sk_unit"))
    )

    if rows_to_expire.limit(1).count() > 0:
        gold_table = DeltaTable.forName(spark, "gold.dim_unit")
        (
            gold_table.alias("target")
            .merge(rows_to_expire.alias("source"), "target.sk_unit = source.sk_unit")
            .whenMatchedUpdate(set={
                "valid_to": date_sub(lit(snapshot_date).cast("date"), 1),
                "is_current": lit(False),
            })
            .execute()
        )

    # 2. Insere a nova versao vigente (unidades novas + unidades que mudaram).
    # sk_unit NAO e informado: gold.dim_unit tem sk_unit BIGINT GENERATED ALWAYS AS IDENTITY,
    # o Delta gera o valor sozinho no append.
    new_versions = (
        rows_to_version
        .select(
            col("unit_code"),
            col(BUSINESS_COLUMNS[0]),
            col(BUSINESS_COLUMNS[1]),
            col(BUSINESS_COLUMNS[2]),
            col("content_hash"),
            lit(snapshot_date).cast("date").alias("valid_from"),
            lit(None).cast("date").alias("valid_to"),
            lit(True).alias("is_current"),
        )
    )

    new_versions.write.format("delta").mode("append").saveAsTable("gold.dim_unit")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, DateType, StringType, TimestampType
import datetime

CONTROL_SCHEMA = StructType([
    StructField("snapshot_date", DateType(), False),
    StructField("source_table", StringType(), False),
    StructField("processed_at", TimestampType(), False),
])

for snapshot_date in pending_dates:
    apply_scd2_snapshot(snapshot_date)

    # Registra o snapshot como processado somente depois do MERGE/INSERT ter sucesso.
    # Se a celula quebrar no meio do apply_scd2_snapshot, este snapshot NAO entra aqui
    # e sera reprocessado na proxima execucao — e exatamente o comportamento desejado.
    control_row = spark.createDataFrame(
        [Row(snapshot_date=snapshot_date, source_table=SOURCE_TABLE, processed_at=datetime.datetime.now())],
        schema=CONTROL_SCHEMA,
    )
    control_row.write.format("delta").mode("append").saveAsTable("control.processed_snapshots")

print(f"Processados: {pending_dates}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT unit_code, COUNT(*) as versoes FROM gold.dim_unit GROUP BY unit_code ORDER BY unit_code