# Databricks notebook source
# MAGIC %md
# MAGIC ## 00_architecture
# MAGIC DDL centralizado do catalogo uber_pipeline: catalog, schemas, volumes e tabelas.
# MAGIC Idempotente (IF NOT EXISTS em tudo) — seguro rodar quantas vezes quiser, sem efeito colateral.
# MAGIC Nenhum notebook de ETL (01 a 05) deve conter DDL de tabela/schema/volume: isso e responsabilidade
# MAGIC exclusiva deste notebook. Excecao deliberada: bronze.daily_trips e silver.daily_trips NAO tem
# MAGIC DDL fixo aqui, porque o schema e inferido da fonte (Auto Loader / CSV), nao e contrato de
# MAGIC engenharia — sao criados implicitamente pelo proprio notebook de ETL na primeira execucao.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS uber_pipeline;
# MAGIC CREATE SCHEMA IF NOT EXISTS uber_pipeline.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS uber_pipeline.bronze.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS uber_pipeline.bronze.landing_dim_unit;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS uber_pipeline.silver;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS uber_pipeline.silver.dim_unit (
# MAGIC     unit_code             STRING,
# MAGIC     unit_name             STRING,
# MAGIC     region                STRING,
# MAGIC     cost_center           STRING,
# MAGIC     snapshot_date         DATE,
# MAGIC     _ingestion_timestamp  TIMESTAMP
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS uber_pipeline.gold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS uber_pipeline.gold.dim_unit (
# MAGIC     sk_unit       BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     unit_code     STRING,
# MAGIC     unit_name     STRING,
# MAGIC     region        STRING,
# MAGIC     cost_center   STRING,
# MAGIC     content_hash  STRING,
# MAGIC     valid_from    DATE,
# MAGIC     valid_to      DATE,
# MAGIC     is_current    BOOLEAN
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS uber_pipeline.control;
# MAGIC CREATE VOLUME IF NOT EXISTS uber_pipeline.control.checkpoints;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS uber_pipeline.control.processed_snapshots (
# MAGIC     snapshot_date DATE,
# MAGIC     source_table  STRING,
# MAGIC     processed_at  TIMESTAMP
# MAGIC ) USING DELTA;