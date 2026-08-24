# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 🥇 Camada Gold — Modelagem Kimball Star Schema
# MAGIC **Responsabilidade:**
# MAGIC 1. Construção das 4 Dimensões analíticas: `dim_customers`, `dim_merchants`, `dim_payment_channels`, `dim_locations`.
# MAGIC 2. Construção da Tabela Fato Central: `fact_financial_transactions` particionada por `year_month`.
# MAGIC 3. Criação de flags analíticas (`is_blocked_fraud`, `is_approved`, `loss_prevented_amount`).

# COMMAND ----------

import os
from pyspark.sql.functions import (
    col,
    lit,
    date_format,
    to_date,
    when
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Leitura da Camada Silver

# COMMAND ----------

# DBTITLE 1,Leitura da Silver via Unity Catalog
# Leitura da camada Silver via tabela gerenciada do Unity Catalog
df_silver = spark.table("silver_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Modelagem das Tabelas de Dimensão

# COMMAND ----------

# DBTITLE 1,Dimensões usando Unity Catalog
# A. Dimensão Clientes
df_dim_customers = df_silver.select(
    col("customer_id"),
    col("customer_name"),
    col("masked_cpf")
).dropDuplicates(["customer_id"])

df_dim_customers.write.format("delta").mode("overwrite").saveAsTable("dim_customers")

# B. Dimensão Estabelecimentos
df_dim_merchants = df_silver.select(
    col("merchant_name"),
    col("merchant_category")
).dropDuplicates(["merchant_name"])

df_dim_merchants.write.format("delta").mode("overwrite").saveAsTable("dim_merchants")

# C. Dimensão Canais
df_dim_channels = df_silver.select(
    col("payment_channel")
).dropDuplicates(["payment_channel"])

df_dim_channels.write.format("delta").mode("overwrite").saveAsTable("dim_payment_channels")

# D. Dimensão Localidades e Macrorregiões
df_dim_locations = df_silver.select(
    col("transaction_city"),
    col("transaction_state")
).dropDuplicates(["transaction_city", "transaction_state"]).withColumn(
    "macro_region",
    when(col("transaction_state").isin("SP", "RJ", "MG", "ES"), lit("Sudeste"))
    .when(col("transaction_state").isin("PR", "RS", "SC"), lit("Sul"))
    .when(col("transaction_state").isin("BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"), lit("Nordeste"))
    .when(col("transaction_state").isin("DF", "GO", "MT", "MS"), lit("Centro-Oeste"))
    .otherwise(lit("Norte"))
)

df_dim_locations.write.format("delta").mode("overwrite").saveAsTable("dim_locations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Modelagem da Tabela Fato Central (`fact_financial_transactions`)

# COMMAND ----------

# DBTITLE 1,Tabela Fato usando Unity Catalog
df_fact = df_silver.select(
    col("transaction_id"),
    col("customer_id"),
    col("merchant_name"),
    col("payment_channel"),
    col("transaction_city"),
    col("transaction_state"),
    col("transaction_amount"),
    col("risk_score"),
    col("fraud_decision"),
    when(col("fraud_decision") == "BLOQUEADA_SUSPEITA_FRAUDE", lit(1)).otherwise(lit(0)).alias("is_blocked_fraud"),
    when(col("fraud_decision") == "ANALISE_MANUAL_MESA", lit(1)).otherwise(lit(0)).alias("is_manual_review"),
    when(col("fraud_decision") == "APROVADA", lit(1)).otherwise(lit(0)).alias("is_approved"),
    when(col("fraud_decision") == "BLOQUEADA_SUSPEITA_FRAUDE", col("transaction_amount")).otherwise(lit(0.0)).alias("loss_prevented_amount"),
    to_date(col("transaction_timestamp")).alias("transaction_date"),
    date_format(col("transaction_timestamp"), "yyyy-MM").alias("year_month"),
    col("transaction_timestamp")
)

df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("year_month") \
    .saveAsTable("fact_financial_transactions")

print(f"✅ Camada Gold Star Schema gerada no Databricks com sucesso!")
display(df_fact.limit(10))