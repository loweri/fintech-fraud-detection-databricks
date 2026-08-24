# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 🥈 Camada Silver — Governança LGPD & Motor de Detecção de Fraude
# MAGIC **Responsabilidade:**
# MAGIC 1. Deduplicação e validação de consistência.
# MAGIC 2. Mascaramento LGPD de CPF e Cartão de Crédito.
# MAGIC 3. Matriz de Score de Risco (0 a 100) e Classificação Anti-Fraude.
# MAGIC 4. Gravação atômica em Delta Lake particionada por `fraud_decision`.

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    lit,
    concat,
    substring,
    hour,
    when,
    current_timestamp
)
df_bronze = spark.table("bronze_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Leitura da Camada Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Deduplicação e Governança LGPD (Mascaramento de PII)

# COMMAND ----------

df_dedup = df_bronze \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_amount") > 0)

# Mascara CPF (***.456.789-**) e Cartão (****-****-****-9876)
df_governance = df_dedup.withColumn(
    "masked_cpf",
    concat(lit("***."), substring(col("customer_cpf"), 5, 7), lit("-**"))
).withColumn(
    "masked_card_number",
    when(
        col("card_number").isNotNull(),
        concat(lit("****-****-****-"), substring(col("card_number"), -4, 4))
    ).otherwise(lit("N/A_CANAL_DIRETO"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Feature Engineering & Motor de Score de Risco (0 a 100)

# COMMAND ----------

df_features = df_governance.withColumn(
    "tx_hour", hour(col("transaction_timestamp"))
).withColumn(
    "is_night_time",
    when(col("tx_hour").between(1, 4), lit(1)).otherwise(lit(0))
).withColumn(
    "is_high_risk_category",
    when(
        col("merchant_category").isin(
            "Apostas & Cassinos Online",
            "Criptoativos & Investimentos P2P"
        ),
        lit(1)
    ).otherwise(lit(0))
).withColumn(
    "is_high_amount",
    when(col("transaction_amount") >= 3000.0, lit(1)).otherwise(lit(0))
).withColumn(
    "is_suspicious_device",
    when(col("device_id").startswith("DEV-SUSPECT"), lit(1)).otherwise(lit(0))
)

# Cálculo da Matriz de Risco
df_scored = df_features.withColumn(
    "risk_score",
    (col("is_night_time") * 25) +
    (col("is_high_risk_category") * 35) +
    (col("is_high_amount") * 30) +
    (col("is_suspicious_device") * 10)
).withColumn(
    "fraud_decision",
    when(col("risk_score") >= 65, lit("BLOQUEADA_SUSPEITA_FRAUDE"))
    .when(col("risk_score") >= 35, lit("ANALISE_MANUAL_MESA"))
    .otherwise(lit("APROVADA"))
).withColumn(
    "silver_processed_timestamp", current_timestamp()
)

# Descarte de colunas sensíveis em conformidade com a LGPD
df_silver_clean = df_scored.drop("customer_cpf", "card_number")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Gravação atômica em Delta Lake particionado por `fraud_decision`

# COMMAND ----------

df_silver_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("fraud_decision") \
    .saveAsTable("silver_transactions")

total_curated = df_silver_clean.count()
print(f"✅ Camada Silver processada com sucesso no Databricks! Total: {total_curated} registros.")
display(df_silver_clean.groupBy("fraud_decision").count())