"""
gold.py — Modelagem Dimensional Analítica (Star Schema Kimball na Camada Gold)
=============================================================================
Responsabilidade:
  1. Criação das Tabelas de Dimensão:
     - dim_customers (Informações dos clientes com CPF mascarado LGPD)
     - dim_merchants (Estabelecimentos comerciais e categorias de risco)
     - dim_payment_channels (Canais de pagamento: PIX, Cartão, TED, etc.)
     - dim_locations (Cidades, Estados e Macrorregiões do Brasil)
  2. Criação da Tabela Fato:
     - fact_financial_transactions (Métricas de valor, risk_score, flags e perdas evitadas)
  3. Particionamento otimizado da Fato em Delta Lake por year_month (Partition Pruning).
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    date_format,
    to_date,
    when,
    round as spark_round,
    current_timestamp
)


def load_gold(spark: SparkSession, silver_path: str, gold_base_path: str) -> dict:
    """
    Executa a modelagem dimensional Star Schema na Camada Gold.
    Lê os dados curados da Silver, extrai dimensões únicas e popula
    a Tabela Fato com particionamento Delta Lake por year_month.
    """
    print("=" * 60)
    print("  🥇 MODELAGEM GOLD (STAR SCHEMA) — Fintech Fraud Lakehouse")
    print(f"  📖 Lendo dados curados da Silver: {silver_path}")
    print("=" * 60)

    # 1. Leitura da Camada Silver
    df_silver = spark.read.format("delta").load(silver_path)

    # -----------------------------------------------------------------------
    # 2. CRIAÇÃO DAS TABELAS DE DIMENSÃO (Contexto de Negócio)
    # -----------------------------------------------------------------------

    # A. Dimensão Clientes (dim_customers)
    df_dim_customers = df_silver.select(
        col("customer_id"),
        col("customer_name"),
        col("masked_cpf")
    ).dropDuplicates(["customer_id"])

    path_dim_customers = os.path.join(gold_base_path, "dim_customers")
    df_dim_customers.write.format("delta").mode("overwrite").save(path_dim_customers)

    # B. Dimensão Estabelecimentos (dim_merchants)
    df_dim_merchants = df_silver.select(
        col("merchant_name"),
        col("merchant_category")
    ).dropDuplicates(["merchant_name"])

    path_dim_merchants = os.path.join(gold_base_path, "dim_merchants")
    df_dim_merchants.write.format("delta").mode("overwrite").save(path_dim_merchants)

    # C. Dimensão Canais de Pagamento (dim_payment_channels)
    df_dim_channels = df_silver.select(
        col("payment_channel")
    ).dropDuplicates(["payment_channel"])

    path_dim_channels = os.path.join(gold_base_path, "dim_payment_channels")
    df_dim_channels.write.format("delta").mode("overwrite").save(path_dim_channels)

    # D. Dimensão Localidades & Regiões (dim_locations)
    # Mapeamento de Macrorregiões Brasileiras
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

    path_dim_locations = os.path.join(gold_base_path, "dim_locations")
    df_dim_locations.write.format("delta").mode("overwrite").save(path_dim_locations)

    # -----------------------------------------------------------------------
    # 3. CRIAÇÃO DA TABELA FATO CENTRAL (fact_financial_transactions)
    # -----------------------------------------------------------------------
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
        # Flags numéricas para agregação veloz no Databricks SQL / BI
        when(col("fraud_decision") == "BLOQUEADA_SUSPEITA_FRAUDE", lit(1)).otherwise(lit(0)).alias("is_blocked_fraud"),
        when(col("fraud_decision") == "ANALISE_MANUAL_MESA", lit(1)).otherwise(lit(0)).alias("is_manual_review"),
        when(col("fraud_decision") == "APROVADA", lit(1)).otherwise(lit(0)).alias("is_approved"),
        # Métrica de Prejuízo Evitado (Loss Prevention)
        when(col("fraud_decision") == "BLOQUEADA_SUSPEITA_FRAUDE", col("transaction_amount")).otherwise(lit(0.0)).alias("loss_prevented_amount"),
        to_date(col("transaction_timestamp")).alias("transaction_date"),
        date_format(col("transaction_timestamp"), "yyyy-MM").alias("year_month"),
        col("transaction_timestamp")
    )

    path_fact = os.path.join(gold_base_path, "fact_financial_transactions")
    df_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year_month") \
        .save(path_fact)

    counts = {
        "fact_financial_transactions": df_fact.count(),
        "dim_customers": df_dim_customers.count(),
        "dim_merchants": df_dim_merchants.count(),
        "dim_payment_channels": df_dim_channels.count(),
        "dim_locations": df_dim_locations.count()
    }

    print("=" * 60)
    print("  ✅ Modelagem Gold Star Schema concluída com sucesso!")
    print(f"  📁 Diretório Base: {gold_base_path}")
    print(f"  📊 Tabela Fato: {counts['fact_financial_transactions']} transações")
    print(f"  👥 Dimensão Clientes: {counts['dim_customers']} registros")
    print(f"  🏪 Dimensão Estabelecimentos: {counts['dim_merchants']} registros")
    print(f"  💳 Dimensão Canais: {counts['dim_payment_channels']} registros")
    print(f"  🗺️ Dimensão Localidades: {counts['dim_locations']} registros")
    print("=" * 60)

    return counts
