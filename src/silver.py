"""
silver.py — Transformação, Limpeza & Detecção de Fraudes (Camada Silver)
========================================================================
Responsabilidade:
  1. Deduplicação por transaction_id e validação de consistência.
  2. Governança e LGPD: Mascaramento de dados sensíveis (CPF e Cartão).
  3. Feature Engineering de Risco: Cálculo do Score de Risco (0 a 100).
  4. Classificação de Decisão Anti-Fraude (BLOQUEADA, ANALISE_MANUAL, APROVADA).
  5. Gravação atômica em Delta Lake particionada por fraud_decision.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    substring,
    hour,
    when,
    round as spark_round,
    current_timestamp
)


def transform_silver(spark: SparkSession, bronze_path: str, silver_path: str) -> int:
    """
    Executa a transformação e engenharia de risco da Camada Silver.
    Lê os dados da Bronze, aplica mascaramento LGPD, calcula regras de fraude
    e persiste em Delta Lake particionado por fraud_decision.
    """
    print("=" * 60)
    print("  🔥 TRANSFORMAÇÃO SILVER — Fintech Fraud Detection Lakehouse")
    print(f"  📖 Lendo dados brutos da Bronze: {bronze_path}")
    print("=" * 60)

    # 1. Leitura da Camada Bronze em Delta Lake
    df_bronze = spark.read.format("delta").load(bronze_path)

    # 2. Deduplicação e Validação de Consistência
    df_dedup = df_bronze \
        .dropDuplicates(["transaction_id"]) \
        .filter(col("transaction_amount") > 0)

    # 3. Governança & LGPD: Mascaramento de Dados Pessoais (PII)
    # CPF mascarado: ***.456.789-**
    df_governance = df_dedup.withColumn(
        "masked_cpf",
        concat(lit("***."), substring(col("customer_cpf"), 5, 7), lit("-**"))
    )

    # Cartão mascarado: ****-****-****-9876
    df_governance = df_governance.withColumn(
        "masked_card_number",
        when(
            col("card_number").isNotNull(),
            concat(lit("****-****-****-"), substring(col("card_number"), -4, 4))
        ).otherwise(lit("N/A_CANAL_DIRETO"))
    )

    # 4. Feature Engineering de Detecção de Fraudes
    # Extrai hora da transação para detectar madrugada
    df_features = df_governance.withColumn(
        "tx_hour", hour(col("transaction_timestamp"))
    )

    # Indicador de horário de alto risco (Madrugada: 01:00 às 04:59)
    df_features = df_features.withColumn(
        "is_night_time",
        when(col("tx_hour").between(1, 4), lit(1)).otherwise(lit(0))
    )

    # Indicador de categoria comercial de alto risco
    df_features = df_features.withColumn(
        "is_high_risk_category",
        when(
            col("merchant_category").isin(
                "Apostas & Cassinos Online",
                "Criptoativos & Investimentos P2P"
            ),
            lit(1)
        ).otherwise(lit(0))
    )

    # Indicador de valor elevado (acima de R$ 3.000,00)
    df_features = df_features.withColumn(
        "is_high_amount",
        when(col("transaction_amount") >= 3000.0, lit(1)).otherwise(lit(0))
    )

    # Indicador de dispositivo suspeito / anômalo
    df_features = df_features.withColumn(
        "is_suspicious_device",
        when(col("device_id").startswith("DEV-SUSPECT"), lit(1)).otherwise(lit(0))
    )

    # 5. Cálculo da Matriz de Score de Risco (0 a 100 pontos)
    # Madrugada (+25) + Categoria Alto Risco (+35) + Valor Alto (+30) + Dispositivo Suspeito (+10)
    df_scored = df_features.withColumn(
        "risk_score",
        (col("is_night_time") * 25) +
        (col("is_high_risk_category") * 35) +
        (col("is_high_amount") * 30) +
        (col("is_suspicious_device") * 10)
    )

    # 6. Classificação da Decisão do Motor Anti-Fraude
    df_final = df_scored.withColumn(
        "fraud_decision",
        when(col("risk_score") >= 65, lit("BLOQUEADA_SUSPEITA_FRAUDE"))
        .when(col("risk_score") >= 35, lit("ANALISE_MANUAL_MESA"))
        .otherwise(lit("APROVADA"))
    ).withColumn(
        "silver_processed_timestamp", current_timestamp()
    )

    # 7. Descarte de colunas originais sensíveis para conformidade LGPD
    df_silver_clean = df_final.drop("customer_cpf", "card_number")

    # 8. Gravação atômica em Delta Lake particionada por fraud_decision
    df_silver_clean.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("fraud_decision") \
        .save(silver_path)

    total_curated = df_silver_clean.count()
    total_blocked = df_silver_clean.filter(col("fraud_decision") == "BLOQUEADA_SUSPEITA_FRAUDE").count()
    total_manual = df_silver_clean.filter(col("fraud_decision") == "ANALISE_MANUAL_MESA").count()
    total_approved = df_silver_clean.filter(col("fraud_decision") == "APROVADA").count()

    print("=" * 60)
    print(f"  ✅ Camada Silver processada com sucesso!")
    print(f"  📁 Destino: {silver_path}")
    print(f"  📊 Transações curadas: {total_curated}")
    print(f"  🚨 Transações Bloqueadas (Fraude): {total_blocked}")
    print(f"  ⚠️ Em Análise Manual (Mesa): {total_manual}")
    print(f"  ✅ Transações Aprovadas: {total_approved}")
    print("=" * 60)

    return total_curated
