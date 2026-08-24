"""
main.py — Orquestrador do Pipeline Fintech Fraud Detection Lakehouse
====================================================================
Executa o fluxo completo de ponta a ponta:
  1. Ingestão Bronze (Faker com Fraudes + Schema Enforcement)
  2. Curadoria Silver (LGPD Masking + Matriz de Risco + Detecção de Fraude)
  3. Modelagem Gold (Kimball Star Schema com Fato e 4 Dimensões)
"""

import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.bronze import ingest_bronze
from src.silver import transform_silver
from src.gold import load_gold


def create_spark_session() -> SparkSession:
    """Cria a SparkSession configurada para Delta Lake local."""
    builder = SparkSession.builder \
        .appName("FintechFraudLakehouse_Pipeline") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4")

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    return session


def run_pipeline():
    """Executa as 3 etapas da arquitetura medalhão em Delta Lake."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    storage_dir = os.path.join(base_dir, "storage")

    bronze_path = os.path.join(storage_dir, "bronze")
    silver_path = os.path.join(storage_dir, "silver")
    gold_path = os.path.join(storage_dir, "gold")

    print("\n" + "#" * 70)
    print("  🚀 INICIANDO PIPELINE: FINTECH FRAUD DETECTION LAKEHOUSE")
    print("#" * 70)

    spark = create_spark_session()

    try:
        # ETAPA 1: BRONZE
        total_bronze = ingest_bronze(spark, bronze_path, num_records=1000)

        # ETAPA 2: SILVER
        total_silver = transform_silver(spark, bronze_path, silver_path)

        # ETAPA 3: GOLD
        gold_metrics = load_gold(spark, silver_path, gold_path)

        print("\n" + "#" * 70)
        print("  🎉 PIPELINE EXECUTADO COM SUCESSO TOTAL!")
        print(f"  🥉 Bronze Ingerida: {total_bronze} transações")
        print(f"  🥈 Silver Curada:   {total_silver} transações")
        print(f"  🥇 Gold Fato:       {gold_metrics['fact_financial_transactions']} transações")
        print(f"  👥 Gold Clientes:   {gold_metrics['dim_customers']} registros")
        print(f"  🏪 Gold Lojas:      {gold_metrics['dim_merchants']} registros")
        print(f"  💳 Gold Canais:     {gold_metrics['dim_payment_channels']} registros")
        print(f"  🗺️ Gold Cidades:    {gold_metrics['dim_locations']} registros")
        print("#" * 70 + "\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
