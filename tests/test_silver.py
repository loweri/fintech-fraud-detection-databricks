"""
test_silver.py — Suíte de Testes Unitários da Camada Silver
===========================================================
Padrão AAA (Arrange, Act, Assert) para validação de:
  1. Governança e Mascaramento LGPD (CPF e Cartão).
  2. Motor de Risco e Bloqueio de Fraude (Madrugada + Categoria Risco + Alto Valor).
  3. Aprovação de Transações Legítimas.
"""

import pytest
import os
import json
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from src.bronze import BRONZE_SCHEMA
from src.silver import transform_silver


@pytest.fixture(scope="session")
def spark():
    """
    Fixture SparkSession configurada com suporte a Delta Lake local.
    """
    builder = SparkSession.builder \
        .appName("FintechAntiFraud_UnitTests") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "2")

    spark_sess = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_sess.sparkContext.setLogLevel("ERROR")
    yield spark_sess
    spark_sess.stop()


def _create_bronze_delta(spark, tmp_path, folder_name: str, rows: list[dict]) -> str:
    """
    Grava dados de teste diretamente em arquivo JSON em disco usando I/O nativo do Python,
    e faz a leitura direta via JVM do Spark para evitar serialização via cloudpickle.
    """
    json_file = os.path.join(str(tmp_path), f"{folder_name}.json")
    with open(json_file, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")

    target_path = os.path.join(str(tmp_path), folder_name)
    df = spark.read.schema(BRONZE_SCHEMA).json(json_file)
    df.write.format("delta").mode("overwrite").partitionBy("payment_channel").save(target_path)
    return target_path


def test_silver_lgpd_data_masking(spark, tmp_path):
    """
    [AAA Pattern] Testa se o mascaramento LGPD é aplicado com sucesso:
      - CPF: 123.456.789-00 -> ***.456.789-**
      - Cartão: 4532891234569876 -> ****-****-****-9876
      - Colunas originais sensíveis são removidas do DataFrame.
    """
    # 1. ARRANGE (Preparação dos dados)
    silver_path = os.path.join(str(tmp_path), "silver_lgpd")
    raw_data = [{
        "transaction_id": "TX-TEST-001",
        "customer_id": "CUST-0001",
        "customer_name": "Ericles Oliveira",
        "customer_cpf": "123.456.789-00",
        "card_number": "4532891234569876",
        "transaction_amount": 150.0,
        "payment_channel": "CARTAO_CREDITO",
        "merchant_name": "Supermercado Exemplo",
        "merchant_category": "Supermercados & Alimentação",
        "transaction_city": "São Paulo",
        "transaction_state": "SP",
        "transaction_timestamp": "2026-08-10 14:30:00",
        "device_id": "DEV-NORMAL-01",
        "ingestion_timestamp": "2026-08-10 14:31:00"
    }]

    bronze_path = _create_bronze_delta(spark, tmp_path, "bronze_lgpd", raw_data)

    # 2. ACT (Execução da transformação)
    total_processed = transform_silver(spark, bronze_path, silver_path)

    # 3. ASSERT (Validação das regras de negócio)
    df_silver = spark.read.format("delta").load(silver_path)
    record = df_silver.filter(df_silver.transaction_id == "TX-TEST-001").collect()[0]

    assert total_processed == 1
    assert record["masked_cpf"] == "***.456.789-**"
    assert record["masked_card_number"] == "****-****-****-9876"
    assert "customer_cpf" not in df_silver.columns
    assert "card_number" not in df_silver.columns


def test_silver_fraud_blocking_logic(spark, tmp_path):
    """
    [AAA Pattern] Testa o motor anti-fraude:
      - Transação de Madrugada (02:00) + Criptoativos + R$ 6.500,00
      - Esperado: risk_score >= 65 e fraud_decision == 'BLOQUEADA_SUSPEITA_FRAUDE'
    """
    # 1. ARRANGE
    silver_path = os.path.join(str(tmp_path), "silver_fraud")
    fraud_data = [{
        "transaction_id": "TX-FRAUD-999",
        "customer_id": "CUST-0099",
        "customer_name": "Suspect Target",
        "customer_cpf": "999.888.777-66",
        "card_number": "5500123456789999",
        "transaction_amount": 6500.0,
        "payment_channel": "CARTAO_CREDITO",
        "merchant_name": "Crypto Exchange P2P",
        "merchant_category": "Criptoativos & Investimentos P2P",
        "transaction_city": "Manaus",
        "transaction_state": "AM",
        "transaction_timestamp": "2026-08-15 02:15:00",  # Madrugada
        "device_id": "DEV-SUSPECT-XYZ123",  # Dispositivo suspeito
        "ingestion_timestamp": "2026-08-15 02:16:00"
    }]

    bronze_path = _create_bronze_delta(spark, tmp_path, "bronze_fraud", fraud_data)

    # 2. ACT
    transform_silver(spark, bronze_path, silver_path)

    # 3. ASSERT
    df_silver = spark.read.format("delta").load(silver_path)
    fraud_record = df_silver.filter(df_silver.transaction_id == "TX-FRAUD-999").collect()[0]

    assert fraud_record["fraud_decision"] == "BLOQUEADA_SUSPEITA_FRAUDE"
    assert fraud_record["risk_score"] >= 65
    assert fraud_record["is_night_time"] == 1
    assert fraud_record["is_high_risk_category"] == 1
    assert fraud_record["is_high_amount"] == 1


def test_silver_legitimate_transaction_approval(spark, tmp_path):
    """
    [AAA Pattern] Testa transação padrão legítima:
      - Transação durante o dia (15:00) + Farmácia + R$ 85,00
      - Esperado: risk_score < 35 e fraud_decision == 'APROVADA'
    """
    # 1. ARRANGE
    silver_path = os.path.join(str(tmp_path), "silver_legit")
    legit_data = [{
        "transaction_id": "TX-LEGIT-100",
        "customer_id": "CUST-0050",
        "customer_name": "Cliente Legítimo",
        "customer_cpf": "111.222.333-44",
        "card_number": None,
        "transaction_amount": 85.0,
        "payment_channel": "PIX",
        "merchant_name": "Drogaria São Paulo",
        "merchant_category": "Farmácias & Saúde",
        "transaction_city": "São Paulo",
        "transaction_state": "SP",
        "transaction_timestamp": "2026-08-18 15:00:00",
        "device_id": "DEV-PRIMARY-55",
        "ingestion_timestamp": "2026-08-18 15:01:00"
    }]

    bronze_path = _create_bronze_delta(spark, tmp_path, "bronze_legit", legit_data)

    # 2. ACT
    transform_silver(spark, bronze_path, silver_path)

    # 3. ASSERT
    df_silver = spark.read.format("delta").load(silver_path)
    legit_record = df_silver.filter(df_silver.transaction_id == "TX-LEGIT-100").collect()[0]

    assert legit_record["fraud_decision"] == "APROVADA"
    assert legit_record["risk_score"] < 35
    assert legit_record["masked_card_number"] == "N/A_CANAL_DIRETO"
