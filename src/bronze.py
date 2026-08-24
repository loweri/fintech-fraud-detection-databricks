"""
bronze.py — Ingestão de Transações Financeiras (Fintech Anti-Fraud)
==================================================================
Responsabilidade: Gerar e ingerir dados brutos de transações financeiras
multicanais (PIX, Cartão, TED, Boleto) com Schema Enforcement estrito
(StructType) e particionamento em Delta Lake por payment_channel.
"""

import os
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)


# ===========================================================================
# 1. CONTRATO DE DADOS: SCHEMA ENFORCEMENT ESTRITO (StructType)
# ===========================================================================
BRONZE_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("customer_cpf", StringType(), False),
    StructField("card_number", StringType(), True),
    StructField("transaction_amount", DoubleType(), False),
    StructField("payment_channel", StringType(), False),
    StructField("merchant_name", StringType(), False),
    StructField("merchant_category", StringType(), False),
    StructField("transaction_city", StringType(), False),
    StructField("transaction_state", StringType(), False),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("device_id", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False)
])


# ===========================================================================
# 2. GERADOR DE TRANSAÇÕES FINANCEIRAS COM PADRÕES DE FRAUDE
# ===========================================================================
def generate_fraud_transactions(num_records: int = 1000) -> list[dict]:
    """
    Gera dados sintéticos realistas de transações bancárias e fintechs.
    Injeta anomalias de fraude propositais para detecção na Camada Silver:
      - 85% Transações legítimas do dia a dia.
      - 5% Valores anômalos em categorias de alto risco (Cripto/Apostas).
      - 5% Pulo de geolocalização impossível (mesmo cliente, cidades distantes).
      - 5% Transações suspeitas na madrugada.
    """
    fake = Faker("pt_BR")
    Faker.seed(42)
    random.seed(42)

    channels = ["PIX", "CARTAO_CREDITO", "CARTAO_DEBITO", "TED", "BOLETO"]
    categories = [
        "Supermercados & Alimentação",
        "Eletrônicos & Informática",
        "Farmácias & Saúde",
        "Moda & Vestuário",
        "Apostas & Cassinos Online",
        "Criptoativos & Investimentos P2P",
        "Passagens Aéreas & Turismo",
        "Postos de Combustível"
    ]

    cities_states = [
        ("São Paulo", "SP"), ("Campinas", "SP"), ("Santos", "SP"),
        ("Rio de Janeiro", "RJ"), ("Niterói", "RJ"),
        ("Belo Horizonte", "MG"), ("Uberlândia", "MG"),
        ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Florianópolis", "SC"),
        ("Salvador", "BA"), ("Recife", "PE"), ("Fortaleza", "CE"),
        ("Brasília", "DF"), ("Goiânia", "GO"), ("Manaus", "AM")
    ]

    # Cria uma base fixa de 200 clientes para simular compras repetidas
    customers = []
    for i in range(1, 201):
        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "customer_name": fake.name(),
            "customer_cpf": fake.cpf(),
            "card_number": fake.credit_card_number(card_type=None),
            "primary_device": f"DEV-{fake.uuid4()[:8].upper()}",
            "home_city_state": random.choice(cities_states)
        })

    transactions = []
    base_time = datetime(2026, 8, 1, 0, 0, 0)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for i in range(1, num_records + 1):
        cust = random.choice(customers)
        channel = random.choice(channels)
        category = random.choice(categories)
        city, state = cust["home_city_state"]
        device = cust["primary_device"]

        # Timestamp distribuído nos últimos 20 dias
        tx_time = base_time + timedelta(
            days=random.randint(0, 20),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        # Regra de Valor Padrão
        if category in ["Supermercados & Alimentação", "Farmácias & Saúde", "Postos de Combustível"]:
            amount = round(random.uniform(20.0, 450.0), 2)
        elif category in ["Eletrônicos & Informática", "Passagens Aéreas & Turismo"]:
            amount = round(random.uniform(500.0, 3500.0), 2)
        else:
            amount = round(random.uniform(50.0, 1200.0), 2)

        # INJEÇÃO PROPOSITAL DE PADRÕES DE FRAUDE:
        fraud_type = random.random()

        # 1. Fraude de Alto Valor na Madrugada em Cripto / Apostas (5% dos casos)
        if fraud_type < 0.05:
            category = random.choice(["Apostas & Cassinos Online", "Criptoativos & Investimentos P2P"])
            amount = round(random.uniform(4000.0, 18000.0), 2)
            # Força horário da madrugada (01:00 às 04:59)
            tx_time = tx_time.replace(hour=random.randint(1, 4))

        # 2. Fraude de Geolocalização Impossível / Dispositivo Desconhecido (5% dos casos)
        elif fraud_type < 0.10:
            distant_cities = [cs for cs in cities_states if cs[1] != cust["home_city_state"][1]]
            city, state = random.choice(distant_cities)
            device = f"DEV-SUSPECT-{fake.uuid4()[:6].upper()}"
            amount = round(random.uniform(1500.0, 7500.0), 2)

        # Se for PIX ou TED, card_number é nulo
        card_num = cust["card_number"] if channel in ["CARTAO_CREDITO", "CARTAO_DEBITO"] else None

        transactions.append({
            "transaction_id": f"TX-{i:06d}",
            "customer_id": cust["customer_id"],
            "customer_name": cust["customer_name"],
            "customer_cpf": cust["customer_cpf"],
            "card_number": card_num,
            "transaction_amount": float(amount),
            "payment_channel": channel,
            "merchant_name": f"{fake.company()} {fake.company_suffix()}",
            "merchant_category": category,
            "transaction_city": city,
            "transaction_state": state,
            "transaction_timestamp": tx_time.strftime("%Y-%m-%d %H:%M:%S"),
            "device_id": device,
            "ingestion_timestamp": now_str
        })

    return transactions


# ===========================================================================
# 3. PIPELINE DE INGESTÃO BRONZE (PySpark + Delta Lake)
# ===========================================================================
def ingest_bronze(spark: SparkSession, output_path: str, num_records: int = 1000) -> int:
    """
    Ingere transações financeiras na Camada Bronze com Delta Lake,
    aplicando Schema Enforcement estrito e particionamento por payment_channel.
    Utiliza I/O nativo JSON da JVM para máxima velocidade e estabilidade.
    """
    print("=" * 60)
    print("  💳 INGESTÃO BRONZE — Fintech Fraud Detection Lakehouse")
    print(f"  ⚡ Gerando {num_records} transações financeiras multicanais...")
    print("=" * 60)

    raw_data = generate_fraud_transactions(num_records)

    # Grava arquivo JSON temporário para leitura direta via JVM
    landing_dir = os.path.join(os.path.dirname(output_path), "_landing_temp")
    os.makedirs(landing_dir, exist_ok=True)
    temp_json = os.path.join(landing_dir, "raw_transactions.json")

    with open(temp_json, "w", encoding="utf-8") as f:
        for record in raw_data:
            f.write(json.dumps(record) + "\n")

    # Leitura nativa com Schema Enforcement estrito
    df_raw = spark.read.schema(BRONZE_SCHEMA).json(temp_json)

    print(f"  🛡️ Schema Enforcement validado ({len(BRONZE_SCHEMA.fields)} colunas).")
    print(f"  💾 Gravando em Delta Lake particionado por 'payment_channel'...")

    # Gravação atômica em Delta Lake
    df_raw.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("payment_channel") \
        .save(output_path)

    # Limpeza do arquivo temporário
    if os.path.exists(temp_json):
        os.remove(temp_json)

    # Contagem segura diretamente da tabela Delta persistida
    total_ingested = spark.read.format("delta").load(output_path).count()

    print("=" * 60)
    print(f"  ✅ Camada Bronze gravada com sucesso!")
    print(f"  📁 Destino: {output_path}")
    print(f"  📊 Total de transações: {total_ingested}")
    print("=" * 60)

    return total_ingested
