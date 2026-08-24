# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Camada Bronze — Ingestão de Transações Financeiras (Fintech Anti-Fraud)
# MAGIC **Responsabilidade:** Gerar e ingerir dados brutos de transações financeiras com Schema Enforcement estrito (`StructType`) e gravação em Delta Lake particionado por `payment_channel`.

# COMMAND ----------

import os
import json
import random
from datetime import datetime, timedelta
from faker import Faker
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Definição do Contrato de Dados (Schema Enforcement Estrito)

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Gerador de Transações Financeiras com Padrões de Fraude

# COMMAND ----------

def generate_fraud_transactions(num_records: int = 1000) -> list[dict]:
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

        tx_time = base_time + timedelta(
            days=random.randint(0, 20),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        if category in ["Supermercados & Alimentação", "Farmácias & Saúde", "Postos de Combustível"]:
            amount = round(random.uniform(20.0, 450.0), 2)
        elif category in ["Eletrônicos & Informática", "Passagens Aéreas & Turismo"]:
            amount = round(random.uniform(500.0, 3500.0), 2)
        else:
            amount = round(random.uniform(50.0, 1200.0), 2)

        fraud_type = random.random()
        if fraud_type < 0.05:
            category = random.choice(["Apostas & Cassinos Online", "Criptoativos & Investimentos P2P"])
            amount = round(random.uniform(4000.0, 18000.0), 2)
            tx_time = tx_time.replace(hour=random.randint(1, 4))
        elif fraud_type < 0.10:
            distant_cities = [cs for cs in cities_states if cs[1] != cust["home_city_state"][1]]
            city, state = random.choice(distant_cities)
            device = f"DEV-SUSPECT-{fake.uuid4()[:6].upper()}"
            amount = round(random.uniform(1500.0, 7500.0), 2)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Execução da Ingestão na Camada Bronze (Delta Lake)

# COMMAND ----------

bronze_output_path = "/tmp/fintech_lakehouse/bronze"
landing_temp = "/tmp/fintech_lakehouse/_landing_temp"
os.makedirs(landing_temp, exist_ok=True)

raw_records = generate_fraud_transactions(num_records=1000)
temp_json = os.path.join(landing_temp, "raw_transactions.json")

with open(temp_json, "w", encoding="utf-8") as f:
    for record in raw_records:
        f.write(json.dumps(record) + "\n")

df_raw = spark.read.schema(BRONZE_SCHEMA).json(temp_json)

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("payment_channel") \
    .save(bronze_output_path)

if os.path.exists(temp_json):
    os.remove(temp_json)

total_bronze = spark.read.format("delta").load(bronze_output_path).count()
print(f"✅ Camada Bronze gravada com sucesso no Databricks! Total: {total_bronze} registros.")
display(df_raw.limit(10))
