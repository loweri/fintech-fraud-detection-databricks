-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🛡️ Governança de Dados com Unity Catalog (Databricks)
-- MAGIC **Objetivo:** Registrar as tabelas do Lakehouse em Catálogos oficiais, schemas e aplicar controle de acesso por papel (RBAC) e políticas de segurança LGPD.

-- COMMAND ----------

-- 1. CRIAÇÃO DE CATÁLOGOS E SCHEMAS
CREATE CATALOG IF NOT EXISTS fintech_lakehouse
COMMENT 'Catálogo Corporativo de Engenharia de Dados e Detecção de Fraudes';

USE CATALOG fintech_lakehouse;

CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Camada de dados brutos e ingestão com Schema Enforcement';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Camada de dados curados, mascaramento LGPD e motor de risco';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Camada analítica modelada em Star Schema Kimball';

-- COMMAND ----------

-- 2. REGISTRO DAS TABELAS DELTA NO UNITY CATALOG
CREATE TABLE IF NOT EXISTS gold.fact_financial_transactions
USING DELTA
LOCATION '/tmp/fintech_lakehouse/gold/fact_financial_transactions';

CREATE TABLE IF NOT EXISTS gold.dim_customers
USING DELTA
LOCATION '/tmp/fintech_lakehouse/gold/dim_customers';

CREATE TABLE IF NOT EXISTS gold.dim_merchants
USING DELTA
LOCATION '/tmp/fintech_lakehouse/gold/dim_merchants';

CREATE TABLE IF NOT EXISTS gold.dim_payment_channels
USING DELTA
LOCATION '/tmp/fintech_lakehouse/gold/dim_payment_channels';

CREATE TABLE IF NOT EXISTS gold.dim_locations
USING DELTA
LOCATION '/tmp/fintech_lakehouse/gold/dim_locations';

-- COMMAND ----------

-- 3. CONTROLE DE ACESSO BASEADO EM FUNÇÃO (RBAC - Role-Based Access Control)
-- Concede acesso de leitura à equipe de analistas de risco para a Camada Gold:
GRANT USAGE ON CATALOG fintech_lakehouse TO `account users`;
GRANT USAGE ON SCHEMA gold TO `account users`;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO `account users`;

-- Garante que analistas NÃO acessem dados brutos não mascarados da Bronze:
REVOKE ALL PRIVILEGES ON SCHEMA bronze FROM `account users`;

-- COMMAND ----------

-- 4. VALIDAÇÃO DE TABELAS NO CATÁLOGO
SHOW TABLES IN gold;
