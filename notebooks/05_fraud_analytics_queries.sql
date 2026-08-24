-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 📊 Databricks SQL — Consultas Executivas Anti-Fraude & Mesa de Risco
-- MAGIC Queries analíticas executadas no Databricks SQL Warehouse sobre o Star Schema da Camada Gold.

-- COMMAND ----------

-- QUERY 1: KPI EXECUTIVO DE PREVENÇÃO A PERDAS (LOSS PREVENTION)
SELECT 
    COUNT(*) AS total_transacoes,
    SUM(transaction_amount) AS volume_financeiro_total,
    SUM(is_blocked_fraud) AS transacoes_bloqueadas,
    SUM(loss_prevented_amount) AS total_prejuizo_evitado_reais,
    ROUND((SUM(is_blocked_fraud) / COUNT(*)) * 100, 2) AS taxa_fraude_percentual
FROM fintech_lakehouse.gold.fact_financial_transactions;

-- COMMAND ----------

-- QUERY 2: ANÁLISE DE RISCO POR CANAL DE PAGAMENTO (PIX vs CARTÃO vs BOLETO)
SELECT 
    payment_channel,
    COUNT(*) AS total_pedidos,
    SUM(is_blocked_fraud) AS fraudes_bloqueadas,
    SUM(loss_prevented_amount) AS prejuizo_evitado_reais,
    ROUND(AVG(risk_score), 1) AS score_medio_risco
FROM fintech_lakehouse.gold.fact_financial_transactions
GROUP BY payment_channel
ORDER BY prejuizo_evitado_reais DESC;

-- COMMAND ----------

-- QUERY 3: TOP CATEGORIAS COM MAIOR INCIDÊNCIA DE FRAUDES
SELECT 
    m.merchant_category,
    COUNT(f.transaction_id) AS total_transacoes,
    SUM(f.is_blocked_fraud) AS tentativas_fraude,
    SUM(f.loss_prevented_amount) AS valor_bloqueado_reais
FROM fintech_lakehouse.gold.fact_financial_transactions f
JOIN fintech_lakehouse.gold.dim_merchants m ON f.merchant_name = m.merchant_name
GROUP BY m.merchant_category
ORDER BY valor_bloqueado_reais DESC;

-- COMMAND ----------

-- QUERY 4: MAPA DE CALOR GEOGRÁFICO DE FRAUDES POR MACRORREGIÃO E ESTADO
SELECT 
    l.macro_region,
    l.transaction_state,
    COUNT(f.transaction_id) AS volume_transacoes,
    SUM(f.is_blocked_fraud) AS fraudes_detectadas,
    SUM(f.loss_prevented_amount) AS montante_bloqueado_reais
FROM fintech_lakehouse.gold.fact_financial_transactions f
JOIN fintech_lakehouse.gold.dim_locations l 
  ON f.transaction_city = l.transaction_city 
 AND f.transaction_state = l.transaction_state
GROUP BY l.macro_region, l.transaction_state
ORDER BY montante_bloqueado_reais DESC;

-- COMMAND ----------

-- QUERY 5: FILA DA MESA DE ANÁLISE MANUAL (ALERT INBOX PARA OS ANALISTAS DE RISCO)
SELECT 
    f.transaction_id,
    f.transaction_timestamp,
    c.customer_name,
    c.masked_cpf,
    f.payment_channel,
    f.merchant_name,
    f.transaction_amount,
    f.risk_score,
    f.fraud_decision
FROM fintech_lakehouse.gold.fact_financial_transactions f
JOIN fintech_lakehouse.gold.dim_customers c ON f.customer_id = c.customer_id
WHERE f.fraud_decision IN ('BLOQUEADA_SUSPEITA_FRAUDE', 'ANALISE_MANUAL_MESA')
ORDER BY f.risk_score DESC, f.transaction_amount DESC;
