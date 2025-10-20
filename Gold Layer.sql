-- ============================================================================
-- GOLD LAYER: STAR SCHEMA FOR ANALYTICS
-- GROUP 7 - Payment Transaction Data Warehouse
-- ============================================================================
-- Purpose: Create analytics-ready star schema from cleaned Silver layer
-- Architecture: Fact table + Dimension tables (no dim_date - simpler approach)
-- ============================================================================

USE DATABASE GROUP7_DB;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- 1. CUSTOMER DIMENSION
-- Source: silver.dim_customer (already cleaned and deduplicated)
CREATE OR REPLACE TABLE gold.dim_customer AS
SELECT 
    customer_id,
    customer_age,
    customer_country,
    primary_payment_method,
    account_status,
    risk_score,
    uses_twofactor_auth,
    account_created_date
FROM silver.dim_customer;

-- 2. MERCHANT DIMENSION
-- Source: silver.dim_merchant (already cleaned and deduplicated)
CREATE OR REPLACE TABLE gold.dim_merchant AS
SELECT 
    merchant_id,
    merchant_name,
    merchant_country,
    merchant_category_code,
    processing_tier,
    merchant_account_status,
    merchant_risk_level,
    merchant_bank_name,
    registration_date
FROM silver.dim_merchant;

-- 3. TRANSACTION LOG AGGREGATION DIMENSION
-- Aggregate logs per transaction for analytical insights
CREATE OR REPLACE TABLE gold.dim_transaction_log_agg AS
SELECT 
    transaction_id,
    COUNT(*) AS total_log_events,
    MIN(event_time) AS first_event_time,
    MAX(event_time) AS last_event_time,
    MAX(CASE WHEN error_code IS NOT NULL AND error_code != 'NONE' THEN 1 ELSE 0 END) AS has_error,
    MAX(CASE WHEN event_type = 'FRAUD_DETECTION' THEN 1 ELSE 0 END) AS has_fraud_detection,
    MAX(CASE WHEN error_code IS NOT NULL AND error_code != 'NONE' THEN error_code ELSE NULL END) AS last_error_code,
    MAX(CASE WHEN error_message IS NOT NULL AND error_message != 'No error message' THEN error_message ELSE NULL END) AS last_error_message,
    ANY_VALUE(system_component) AS last_system_component
FROM silver.DIM_TRANSACTION_LOG
GROUP BY transaction_id;

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- 4. TRANSACTION FACT TABLE
-- Main fact table linking all dimensions with transaction metrics
CREATE OR REPLACE TABLE gold.fact_transactions AS
SELECT 
    t.transaction_id,
    t.merchant_id,
    t.customer_id,
    t.transaction_date,
    CAST(t.transaction_date AS DATE) AS transaction_date_key,  -- For daily aggregations
    t.payment_type,
    t.transaction_status,
    t.failure_category,
    t.is_fraud,
    t.amount_usd,
    t.amount_aud,
    t.fx_rate,
    t.bank_name,
    t.load_timestamp AS src_load_timestamp
FROM silver.FACT_TRANSACTION t;

-- ============================================================================
-- DATA QUALITY VALIDATION
-- ============================================================================

-- Validation 1: Check for orphaned customer records
SELECT 
    'Orphaned Customers in Fact Table' AS validation_check,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM gold.fact_transactions f
LEFT JOIN gold.dim_customer c ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Validation 2: Check for orphaned merchant records
SELECT 
    'Orphaned Merchants in Fact Table' AS validation_check,
    COUNT(*) AS orphan_count,
    CASE WHEN COUNT(*) = 0 THEN '✓ PASS' ELSE '✗ FAIL' END AS status
FROM gold.fact_transactions f
LEFT JOIN gold.dim_merchant m ON f.merchant_id = m.merchant_id
WHERE m.merchant_id IS NULL;

--Count distinct orphan merchants







-- Validation 3: Row count summary
SELECT 
    'dim_customer' AS table_name, 
    COUNT(*) AS row_count,
    'Dimension' AS table_type
FROM gold.dim_customer
UNION ALL
SELECT 
    'dim_merchant', 
    COUNT(*),
    'Dimension'
FROM gold.dim_merchant
UNION ALL
SELECT 
    'dim_transaction_log_agg', 
    COUNT(*),
    'Dimension'
FROM gold.dim_transaction_log_agg
UNION ALL
SELECT 
    'fact_transactions', 
    COUNT(*),
    'Fact'
FROM gold.fact_transactions
ORDER BY table_type, table_name;

-- Validation 4: Check data quality metrics
SELECT 
    'Data Quality Summary' AS check_name,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN transaction_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN failure_category != 'NONE' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(SUM(amount_aud), 2) AS total_revenue_aud
FROM gold.fact_transactions;

-- ============================================================================
-- DASHBOARD QUERIES
-- ============================================================================

-- QUERY 1: Success/Failure Ratio with Revenue Gain/Loss
-- Business Question: What's our transaction success rate and how much revenue are we losing?
SELECT 
    transaction_status,
    COUNT(*) AS transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(SUM(amount_aud), 2) AS total_amount_aud,
    CASE 
        WHEN transaction_status = 'APPROVED' THEN 'Revenue Gained'
        ELSE 'Revenue Lost'
    END AS revenue_impact
FROM gold.fact_transactions
GROUP BY transaction_status
ORDER BY transaction_count DESC;

-- QUERY 2: Failure Categories Breakdown
-- Business Question: What types of failures are we experiencing and what's the financial impact?
SELECT 
    failure_category,
    COUNT(*) AS failure_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage_of_total,
    ROUND(SUM(amount_aud), 2) AS lost_revenue_aud,
    ROUND(AVG(amount_aud), 2) AS avg_transaction_aud
FROM gold.fact_transactions
WHERE failure_category != 'NONE'
GROUP BY failure_category
ORDER BY failure_count DESC;

-- QUERY 3: Daily Volume by Country (TPS - Transactions Per Second Analysis)
-- Business Question: What's our transaction volume and revenue by country and date?
SELECT 
    c.customer_country,
    CAST(f.transaction_date AS DATE) AS transaction_date,
    COUNT(*) AS daily_transaction_count,
    ROUND(COUNT(*) / 86400.0, 4) AS avg_tps,  -- Transactions per second (assuming even distribution)
    ROUND(SUM(f.amount_aud), 2) AS daily_revenue_aud,
    ROUND(AVG(f.amount_aud), 2) AS avg_transaction_aud,
    SUM(CASE WHEN f.transaction_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN f.is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count
FROM gold.fact_transactions f
JOIN gold.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.customer_country, CAST(f.transaction_date AS DATE)
ORDER BY transaction_date DESC, daily_transaction_count DESC
LIMIT 100;

-- QUERY 4: Global Failure Map - Merchant Country Analysis
-- Business Question: Which merchant countries have the highest failure and fraud rates?
SELECT 
    m.merchant_country,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN f.transaction_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN f.transaction_status != 'APPROVED' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(SUM(CASE WHEN f.transaction_status != 'APPROVED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS failure_rate_pct,
    SUM(CASE WHEN f.is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS fraud_rate_pct,
    ROUND(SUM(f.amount_aud), 2) AS total_revenue_aud,
    ROUND(SUM(CASE WHEN f.transaction_status != 'APPROVED' THEN f.amount_aud ELSE 0 END), 2) AS lost_revenue_aud
FROM gold.fact_transactions f
JOIN gold.dim_merchant m ON f.merchant_id = m.merchant_id
GROUP BY m.merchant_country
ORDER BY failure_rate_pct DESC;

-- QUERY 5: High-Risk Customer Analysis
-- Business Question: Which high-risk customers are generating fraud or failed transactions?
SELECT 
    c.customer_id,
    c.customer_country,
    c.customer_age,
    c.risk_score,
    c.uses_twofactor_auth,
    COUNT(f.transaction_id) AS total_transactions,
    SUM(CASE WHEN f.is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
    SUM(CASE WHEN f.transaction_status != 'APPROVED' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(SUM(f.amount_aud), 2) AS total_attempted_aud,
    ROUND(SUM(CASE WHEN f.transaction_status = 'APPROVED' THEN f.amount_aud ELSE 0 END), 2) AS total_approved_aud,
    ROUND(SUM(CASE WHEN f.is_fraud = TRUE THEN f.amount_aud ELSE 0 END), 2) AS fraud_amount_aud
FROM gold.dim_customer c
JOIN gold.fact_transactions f ON c.customer_id = f.customer_id
WHERE c.risk_score > 700 OR c.uses_twofactor_auth = FALSE
GROUP BY c.customer_id, c.customer_country, c.customer_age, c.risk_score, c.uses_twofactor_auth
HAVING fraud_count > 0 OR failed_count > 2
ORDER BY fraud_count DESC, failed_count DESC
LIMIT 50;

-- QUERY 6: Payment Type Performance Analysis
-- Business Question: Which payment types have the best success rates?
SELECT 
    f.payment_type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN f.transaction_status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
    ROUND(SUM(CASE WHEN f.transaction_status = 'APPROVED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct,
    SUM(CASE WHEN f.is_fraud = TRUE THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(f.amount_aud), 2) AS total_revenue_aud,
    ROUND(AVG(f.amount_aud), 2) AS avg_transaction_aud
FROM gold.fact_transactions f
GROUP BY f.payment_type
ORDER BY total_transactions DESC;

-- QUERY 7: Transaction Log Insights
-- Business Question: How are system errors impacting our transactions?
SELECT 
    l.last_system_component AS system_component,
    COUNT(*) AS transaction_count,
    SUM(l.has_error) AS error_count,
    SUM(l.has_fraud_detection) AS fraud_detection_count,
    ROUND(SUM(l.has_error) * 100.0 / COUNT(*), 2) AS error_rate_pct,
    ROUND(AVG(l.total_log_events), 2) AS avg_log_events_per_transaction
FROM gold.fact_transactions f
JOIN gold.dim_transaction_log_agg l ON f.transaction_id = l.transaction_id
GROUP BY l.last_system_component
ORDER BY error_count DESC;

-- ============================================================================
-- ANALYTICAL VIEWS FOR BI TOOLS
-- ============================================================================

-- Create denormalized view for easy dashboard consumption
CREATE OR REPLACE VIEW gold.vw_transactions_analytics AS
SELECT 
    -- Transaction identifiers
    f.transaction_id,
    f.transaction_date,
    f.transaction_date_key,
    
    -- Customer attributes
    c.customer_id,
    c.customer_country,
    c.customer_age,
    c.risk_score AS customer_risk_score,
    c.uses_twofactor_auth,
    c.account_status AS customer_account_status,
    c.primary_payment_method AS customer_primary_payment_method,
    
    -- Merchant attributes
    m.merchant_id,
    m.merchant_name,
    m.merchant_country,
    m.merchant_category_code,
    m.merchant_risk_level,
    m.processing_tier,
    m.merchant_account_status,
    
    -- Transaction metrics
    f.payment_type,
    f.transaction_status,
    f.failure_category,
    f.is_fraud,
    f.amount_usd,
    f.amount_aud,
    f.fx_rate,
    f.bank_name,
    
    -- Log aggregation insights
    l.total_log_events,
    l.has_error,
    l.has_fraud_detection,
    l.last_error_code,
    l.last_error_message,
    l.last_system_component,
    
    -- Derived fields for analysis
    CASE WHEN f.transaction_status = 'APPROVED' THEN 1 ELSE 0 END AS is_approved,
    CASE WHEN f.transaction_status != 'APPROVED' THEN 1 ELSE 0 END AS is_failed,
    CASE WHEN c.customer_country = m.merchant_country THEN 'Domestic' ELSE 'International' END AS transaction_type
    
FROM gold.fact_transactions f
LEFT JOIN gold.dim_customer c ON f.customer_id = c.customer_id
LEFT JOIN gold.dim_merchant m ON f.merchant_id = m.merchant_id
LEFT JOIN gold.dim_transaction_log_agg l ON f.transaction_id = l.transaction_id;

-- ============================================================================
-- FINAL VERIFICATION
-- ============================================================================

-- Show sample of denormalized view
SELECT * FROM gold.vw_transactions_analytics LIMIT 10;

-- Summary statistics
SELECT 
    '=== GOLD LAYER BUILD COMPLETE ===' AS status,
    (SELECT COUNT(*) FROM gold.dim_customer) AS customers,
    (SELECT COUNT(*) FROM gold.dim_merchant) AS merchants,
    (SELECT COUNT(*) FROM gold.dim_transaction_log_agg) AS log_aggregations,
    (SELECT COUNT(*) FROM gold.fact_transactions) AS transactions,
    (SELECT ROUND(SUM(amount_aud), 2) FROM gold.fact_transactions) AS total_revenue_aud;