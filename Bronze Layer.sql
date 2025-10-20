--DROP DATABASE IF EXISTS GROUP7_DB;

-- =========================
-- BRONZE LAYER (relevant parts)
-- =========================
CREATE DATABASE GROUP7_DB;
USE DATABASE GROUP7_DB;
CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;

-- 1) MERCHANT (unchanged from your version; keep if already created)
CREATE OR REPLACE TABLE bronze.merchant (
    merchant_id STRING PRIMARY KEY,
    merchant_name STRING,
    merchant_country STRING,
    merchant_category_code INT,
    processing_tier STRING,
    merchant_account_status STRING,
    merchant_risk_level STRING,
    merchant_bank_name STRING,
    registration_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 2) CUSTOMER (ADDED customer_bank_name)
CREATE OR REPLACE TABLE bronze.customer (
    customer_id INT PRIMARY KEY,
    customer_age INT,
    customer_country STRING,
    primary_payment_method STRING,
    account_status STRING,
    risk_score INT,
    uses_twofactor_auth BOOLEAN,
    account_created_date DATE,
    customer_bank_name STRING,                 -- NEW: needed to help validate fraud vs issuer
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3) TRANSACTION_DATA (unchanged structure)
CREATE OR REPLACE TABLE bronze.transaction_data (
    transaction_id STRING PRIMARY KEY,
    merchant_id STRING,
    customer_id INT,
    transaction_date TIMESTAMP,
    amount_usd FLOAT,
    currency STRING,
    payment_type STRING,
    transaction_status STRING,
    bank_name STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 4) TRANSACTION_LOG (will ensure ≤ 1 log per transaction)
CREATE OR REPLACE TABLE bronze.transaction_log (
    log_id STRING PRIMARY KEY,
    transaction_id STRING,
    event_time TIMESTAMP,
    event_type STRING,
    error_code STRING,
    error_message STRING,
    system_component STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- -------------------------
-- SAMPLE DATA GENERATION
-- -------------------------

-- Merchants (reuse your generator or keep existing)
INSERT INTO bronze.merchant
SELECT
    'MER-' || LPAD(SEQ4()::STRING, 6, '0') AS merchant_id,
    ARRAY_CONSTRUCT('Amazon','Walmart','Target','BestBuy','HomeDepot','Starbucks','McDonald',
                    'Netflix','Uber','Airbnb','Apple Store','Nike','Costco','Whole Foods','CVS Pharmacy')
                    [UNIFORM(0,14,RANDOM())]::STRING AS merchant_name,
    ARRAY_CONSTRUCT('US','CA','UK','DE','FR','JP','AU','BR','IN','SG')[UNIFORM(0,9,RANDOM())]::STRING AS merchant_country,
    UNIFORM(5411, 5999, RANDOM()) AS merchant_category_code,
    ARRAY_CONSTRUCT('PREMIUM','STANDARD','BASIC')[UNIFORM(0,2,RANDOM())]::STRING AS processing_tier,
    ARRAY_CONSTRUCT('ACTIVE','SUSPENDED','PENDING')[UNIFORM(0,2,RANDOM())]::STRING AS merchant_account_status,
    ARRAY_CONSTRUCT('LOW','MEDIUM','HIGH')[UNIFORM(0,2,RANDOM())]::STRING AS merchant_risk_level,
    ARRAY_CONSTRUCT('Chase','BoA','Wells Fargo','Citi','Goldman Sachs')[UNIFORM(0,4,RANDOM())]::STRING AS merchant_bank_name,
    DATEADD(DAY, UNIFORM(-1095, -30, RANDOM()), CURRENT_DATE()) AS registration_date,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Customers (with customer_bank_name)
INSERT INTO bronze.customer
SELECT
    1000000 + SEQ4() AS customer_id,
    UNIFORM(18, 80, RANDOM()) AS customer_age,
    ARRAY_CONSTRUCT('US','CA','UK','DE','FR','JP','AU','BR','IN','SG')[UNIFORM(0,9,RANDOM())]::STRING AS customer_country,
    ARRAY_CONSTRUCT('CREDIT_CARD','DEBIT_CARD','PAYPAL','APPLE_PAY','GOOGLE_PAY','BANK_TRANSFER')[UNIFORM(0,5,RANDOM())]::STRING AS primary_payment_method,
    ARRAY_CONSTRUCT('ACTIVE','SUSPENDED','CLOSED')[UNIFORM(0,2,RANDOM())]::STRING AS account_status,
    UNIFORM(300, 850, RANDOM()) AS risk_score,
    CASE WHEN UNIFORM(0,1,RANDOM())=1 THEN TRUE ELSE FALSE END AS uses_twofactor_auth,
    DATEADD(DAY, UNIFORM(-730, -60, RANDOM()), CURRENT_DATE()) AS account_created_date,
    ARRAY_CONSTRUCT('Chase','BoA','Wells Fargo','Citi','Capital One','Discover')[UNIFORM(0,5,RANDOM())]::STRING AS customer_bank_name, -- NEW
    CURRENT_TIMESTAMP() AS load_timestamp
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- Transactions
INSERT INTO bronze.transaction_data
SELECT
    'TXN-' || LPAD(SEQ4()::STRING, 8, '0') AS transaction_id,
    'MER-' || LPAD(UNIFORM(1, 1000, RANDOM())::STRING, 6, '0') AS merchant_id,
    1000000 + UNIFORM(0, 9999, RANDOM()) AS customer_id,
    DATEADD(MINUTE, UNIFORM(-43200, 0, RANDOM()), CURRENT_TIMESTAMP()) AS transaction_date,
    ROUND(UNIFORM(1, 5000, RANDOM()) + UNIFORM(0, 99, RANDOM()) / 100.0, 2) AS amount_usd,
    ARRAY_CONSTRUCT('USD','EUR','GBP','JPY','CAD','AUD')[UNIFORM(0,5,RANDOM())]::STRING AS currency,
    ARRAY_CONSTRUCT('PURCHASE','REFUND','PREAUTH','RECURRING')[UNIFORM(0,3,RANDOM())]::STRING AS payment_type,
    /* status skew realistic */
    CASE
      WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN 'APPROVED'
      WHEN UNIFORM(1,100,RANDOM()) <= 93 THEN 'DECLINED'
      WHEN UNIFORM(1,100,RANDOM()) <= 97 THEN 'TIMED_OUT'
      WHEN UNIFORM(1,100,RANDOM()) <= 99 THEN 'FRAUD_DETECTED'
      ELSE 'ERROR'
    END AS transaction_status,
    ARRAY_CONSTRUCT('Chase','BoA','Wells Fargo','Citi','Capital One','Discover')[UNIFORM(0,5,RANDOM())]::STRING AS bank_name,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM TABLE(GENERATOR(ROWCOUNT => 50000));

-- (Optional) a few duplicates for dedupe testing in Silver (<= 1%)
INSERT INTO bronze.transaction_data
SELECT
    transaction_id,
    merchant_id, customer_id, DATEADD(SECOND, 3, transaction_date),
    amount_usd, currency, payment_type, transaction_status, bank_name,
    CURRENT_TIMESTAMP()
FROM bronze.transaction_data
WHERE UNIFORM(0,100,RANDOM())=0
LIMIT 500;

-- -------------------------
-- SINGLE LOG PER TRANSACTION (≤ transactions)
-- -------------------------
-- Clean slate
TRUNCATE TABLE bronze.transaction_log;

INSERT INTO bronze.transaction_log
SELECT
    'LOG-' || LPAD(SEQ4()::STRING, 10, '0') AS log_id,
    t.transaction_id,
    -- ensure event_time >= transaction_date (0..5 sec after)
    DATEADD(SECOND, UNIFORM(0,5,RANDOM()), t.transaction_date) AS event_time,
    CASE
      WHEN t.transaction_status = 'APPROVED'        THEN 'COMPLETION'
      WHEN t.transaction_status = 'DECLINED'        THEN 'ERROR'
      WHEN t.transaction_status = 'TIMED_OUT'       THEN 'ERROR'
      WHEN t.transaction_status = 'FRAUD_DETECTED'  THEN 'FRAUD_DETECTION'
      ELSE 'ERROR'
    END AS event_type,
    CASE
      WHEN t.transaction_status = 'APPROVED'       THEN NULL
      WHEN t.transaction_status = 'TIMED_OUT'      THEN 'TIMEOUT'
      WHEN t.transaction_status = 'FRAUD_DETECTED' THEN 'FRAUD_ALERT'
      WHEN t.transaction_status = 'DECLINED'       THEN 'DO_NOT_HONOR'
      ELSE 'SYSTEM_ERROR'
    END AS error_code,
    CASE
      WHEN t.transaction_status = 'APPROVED'       THEN NULL
      WHEN t.transaction_status = 'TIMED_OUT'      THEN 'Transaction timeout'
      WHEN t.transaction_status = 'FRAUD_DETECTED' THEN 'Suspicious activity detected'
      WHEN t.transaction_status = 'DECLINED'       THEN 'Card declined by issuer'
      ELSE 'System error'
    END AS error_message,
    ARRAY_CONSTRUCT('GATEWAY','PROCESSOR','BANK_API','FRAUD_ENGINE','ROUTING')[UNIFORM(0,4,RANDOM())]::STRING AS system_component,
    CURRENT_TIMESTAMP() AS load_timestamp
FROM bronze.transaction_data t;

-- Quick sanity: logs ≤ transactions
SELECT (SELECT COUNT(*) FROM bronze.transaction_log)    AS log_rows,
       (SELECT COUNT(DISTINCT transaction_id) FROM bronze.transaction_data) AS txn_rows;