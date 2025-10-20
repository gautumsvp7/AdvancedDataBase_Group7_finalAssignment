
USE DATABASE GROUP7_DB;
CREATE SCHEMA IF NOT EXISTS silver;
USE SCHEMA silver;


-- 1. CUSTOMER DIMENSION CLEANING


CREATE OR REPLACE TEMP VIEW customer_issues_analysis AS
WITH latest AS (
  SELECT *
  FROM (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM bronze.customer c
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    customer_id,
    customer_age,
    customer_country,
    account_status,
    primary_payment_method,
    risk_score,
    uses_twofactor_auth,
    account_created_date,
    customer_bank_name,
    load_timestamp,
    -- Standardization columns
    TRY_TO_NUMBER(customer_age) AS age_num,
    UPPER(TRIM(customer_country)) AS country_std,
    TRY_TO_NUMBER(risk_score) AS risk_num,
    UPPER(TRIM(account_status)) AS status_std,
    UPPER(TRIM(primary_payment_method)) AS payment_std
  FROM latest
)
SELECT 'customer_bronze_total' AS metric, COUNT(*) AS value FROM bronze.customer
UNION ALL SELECT 'customer_distinct_ids', COUNT(*) FROM latest
UNION ALL SELECT 'customer_duplicates', COUNT(*) FROM bronze.customer WHERE customer_id IN (
  SELECT customer_id FROM bronze.customer GROUP BY customer_id HAVING COUNT(*) > 1
)
UNION ALL SELECT 'id_null', COUNT(*) FROM standardized WHERE customer_id IS NULL
UNION ALL SELECT 'age_null', COUNT(*) FROM standardized WHERE age_num IS NULL
UNION ALL SELECT 'age_under_18', COUNT(*) FROM standardized WHERE age_num < 18
UNION ALL SELECT 'age_over_100', COUNT(*) FROM standardized WHERE age_num > 100
UNION ALL SELECT 'country_null', COUNT(*) FROM standardized WHERE country_std IS NULL
UNION ALL SELECT 'risk_null', COUNT(*) FROM standardized WHERE risk_num IS NULL
UNION ALL SELECT 'risk_below_100', COUNT(*) FROM standardized WHERE risk_num < 100
UNION ALL SELECT 'risk_above_850', COUNT(*) FROM standardized WHERE risk_num > 850;

-- View issues
SELECT * FROM customer_issues_analysis ORDER BY metric;


CREATE OR REPLACE TABLE silver.dim_customer AS
WITH latest AS (
  SELECT *
  FROM (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM bronze.customer c
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    customer_id,
    TRY_TO_NUMBER(customer_age) AS customer_age,
    UPPER(TRIM(customer_country)) AS customer_country,
    -- Account status: valid values or UNKNOWN
    CASE
      WHEN UPPER(TRIM(account_status)) IN ('ACTIVE','SUSPENDED','CLOSED') 
        THEN UPPER(TRIM(account_status))
      ELSE 'UNKNOWN'
    END AS account_status,
    -- Payment method: valid values, OTHER, or NULL
    CASE
      WHEN UPPER(TRIM(primary_payment_method)) IN (
           'CREDIT_CARD','DEBIT_CARD','PAYPAL','APPLE_PAY','GOOGLE_PAY','BANK_TRANSFER')
        THEN UPPER(TRIM(primary_payment_method))
      WHEN TRIM(primary_payment_method) IS NULL OR TRIM(primary_payment_method) = '' 
        THEN NULL
      ELSE 'OTHER'
    END AS primary_payment_method,
    TRY_TO_NUMBER(risk_score) AS risk_score,
    IFF(uses_twofactor_auth IS NULL, NULL, uses_twofactor_auth::BOOLEAN) AS uses_twofactor_auth,
    TRY_TO_DATE(account_created_date) AS account_created_date,
    UPPER(TRIM(customer_bank_name)) AS customer_bank_name,
    load_timestamp
  FROM latest
)
SELECT
  customer_id,
  customer_age::NUMBER(3,0) AS customer_age,
  customer_country,
  account_status,
  primary_payment_method,
  risk_score::NUMBER(4,0) AS risk_score,
  uses_twofactor_auth,
  account_created_date,
  customer_bank_name,
  load_timestamp
FROM standardized
WHERE customer_id IS NOT NULL
  AND customer_country IS NOT NULL
  AND customer_age BETWEEN 18 AND 100
  AND risk_score BETWEEN 100 AND 850;


CREATE OR REPLACE TABLE silver.dim_customer_rejects AS
WITH latest AS (
  SELECT *
  FROM (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM bronze.customer c
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    *,
    TRY_TO_NUMBER(customer_age) AS age_num,
    UPPER(TRIM(customer_country)) AS country_std,
    TRY_TO_NUMBER(risk_score) AS risk_num
  FROM latest
)
SELECT
  customer_id, customer_age, customer_country, account_status, 
  primary_payment_method, risk_score, uses_twofactor_auth, 
  account_created_date, customer_bank_name, load_timestamp,
  CASE
    WHEN customer_id IS NULL THEN 'ID_NULL'
    WHEN country_std IS NULL THEN 'COUNTRY_NULL'
    WHEN age_num IS NULL THEN 'AGE_NOT_NUMERIC'
    WHEN age_num < 18 OR age_num > 100 THEN 'AGE_OUT_OF_RANGE'
    WHEN risk_num IS NULL THEN 'RISK_NOT_NUMERIC'
    WHEN risk_num < 100 OR risk_num > 850 THEN 'RISK_OUT_OF_RANGE'
    ELSE 'OTHER'
  END AS reject_reason
FROM standardized
WHERE customer_id IS NULL
   OR country_std IS NULL
   OR age_num IS NULL OR age_num < 18 OR age_num > 100
   OR risk_num IS NULL OR risk_num < 100 OR risk_num > 850;


-- 2. MERCHANT DIMENSION CLEANING



CREATE OR REPLACE TEMP VIEW merchant_issues_analysis AS
WITH latest AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM bronze.merchant m
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    merchant_id,
    merchant_name,
    merchant_country,
    merchant_category_code,
    processing_tier,
    merchant_account_status,
    merchant_risk_level,
    merchant_bank_name,
    registration_date,
    load_timestamp,
    -- Standardization
    UPPER(TRIM(merchant_country)) AS country_std,
    TRY_TO_NUMBER(merchant_category_code) AS mcc_num,
    UPPER(TRIM(merchant_account_status)) AS status_std,
    UPPER(TRIM(merchant_risk_level)) AS risk_std,
    UPPER(TRIM(processing_tier)) AS tier_std
  FROM latest
)
SELECT 'merchant_bronze_total' AS metric, COUNT(*) AS value FROM bronze.merchant
UNION ALL SELECT 'merchant_distinct_ids', COUNT(*) FROM latest
UNION ALL SELECT 'merchant_duplicates', COUNT(*) FROM bronze.merchant WHERE merchant_id IN (
  SELECT merchant_id FROM bronze.merchant GROUP BY merchant_id HAVING COUNT(*) > 1
)
UNION ALL SELECT 'id_null', COUNT(*) FROM standardized WHERE merchant_id IS NULL
UNION ALL SELECT 'country_null', COUNT(*) FROM standardized WHERE country_std IS NULL
UNION ALL SELECT 'mcc_invalid', COUNT(*) FROM standardized WHERE mcc_num IS NULL OR mcc_num < 5000 OR mcc_num > 5999
UNION ALL SELECT 'status_invalid', COUNT(*) FROM standardized WHERE status_std NOT IN ('ACTIVE','SUSPENDED','PENDING')
UNION ALL SELECT 'risk_invalid', COUNT(*) FROM standardized WHERE risk_std NOT IN ('LOW','MEDIUM','HIGH');

-- View issues
SELECT * FROM merchant_issues_analysis ORDER BY metric;


CREATE OR REPLACE TABLE silver.dim_merchant AS
WITH latest AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM bronze.merchant m
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    merchant_id,
    UPPER(TRIM(REGEXP_REPLACE(merchant_name, '\\s+', ' '))) AS merchant_name,
    UPPER(TRIM(merchant_country)) AS merchant_country,
    TRY_TO_NUMBER(merchant_category_code) AS merchant_category_code,
    -- Processing tier: normalize variants to STANDARD
    CASE
      WHEN UPPER(TRIM(processing_tier)) IN ('PREMIUM','STANDARD','BASIC') 
        THEN UPPER(TRIM(processing_tier))
      WHEN UPPER(TRIM(processing_tier)) IN ('GOLD','STANDRD') 
        THEN 'STANDARD'
      ELSE 'STANDARD'
    END AS processing_tier,
    -- Account status validation
    CASE
      WHEN UPPER(TRIM(merchant_account_status)) IN ('ACTIVE','SUSPENDED','PENDING') 
        THEN UPPER(TRIM(merchant_account_status))
      ELSE NULL
    END AS merchant_account_status,
    -- Risk level validation
    CASE
      WHEN UPPER(TRIM(merchant_risk_level)) IN ('LOW','MEDIUM','HIGH') 
        THEN UPPER(TRIM(merchant_risk_level))
      ELSE NULL
    END AS merchant_risk_level,
    UPPER(TRIM(merchant_bank_name)) AS merchant_bank_name,
    TRY_TO_DATE(registration_date) AS registration_date,
    load_timestamp
  FROM latest
)
SELECT
  merchant_id,
  merchant_name,
  merchant_country,
  merchant_category_code::NUMBER(4,0) AS merchant_category_code,
  processing_tier,
  merchant_account_status,
  merchant_risk_level,
  merchant_bank_name,
  registration_date,
  load_timestamp
FROM standardized
WHERE merchant_id IS NOT NULL
  AND merchant_country IS NOT NULL
  AND merchant_category_code BETWEEN 5000 AND 5999
  AND processing_tier IS NOT NULL
  AND merchant_account_status IS NOT NULL
  AND merchant_risk_level IS NOT NULL;


CREATE OR REPLACE TABLE silver.dim_merchant_rejects AS
WITH latest AS (
  SELECT *
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY merchant_id ORDER BY load_timestamp DESC NULLS LAST) AS rn
    FROM bronze.merchant m
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    *,
    UPPER(TRIM(merchant_country)) AS country_std,
    TRY_TO_NUMBER(merchant_category_code) AS mcc_num,
    CASE WHEN UPPER(TRIM(merchant_account_status)) IN ('ACTIVE','SUSPENDED','PENDING')
         THEN UPPER(TRIM(merchant_account_status)) END AS status_std,
    CASE WHEN UPPER(TRIM(merchant_risk_level)) IN ('LOW','MEDIUM','HIGH')
         THEN UPPER(TRIM(merchant_risk_level)) END AS risk_std
  FROM latest
)
SELECT
  merchant_id, merchant_name, merchant_country, merchant_category_code,
  processing_tier, merchant_account_status, merchant_risk_level, 
  merchant_bank_name, registration_date, load_timestamp,
  CASE
    WHEN merchant_id IS NULL THEN 'ID_NULL'
    WHEN country_std IS NULL THEN 'COUNTRY_NULL'
    WHEN mcc_num IS NULL OR mcc_num < 5000 OR mcc_num > 5999 THEN 'MCC_INVALID'
    WHEN status_std IS NULL THEN 'STATUS_INVALID'
    WHEN risk_std IS NULL THEN 'RISK_INVALID'
    ELSE 'OTHER'
  END AS reject_reason
FROM standardized
WHERE merchant_id IS NULL
   OR country_std IS NULL
   OR mcc_num IS NULL OR mcc_num < 5000 OR mcc_num > 5999
   OR status_std IS NULL
   OR risk_std IS NULL;


-- 3. TRANSACTION FACT CLEANING



CREATE OR REPLACE TEMP VIEW transaction_issues_analysis AS
WITH deduped AS (
  SELECT *
  FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY load_timestamp DESC) AS rn
    FROM bronze.transaction_data t
  )
  WHERE rn = 1
)
SELECT 'transaction_bronze_total' AS metric, COUNT(*) AS value FROM bronze.transaction_data
UNION ALL SELECT 'transaction_distinct_ids', COUNT(*) FROM deduped
UNION ALL SELECT 'transaction_duplicates', COUNT(*) - COUNT(DISTINCT transaction_id) FROM bronze.transaction_data
UNION ALL SELECT 'txn_id_null', COUNT(*) FROM deduped WHERE transaction_id IS NULL
UNION ALL SELECT 'merchant_id_null', COUNT(*) FROM deduped WHERE merchant_id IS NULL
UNION ALL SELECT 'customer_id_null', COUNT(*) FROM deduped WHERE customer_id IS NULL
UNION ALL SELECT 'amount_null', COUNT(*) FROM deduped WHERE amount_usd IS NULL
UNION ALL SELECT 'amount_zero_or_negative', COUNT(*) FROM deduped WHERE amount_usd <= 0
UNION ALL SELECT 'currency_invalid', COUNT(*) FROM deduped 
  WHERE UPPER(TRIM(REGEXP_REPLACE(currency, '[^A-Z]', ''))) NOT IN ('USD','EUR','GBP','JPY','CAD','AUD')
UNION ALL SELECT 'status_invalid', COUNT(*) FROM deduped 
  WHERE UPPER(TRIM(transaction_status)) NOT IN ('APPROVED','DECLINED','TIMED_OUT','FRAUD_DETECTED','ERROR')
UNION ALL SELECT 'payment_type_null', COUNT(*) FROM deduped WHERE payment_type IS NULL;

-- View issues
SELECT * FROM transaction_issues_analysis ORDER BY metric;


CREATE OR REPLACE TABLE silver.fact_transaction AS
WITH deduped AS (
  SELECT *
  FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY load_timestamp DESC) AS rn
    FROM bronze.transaction_data t
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    transaction_id,
    merchant_id,
    customer_id,
    transaction_date,
    amount_usd,
    UPPER(TRIM(REGEXP_REPLACE(currency, '[^A-Z]', ''))) AS currency,
    UPPER(TRIM(REGEXP_REPLACE(payment_type, '[^A-Z0-9]+', '_'))) AS payment_type_raw,
    UPPER(TRIM(transaction_status)) AS transaction_status,
    UPPER(TRIM(bank_name)) AS bank_name,
    load_timestamp
  FROM deduped
  WHERE transaction_id IS NOT NULL
    AND merchant_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND amount_usd IS NOT NULL
    AND amount_usd > 0
    AND UPPER(TRIM(REGEXP_REPLACE(currency, '[^A-Z]', ''))) IN ('USD','EUR','GBP','JPY','CAD','AUD')
    AND UPPER(TRIM(transaction_status)) IN ('APPROVED','DECLINED','TIMED_OUT','FRAUD_DETECTED','ERROR')
),
with_fx AS (
  SELECT
    transaction_id,
    merchant_id,
    customer_id,
    transaction_date,
    amount_usd,
    currency,
    -- Standardize payment type
    CASE
      WHEN payment_type_raw IN ('CREDIT_CARD','DEBIT_CARD','PAYPAL','APPLE_PAY','GOOGLE_PAY','BANK_TRANSFER')
        THEN payment_type_raw
      ELSE 'OTHER'
    END AS payment_type,
    transaction_status,
    bank_name,
    -- FX Rate (static demo rates)
    CASE currency
      WHEN 'USD' THEN 1.53
      WHEN 'EUR' THEN 1.65
      WHEN 'GBP' THEN 1.90
      WHEN 'JPY' THEN 0.013
      WHEN 'CAD' THEN 1.12
      WHEN 'AUD' THEN 1.00
    END AS fx_rate,
    load_timestamp
  FROM standardized
)
SELECT
  transaction_id,
  merchant_id,
  customer_id,
  transaction_date,
  amount_usd,
  currency,
  ROUND(amount_usd * fx_rate, 2) AS amount_aud,
  fx_rate,
  payment_type,
  transaction_status,
  -- Fraud flag
  (transaction_status = 'FRAUD_DETECTED') AS is_fraud,
  -- Failure category
  CASE
    WHEN transaction_status = 'FRAUD_DETECTED' THEN 'FRAUD'
    WHEN transaction_status = 'TIMED_OUT' THEN 'TIMEOUT'
    WHEN transaction_status = 'ERROR' THEN 'SYSTEM_ERROR'
    WHEN transaction_status = 'DECLINED' THEN 'DECLINED'
    WHEN transaction_status = 'APPROVED' THEN 'NONE'
    ELSE 'OTHER'
  END AS failure_category,
  bank_name,
  load_timestamp
FROM with_fx;


CREATE OR REPLACE TABLE silver.fact_transaction_rejects AS
WITH deduped AS (
  SELECT *
  FROM (
    SELECT t.*,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY load_timestamp DESC) AS rn
    FROM bronze.transaction_data t
  )
  WHERE rn = 1
),
all_with_validation AS (
  SELECT
    *,
    UPPER(TRIM(REGEXP_REPLACE(currency, '[^A-Z]', ''))) AS currency_std,
    UPPER(TRIM(transaction_status)) AS status_std
  FROM deduped
)
SELECT
  transaction_id, merchant_id, customer_id, transaction_date,
  amount_usd, currency, payment_type, transaction_status, bank_name, load_timestamp,
  CASE
    WHEN transaction_id IS NULL THEN 'TXN_ID_NULL'
    WHEN merchant_id IS NULL THEN 'MERCHANT_ID_NULL'
    WHEN customer_id IS NULL THEN 'CUSTOMER_ID_NULL'
    WHEN amount_usd IS NULL THEN 'AMOUNT_NULL'
    WHEN amount_usd <= 0 THEN 'AMOUNT_INVALID'
    WHEN currency_std NOT IN ('USD','EUR','GBP','JPY','CAD','AUD') THEN 'CURRENCY_INVALID'
    WHEN status_std NOT IN ('APPROVED','DECLINED','TIMED_OUT','FRAUD_DETECTED','ERROR') THEN 'STATUS_INVALID'
    ELSE 'OTHER'
  END AS reject_reason
FROM all_with_validation
WHERE transaction_id IS NULL
   OR merchant_id IS NULL
   OR customer_id IS NULL
   OR amount_usd IS NULL
   OR amount_usd <= 0
   OR currency_std NOT IN ('USD','EUR','GBP','JPY','CAD','AUD')
   OR status_std NOT IN ('APPROVED','DECLINED','TIMED_OUT','FRAUD_DETECTED','ERROR');


-- 4. TRANSACTION LOG CLEANING



CREATE OR REPLACE TEMP VIEW log_issues_analysis AS
WITH log_deduped AS (
  SELECT *
  FROM (
    SELECT l.*,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY event_time DESC, load_timestamp DESC) AS rn
    FROM bronze.transaction_log l
  )
  WHERE rn = 1
)
SELECT 'log_bronze_total' AS metric, COUNT(*) AS value FROM bronze.transaction_log
UNION ALL SELECT 'log_distinct_txn_ids', COUNT(DISTINCT transaction_id) FROM bronze.transaction_log
UNION ALL SELECT 'log_deduped_count', COUNT(*) FROM log_deduped
UNION ALL SELECT 'log_id_null', COUNT(*) FROM log_deduped WHERE log_id IS NULL
UNION ALL SELECT 'log_txn_id_null', COUNT(*) FROM log_deduped WHERE transaction_id IS NULL
UNION ALL SELECT 'log_orphaned', COUNT(*) FROM log_deduped l 
  WHERE NOT EXISTS (SELECT 1 FROM silver.fact_transaction t WHERE t.transaction_id = l.transaction_id);

-- View issues
SELECT * FROM log_issues_analysis ORDER BY metric;


CREATE OR REPLACE TABLE silver.dim_transaction_log AS
WITH valid_logs AS (
  -- Only keep logs for transactions that made it to clean fact table
  SELECT l.*
  FROM bronze.transaction_log l
  INNER JOIN silver.fact_transaction t
    ON t.transaction_id = l.transaction_id
  WHERE l.log_id IS NOT NULL
    AND l.transaction_id IS NOT NULL
),
deduped AS (
  -- Keep only one log per transaction (latest event)
  SELECT *
  FROM (
    SELECT
      l.*,
      ROW_NUMBER() OVER (
        PARTITION BY l.transaction_id 
        ORDER BY l.event_time DESC, l.load_timestamp DESC
      ) AS rn
    FROM valid_logs l
  )
  WHERE rn = 1
),
standardized AS (
  SELECT
    log_id,
    transaction_id,
    event_time,
    -- Standardize event type
    CASE
      WHEN UPPER(TRIM(event_type)) IN ('TRANSACTION_START','VALIDATION','PROCESSING','COMPLETION','ERROR','FRAUD_DETECTION')
        THEN UPPER(TRIM(event_type))
      ELSE 'OTHER'
    END AS event_type,
    -- Standardize error code
    CASE
      WHEN error_code IS NULL OR TRIM(error_code) = '' THEN 'NONE'
      ELSE UPPER(TRIM(error_code))
    END AS error_code,
    -- Standardize error message
    CASE
      WHEN error_message IS NULL OR TRIM(error_message) = '' THEN 'NONE'
      ELSE TRIM(error_message)
    END AS error_message,
    UPPER(TRIM(system_component)) AS system_component,
    load_timestamp
  FROM deduped
)
SELECT
  log_id,
  transaction_id,
  event_time,
  event_type,
  error_code,
  error_message,
  system_component,
  load_timestamp
FROM standardized;


-- 5. DATA QUALITY SUMMARY


CREATE OR REPLACE VIEW silver.data_quality_summary AS
SELECT 'CUSTOMERS' AS table_name, 'Bronze Total' AS metric, COUNT(*) AS value 
FROM bronze.customer
UNION ALL SELECT 'CUSTOMERS', 'Silver Clean', COUNT(*) FROM silver.dim_customer
UNION ALL SELECT 'CUSTOMERS', 'Silver Rejects', COUNT(*) FROM silver.dim_customer_rejects
UNION ALL SELECT 'CUSTOMERS', 'Clean %', 
  ROUND(100.0 * (SELECT COUNT(*) FROM silver.dim_customer) / NULLIF((SELECT COUNT(*) FROM bronze.customer), 0), 2)

UNION ALL SELECT 'MERCHANTS', 'Bronze Total', COUNT(*) FROM bronze.merchant
UNION ALL SELECT 'MERCHANTS', 'Silver Clean', COUNT(*) FROM silver.dim_merchant
UNION ALL SELECT 'MERCHANTS', 'Silver Rejects', COUNT(*) FROM silver.dim_merchant_rejects
UNION ALL SELECT 'MERCHANTS', 'Clean %', 
  ROUND(100.0 * (SELECT COUNT(*) FROM silver.dim_merchant) / NULLIF((SELECT COUNT(*) FROM bronze.merchant), 0), 2)

UNION ALL SELECT 'TRANSACTIONS', 'Bronze Total', COUNT(*) FROM bronze.transaction_data
UNION ALL SELECT 'TRANSACTIONS', 'Silver Clean', COUNT(*) FROM silver.fact_transaction
UNION ALL SELECT 'TRANSACTIONS', 'Silver Rejects', COUNT(*) FROM silver.fact_transaction_rejects
UNION ALL SELECT 'TRANSACTIONS', 'Clean %', 
  ROUND(100.0 * (SELECT COUNT(*) FROM silver.fact_transaction) / NULLIF((SELECT COUNT(*) FROM bronze.transaction_data), 0), 2)

UNION ALL SELECT 'TRANSACTION_LOGS', 'Bronze Total', COUNT(*) FROM bronze.transaction_log
UNION ALL SELECT 'TRANSACTION_LOGS', 'Silver Clean', COUNT(*) FROM silver.dim_transaction_log
UNION ALL SELECT 'TRANSACTION_LOGS', 'Orphaned (removed)', 
  (SELECT COUNT(*) FROM bronze.transaction_log) - (SELECT COUNT(*) FROM silver.dim_transaction_log)
UNION ALL SELECT 'TRANSACTION_LOGS', 'Clean %', 
  ROUND(100.0 * (SELECT COUNT(*) FROM silver.dim_transaction_log) / NULLIF((SELECT COUNT(*) FROM bronze.transaction_log), 0), 2)

ORDER BY table_name, metric;


-- 6. CREATE FINAL VIEWS


CREATE OR REPLACE VIEW silver.v_dim_customer AS
SELECT * FROM silver.dim_customer;

CREATE OR REPLACE VIEW silver.v_dim_merchant AS
SELECT * FROM silver.dim_merchant;

CREATE OR REPLACE VIEW silver.v_fact_transaction AS
SELECT * FROM silver.fact_transaction;

CREATE OR REPLACE VIEW silver.v_dim_transaction_log AS
SELECT * FROM silver.dim_transaction_log;


-- 7. VALIDATION QUERIES


-- View overall data quality
SELECT * FROM silver.data_quality_summary;

-- Sample clean data
SELECT 'CUSTOMERS' AS entity, COUNT(*) AS count FROM silver.dim_customer
UNION ALL SELECT 'MERCHANTS', COUNT(*) FROM silver.dim_merchant
UNION ALL SELECT 'TRANSACTIONS', COUNT(*) FROM silver.fact_transaction
UNION ALL SELECT 'LOGS', COUNT(*) FROM silver.dim_transaction_log;

-- Check referential integrity
SELECT 
  'Transactions with invalid merchant_id' AS check_type,
  COUNT(*) AS count
FROM silver.fact_transaction t
LEFT JOIN silver.dim_merchant m ON t.merchant_id = m.merchant_id
WHERE m.merchant_id IS NULL

UNION ALL

SELECT 
  'Transactions with invalid customer_id',
  COUNT(*)
FROM silver.fact_transaction t
LEFT JOIN silver.dim_customer c ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL

UNION ALL

SELECT 
  'Logs without matching transaction',
  COUNT(*)
FROM silver.dim_transaction_log l
LEFT JOIN silver.fact_transaction t ON l.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Fraud summary
SELECT 
  is_fraud,
  failure_category,
  COUNT(*) AS transaction_count,
  SUM(amount_aud) AS total_amount_aud,
  AVG(amount_aud) AS avg_amount_aud
FROM silver.fact_transaction
GROUP BY is_fraud, failure_category
ORDER BY is_fraud DESC, transaction_count DESC;



select * from silver.fact_transaction where transaction_status ='FRAUD_DETECTED' ;