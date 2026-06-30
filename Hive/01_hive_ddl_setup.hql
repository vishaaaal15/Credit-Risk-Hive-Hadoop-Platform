-- =============================================================================
-- FILE: 01_hive_ddl_setup.hql
-- PROJECT: Credit Risk Big Data Platform — Hive on Hadoop
-- AUTHOR: Vishal Singh
-- PURPOSE: Create Hive external tables over HDFS data files
--          Simulates a real bank data lake architecture
-- =============================================================================

-- Create dedicated database
CREATE DATABASE IF NOT EXISTS credit_risk_db
COMMENT 'Credit Risk Big Data Platform — AmEx GRC Analytics'
LOCATION '/user/hive/warehouse/credit_risk_db.db';

USE credit_risk_db;

-- -----------------------------------------------------------------------------
-- TABLE 1: CUSTOMER PROFILES (External table — raw HDFS landing zone)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS customer_profiles (
    customer_id       STRING        COMMENT 'Unique customer identifier',
    age               INT           COMMENT 'Customer age in years',
    state             STRING        COMMENT 'Home state',
    employment_type   STRING        COMMENT 'Salaried / Self-Employed / Business / Freelancer / Government',
    segment           STRING        COMMENT 'Risk segment: Retail / SME / Corporate / Premium / Mass',
    monthly_income    BIGINT        COMMENT 'Monthly income in INR',
    credit_score      INT           COMMENT 'Bureau credit score 300–900',
    years_with_bank   INT           COMMENT 'Customer tenure in years',
    existing_loans    INT           COMMENT 'Number of live loans at onboarding',
    onboarding_year   INT           COMMENT 'Year customer joined the bank'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/credit_risk/customer_profiles/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- -----------------------------------------------------------------------------
-- TABLE 2: LOAN APPLICATIONS (Partitioned by year for performance)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS loan_applications (
    loan_id               STRING   COMMENT 'Unique loan identifier',
    customer_id           STRING   COMMENT 'FK to customer_profiles',
    loan_type             STRING   COMMENT 'Personal / Home / Vehicle / Business / Education / Credit Card',
    loan_amount           BIGINT   COMMENT 'Sanctioned loan amount in INR',
    loan_tenure_months    INT      COMMENT 'Repayment tenure in months',
    interest_rate         DOUBLE   COMMENT 'Annual interest rate %',
    loan_status           STRING   COMMENT 'Active / Closed / Default / NPA / Written-Off / Restructured',
    disbursement_month    INT      COMMENT 'Month of loan disbursement',
    emi_amount            DOUBLE   COMMENT 'Monthly EMI in INR',
    dpd_days              INT      COMMENT 'Days Past Due — key default indicator',
    state                 STRING   COMMENT 'Loan origination state',
    branch_code           STRING   COMMENT 'Originating branch'
)
PARTITIONED BY (disbursement_year INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/credit_risk/loan_applications/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Load partitions (Hive auto-discover)
MSCK REPAIR TABLE loan_applications;

-- -----------------------------------------------------------------------------
-- TABLE 3: TRANSACTIONS (ORC format for analytics performance)
-- -----------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS transactions_raw (
    transaction_id      STRING   COMMENT 'Unique transaction identifier',
    loan_id             STRING   COMMENT 'FK to loan_applications',
    transaction_type    STRING   COMMENT 'EMI_Payment / Cash_Withdrawal / UPI_Transfer etc.',
    amount              DOUBLE   COMMENT 'Transaction amount in INR',
    channel             STRING   COMMENT 'Mobile_App / Net_Banking / ATM / Branch / POS / UPI',
    transaction_year    INT      COMMENT 'Year of transaction',
    transaction_month   INT      COMMENT 'Month of transaction',
    transaction_hour    INT      COMMENT 'Hour of day (0-23)',
    is_suspicious_flag  INT      COMMENT '1 = flagged by rule engine, 0 = clean',
    reversal_flag       INT      COMMENT '1 = reversed transaction',
    state               STRING   COMMENT 'Transaction state'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/credit_risk/transactions/'
TBLPROPERTIES ('skip.header.line.count'='1');

-- Optimized ORC version for fast analytics
CREATE TABLE IF NOT EXISTS transactions
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY')
AS SELECT * FROM transactions_raw;

-- -----------------------------------------------------------------------------
-- VERIFICATION
-- -----------------------------------------------------------------------------
SHOW TABLES IN credit_risk_db;
DESCRIBE FORMATTED loan_applications;
