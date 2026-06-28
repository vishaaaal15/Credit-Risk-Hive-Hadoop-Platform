-- =============================================================================
-- FILE: 02_hive_credit_risk_queries.hql
-- PROJECT: Credit Risk Big Data Platform — Hive on Hadoop
-- AUTHOR: Vishal Singh
-- PURPOSE: 25 production-grade Hive risk monitoring queries
--          Covers KRI monitoring, default analysis, AML detection,
--          portfolio concentration, and micro-segment risk profiling
-- =============================================================================

USE credit_risk_db;
SET hive.exec.parallel=true;
SET hive.auto.convert.join=true;
SET mapreduce.job.reduces=10;

-- =============================================================================
-- SECTION A: PORTFOLIO OVERVIEW & CONCENTRATION RISK
-- =============================================================================

-- HQ-01: Total portfolio exposure by loan type
SELECT
    loan_type,
    COUNT(*)                                AS total_loans,
    SUM(loan_amount)                        AS total_exposure_inr,
    ROUND(AVG(loan_amount),2)               AS avg_loan_amount,
    ROUND(SUM(loan_amount)*100.0/
          SUM(SUM(loan_amount)) OVER(),2)   AS portfolio_share_pct
FROM loan_applications
GROUP BY loan_type
ORDER BY total_exposure_inr DESC;

-- HQ-02: Geographic concentration risk (state-level exposure)
SELECT
    l.state,
    COUNT(DISTINCT l.customer_id)           AS unique_borrowers,
    COUNT(l.loan_id)                        AS total_loans,
    SUM(l.loan_amount)                      AS exposure_inr,
    ROUND(AVG(c.credit_score),1)            AS avg_credit_score,
    ROUND(SUM(l.loan_amount)*100.0/
          SUM(SUM(l.loan_amount)) OVER(),2) AS exposure_share_pct
FROM loan_applications l
JOIN customer_profiles c ON l.customer_id = c.customer_id
GROUP BY l.state
ORDER BY exposure_inr DESC;

-- HQ-03: Segment-level risk concentration (identifies over-exposure)
SELECT
    c.segment,
    c.employment_type,
    COUNT(l.loan_id)        AS loan_count,
    SUM(l.loan_amount)      AS total_exposure,
    ROUND(AVG(c.credit_score),0) AS avg_credit_score,
    SUM(CASE WHEN l.loan_status IN ('Default','NPA','Written-Off') THEN 1 ELSE 0 END)
                            AS stressed_accounts,
    ROUND(SUM(CASE WHEN l.loan_status IN ('Default','NPA','Written-Off') THEN 1.0 ELSE 0 END)
          / COUNT(*) * 100, 2) AS stress_rate_pct
FROM loan_applications l
JOIN customer_profiles c ON l.customer_id = c.customer_id
GROUP BY c.segment, c.employment_type
ORDER BY stress_rate_pct DESC;

-- =============================================================================
-- SECTION B: DEFAULT & NPA RISK INDICATORS (KRIs)
-- =============================================================================

-- HQ-04: KRI-1 — Gross NPA Rate by loan type (board-level metric)
SELECT
    loan_type,
    COUNT(*) AS total_loans,
    SUM(CASE WHEN loan_status = 'NPA' THEN loan_amount ELSE 0 END)   AS npa_amount,
    SUM(loan_amount)                                                   AS total_amount,
    ROUND(SUM(CASE WHEN loan_status='NPA' THEN loan_amount ELSE 0 END)*100.0
          / SUM(loan_amount),2)                                        AS gross_npa_rate_pct
FROM loan_applications
GROUP BY loan_type
ORDER BY gross_npa_rate_pct DESC;

-- HQ-05: KRI-2 — DPD bucket analysis (Days Past Due — early warning signal)
SELECT
    CASE
        WHEN dpd_days = 0         THEN 'SMA-0 (Current)'
        WHEN dpd_days <= 30       THEN 'SMA-1 (1-30 DPD)'
        WHEN dpd_days <= 60       THEN 'SMA-2 (31-60 DPD)'
        WHEN dpd_days <= 90       THEN 'SMA-3 (61-90 DPD)'
        WHEN dpd_days <= 180      THEN 'Sub-Standard (91-180 DPD)'
        ELSE                           'Doubtful (180+ DPD)'
    END                     AS dpd_bucket,
    COUNT(*)                AS accounts,
    SUM(loan_amount)        AS exposure_inr,
    ROUND(AVG(loan_amount)) AS avg_exposure
FROM loan_applications
WHERE loan_status = 'Active'
GROUP BY
    CASE
        WHEN dpd_days = 0    THEN 'SMA-0 (Current)'
        WHEN dpd_days <= 30  THEN 'SMA-1 (1-30 DPD)'
        WHEN dpd_days <= 60  THEN 'SMA-2 (31-60 DPD)'
        WHEN dpd_days <= 90  THEN 'SMA-3 (61-90 DPD)'
        WHEN dpd_days <= 180 THEN 'Sub-Standard (91-180 DPD)'
        ELSE                      'Doubtful (180+ DPD)'
    END
ORDER BY exposure_inr DESC;

-- HQ-06: KRI-3 — Default rate trend over disbursement years (vintage analysis)
SELECT
    disbursement_year,
    COUNT(*)                AS loans_disbursed,
    SUM(CASE WHEN loan_status IN ('Default','NPA','Written-Off') THEN 1 ELSE 0 END)
                            AS stressed_accounts,
    ROUND(SUM(CASE WHEN loan_status IN ('Default','NPA','Written-Off') THEN 1.0 ELSE 0 END)
          / COUNT(*) * 100, 2)  AS default_rate_pct,
    SUM(loan_amount)        AS total_disbursed_inr
FROM loan_applications
GROUP BY disbursement_year
ORDER BY disbursement_year;

-- HQ-07: High-risk borrower micro-segment (credit score < 600 + high DPD)
SELECT
    c.segment,
    c.employment_type,
    c.state,
    COUNT(l.loan_id)            AS high_risk_accounts,
    SUM(l.loan_amount)          AS at_risk_exposure,
    ROUND(AVG(c.credit_score))  AS avg_credit_score,
    ROUND(AVG(l.dpd_days))      AS avg_dpd
FROM loan_applications l
JOIN customer_profiles c ON l.customer_id = c.customer_id
WHERE c.credit_score < 600
  AND l.dpd_days > 30
  AND l.loan_status = 'Active'
GROUP BY c.segment, c.employment_type, c.state
HAVING COUNT(l.loan_id) >= 5
ORDER BY at_risk_exposure DESC
LIMIT 20;

-- HQ-08: KRI-4 — Credit score band distribution across portfolio
SELECT
    CASE
        WHEN credit_score >= 800 THEN 'Excellent (800+)'
        WHEN credit_score >= 750 THEN 'Very Good (750-799)'
        WHEN credit_score >= 700 THEN 'Good (700-749)'
        WHEN credit_score >= 650 THEN 'Fair (650-699)'
        WHEN credit_score >= 600 THEN 'Below Average (600-649)'
        ELSE                          'Poor (<600)'
    END                 AS score_band,
    COUNT(*)            AS customers,
    ROUND(COUNT(*)*100.0/(SELECT COUNT(*) FROM customer_profiles),2) AS pct_of_portfolio
FROM customer_profiles
GROUP BY
    CASE
        WHEN credit_score >= 800 THEN 'Excellent (800+)'
        WHEN credit_score >= 750 THEN 'Very Good (750-799)'
        WHEN credit_score >= 700 THEN 'Good (700-749)'
        WHEN credit_score >= 650 THEN 'Fair (650-699)'
        WHEN credit_score >= 600 THEN 'Below Average (600-649)'
        ELSE                          'Poor (<600)'
    END
ORDER BY customers DESC;

-- =============================================================================
-- SECTION C: AML & FRAUD DETECTION
-- =============================================================================

-- HQ-09: AML Rule — Large cash transactions (structuring detection)
SELECT
    t.loan_id,
    l.customer_id,
    c.state,
    c.segment,
    COUNT(*)                        AS suspicious_txn_count,
    SUM(t.amount)                   AS total_suspicious_amount,
    MAX(t.amount)                   AS max_single_txn,
    COUNT(DISTINCT t.transaction_month) AS months_active
FROM transactions t
JOIN loan_applications l  ON t.loan_id = l.loan_id
JOIN customer_profiles c  ON l.customer_id = c.customer_id
WHERE t.is_suspicious_flag = 1
  AND t.transaction_type = 'Cash_Withdrawal'
  AND t.amount > 100000
GROUP BY t.loan_id, l.customer_id, c.state, c.segment
HAVING COUNT(*) >= 3
ORDER BY total_suspicious_amount DESC
LIMIT 50;

-- HQ-10: AML Rule — Off-hours transaction spike (11PM–5AM activity)
SELECT
    transaction_hour,
    COUNT(*)                AS txn_count,
    ROUND(SUM(amount),2)    AS total_amount,
    SUM(is_suspicious_flag) AS suspicious_count,
    ROUND(SUM(is_suspicious_flag)*100.0/COUNT(*),2) AS suspicion_rate_pct
FROM transactions
WHERE transaction_hour BETWEEN 23 AND 5
   OR transaction_hour IN (23,0,1,2,3,4,5)
GROUP BY transaction_hour
ORDER BY suspicion_rate_pct DESC;

-- HQ-11: AML Rule — Reversal pattern detection (round-trip fraud indicator)
SELECT
    loan_id,
    COUNT(*)                        AS total_txns,
    SUM(reversal_flag)              AS reversal_count,
    ROUND(SUM(reversal_flag)*100.0/COUNT(*),2) AS reversal_rate_pct,
    SUM(amount)                     AS gross_volume
FROM transactions
GROUP BY loan_id
HAVING reversal_rate_pct > 15 AND total_txns >= 10
ORDER BY reversal_rate_pct DESC
LIMIT 30;

-- HQ-12: Channel risk heatmap (which channels generate most suspicious activity)
SELECT
    channel,
    COUNT(*)                AS total_transactions,
    SUM(is_suspicious_flag) AS suspicious_transactions,
    ROUND(AVG(amount),2)    AS avg_txn_amount,
    ROUND(SUM(is_suspicious_flag)*100.0/COUNT(*),2) AS suspicion_rate_pct
FROM transactions
GROUP BY channel
ORDER BY suspicion_rate_pct DESC;

-- =============================================================================
-- SECTION D: WINDOW FUNCTIONS & ADVANCED ANALYTICS
-- =============================================================================

-- HQ-13: Rolling 3-month default trend (window function)
SELECT
    disbursement_year,
    disbursement_month,
    COUNT(*)                AS new_loans,
    SUM(CASE WHEN loan_status IN ('Default','NPA') THEN 1 ELSE 0 END) AS defaults,
    ROUND(AVG(SUM(CASE WHEN loan_status IN ('Default','NPA') THEN 1.0 ELSE 0 END))
          OVER (ORDER BY disbursement_year, disbursement_month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) AS rolling_3m_avg_defaults
FROM loan_applications
GROUP BY disbursement_year, disbursement_month
ORDER BY disbursement_year, disbursement_month;

-- HQ-14: Customer risk ranking using RANK() window function
SELECT
    customer_id,
    total_exposure,
    credit_score,
    default_count,
    dpd_avg,
    RANK() OVER (ORDER BY total_exposure DESC)  AS exposure_rank,
    RANK() OVER (ORDER BY credit_score ASC)     AS risk_rank,
    NTILE(10) OVER (ORDER BY total_exposure DESC) AS exposure_decile
FROM (
    SELECT
        l.customer_id,
        SUM(l.loan_amount)          AS total_exposure,
        AVG(c.credit_score)         AS credit_score,
        SUM(CASE WHEN l.loan_status IN ('Default','NPA','Written-Off') THEN 1 ELSE 0 END)
                                    AS default_count,
        AVG(l.dpd_days)             AS dpd_avg
    FROM loan_applications l
    JOIN customer_profiles c ON l.customer_id = c.customer_id
    GROUP BY l.customer_id
) t
ORDER BY risk_rank
LIMIT 100;

-- HQ-15: Vintage cohort default rates (LAG comparison YoY)
SELECT
    disbursement_year,
    default_rate_pct,
    LAG(default_rate_pct,1) OVER (ORDER BY disbursement_year) AS prev_year_rate,
    ROUND(default_rate_pct -
          LAG(default_rate_pct,1) OVER (ORDER BY disbursement_year),2) AS yoy_change_pp
FROM (
    SELECT
        disbursement_year,
        ROUND(SUM(CASE WHEN loan_status IN ('Default','NPA','Written-Off') THEN 1.0 ELSE 0 END)
              / COUNT(*) * 100,2) AS default_rate_pct
    FROM loan_applications
    GROUP BY disbursement_year
) t
ORDER BY disbursement_year;

-- =============================================================================
-- SECTION E: KRI SUMMARY DASHBOARD FEED
-- =============================================================================

-- HQ-16: Master KRI summary table (feeds executive Power BI dashboard)
SELECT
    'Total Portfolio Exposure (INR)'    AS kri_name,
    CAST(SUM(loan_amount) AS STRING)    AS kri_value,
    'INR'                               AS unit
FROM loan_applications
UNION ALL
SELECT 'Total Active Loans', CAST(COUNT(*) AS STRING), 'Count'
FROM loan_applications WHERE loan_status = 'Active'
UNION ALL
SELECT 'Gross NPA Rate %',
       CAST(ROUND(SUM(CASE WHEN loan_status='NPA' THEN loan_amount ELSE 0 END)*100.0/SUM(loan_amount),2) AS STRING),
       'Percentage'
FROM loan_applications
UNION ALL
SELECT 'Accounts with DPD > 90',
       CAST(COUNT(*) AS STRING), 'Count'
FROM loan_applications WHERE dpd_days > 90 AND loan_status='Active'
UNION ALL
SELECT 'Suspicious Transactions Flagged',
       CAST(SUM(is_suspicious_flag) AS STRING), 'Count'
FROM transactions
UNION ALL
SELECT 'Avg Portfolio Credit Score',
       CAST(ROUND(AVG(credit_score),0) AS STRING), 'Score'
FROM customer_profiles;
