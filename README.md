# Credit Risk Big Data Platform — Hive on Hadoop

**Author:** Vishal Singh  
**Stack:** Apache Hive · Hadoop (HDFS) · Python (pandas, numpy) · Power BI  
**Dataset:** 230,000 synthetic records across 3 tables (50K loans, 30K customers, 150K transactions)  
**Portfolio Exposure Modelled:** ₹126.33B

---

## Project Overview

End-to-end big data credit risk analytics platform simulating a bank's Hadoop data lake architecture. Raw financial data lands in **HDFS**, is queryable via **Hive external/partitioned tables**, and produces KRI outputs consumed by executive dashboards.

Designed to replicate the analytical stack used by GRC/Risk teams at large financial institutions (Hadoop + Hive + Python + BI layer).

---

## Architecture

```
HDFS Data Lake (raw CSVs)
    │
    ├── /data/credit_risk/customer_profiles/     ← 30,000 rows
    ├── /data/credit_risk/loan_applications/     ← 50,000 rows (partitioned by year)
    └── /data/credit_risk/transactions/          ← 150,000 rows
         │
         ▼
Hive Metastore (External Tables + ORC optimized)
         │
         ▼
16 Risk Monitoring HQL Queries (DDL + Analytics)
         │
         ▼
KRI Outputs → Power BI Executive Dashboards
```

---

## Files

```
hive_credit_risk/
│
├── data/
│   ├── generate_data.py          # Generates all 3 datasets
│   ├── customer_profiles.csv     # 30,000 customer records
│   ├── loan_applications.csv     # 50,000 loan records
│   └── transactions.csv          # 150,000 transaction records
│
├── hive_queries/
│   ├── 01_hive_ddl_setup.hql     # Database + table creation (partitioned, ORC)
│   └── 02_hive_credit_risk_queries.hql  # 16 risk monitoring HQL queries
│
├── python_analysis/
│   └── hive_simulation.py        # Pandas simulation of Hive query outputs
│
└── outputs/
    ├── HQ01_portfolio_by_loan_type.csv
    ├── HQ04_npa_rate_by_loan_type.csv
    ├── HQ05_dpd_bucket_analysis.csv
    ├── HQ06_vintage_default_trend.csv
    ├── HQ08_credit_score_distribution.csv
    ├── HQ09_aml_suspicious_accounts.csv
    ├── HQ12_channel_risk_heatmap.csv
    └── HQ16_kri_master_summary.csv
```

---

## Key Risk Findings (from actual query outputs)

| KRI | Value | Status |
|-----|-------|--------|
| Total Portfolio Exposure | ₹126.33B | MONITOR |
| Gross NPA Rate | 4.30% | 🔴 ALERT |
| Accounts DPD > 90 Days | 1,633 | 🔴 ALERT |
| Credit Card NPA Rate | 4.89% (highest) | 🔴 ALERT |
| Avg Portfolio Credit Score | 679 | NORMAL |
| Suspicious Transactions | 6 flagged | REVIEW |
| Written-Off Accounts | 883 | 🔴 ALERT |
| Restructured Loans | 2,618 | MONITOR |

---

## Hive Concepts Demonstrated

- External tables with HDFS `LOCATION`
- Partitioned tables (`PARTITIONED BY disbursement_year`)
- ORC + Snappy compression for analytics performance
- `MSCK REPAIR TABLE` for partition discovery
- Window functions: `RANK()`, `LAG()`, `NTILE()`, `OVER()`
- Subqueries and CTEs
- AML rule-based detection via SQL predicates
- DPD bucket analysis using `CASE WHEN`
- Vintage cohort analysis with YoY `LAG()` comparison
- Multi-table JOINs (3-way: loans + customers + transactions)
- `UNION ALL` for KRI master feed

---

## How to Run on a Real Hadoop Cluster

```bash
# 1. Copy data to HDFS
hdfs dfs -mkdir -p /data/credit_risk/customer_profiles/
hdfs dfs -put data/customer_profiles.csv /data/credit_risk/customer_profiles/
hdfs dfs -put data/loan_applications.csv /data/credit_risk/loan_applications/
hdfs dfs -put data/transactions.csv /data/credit_risk/transactions/

# 2. Run Hive DDL
hive -f hive_queries/01_hive_ddl_setup.hql

# 3. Run risk queries
hive -f hive_queries/02_hive_credit_risk_queries.hql

# 4. Or simulate locally
python python_analysis/hive_simulation.py
```

---

## Skills Demonstrated
`Apache Hive` `Hadoop HDFS` `HQL` `Big Data Processing` `Partitioned Tables` `ORC/Snappy` `Window Functions` `Credit Risk Analytics` `AML Detection` `KRI Development` `Python` `pandas` `Data Lake Architecture`
