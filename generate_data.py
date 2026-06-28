"""
Data Generator — Credit Risk Big Data Platform
Generates 3 datasets simulating a bank's raw data lake (HDFS-style CSV files):
  1. loan_applications.csv   — 50,000 rows
  2. transactions.csv        — 150,000 rows
  3. customer_profiles.csv   — 30,000 rows
These are the "raw files" you would land in HDFS and query with Hive.
"""

import pandas as pd
import numpy as np
from faker import Faker
import random, os

fake = Faker('en_IN')
np.random.seed(42)
random.seed(42)

OUT = os.path.dirname(__file__)

# ── 1. CUSTOMER PROFILES ────────────────────────────────────────────────────
N_CUST = 30_000
states = ['Maharashtra','Delhi','Karnataka','Tamil Nadu','Gujarat',
          'Telangana','West Bengal','Rajasthan','UP','Punjab']
employment = ['Salaried','Self-Employed','Business Owner','Freelancer','Government']
segments   = ['Retail','SME','Corporate','Premium','Mass']

cust = pd.DataFrame({
    'customer_id':     [f'CUST{str(i).zfill(6)}' for i in range(1, N_CUST+1)],
    'age':             np.random.randint(22, 65, N_CUST),
    'state':           np.random.choice(states, N_CUST),
    'employment_type': np.random.choice(employment, N_CUST, p=[0.50,0.20,0.15,0.08,0.07]),
    'segment':         np.random.choice(segments, N_CUST, p=[0.40,0.25,0.15,0.10,0.10]),
    'monthly_income':  np.random.randint(15000, 250000, N_CUST),
    'credit_score':    np.clip(np.random.normal(680, 90, N_CUST).astype(int), 300, 900),
    'years_with_bank': np.random.randint(0, 20, N_CUST),
    'existing_loans':  np.random.randint(0, 5, N_CUST),
    'onboarding_year': np.random.randint(2005, 2024, N_CUST),
})
cust.to_csv(f'{OUT}/customer_profiles.csv', index=False)
print(f"customer_profiles.csv: {len(cust):,} rows")

# ── 2. LOAN APPLICATIONS ────────────────────────────────────────────────────
N_LOANS = 50_000
loan_types   = ['Personal','Home','Vehicle','Business','Education','Credit Card']
loan_status  = ['Active','Closed','Default','NPA','Written-Off','Restructured']

cust_ids = cust['customer_id'].tolist()
credit_scores = dict(zip(cust['customer_id'], cust['credit_score']))
incomes       = dict(zip(cust['customer_id'], cust['monthly_income']))

loan_cust = np.random.choice(cust_ids, N_LOANS)
loan_amounts = np.random.randint(50000, 5000000, N_LOANS)

# Default probability inversely related to credit score
def default_prob(cid):
    cs = credit_scores.get(cid, 650)
    return max(0.02, min(0.45, (850 - cs) / 1000))

statuses = []
for cid in loan_cust:
    p = default_prob(cid)
    s = np.random.choice(loan_status,
        p=[0.50, 0.25, p*0.6, p*0.25, p*0.1, max(0.01, 0.05-p*0.05)])
    # normalise
    probs = [0.50, 0.25, p*0.6, p*0.25, p*0.1, max(0.01,0.05-p*0.05)]
    total = sum(probs)
    probs = [x/total for x in probs]
    statuses.append(np.random.choice(loan_status, p=probs))

disbursement_years = np.random.randint(2018, 2025, N_LOANS)
disbursement_months = np.random.randint(1, 13, N_LOANS)

loans = pd.DataFrame({
    'loan_id':            [f'LN{str(i).zfill(7)}' for i in range(1, N_LOANS+1)],
    'customer_id':        loan_cust,
    'loan_type':          np.random.choice(loan_types, N_LOANS, p=[0.30,0.25,0.15,0.15,0.10,0.05]),
    'loan_amount':        loan_amounts,
    'loan_tenure_months': np.random.choice([12,24,36,48,60,84,120,180,240], N_LOANS),
    'interest_rate':      np.round(np.random.uniform(7.5, 24.0, N_LOANS), 2),
    'loan_status':        statuses,
    'disbursement_year':  disbursement_years,
    'disbursement_month': disbursement_months,
    'emi_amount':         np.round(loan_amounts / np.random.randint(12, 241, N_LOANS), 2),
    'dpd_days':           np.where(np.array(statuses) == 'Active',
                                   np.random.choice([0,30,60,90,120,180], N_LOANS,
                                                    p=[0.70,0.10,0.08,0.06,0.04,0.02]),
                                   0),
    'state':              np.random.choice(states, N_LOANS),
    'branch_code':        [f'BR{str(np.random.randint(1,500)).zfill(4)}' for _ in range(N_LOANS)],
})
loans.to_csv(f'{OUT}/loan_applications.csv', index=False)
print(f"loan_applications.csv: {len(loans):,} rows")

# ── 3. TRANSACTIONS ─────────────────────────────────────────────────────────
N_TXN = 150_000
txn_types = ['EMI_Payment','Cash_Withdrawal','UPI_Transfer','NEFT','RTGS',
             'Card_Payment','Loan_Disbursement','Interest_Credit','Reversal']
channels  = ['Mobile_App','Net_Banking','ATM','Branch','POS','UPI']

txn_loan_ids = np.random.choice(loans['loan_id'].tolist(), N_TXN)
txn_years    = np.random.randint(2020, 2025, N_TXN)
txn_months   = np.random.randint(1, 13, N_TXN)
txn_amounts  = np.round(np.random.exponential(25000, N_TXN), 2)

# Flag suspicious: large round amounts + odd hours = higher suspicion
is_suspicious = ((txn_amounts > 200000) & (np.random.rand(N_TXN) < 0.15)).astype(int)

txns = pd.DataFrame({
    'transaction_id':    [f'TXN{str(i).zfill(8)}' for i in range(1, N_TXN+1)],
    'loan_id':           txn_loan_ids,
    'transaction_type':  np.random.choice(txn_types, N_TXN,
                          p=[0.35,0.12,0.18,0.10,0.05,0.10,0.04,0.04,0.02]),
    'amount':            txn_amounts,
    'channel':           np.random.choice(channels, N_TXN,
                          p=[0.35,0.25,0.15,0.05,0.10,0.10]),
    'transaction_year':  txn_years,
    'transaction_month': txn_months,
    'transaction_hour':  np.random.randint(0, 24, N_TXN),
    'is_suspicious_flag':is_suspicious,
    'reversal_flag':     np.random.choice([0,1], N_TXN, p=[0.97,0.03]),
    'state':             np.random.choice(states, N_TXN),
})
txns.to_csv(f'{OUT}/transactions.csv', index=False)
print(f"transactions.csv: {len(txns):,} rows")
print("\nAll datasets generated successfully.")
