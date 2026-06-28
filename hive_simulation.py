"""
Python Hive Simulation — Credit Risk Big Data Platform
Executes the same logic as the .hql queries using pandas/numpy
(In production this runs on Hive/Spark on a Hadoop cluster)
Outputs: KRI summary CSV, default trend CSV, AML flags CSV
"""

import pandas as pd
import numpy as np
import os

DATA = '/home/claude/hive_credit_risk/data'
OUT  = '/home/claude/hive_credit_risk/outputs'
os.makedirs(OUT, exist_ok=True)

print("Loading data lake tables...")
loans = pd.read_csv(f'{DATA}/loan_applications.csv')
cust  = pd.read_csv(f'{DATA}/customer_profiles.csv')
txns  = pd.read_csv(f'{DATA}/transactions.csv')
print(f"  loans: {len(loans):,}  |  customers: {len(cust):,}  |  transactions: {len(txns):,}")

# ── MERGE (simulates Hive JOIN) ──────────────────────────────────────────────
lc = loans.merge(cust, on='customer_id', how='left')

# ── HQ-01: Portfolio exposure by loan type ───────────────────────────────────
port = loans.groupby('loan_type').agg(
    total_loans=('loan_id','count'),
    total_exposure=('loan_amount','sum'),
    avg_loan_amount=('loan_amount','mean')
).reset_index()
port['portfolio_share_pct'] = (port['total_exposure']/port['total_exposure'].sum()*100).round(2)
port['avg_loan_amount'] = port['avg_loan_amount'].round(0).astype(int)
port.sort_values('total_exposure', ascending=False, inplace=True)
port.to_csv(f'{OUT}/HQ01_portfolio_by_loan_type.csv', index=False)
print(f"\nHQ-01 Portfolio Exposure:\n{port.to_string(index=False)}")

# ── HQ-04: Gross NPA Rate ────────────────────────────────────────────────────
npa = loans.groupby('loan_type').apply(lambda x: pd.Series({
    'total_loans': len(x),
    'npa_amount':  x.loc[x['loan_status']=='NPA','loan_amount'].sum(),
    'total_amount':x['loan_amount'].sum(),
})).reset_index()
npa['gross_npa_rate_pct'] = (npa['npa_amount']/npa['total_amount']*100).round(2)
npa.sort_values('gross_npa_rate_pct', ascending=False, inplace=True)
npa.to_csv(f'{OUT}/HQ04_npa_rate_by_loan_type.csv', index=False)
print(f"\nHQ-04 Gross NPA Rate:\n{npa[['loan_type','gross_npa_rate_pct','npa_amount']].to_string(index=False)}")

# ── HQ-05: DPD Bucket Analysis ───────────────────────────────────────────────
def dpd_bucket(d):
    if d == 0:    return 'SMA-0 Current'
    elif d <= 30: return 'SMA-1 (1-30 DPD)'
    elif d <= 60: return 'SMA-2 (31-60 DPD)'
    elif d <= 90: return 'SMA-3 (61-90 DPD)'
    elif d <= 180:return 'Sub-Standard (91-180 DPD)'
    else:         return 'Doubtful (180+ DPD)'

active = loans[loans['loan_status']=='Active'].copy()
active['dpd_bucket'] = active['dpd_days'].apply(dpd_bucket)
dpd = active.groupby('dpd_bucket').agg(
    accounts=('loan_id','count'),
    exposure_inr=('loan_amount','sum')
).reset_index().sort_values('exposure_inr', ascending=False)
dpd.to_csv(f'{OUT}/HQ05_dpd_bucket_analysis.csv', index=False)
print(f"\nHQ-05 DPD Bucket Analysis:\n{dpd.to_string(index=False)}")

# ── HQ-06: Vintage Default Trend ─────────────────────────────────────────────
vintage = loans.groupby('disbursement_year').apply(lambda x: pd.Series({
    'loans_disbursed': len(x),
    'stressed': x['loan_status'].isin(['Default','NPA','Written-Off']).sum(),
    'total_disbursed_inr': x['loan_amount'].sum()
})).reset_index()
vintage['default_rate_pct'] = (vintage['stressed']/vintage['loans_disbursed']*100).round(2)
vintage.to_csv(f'{OUT}/HQ06_vintage_default_trend.csv', index=False)
print(f"\nHQ-06 Vintage Default Trend:\n{vintage[['disbursement_year','default_rate_pct','loans_disbursed']].to_string(index=False)}")

# ── HQ-08: Credit Score Band Distribution ────────────────────────────────────
def score_band(s):
    if s>=800: return 'Excellent (800+)'
    elif s>=750: return 'Very Good (750-799)'
    elif s>=700: return 'Good (700-749)'
    elif s>=650: return 'Fair (650-699)'
    elif s>=600: return 'Below Average (600-649)'
    else: return 'Poor (<600)'
cust['score_band'] = cust['credit_score'].apply(score_band)
sb = cust.groupby('score_band').agg(customers=('customer_id','count')).reset_index()
sb['pct_of_portfolio'] = (sb['customers']/len(cust)*100).round(2)
sb.sort_values('customers',ascending=False,inplace=True)
sb.to_csv(f'{OUT}/HQ08_credit_score_distribution.csv', index=False)
print(f"\nHQ-08 Credit Score Bands:\n{sb.to_string(index=False)}")

# ── HQ-09: AML Suspicious Transactions ───────────────────────────────────────
susp = txns[(txns['is_suspicious_flag']==1) &
            (txns['transaction_type']=='Cash_Withdrawal') &
            (txns['amount']>100000)].copy()
aml = susp.groupby('loan_id').agg(
    suspicious_txn_count=('transaction_id','count'),
    total_suspicious_amount=('amount','sum'),
    max_single_txn=('amount','max')
).reset_index()
aml = aml[aml['suspicious_txn_count']>=3].sort_values('total_suspicious_amount',ascending=False).head(20)
aml.to_csv(f'{OUT}/HQ09_aml_suspicious_accounts.csv', index=False)
print(f"\nHQ-09 AML Flags: {len(aml)} high-risk accounts identified")

# ── HQ-12: Channel Risk Heatmap ──────────────────────────────────────────────
ch = txns.groupby('channel').agg(
    total_txns=('transaction_id','count'),
    suspicious=('is_suspicious_flag','sum'),
    avg_amount=('amount','mean')
).reset_index()
ch['suspicion_rate_pct'] = (ch['suspicious']/ch['total_txns']*100).round(2)
ch['avg_amount'] = ch['avg_amount'].round(2)
ch.sort_values('suspicion_rate_pct',ascending=False,inplace=True)
ch.to_csv(f'{OUT}/HQ12_channel_risk_heatmap.csv', index=False)
print(f"\nHQ-12 Channel Risk:\n{ch[['channel','suspicion_rate_pct','total_txns']].to_string(index=False)}")

# ── HQ-16: KRI MASTER SUMMARY ────────────────────────────────────────────────
kri = pd.DataFrame([
    {'KRI': 'Total Portfolio Exposure (INR)',   'Value': f"₹{loans['loan_amount'].sum()/1e9:.2f}B",     'Status': 'MONITOR'},
    {'KRI': 'Total Active Loans',               'Value': str((loans['loan_status']=='Active').sum()),   'Status': 'NORMAL'},
    {'KRI': 'Gross NPA Rate %',                 'Value': f"{(loans[loans['loan_status']=='NPA']['loan_amount'].sum()/loans['loan_amount'].sum()*100):.2f}%", 'Status': 'ALERT'},
    {'KRI': 'Accounts DPD > 90 Days',           'Value': str(((loans['dpd_days']>90)&(loans['loan_status']=='Active')).sum()), 'Status': 'ALERT'},
    {'KRI': 'Suspicious Transactions Flagged',  'Value': str(txns['is_suspicious_flag'].sum()),         'Status': 'REVIEW'},
    {'KRI': 'Avg Portfolio Credit Score',        'Value': str(round(cust['credit_score'].mean())),      'Status': 'NORMAL'},
    {'KRI': 'Written-Off Accounts',             'Value': str((loans['loan_status']=='Written-Off').sum()),'Status': 'ALERT'},
    {'KRI': 'Restructured Loans',               'Value': str((loans['loan_status']=='Restructured').sum()),'Status': 'MONITOR'},
    {'KRI': 'Total Unique Borrowers',           'Value': str(loans['customer_id'].nunique()),           'Status': 'NORMAL'},
    {'KRI': 'Avg Loan Amount (INR)',             'Value': f"₹{loans['loan_amount'].mean():,.0f}",       'Status': 'NORMAL'},
])
kri.to_csv(f'{OUT}/HQ16_kri_master_summary.csv', index=False)
print(f"\nHQ-16 KRI Master Summary:\n{kri.to_string(index=False)}")

print("\n" + "="*60)
print("ALL OUTPUTS SAVED TO outputs/ folder")
print("="*60)
print(f"Files: {os.listdir(OUT)}")
