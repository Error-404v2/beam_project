import pandas as pd
import numpy as np

# Load datasets
patients_df = pd.read_csv('input/data/mimic-iv-clinical-database-demo-2.2/hosp/patients.csv')
diagnoses_df = pd.read_csv('input/data/mimic-iv-clinical-database-demo-2.2/hosp/diagnoses_icd.csv')
lookup_df = pd.read_csv('input/data/mimic-iv-clinical-database-demo-2.2/hosp/d_icd_diagnoses.csv')

# 1. Mortality distribution across months
patients_df['dod'] = pd.to_datetime(patients_df['dod'])
deceased_patients = patients_df[patients_df['dod'].notna()].copy()
deceased_patients['death_month'] = deceased_patients['dod'].dt.month
month_counts = deceased_patients['death_month'].value_counts().sort_index()
print("--- Mortality by Month ---")
print(month_counts)

# 2. Leading causes of death
# Get subject_ids of deceased patients
deceased_subject_ids = deceased_patients['subject_id']

# Filter diagnoses for deceased patients
deceased_diagnoses = diagnoses_df[diagnoses_df['subject_id'].isin(deceased_subject_ids)]

# Merge with lookup to get diagnosis titles
deceased_diagnoses_with_titles = pd.merge(deceased_diagnoses, lookup_df, on=['icd_code', 'icd_version'], how='left')

# Get top 10 causes of death
top_causes = deceased_diagnoses_with_titles['long_title'].value_counts().head(10)
print("\n--- Leading Causes of Death ---")
print(top_causes)

# 3. Correlation between age, number of diagnoses, and mortality
diag_counts = diagnoses_df.groupby('subject_id').size().reset_index(name='total_diagnoses')
df_merged = pd.merge(patients_df, diag_counts, on='subject_id', how='left')
df_merged['is_deceased'] = df_merged['dod'].notna()

corr_matrix = df_merged[['anchor_age', 'total_diagnoses', 'is_deceased']].corr()
print("\n--- Correlation Matrix ---")
print(corr_matrix)
