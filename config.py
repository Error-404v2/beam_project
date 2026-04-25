"""
Configuration module.

Contains file paths, options, and constants used across the pipeline.
"""

DATA_DIR = "input/data/mimic-iv-clinical-database-demo-2.2/hosp"
PATIENTS_CSV  = f"{DATA_DIR}/patients.csv"
DIAGNOSES_CSV = f"{DATA_DIR}/diagnoses_icd.csv"
LOOKUP_CSV    = f"{DATA_DIR}/d_icd_diagnoses.csv"

OUTPUT_FILE = "output/profiling_report.txt"
