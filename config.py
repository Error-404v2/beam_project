"""
Configuration module.

Contains file paths, options, and constants used across the pipeline.
"""

DATA_DIR = "input/data/mimic-iv-clinical-database-demo-2.2/hosp"
PATIENTS_CSV  = f"{DATA_DIR}/patients.csv"
DIAGNOSES_CSV = f"{DATA_DIR}/diagnoses_icd.csv"
LOOKUP_CSV    = f"{DATA_DIR}/d_icd_diagnoses.csv"

OUTPUT_FILE = "output/profiling_report.txt"
DLQ_PATIENTS_FILE = "output/dlq_patients.txt"
DLQ_DIAGNOSES_FILE = "output/dlq_diagnoses.txt"
DLQ_LOOKUP_FILE = "output/dlq_lookup.txt"
GENERATED_OUTPUTS = [
    OUTPUT_FILE,
    DLQ_PATIENTS_FILE,
    DLQ_DIAGNOSES_FILE,
    DLQ_LOOKUP_FILE,
]
