"""
Configuration module.

Contains file paths, options, and constants used across the pipeline.
"""

GCS_DATA_DIR = "gs://cloudypedia-intern-hospital-data/hosp"
GCS_TEMP_LOCATION = "gs://cloudypedia-intern-hospital-data/temp"

INPUT_FILES = {
    "patients": "patients.csv",
    "diagnoses": "diagnoses_icd.csv",
    "diagnosis_lookup": "d_icd_diagnoses.csv",
    "transfers": "transfers.csv",
    "procedures": "procedures_icd.csv",
    "prescriptions": "prescriptions.csv",
    "labs": "labevents.csv",
    "lab_lookup": "d_labitems.csv",
}


def get_input_paths():
    """Return input CSV paths from GCS."""

    return {
        name: f"{GCS_DATA_DIR}/{file_name}"
        for name, file_name in INPUT_FILES.items()
    }


# BigQuery Settings
BQ_PROJECT = "cloudypedia-intern"
BQ_DATASET = "hospital_profiling"
BQ_SUMMARY_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.summary_stats"
BQ_TOP_DIAG_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.top_diagnoses"
BQ_PATIENT_DASHBOARD_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.patient_dashboard_summary"
BQ_ADMISSIONS_TIMELINE_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.patient_admissions_timeline"
BQ_LAB_HISTORY_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.patient_lab_history"
