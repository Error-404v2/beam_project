"""
Configuration module.

Contains file paths, options, and constants used across the pipeline.
"""

LOCAL_DATA_DIR = "input/data/mimic-iv-clinical-database-demo-2.2/hosp"
GCS_DATA_DIR = "gs://cloudypedia-intern-hospital-data/hosp"

DEFAULT_INPUT_SOURCE = "gcs"
INPUT_SOURCES = {
    "local": LOCAL_DATA_DIR,
    "gcs": GCS_DATA_DIR,
}

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


def get_input_paths(input_source=DEFAULT_INPUT_SOURCE):
    """Return input CSV paths for a configured source."""
    try:
        data_dir = INPUT_SOURCES[input_source]
    except KeyError as exc:
        valid_sources = ", ".join(sorted(INPUT_SOURCES))
        raise ValueError(
            f"Unknown input source '{input_source}'. Use one of: {valid_sources}."
        ) from exc

    return {
        name: f"{data_dir}/{file_name}"
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
