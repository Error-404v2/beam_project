"""
Configuration module.

Contains file paths, options, and constants used across the pipeline.
"""

LOCAL_DATA_DIR = "input/data/mimic-iv-clinical-database-demo-2.2/hosp"
GCS_DATA_DIR = "gs://cloudypedia-intern-hospital-data/hosp"

DEFAULT_INPUT_SOURCE = "local"
INPUT_SOURCES = {
    "local": LOCAL_DATA_DIR,
    "gcs": GCS_DATA_DIR,
}

def get_input_paths(input_source=DEFAULT_INPUT_SOURCE):
    """Return the three input CSV paths for a configured source."""
    try:
        data_dir = INPUT_SOURCES[input_source]
    except KeyError as exc:
        valid_sources = ", ".join(sorted(INPUT_SOURCES))
        raise ValueError(
            f"Unknown input source '{input_source}'. Use one of: {valid_sources}."
        ) from exc

    return {
        "patients": f"{data_dir}/patients.csv",
        "diagnoses": f"{data_dir}/diagnoses_icd.csv",
        "lookup": f"{data_dir}/d_icd_diagnoses.csv",
    }

# BigQuery Settings
BQ_PROJECT = "cloudypedia-intern"
BQ_DATASET = "hospital_profiling"
BQ_SUMMARY_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.summary_stats"
BQ_TOP_DIAG_TABLE = f"{BQ_PROJECT}:{BQ_DATASET}.top_diagnoses"
