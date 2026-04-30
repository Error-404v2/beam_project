# Patient-Centric MIMIC Dashboard Pipeline

A compact Apache Beam pipeline for the MIMIC-IV Clinical Database Demo. It runs locally with DirectRunner for validation, runs on Google Cloud Dataflow for production, and writes Looker Studio-ready tables to BigQuery.

## What It Does

The pipeline reads MIMIC hospital CSV files, keeps patient-level diagnosis deduplication, and creates both global profiling outputs and patient-centric dashboard marts.

Existing profiling outputs:

| BigQuery table | Purpose |
|---|---|
| `summary_stats` | Total patients, unique diagnoses, age stats, alive/deceased counts, mortality rate |
| `top_diagnoses` | Top diagnosis codes with readable ICD descriptions |

Dashboard outputs:

| BigQuery table | Purpose |
|---|---|
| `patient_dashboard_summary` | One row per patient with first contact, admissions, procedures, meds, and diagnoses |
| `patient_admissions_timeline` | One row per patient admission for admissions-over-year charts |
| `patient_lab_history` | Numeric lab history joined to lab labels for trend charts |

## Patient Metrics

- Patient ID: `subject_id`
- First contact: earliest `transfers.intime`
- Admissions: distinct `hadm_id` values from `transfers.csv`
- Procedures: distinct `(hadm_id, icd_code, icd_version)` from `procedures_icd.csv`
- Unique meds dispensed: distinct normalized `drug` values from `prescriptions.csv`
- Unique diagnoses: distinct `(subject_id, icd_code, icd_version)`, so repeat admissions do not inflate the count
- Labs: numeric `labevents.valuenum` rows joined to `d_labitems.csv`

## Project Structure

```text
beam_project/
|-- main.py           # Entry point and app-specific CLI options
|-- pipeline.py       # Beam graph: read, transform, write
|-- config.py         # Input paths and BigQuery table identifiers
|-- schemas.py        # CSV row parsers
|-- transforms.py     # Custom CombineFns
|-- utils.py          # Logging and workspace cleanup
|-- setup.py          # Packages modules for remote Dataflow workers
|-- trigger_cf.py     # Cloud Function source for GCS-triggered Dataflow runs
|-- requirements.txt  # Runtime dependency list
`-- REBUILD_GUIDE.md  # GCP rebuild instructions
```

## How to Run

### BigQuery write from local machine

```bash
conda activate DataEng
python main.py
```

The pipeline writes dashboard tables to BigQuery. It does not write local JSONL files to `output/`.

### Dataflow template build

Run this whenever pipeline code changes.

```bash
conda activate DataEng
python main.py \
  --project cloudypedia-intern \
  --region me-central1 \
  --staging_location gs://cloudypedia-intern-hospital-data/staging \
  --setup_file ./setup.py \
  --runner DataflowRunner \
  --template_location gs://cloudypedia-intern-hospital-data/templates/hospital_profiling_v1
```

## Dataset

Uses the MIMIC-IV Clinical Database Demo v2.2 hospital files:

- `patients.csv`
- `diagnoses_icd.csv`
- `d_icd_diagnoses.csv`
- `transfers.csv`
- `procedures_icd.csv`
- `prescriptions.csv`
- `labevents.csv`
- `d_labitems.csv`
