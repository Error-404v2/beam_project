# Hospital Data Profiling with Apache Beam

A production-ready, cloud-native data profiling pipeline for the MIMIC-IV Clinical Database Demo. Built with **Apache Beam**, runs on **Google Cloud Dataflow**, and sinks results directly into **BigQuery**. Automatically re-runs whenever source data is updated in GCS, via a serverless **Cloud Function** trigger.

---

## What It Does

Reads three MIMIC-IV CSV files (patients, diagnoses, ICD lookup), computes profiling metrics with **patient-level deduplication** (a patient with 10 visits for the same condition counts as 1), and outputs results exclusively to **BigQuery**.

| Environment | Output |
|---|---|
| **Local** (DirectRunner) | BigQuery tables `summary_stats` + `top_diagnoses` |
| **Cloud** (DataflowRunner) | BigQuery tables `summary_stats` + `top_diagnoses` |

### Report Sections
1. **Dataset Overview** — total patients, unique diagnoses, unique ICD codes
2. **Gender Distribution** — male vs female counts
3. **Age Distribution** — min, max, average
4. **Top 10 Diagnoses** — most frequent conditions with readable ICD names
5. **Mortality Insight** — alive/deceased counts and mortality rate

---

## Project Structure

```
beam_project/
├── main.py           # Entry point — arg parsing, env detection, pipeline orchestration
├── pipeline.py       # Beam graph — read, deduplicate, compute, sink to BQ
├── config.py         # All paths and BigQuery table identifiers
├── schemas.py        # CSV row parsers for patients, diagnoses, lookup
├── transforms.py     # Custom CombineFns: CountFn, AverageFn, MinMaxFn
├── utils.py          # Env detection, runner normalization, workspace cleanup
├── setup.py          # Packages modules for remote Dataflow workers
├── trigger_cf.py     # Cloud Function source — triggers Dataflow on GCS file changes
├── .gitignore        # Excludes __pycache__, *.egg-info, beam-temp-*
├── requirements.txt  # Python dependencies
└── REBUILD_GUIDE.md  # Step-by-step guide to rebuild the entire GCP setup from scratch
```

---

## How to Run

> [!IMPORTANT]
> Because this pipeline uses `WRITE_TRUNCATE` for BigQuery, a GCS temporary location is **required** even for local execution.

### Locally (DirectRunner)
```bash
conda activate DataEng
python main.py --temp_location gs://cloudypedia-intern-hospital-data/temp
```

### On Google Cloud Dataflow (manual)
```bash
conda activate DataEng
python main.py \
  --input-source gcs \
  --project cloudypedia-intern \
  --region me-central1 \
  --temp_location gs://cloudypedia-intern-hospital-data/temp \
  --staging_location gs://cloudypedia-intern-hospital-data/staging \
  --setup_file ./setup.py
```

### Automated (no command needed)
Upload or re-upload any file to `gs://cloudypedia-intern-hospital-data/hosp/`.  
The Cloud Function detects the change and launches Dataflow automatically.

### Syncing Local Code to the Cloud
If you modify `pipeline.py`, `schemas.py`, or any other logic, the cloud environment will **not** automatically run the new code. You must rebuild the Dataflow Template to sync your changes to the cloud:

```bash
conda activate DataEng
python main.py \
  --input-source gcs \
  --project cloudypedia-intern \
  --region me-central1 \
  --temp_location gs://cloudypedia-intern-hospital-data/temp \
  --staging_location gs://cloudypedia-intern-hospital-data/staging \
  --setup_file ./setup.py \
  --runner DataflowRunner \
  --template_location gs://cloudypedia-intern-hospital-data/templates/hospital_profiling_v1
```
After running this, any new files uploaded to the bucket will trigger Dataflow using your updated code.

---

## Beam Concepts Used

| Concept | Where Used |
|---|---|
| `ReadFromText` | Reading CSV files from local disk or GCS |
| `FlatMap` | Parsing rows, tagging valid data |
| `Distinct` | Deduplicating diagnoses per patient |
| `CombineGlobally` / `CombinePerKey` | Aggregating stats across the dataset |
| `CombineFn` | Custom `CountFn`, `AverageFn`, `MinMaxFn` |
| `Top.Of` | Finding the top 10 diagnoses |
| `AsSingleton` / `AsList` / `AsDict` | Passing side inputs into transforms |
| `WriteToBigQuery` | Sinking structured metrics to the data warehouse |

---

## GCP Infrastructure

| Resource | Name / ID |
|---|---|
| GCS Bucket | `cloudypedia-intern-hospital-data` |
| BigQuery Dataset | `hospital_profiling` |
| BigQuery Tables | `summary_stats`, `top_diagnoses` |
| Dataflow Template | `gs://.../templates/hospital_profiling_v1` |
| Cloud Function | `hospital-auto-trigger` (Gen 2, `me-central1`) |

See `REBUILD_GUIDE.md` to recreate this infrastructure from scratch.

---

## Dataset

**MIMIC-IV Clinical Database Demo v2.2** — 100 de-identified patients.

- `patients.csv` — gender, anchor age, date of death
- `diagnoses_icd.csv` — diagnosis records per hospital admission
- `d_icd_diagnoses.csv` — ICD code → readable description lookup
