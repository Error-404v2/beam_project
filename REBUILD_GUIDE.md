# GCP Infrastructure Rebuild Guide

This guide rebuilds the GCP pieces for the Beam/Dataflow pipeline in this repo.

## Prerequisites

- `gcloud` CLI installed and authenticated
- `conda` with the `DataEng` environment
- Access to the `cloudypedia-intern` GCP project

## 1. Enable APIs

```bash
gcloud services enable \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com \
  logging.googleapis.com \
  pubsub.googleapis.com \
  --project=cloudypedia-intern
```

## 2. Grant IAM Permissions

```bash
gcloud projects add-iam-policy-binding cloudypedia-intern \
  --member="user:ahmed.hosam12345678@gmail.com" \
  --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding cloudypedia-intern \
  --member="serviceAccount:service-873987863395@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
```

## 3. Create the GCS Bucket and Upload Inputs

```bash
gcloud storage buckets create gs://cloudypedia-intern-hospital-data \
  --location=me-central1 \
  --uniform-bucket-level-access
```

Upload the hospital source files used by the dashboard pipeline:

```bash
gsutil -m cp \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/patients.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/diagnoses_icd.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/d_icd_diagnoses.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/transfers.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/procedures_icd.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/prescriptions.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/labevents.csv \
  input/data/mimic-iv-clinical-database-demo-2.2/hosp/d_labitems.csv \
  gs://cloudypedia-intern-hospital-data/hosp/
```

## 4. Create the BigQuery Dataset

```bash
bq mk --location=me-central1 cloudypedia-intern:hospital_profiling
```

The pipeline creates these tables automatically:

- `summary_stats`
- `top_diagnoses`
- `patient_dashboard_summary`
- `patient_admissions_timeline`
- `patient_lab_history`

## 5. Validate Locally

```bash
conda activate DataEng
python main.py --input-source local --output-mode local --runner DirectRunner
```

Expected local output prefixes:

- `output/summary_stats*.jsonl`
- `output/top_diagnoses*.jsonl`
- `output/patient_dashboard_summary*.jsonl`
- `output/patient_admissions_timeline*.jsonl`
- `output/patient_lab_history*.jsonl`

## 6. Build and Save the Dataflow Template

Run this every time the pipeline code changes.

```bash
conda activate DataEng
python main.py \
  --input-source gcs \
  --output-mode bigquery \
  --project cloudypedia-intern \
  --region me-central1 \
  --temp_location gs://cloudypedia-intern-hospital-data/temp \
  --staging_location gs://cloudypedia-intern-hospital-data/staging \
  --setup_file ./setup.py \
  --runner DataflowRunner \
  --template_location gs://cloudypedia-intern-hospital-data/templates/hospital_profiling_v1
```

## 7. Deploy the Cloud Function

Create a clean deployment folder containing `trigger_cf.py` as `main.py` and a Cloud Function-specific requirements file, then deploy:

```bash
gcloud functions deploy hospital-auto-trigger \
  --gen2 \
  --runtime=python311 \
  --region=me-central1 \
  --source=./deploy_cf \
  --entry-point=trigger_hospital_pipeline \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=cloudypedia-intern-hospital-data"
```

## 8. Verify Automation

```bash
gsutil cp input/data/mimic-iv-clinical-database-demo-2.2/hosp/patients.csv \
  gs://cloudypedia-intern-hospital-data/hosp/patients.csv

gcloud dataflow jobs list --region=me-central1 --limit=3 \
  --format="table(id,jobName,state,creationTime)"
```
