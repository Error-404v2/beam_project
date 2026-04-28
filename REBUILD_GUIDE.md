# REBUILD_GUIDE.md
# GCP Infrastructure Rebuild Guide

This guide lets you tear everything down and rebuild the full GCP setup
from scratch, given that all pipeline code is already present in this repo.

---

## Prerequisites
- `gcloud` CLI installed and authenticated (`gcloud auth login`)
- `conda` with the `DataEng` environment
- Access to the `cloudypedia-intern` GCP project

---

## Step 1 — Enable APIs (one-time)

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

---

## Step 2 — Grant IAM Permissions (one-time)

These are the MINIMUM required permissions. Nothing extra.

```bash
# 2a. Allow your account to deploy Cloud Functions that use service accounts
gcloud projects add-iam-policy-binding cloudypedia-intern \
  --member="user:ahmed.hosam12345678@gmail.com" \
  --role="roles/iam.serviceAccountUser"

# 2b. Allow the hidden GCS messenger robot to send Pub/Sub events (needed by Eventarc)
gcloud projects add-iam-policy-binding cloudypedia-intern \
  --member="serviceAccount:service-873987863395@gs-project-accounts.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
```

> **Note:** The Compute Engine service account (`873987863395-compute@...`) already
> has the `Editor` role, which covers all Dataflow permissions. No extra Dataflow
> roles are needed.

---

## Step 3 — Create the GCS Bucket

```bash
gcloud storage buckets create gs://cloudypedia-intern-hospital-data \
  --location=me-central1 \
  --uniform-bucket-level-access
```

Create the required folder structure by uploading the source data:

```bash
gsutil -m cp input/data/mimic-iv-clinical-database-demo-2.2/hosp/patients.csv \
             input/data/mimic-iv-clinical-database-demo-2.2/hosp/diagnoses_icd.csv \
             input/data/mimic-iv-clinical-database-demo-2.2/hosp/d_icd_diagnoses.csv \
             gs://cloudypedia-intern-hospital-data/hosp/
```

---

## Step 4 — Create the BigQuery Dataset

```bash
bq mk --location=me-central1 cloudypedia-intern:hospital_profiling
```

> Tables (`summary_stats`, `top_diagnoses`) are created automatically by the
> pipeline on the first run via `CREATE_IF_NEEDED`.

---

## Step 5 — Build and Save the Dataflow Template

Run this every time the pipeline code changes.

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

This saves a reusable "recipe" at:
`gs://cloudypedia-intern-hospital-data/templates/hospital_profiling_v1`

---

## Step 6 — Deploy the Cloud Function

The Cloud Function source is `trigger_cf.py` in this repo.
It needs its own minimal folder with only its own requirements.

```bash
# 6a. Create a clean deployment folder
mkdir deploy_cf

# 6b. Copy the trigger source as main.py (required by Cloud Functions runtime)
copy trigger_cf.py deploy_cf\main.py

# 6c. Create the requirements file
echo functions-framework==3.* > deploy_cf\requirements.txt
echo google-api-python-client^>=2.0.0 >> deploy_cf\requirements.txt
echo google-auth-httplib2^>=0.1.0 >> deploy_cf\requirements.txt
echo google-auth-oauthlib^>=0.5.0 >> deploy_cf\requirements.txt

# 6d. Deploy
gcloud functions deploy hospital-auto-trigger \
  --gen2 \
  --runtime=python311 \
  --region=me-central1 \
  --source=./deploy_cf \
  --entry-point=trigger_hospital_pipeline \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=cloudypedia-intern-hospital-data"

# 6e. Clean up the temporary deployment folder
rmdir /s /q deploy_cf
```

---

## Step 7 — Verify Everything Works

```bash
# Manually trigger the automation by uploading a file
gsutil cp input/data/mimic-iv-clinical-database-demo-2.2/hosp/patients.csv \
          gs://cloudypedia-intern-hospital-data/hosp/patients.csv

# Wait ~30 seconds, then check if a Dataflow job was created
gcloud dataflow jobs list --region=me-central1 --limit=3 \
  --format="table(id,jobName,state,creationTime)"

# Check Cloud Function logs
gcloud functions logs read hospital-auto-trigger \
  --gen2 --region=me-central1 --limit=10
```

Expected log output:
```
File hosp/patients.csv updated! Launching Dataflow Template...
✅ Dataflow job launched! Job ID: 2026-...
```

---

## Current IAM Policy (Minimal — What Is Actually Required)

| Principal | Role | Why |
|---|---|---|
| `ahmed.hosam12345678@gmail.com` | `roles/owner` | Project owner |
| `ahmed.hosam12345678@gmail.com` | `roles/iam.serviceAccountUser` | Required to deploy Cloud Functions Gen 2 |
| `873987863395-compute@...` | `roles/editor` | Auto-assigned by GCP; covers all Dataflow and GCS access for the function |
| `service-873987863395@gs-project-accounts...` | `roles/pubsub.publisher` | Allows GCS to send Eventarc event notifications |
| Various `service-*` accounts | Service Agent roles | Auto-managed by GCP; do not modify |

---

## Day-to-Day Commands

```bash
# Run pipeline locally (requires temp_location for BQ)
python main.py --temp_location gs://cloudypedia-intern-hospital-data/temp

# Run pipeline on Dataflow manually
python main.py --input-source gcs --project cloudypedia-intern \
  --region me-central1 \
  --temp_location gs://cloudypedia-intern-hospital-data/temp \
  --staging_location gs://cloudypedia-intern-hospital-data/staging \
  --setup_file ./setup.py

# Trigger automation (just upload a file)
gsutil cp <any_file> gs://cloudypedia-intern-hospital-data/hosp/<any_file>

# Query results in BigQuery
# → https://console.cloud.google.com/bigquery?project=cloudypedia-intern
```
