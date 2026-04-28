"""
trigger_cf.py

Cloud Function (Gen 2) that is triggered by a GCS Object Finalize event.
When any file inside the 'hosp/' folder is created or re-uploaded, this
function launches the hospital_profiling_v1 Dataflow template.

Deployment command:
    gcloud functions deploy hospital-auto-trigger \
      --gen2 --runtime=python311 --region=me-central1 \
      --source=. --entry-point=trigger_hospital_pipeline \
      --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
      --trigger-event-filters="bucket=cloudypedia-intern-hospital-data"

Requirements (cf_requirements.txt):
    functions-framework==3.*
    google-api-python-client>=2.0.0
    google-auth-httplib2>=0.1.0
    google-auth-oauthlib>=0.5.0
"""
import functions_framework
from googleapiclient.discovery import build
import datetime
import traceback

# --- Configuration ---
PROJECT     = "cloudypedia-intern"
REGION      = "me-central1"
TEMPLATE    = "gs://cloudypedia-intern-hospital-data/templates/hospital_profiling_v1"
TEMP_LOCATION = "gs://cloudypedia-intern-hospital-data/temp"
WATCH_PREFIX  = "hosp/"  # Only files in this folder will trigger a run


@functions_framework.cloud_event
def trigger_hospital_pipeline(cloud_event):
    """
    Entry point for the Cloud Function.
    Receives a GCS CloudEvent and launches a Dataflow template job.
    """
    file_name = cloud_event.data["name"]

    # Only process files uploaded to the watched folder
    if not file_name.startswith(WATCH_PREFIX):
        print(f"ℹ️  Ignoring '{file_name}' — not inside '{WATCH_PREFIX}'. No action taken.")
        return

    print(f"📥 File '{file_name}' updated. Launching Dataflow Template...")

    try:
        service  = build("dataflow", "v1b3")
        job_name = f"auto-profile-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"

        body = {
            "jobName": job_name,
            "environment": {
                "tempLocation": TEMP_LOCATION
            }
        }

        response = (
            service.projects()
                   .locations()
                   .templates()
                   .launch(projectId=PROJECT, location=REGION, gcsPath=TEMPLATE, body=body)
                   .execute()
        )

        print(f"✅ Dataflow job launched! Job ID: {response['job']['id']}")

    except Exception as e:
        print(f"❌ ERROR launching Dataflow job: {e}")
        print(traceback.format_exc())
