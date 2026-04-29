"""
trigger_cf.py

Cloud Function (Gen 2) that is triggered by a GCS Object Finalize event.
When any file inside the 'hosp/' folder is created or re-uploaded, this
function launches the hospital_profiling_v1 Dataflow template.
"""
import datetime
import traceback

import functions_framework
from googleapiclient.discovery import build


PROJECT = "cloudypedia-intern"
REGION = "me-central1"
TEMPLATE = "gs://cloudypedia-intern-hospital-data/templates/hospital_profiling_v1"
TEMP_LOCATION = "gs://cloudypedia-intern-hospital-data/temp"
WATCH_PREFIX = "hosp/"


@functions_framework.cloud_event
def trigger_hospital_pipeline(cloud_event):
    """Launch the Dataflow template when a watched GCS object is finalized."""
    file_name = cloud_event.data["name"]

    if not file_name.startswith(WATCH_PREFIX):
        print(f"Ignoring '{file_name}' because it is not inside '{WATCH_PREFIX}'. No action taken.")
        return

    print(f"File '{file_name}' updated. Launching Dataflow Template...")

    try:
        service = build("dataflow", "v1b3")
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

        print(f"Dataflow job launched. Job ID: {response['job']['id']}")

    except Exception as exc:
        print(f"ERROR launching Dataflow job: {exc}")
        print(traceback.format_exc())
