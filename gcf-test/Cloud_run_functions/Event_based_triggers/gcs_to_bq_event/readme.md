gcloud functions deploy gcs_to_bq_event `    --runtime=python310`
    --trigger-event=google.storage.object.finalize `    --trigger-resource=crf-event`
    --entry-point=gcs_to_bq_event `    --timeout=300`
    --region=us-central1 `    --allow-unauthenticated`
    --gen2 `
    --source="C:\ENV\PREP\Cloud_run_functions\Event_based_triggers\gcs_to_bq_event"
