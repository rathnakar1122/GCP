gcloud functions deploy gcs-to-bq-event `  --runtime python310`
  --trigger-event google.storage.object.finalize `  --trigger-resource crf-event`
  --entry-point gcs_to_bq_cloud_crf `  --region us-central1`
  --no-gen2 `
  --source "C:\ENV\PREP\Cloud_run_functions\Event_based_triggers\gcs-to-webpage"
