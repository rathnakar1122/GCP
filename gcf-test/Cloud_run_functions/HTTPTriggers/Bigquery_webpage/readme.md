gcloud functions deploy http_trigger
  --runtime python311
  --trigger-http
  --allow-unauthenticated
  --entry-point http_trigger
  --region YOUR_REGION
