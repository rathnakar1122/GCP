gcloud functions deploy get_top_rows `
    --entry-point get_top_rows `
    --runtime python311 `
    --trigger-http `
    --allow-unauthenticated
