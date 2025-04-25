	gcloud functions deploy localhttp-test-func `
    --runtime python311 `
    --trigger-http `
    --allow-unauthenticated `
    --entry-point hello_http_trigger
