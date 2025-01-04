
gcloud functions deploy square-digit-func `
    --runtime python311 `
    --trigger-http `
    --allow-unauthenticated `
    --entry-point square_digit
