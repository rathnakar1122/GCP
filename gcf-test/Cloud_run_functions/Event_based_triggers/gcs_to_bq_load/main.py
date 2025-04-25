from google.cloud import bigquery
import functions_framework

@functions_framework.cloud_event
def gcs_to_bq_load(cloud_event):
    data = cloud_event.data
    bucket = data['bucket']
    object_name = data['name']


    # Define your project, dataset, and table information
    project_id = 'rathnakar-18m85a0320-hiscox'
    dataset_id = 'MAIN'
    table_id = 'CUSTOMER_INFO'
    uri = f"gs://{bucket}/{object_name}"

    # Configure the BigQuery load job
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load the data into BigQuery
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    load_job = client.load_table_from_uri(
        uri,
        table_ref,
        job_config=job_config,
    )

    # Wait for the job to complete
    load_job.result()
    print(f"Loaded {load_job.output_rows} rows into {table_ref}.")
