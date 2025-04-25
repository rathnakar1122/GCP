import functions_framework
from google.cloud import bigquery

@functions_framework.cloud_event
def gcs_to_bq_event(cloud_event):
    """
    Cloud Function triggered by a GCS event to load data into BigQuery.

    Args:
        cloud_event (dict): The Cloud Event data, which contains information about the GCS event.
    """
    try:
        # Extract data from the Cloud Event
        data = cloud_event.data
        bucket = data.get("bucket")  # Get the bucket name
        file_name = data.get("name")  # Get the file name in the bucket

        if not bucket or not file_name:
            raise ValueError("Bucket or file name is missing in the event data.")

        print(f"Bucket: {bucket}")
        print(f"File: {file_name}")

        # Initialize BigQuery client
        client = bigquery.Client()

        # Define the table ID (update with your project, dataset, and table)
        table_id = "rathnakar-18m85a0320-hiscox.pation_load.gcs_to_bq_load_table"

        # Configure the load job with WRITE_APPEND disposition
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Automatically detect the schema
            skip_leading_rows=1,  # Skip the header row
            source_format=bigquery.SourceFormat.CSV,  # Input format is CSV
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append to the table
        )

        # Define the GCS file URI
        uri = f"gs://{bucket}/{file_name}"

        print(f"Loading data from {uri} to BigQuery table {table_id}...")

        # Load data from GCS to BigQuery
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)

        # Wait for the job to complete
        load_job.result()

        # Get the destination table and print the loaded row count
        destination_table = client.get_table(table_id)
        print(f"Loaded {destination_table.num_rows} rows into {table_id}.")
        return "Success"

    except Exception as e:
        print(f"Error during BigQuery load job: {e}")
        return f"Error: {str(e)}"
