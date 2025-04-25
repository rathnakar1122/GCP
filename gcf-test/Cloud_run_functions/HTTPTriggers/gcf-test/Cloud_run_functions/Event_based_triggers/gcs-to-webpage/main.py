import functions_framework
from google.cloud import bigquery
import logging

# Triggered by a change in a Cloud Storage bucket
@functions_framework.cloud_event
def gcs_to_bq_cloud_crf(cloud_event):
    """
    Cloud Function to load data from GCS to BigQuery.
    Triggered by a Cloud Storage event.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    data = cloud_event.data

    # Validate the incoming event data
    if "bucket" not in data or "name" not in data:
        logging.error("Missing 'bucket' or 'name' attributes in the event data.")
        return "Error: Missing 'bucket' or 'name' in event data", 400

    bucket = data["bucket"]
    name = data["name"]
    logging.info(f"Received event for bucket: {bucket}, file: {name}")

    # Initialize BigQuery client
    client = bigquery.Client()

    # Set the destination BigQuery table ID
    table_id = "rathnakar-18m85a0320-hiscox.pation_load.gcs_to_bq_load_table"

    # Configure the load job with autodetect
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Enable schema autodetection
        skip_leading_rows=1,  # Skip header row
        source_format=bigquery.SourceFormat.CSV,  # File format
    )

    # Construct the GCS file URI
    uri = f"gs://{bucket}/{name}"

    # Load data from GCS into BigQuery
    try:
        logging.info(f"Initiating data load from {uri} to table {table_id}.")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        logging.info("Data successfully loaded into BigQuery.")
    except Exception as e:
        logging.error(f"Failed to load data into BigQuery: {e}")
        return f"Error loading data into BigQuery: {e}", 500

    # Fetch and log the table details after the load
    try:
        destination_table = client.get_table(table_id)
        logging.info(
            f"Loaded {destination_table.num_rows} rows into table {table_id}."
        )
    except Exception as e:
        logging.error(f"Error retrieving metadata for table {table_id}: {e}")
        return f"Error retrieving metadata for table {table_id}: {e}", 500

    return f"Data from {name} successfully loaded into {table_id}.", 200
