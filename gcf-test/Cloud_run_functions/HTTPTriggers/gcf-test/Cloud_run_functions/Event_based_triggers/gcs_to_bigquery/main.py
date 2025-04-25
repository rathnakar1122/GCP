# Requirement:

# whenever a clinet or a vendor uploads a text file or onto a gcs bucket a cloud fucntion to be triggered and it should process the data in that 
# object and it is to be loaded into a bigquery table. 

# event is an action taken place in the cloud specific service


#if the trigger type is https:

# @function_framework.http
# def http_trigger(request):
#     request_json = request.get_json(silent = True)
#     request_args = request.args
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
    data = cloud_event.data

    # Validate the incoming event data
    if "bucket" not in data or "name" not in data:
        logging.error("Missing 'bucket' or 'name' attributes in the event data.")
        return

    bucket = data["bucket"]
    name = data["name"]
    logging.info(f"Received event for bucket: {bucket}, file: {name}")

    # Initialize BigQuery client
    client = bigquery.Client()

    # Set the destination BigQuery table ID
    table_id = "rathnakar-18m85a0320-hiscox.pation_load.gcs_to_bq_load_table"

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ID", "INTEGER"),
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Age", "INTEGER"),
            bigquery.SchemaField("City", "STRING"),
            bigquery.SchemaField("Salary", "INTEGER"),
        ],  # Explicit schema definition
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
        return

    # Fetch and log the table details after the load
    try:
        destination_table = client.get_table(table_id)
        logging.info(
            f"Loaded {destination_table.num_rows} rows into table {table_id}."
        )
    except Exception as e:
        logging.error(f"Error retrieving metadata for table {table_id}: {e}")


    #metageneration = data["metageneration"]
    #timeCreated = data["timeCreated"]
    #updated = data["updated"]

    # print(f"Event ID: {event_id}")
    # print(f"Event type: {event_type}")
    # print(f"Bucket: {bucket}")
    # print(f"File: {name}")
    # print(f"Metageneration: {metageneration}")
    # print(f"Created: {timeCreated}")
    # print(f"Updated: {updated}")



# import function_framework
# from google.cloud import bigquery

# @ function_frameowkr.cloud_event

# def gcs_to_bq(cloud_event):
#     data = cloud_event.data
#     bucket = data ['bucket']
#     name= data ['name']
#     client = bigqueru.Clinet(Projecy = 'myfirstid')
#     table_id = ' prokect_id,dataset_id,table_id'

#     print[f"bucket:{bucket}"]
#     print[f:File:{name}")]
          
#     table_id = 'rathnakar-18m85a0320-hiscox.pation_load.gcs_to_bq_load_table'

#     job_config = bigquery.LoadJobConfig(
#         schema=[
#             bigquery.SchemaField("ID", "INTEGER"),
# )