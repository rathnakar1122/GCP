import functions_framework

def detect_face(data, context):
    """
    Triggered by a change to a Cloud Storage bucket.

    Args:
        data (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    # Access file data from the event payload
    file_data = data
    file_name = file_data["name"]  # Corrected key from "data" to "name"
    bucket_name = file_data["bucket"]  # Corrected this part to align with GCS event payload keys

    # Log information about the processed file
    print(f"Processing file: {file_name}.")
    print(f"Bucket: {bucket_name}.")
