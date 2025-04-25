import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager
import functions_framework
import json
import pandas as pd

@functions_framework.http
def bq_to_gsheets_cloud_func(request):
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']

    # Set up credentials for the Google Sheets API and BigQuery API
    credentials = Credentials.from_service_account_file(
        r"C:\ENV\rathnakar-18m85a0320-hiscox-0d1ad7b79faf.json", scopes=scopes)
    
    # Access the secret version from Secret Manager
    client = secretmanager.SecretManagerServiceClient()  # Corrected spelling here
    name = "projects/1234556789/secrets/bqtogsheets/versions/1"  # Fixed typo in 'projects'
    response = client.access_secret_version(name=name)  # Fixed spelling of 'response'
    payload = response.payload.data.decode("UTF-8")
    service_account_info = json.loads(payload)

    # Create credentials using the service account info from Secret Manager
    credentials = Credentials.from_service_account_info(service_account_info, scopes=scopes)

    # Authorize the Google Sheets API client
    gc = gspread.authorize(credentials)

    # Specify your sheet key and open the Google Sheets document
    your_sheet_key = "your_google_sheet_key"  # Replace this with your actual sheet key
    worksheet = gc.open_by_key(your_sheet_key).worksheet("sheet1")

    # Create BigQuery client
    client = bigquery.Client(credentials=credentials)  # Use the correct credentials here

    # Define your BigQuery query
    query = """
            SELECT * FROM `your_project_id.your_dataset_id.your_table`
    """
    
    # Run the query and get the results as a DataFrame
    df = client.query(query).to_dataframe()

    # Clear the existing data in the sheet and upload the new data
    worksheet.clear()
    set_with_dataframe(worksheet=worksheet, dataframe=df, include_index=False, include_column_header=True, resize=True)

    return 'Data successfully inserted from BigQuery to Google Sheets'
