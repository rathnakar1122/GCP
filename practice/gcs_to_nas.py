import os
from datetime import datetime
from google.cloud import storage
import pysftp

# Set up Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\ENV\rathnakar-18m85a0320-hiscox-0d1ad7b79faf.json"

# GCS Configuration
bucket_name = "your-bucket-name"
folders = ["out_files/floor-space", "out_files/floor"]  # Fixed folder names
destination_path = r"C:\your\destination\path"

# SFTP Configuration
sftp_host = "sftp_host_a"
sftp_username = "sftp_username_a"
sftp_password = "sftp_password_a"
sftp_remote_path = "/remote/destination/path"  # Adjust as needed

# Ensure local destination directory exists
os.makedirs(destination_path, exist_ok=True)

# Initialize GCS client
client = storage.Client()
bucket = client.bucket(bucket_name)

# Get today's date
today_date = datetime.today().strftime('%Y-%m-%d')

# Initialize an empty list for downloaded files before the loop
downloaded_files = []

# Download files from GCS
for folder in folders:
    blobs = client.list_blobs(bucket_name, prefix=folder)
    for blob in blobs:
        file_name = os.path.basename(blob.name)
        if today_date in file_name:  # Filter files based on today's date
            local_file_path = os.path.join(destination_path, file_name)
            try:
                blob.download_to_filename(local_file_path)
                downloaded_files.append(local_file_path)
                print(f"✅ Downloaded: {blob.name} --> {local_file_path}")
            except Exception as e:
                print(f"❌ Error downloading {blob.name}: {e}")

# Ensure downloaded_files list is defined before checking its contents
if 'downloaded_files' not in locals():
    downloaded_files = []

# Upload downloaded files to SFTP only if there are files
if downloaded_files:
    try:
        with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password) as sftp:
            for local_file in downloaded_files:
                remote_file_path = os.path.join(sftp_remote_path, os.path.basename(local_file))
                sftp.put(local_file, remote_file_path)
                print(f"✅ Uploaded: {local_file} --> {remote_file_path}")
    except Exception as e:
        print(f"❌ SFTP upload failed: {e}")

print("✅ Process completed successfully.")




from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("")

data = {
    "id": 1, "name": "Rathnakar", 
}

df =spark.createDataFrame(data = data, )
df.show()