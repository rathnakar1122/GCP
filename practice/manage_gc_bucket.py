import os
from datetime import datetime
from google.cloud import storage
import pysftp

# Set up Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\45375853\Documents\cs_data_plotform\service_account\dbsr-pdtest-dev-svc-account.json'
os.environ['http_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128"
os.environ['https_proxy'] = "http://googleapis-dev.gcp.cloud.uk.hsbc:3128"

# GCS Configuration
BUCKET_NAME = "cs_data_platform_jll_dev"
FOLDERS = ["out_files/floor-space/", "out_files/floor/"] 
DESTINATION_PATH = r"/int2dev/feeds/ICE/SFG02000/data"

# SFTP Configuration
sftp_host = "int2dev-cd.systems.uk.hsbc" 
sftp_username = "sfg02000"  
PRIVATE_KEY_PATH = r"C:\Users\45375853\.ssh\JLLkey.ppk"  # Update with correct path
sftp_remote_path = DESTINATION_PATH  # Use variable instead of a hardcoded string

# Ensure local destination directory exists
os.makedirs(DESTINATION_PATH, exist_ok=True)

# Initialize GCS client
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
print("✅ Connection to GCS established.")

# Get today's date
today_date = "2025-02-13"  # Manually set for testing

# Initialize an empty list for downloaded files
downloaded_files = []

# Download files from GCS
for folder in FOLDERS:
    print(f"📂 Checking folder: {folder}")
    blobs = client.list_blobs(BUCKET_NAME, prefix=folder)
    
    for blob in blobs:
        file_name = os.path.basename(blob.name)
        if today_date in file_name:  # Filter files by date
            local_file_path = os.path.join(DESTINATION_PATH, file_name)
            try:
                blob.download_to_filename(local_file_path)
                downloaded_files.append(local_file_path)
                print(f"✅ Downloaded: {blob.name} --> {local_file_path}")
            except Exception as e:
                print(f"❌ Error downloading {blob.name}: {e}")

# If no files were downloaded, skip SFTP upload
if not downloaded_files:
    print("⚠️ No files to upload. Exiting.")
    exit()

# SFTP Upload
try:
    print("🚀 Starting SFTP upload...")

    # Disable HostKey checking (Temporary; enable in production)
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None  # This disables host key verification

    # Ensure private key file exists
    if not os.path.exists(PRIVATE_KEY_PATH):
        raise FileNotFoundError(f"❌ Private key file not found: {PRIVATE_KEY_PATH}")

    with pysftp.Connection(sftp_host, username=sftp_username, private_key=PRIVATE_KEY_PATH, cnopts=cnopts) as sftp:
        print("✅ SFTP Connection established.")
        
        for local_file in downloaded_files:
            remote_file_path = os.path.join(sftp_remote_path, os.path.basename(local_file))
            sftp.put(local_file, remote_file_path)
            print(f"✅ Uploaded: {local_file} --> {remote_file_path}")

except Exception as e:
    print(f"❌ SFTP upload failed: {e}")

print("🎉 ✅ Process completed successfully.")
