import json
import os
import time
from datetime import datetime
import requests
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connection parameters for Google Cloud
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.getenv("GCS_BUCKET_NAME")
full_table_id = os.getenv("FULL_TABLE_ID")  # Assuming this is the full ID obtained from the table creation script

# Create GCS and BigQuery clients
credentials = service_account.Credentials.from_service_account_file(service_account_path)
storage_client = storage.Client(credentials=credentials, project=os.getenv("GCP_PROJECT_ID"))
bigquery_client = bigquery.Client(credentials=credentials, project=os.getenv("GCP_PROJECT_ID"))

# API request URL and parameters
url = "https://data.europa.eu/api/hub/search/search"  # Corrected URL
params = {
    'q': 'grain trade',
    'minDate': '2022-01-01T00:00:00Z',
    'maxDate': '2024-12-31T23:59:59Z',
    'limit': 100,
}

# Generate a unique file name
file_name = f"data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
file_path = os.path.join("temp", file_name)

# Ensure the 'temp' directory exists
os.makedirs(os.path.dirname(file_path), exist_ok=True)

# Send the GET request
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()

    # Save the data to a temporary file
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"File {file_name} has been saved locally.")

    # Upload the file to the GCS bucket
    blob = storage_client.bucket(bucket_name).blob(file_name)
    blob.upload_from_filename(file_path)
    print(f"File {file_name} has been uploaded to the GCS bucket {bucket_name}.")

    # Small delay to ensure the file is fully uploaded and ready
    time.sleep(10)  # Delay for 10 seconds

    # Attempt to load the file into BigQuery
    uri = f"gs://{bucket_name}/{file_name}"
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    try:
        load_job = bigquery_client.load_table_from_uri(uri, full_table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print(f"File {file_name} has been successfully loaded into BigQuery table {full_table_id}.")
    except Exception as e:
        print(f"Failed to load the file into BigQuery: {e}")
else:
    print(f"Failed to retrieve data from the API: {response.status_code}")

# Clean up the temporary file
if os.path.exists(file_path):
    os.remove(file_path)
    print(f"Temporary file {file_name} has been deleted.")
