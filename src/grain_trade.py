import json  # Import the json library for parsing JSON data.
import os  # Import the os library to interact with the operating system.
from datetime import datetime  # Import datetime to work with dates and times.
import requests  # Import requests to make HTTP requests.
from google.cloud import storage, \
    bigquery  # Import Google Cloud Storage and BigQuery libraries for data storage and analysis.
from google.oauth2 import service_account  # Import service account for Google Cloud authentication.
from dotenv import load_dotenv  # Import dotenv to load environment variables from a .env file.
from google.cloud.exceptions import NotFound  # Import NotFound to catch exceptions where a resource is not found.


# Define a function to update or add a key-value pair in the .env file.
def update_env_file(env_path, key, value):
    # Open the .env file in read mode to read its current contents.
    with open(env_path, "r") as file:
        lines = file.readlines()

    # Initialize a flag to determine if the key already exists.
    found = False
    # Loop through each line in the file to find if the key exists.
    for i, line in enumerate(lines):
        if line.startswith(f"{key}="):
            # If the key exists, update the line with the new value.
            lines[i] = f'{key}="{value}"\n'
            found = True
            break

    # If the key was not found, append it to the end of the file.
    if not found:
        lines.append(f'{key}="{value}"\n')

    # Open the .env file in write mode to update its contents.
    with open(env_path, "w") as file:
        file.writelines(lines)


# Set the path to the .env file and load its contents.
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Retrieve configuration values from environment variables.
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.getenv("GCS_BUCKET_NAME")
project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
table_id = os.getenv("BIGQUERY_TABLE_ID")

# Initialize a BigQuery client with credentials.
bigquery_client = bigquery.Client.from_service_account_json(service_account_path)

# Check if the table ID is provided; if so, attempt to use the specified table.
if table_id:
    try:
        # Attempt to retrieve the specified table to ensure it exists.
        bigquery_client.get_table(f"{project_id}.{dataset_id}.{table_id}")
        print(f"Using the existing table: {project_id}.{dataset_id}.{table_id}")
    except NotFound:
        # If the table doesn't exist, log a message and continue using the provided ID.
        print(f"Table {project_id}.{dataset_id}.{table_id} not found. Using the specified ID to create a new table.")
else:
    # If no table ID is provided, generate a new one using the current timestamp.
    new_table_id = datetime.now().strftime('table_%Y%m%d_%H%M%S')
    table_id = new_table_id
    # Update the .env file with the new table ID.
    update_env_file(dotenv_path, "BIGQUERY_TABLE_ID", new_table_id)
    print(f"Creating a new table with ID: {new_table_id}")

# Set parameters for an API request to retrieve data.
url = "https://data.europa.eu/api/hub/search/search"
params = {'q': 'grain trade', 'minDate': '2022-01-01T00:00:00Z', 'maxDate': '2024-12-31T23:59:59Z', 'limit': 100}

# Generate a unique filename for the JSON file to be saved.
json_file_name = f"data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
json_file_path = os.path.join("temp", json_file_name)
# Ensure the directory for the file exists.
os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

# Make the HTTP request and process the response.
response = requests.get(url, params=params)
if response.status_code == 200:
    # Extract data from the response.
    data = response.json()['result']['results']
    # Open the JSON file for writing.
    with open(json_file_path, 'w', encoding='utf-8') as file:
        # Process each item in the data.
        for item in data:
            # Convert any list values within items to JSON strings.
            for key, value in item.items():
                if isinstance(value, list):
                    item[key] = json.dumps(
                        value)  # Convert lists within each item to JSON strings for compatibility with BigQuery.
            # Write each processed item to the JSON file as a string.
            file.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"JSON file saved: {json_file_name}")  # Notify that the JSON file has been successfully saved.

    # Initialize Google Cloud Storage client with the service account credentials.
    storage_client = storage.Client(
        credentials=service_account.Credentials.from_service_account_file(service_account_path), project=project_id)
    # Reference the specific file (blob) in the designated bucket.
    blob = storage_client.bucket(bucket_name).blob(json_file_name)
    # Upload the JSON file to Google Cloud Storage.
    blob.upload_from_filename(json_file_path)
    print(f"File uploaded to GCS: {json_file_name}")  # Notify that the file has been uploaded to Google Cloud Storage.

    # Prepare to load the data into BigQuery.
    uri = f"gs://{bucket_name}/{json_file_name}"  # Specify the URI of the file in Google Cloud Storage.
    # Configure the job to load data from the JSON file into BigQuery, enabling schema auto-detection.
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=True,
                                        write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    # Start the load job to transfer data from GCS to BigQuery and waits for it to complete.
    load_job = bigquery_client.load_table_from_uri(uri, f"{project_id}.{dataset_id}.{table_id}", job_config=job_config)
    load_job.result()  # Wait for the job to complete.
    print(f"Data loaded to BigQuery: {load_job.destination}")  # Inform that the data has been loaded into BigQuery.

    # Cleanup: deletes the temporary JSON file after the data is loaded to BigQuery.
    os.remove(json_file_path)
    print("Temporary file deleted.")  # Inform that the temporary file has been deleted.
else:
    # Inform if there was a failure in retrieving data from the API.
    print("Failed to retrieve data.")
