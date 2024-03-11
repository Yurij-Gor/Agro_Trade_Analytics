import os
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Table
from google.oauth2 import service_account
from dotenv import load_dotenv
from google.cloud.exceptions import NotFound
from datetime import datetime

# Load environment variables from the .env file located in the project's root directory
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Google Cloud connection parameters
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
# If the table name is not set, use the current time to create a unique name
table_id = os.getenv("BIGQUERY_TABLE_ID") or f"table_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# Create a BigQuery client
credentials = service_account.Credentials.from_service_account_file(service_account_path)
bigquery_client = bigquery.Client(credentials=credentials, project=project_id)

def append_to_env_file(env_var, value, env_file_path):
    """Adds a variable and its value to the .env file located at the specified path."""
    with open(env_file_path, 'a') as f:
        f.write(f'{env_var}="{value}"\n')

# Function to create a table if it does not exist
def create_table(client, ds_id, tbl_id):
    full_table_id = f"{project_id}.{ds_id}.{tbl_id}"
    try:
        client.get_table(full_table_id)  # Try to get the table
        print(f"Table {tbl_id} already exists.")
    except NotFound:
        # Define the table schema
        schema = [
            SchemaField("identifier", "STRING", mode="NULLABLE"),
            SchemaField("country", "STRING", mode="NULLABLE"),
            SchemaField("creator", "STRING", mode="NULLABLE"),
            SchemaField("keywords", "STRING", mode="NULLABLE"),
            SchemaField("resource", "STRING", mode="NULLABLE"),
            SchemaField("catalog", "STRING", mode="NULLABLE"),
            SchemaField("description", "STRING", mode="NULLABLE"),
            SchemaField("landing_page", "STRING", mode="NULLABLE"),
            SchemaField("version_info", "STRING", mode="NULLABLE"),
            SchemaField("title", "STRING", mode="NULLABLE"),
            SchemaField("distributions", "STRING", mode="NULLABLE"),
        ]
        table = Table(full_table_id, schema=schema)
        created_table = client.create_table(table)  # Create the table
        print(f"Table {created_table.table_id} created.")
        return created_table.table_id  # Return the full identifier of the created table

# Create the table and get its full identifier
created_table_id = create_table(bigquery_client, dataset_id, table_id)

# The path to the .env file in the project's root directory must be correctly specified
env_file_path = os.path.join(os.path.dirname(__file__), '..', '.env')

# Pass all three arguments to the function
append_to_env_file('BIGQUERY_TABLE_ID', created_table_id, env_file_path)

print(f"Table identifier {created_table_id} added to the .env file.")
