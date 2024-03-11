import json
import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from google.cloud import storage, bigquery
from google.cloud.bigquery import DatasetReference, SchemaField
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

# Загружаем переменные окружения из файла .env
load_dotenv()

# Параметры для подключения к Google Cloud
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.getenv("GCS_BUCKET_NAME")
project_id = os.getenv("GCP_PROJECT_ID")
dataset_id = os.getenv("BIGQUERY_DATASET_ID")
table_id = os.getenv("BIGQUERY_TABLE_ID")

# Проверяем наличие необходимых переменных
if not all([service_account_path, bucket_name, project_id, dataset_id, table_id]):
    raise EnvironmentError("Some environment variables are not set.")

# Создаем клиенты GCS и BigQuery
credentials = service_account.Credentials.from_service_account_file(service_account_path)
storage_client = storage.Client(credentials=credentials, project=project_id)
bigquery_client = bigquery.Client(credentials=credentials, project=project_id)


# Функция создания таблицы, если она не существует
def create_table_if_not_exists(client, dataset, table):
    # Создаем ссылку на набор данных и таблицу
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)

    # Попытка получить таблицу для проверки ее существования
    try:
        client.get_table(table_ref)
        print(f"Таблица {table} уже существует.")
    except NotFound:
        # Определение схемы таблицы
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
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Создана таблица {table.table_id} в наборе данных {dataset}.")


# Проверяем и создаем таблицу, если нужно
create_table_if_not_exists(bigquery_client, dataset_id, table_id)

# URL для запроса к API
url = "https://data.europa.eu/api/hub/search"

# Параметры запроса
params = {
    'q': 'grain trade',
    'minDate': '2022-01-01T00:00:00Z',
    'maxDate': '2024-12-31T23:59:59Z',
    'limit': 100,
}

# Имя файла с уникальным временным штампом
file_name = f"data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
file_path = f"temp/{file_name}"


# Отправляем GET-запрос
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()

    # Сохраняем данные во временный файл
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    # Загружаем файл в бакет
    blob = storage_client.bucket(bucket_name).blob(file_name)
    blob.upload_from_filename(file_path)

    print(f"Файл {file_name} успешно загружен в бакет {bucket_name}.")

    # Загружаем данные из GCS в BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    uri = f"gs://{bucket_name}/{file_name}"
    # Создаем объекты DatasetReference и TableReference
    dataset_ref = DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)
    # Загружаем данные из GCS в BigQuery
    load_job = bigquery_client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Ожидаем завершения загрузки

    print(f"Файл {file_name} успешно загружен в BigQuery в таблицу {table_id}.")

else:
    print("Ошибка запроса:", response.status_code, response.text)

# Удаляем временный файл
if os.path.exists(file_path):
    os.remove(file_path)
