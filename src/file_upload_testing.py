from google.cloud import storage
from dotenv import load_dotenv
import os

# Загружает переменные окружения из файла .env
load_dotenv()

# Теперь вы можете безопасно использовать переменные окружения, например:
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Создание клиента Google Cloud Storage
storage_client = storage.Client()

# Название вашего бакета
bucket_name = 'my_agro_bucket'

# Получение объекта бакета
bucket = storage_client.bucket(bucket_name)


# Функция для загрузки файла в бакет
def upload_to_bucket(blob_name, file_path):
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)
    print(f"Файл {file_path} успешно загружен в бакет {bucket_name} как {blob_name}.")


# Пример использования
file_path = "D:/Python/Agro_Trade_Analytics_Sampler/data/data.json"  # Путь к файлу, который вы хотите загрузить
blob_name = "data.json"  # Имя, под которым файл будет сохранён в бакете
upload_to_bucket(blob_name, file_path)
