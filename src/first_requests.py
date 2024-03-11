import requests
import json
from dotenv import load_dotenv
import os

# Загружает переменные окружения из файла .env
load_dotenv()

# Теперь вы можете безопасно использовать переменные окружения, например:
service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# URL для запроса к API
url = "https://data.europa.eu/api/hub/search/search"  # Исправленный URL согласно документации

# Параметры запроса
params = {
    'q': 'grain trade',  # Пример ключевого слова для поиска
    'minDate': '2022-01-01T00:00:00Z',  # Начало периода
    'maxDate': '2024-12-31T23:59:59Z',  # Конец периода
    'limit': 100,  # Пример лимита на количество результатов
}

# Путь к директории data относительно текущего файла скрипта
data_directory = os.path.join(os.path.dirname(__file__), '..', 'data')
if not os.path.exists(data_directory):
    os.makedirs(data_directory)

# Отправляем GET-запрос
response = requests.get(url, params=params)

# Проверяем статус ответа
if response.status_code == 200:
    # Преобразуем ответ в JSON и обрабатываем данные
    data = response.json()

    # Сохраняем данные в файл в директории data
    with open(os.path.join(data_directory, 'data.json'), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("Данные успешно сохранены в файл 'data/data.json'.")
else:
    print("Ошибка запроса:", response.status_code, response.text)
