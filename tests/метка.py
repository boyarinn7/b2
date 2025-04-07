import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Пути
LOCAL_FILE_PATH = r"C:\Users\boyar\777\topics_tracker.json"
REMOTE_FILE_PATH = "config/topics_tracker.json"

# Проверяем переменные окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# Создаём клиент B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def upload_file():
    """Загружает локальный файл в B2 в config/topics_tracker.json."""
    try:
        print(f"🔄 Загружаем {LOCAL_FILE_PATH} -> {REMOTE_FILE_PATH} в B2")
        s3.upload_file(LOCAL_FILE_PATH, B2_BUCKET_NAME, REMOTE_FILE_PATH)
        print(f"✅ Файл успешно загружен в B2: {REMOTE_FILE_PATH}")
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")

if __name__ == "__main__":
    upload_file()
