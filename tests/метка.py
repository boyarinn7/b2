import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Локальный путь для сохранения файла
LOCAL_FILE_PATH = r"C:\Users\boyar\777\topics_tracker.json"
REMOTE_FILE_PATH = "data/topics_tracker.json"

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

def download_file():
    """Скачивает файл из хранилища и сохраняет его локально."""
    try:
        print(f"🔄 Скачиваем {REMOTE_FILE_PATH} -> {LOCAL_FILE_PATH}")
        s3.download_file(B2_BUCKET_NAME, REMOTE_FILE_PATH, LOCAL_FILE_PATH)
        print(f"✅ Файл успешно сохранён: {LOCAL_FILE_PATH}")
    except Exception as e:
        print(f"❌ Ошибка при скачивании файла: {e}")

if __name__ == "__main__":
    download_file()
