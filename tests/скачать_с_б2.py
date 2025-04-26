import os
import boto3

# Проверка переменных окружения для доступа к B2
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# Инициализация клиента B2 через boto3 (S3-совместимый)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

# Константы: конкретный PNG-файл и локальная папка назначения
SPECIFIC_PNG_KEY = "666/20250426-1533.png"
LOCAL_DESTINATION_DIR = r"C:\Users\boyar\777\555"


def ensure_local_dir(directory: str):
    """Создаёт папку назначения, если её нет."""
    if not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        print(f"Создана директория: {directory}")


def download_png():
    """Скачивает один PNG-файл из B2 в локальную папку."""
    ensure_local_dir(LOCAL_DESTINATION_DIR)
    local_path = os.path.join(LOCAL_DESTINATION_DIR, os.path.basename(SPECIFIC_PNG_KEY))
    try:
        print(f"⬇️ Скачиваем {SPECIFIC_PNG_KEY} -> {local_path}")
        s3.download_file(B2_BUCKET_NAME, SPECIFIC_PNG_KEY, local_path)
        print(f"✅ Файл сохранён локально: {local_path}")
    except Exception as e:
        print(f"💥 Ошибка при скачивании {SPECIFIC_PNG_KEY}: {e}")


if __name__ == "__main__":
    download_png()
    print("--- Скрипт завершил работу ---")
