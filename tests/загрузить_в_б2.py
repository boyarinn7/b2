import os
import boto3

# Настройки B2
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Пути
LOCAL_FILE_PATH = r"C:\Users\boyar\777\config_public.json"
REMOTE_FILE_PATH = "config/config_public.json"

# Инициализация клиента B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

def upload_file_to_b2():
    """Загружает локальный файл в B2."""
    try:
        if not os.path.isfile(LOCAL_FILE_PATH):
            raise FileNotFoundError(f"❌ Локальный файл не найден: {LOCAL_FILE_PATH}")

        print(f"🔼 Загружаем {LOCAL_FILE_PATH} → {REMOTE_FILE_PATH}...")
        s3.upload_file(LOCAL_FILE_PATH, B2_BUCKET_NAME, REMOTE_FILE_PATH)
        print(f"✅ Файл успешно загружен в B2: {REMOTE_FILE_PATH}")
    except Exception as e:
        print(f"❌ Ошибка загрузки файла: {e}")

if __name__ == "__main__":
    upload_file_to_b2()
