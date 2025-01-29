import os
import boto3

# Настройки B2
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Настройки загрузки
B2_FILE_PATH = "555/20250129-1300.json"  # Путь в B2
LOCAL_SAVE_PATH = r"C:\Users\boyar\a1\data\downloaded\20250129-1300.json"  # Локальный путь

# Инициализация клиента B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

def download_file_from_b2():
    """Скачивает файл из B2 в локальное хранилище."""
    try:
        os.makedirs(os.path.dirname(LOCAL_SAVE_PATH), exist_ok=True)
        s3.download_file(B2_BUCKET_NAME, B2_FILE_PATH, LOCAL_SAVE_PATH)
        print(f"✅ Файл успешно загружен: {LOCAL_SAVE_PATH}")
    except Exception as e:
        print(f"❌ Ошибка загрузки файла: {e}")

if __name__ == "__main__":
    download_file_from_b2()
