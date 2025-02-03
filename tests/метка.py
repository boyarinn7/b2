import os
import boto3

# 🔹 Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# 🔹 Настраиваем B2-клиент через boto3 (S3 API)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

def download_file():
    """Скачивает файл 666/20250203-0051.json из B2 в локальную папку."""
    b2_file_path = "666/20250203-0051.json"
    local_dir = r"C:\Users\boyar\hw"  # ✅ Используем raw-строку для Windows
    local_file_path = os.path.join(local_dir, os.path.basename(b2_file_path))

    try:
        os.makedirs(local_dir, exist_ok=True)  # ✅ Создаём папку, если её нет

        with open(local_file_path, "wb") as f:
            s3.download_fileobj(B2_BUCKET_NAME, b2_file_path, f)

        print(f"✅ Файл {b2_file_path} успешно скачан в {local_file_path}")
    except Exception as e:
        print(f"❌ Ошибка при скачивании {b2_file_path}: {e}")

if __name__ == "__main__":
    download_file()
