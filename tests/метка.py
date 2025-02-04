import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

REMOTE_PATH = "config/config_public.json"  # Файл в B2
LOCAL_PATH = r"C:\Users\boyar\hw\config_public.json"  # Путь сохранения

def download_from_b2():
    """Скачивает файл из Backblaze B2 и сохраняет его локально."""
    if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
        print("❌ Ошибка: не заданы переменные окружения B2.")
        return

    # Создаём клиент B2
    s3 = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY
    )

    try:
        # Скачивание файла
        print(f"🔄 Скачивание {REMOTE_PATH} из B2...")
        os.makedirs(os.path.dirname(LOCAL_PATH), exist_ok=True)  # Создаём папку, если её нет
        s3.download_file(B2_BUCKET_NAME, REMOTE_PATH, LOCAL_PATH)
        print(f"✅ Файл успешно загружен в {LOCAL_PATH}")
    except Exception as e:
        print(f"❌ Ошибка при скачивании: {e}")

if __name__ == "__main__":
    download_from_b2()
