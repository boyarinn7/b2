import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

LOCAL_FILE_PATH = r"C:\Users\boyar\hw\topics_tracker.json"  # Локальный файл
REMOTE_PATH = "data/topics_tracker.json"  # Путь в B2


def upload_local_file_to_b2():
    """Загружает локальный файл в Backblaze B2."""
    if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
        print("❌ Ошибка: не заданы переменные окружения B2.")
        return

    if not os.path.exists(LOCAL_FILE_PATH):
        print(f"❌ Ошибка: локальный файл {LOCAL_FILE_PATH} не найден.")
        return

    # Создаём клиент B2
    s3 = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY
    )

    try:
        # Читаем содержимое локального файла
        with open(LOCAL_FILE_PATH, "rb") as file:
            file_data = file.read()

        # Загружаем файл в B2
        print(f"🔄 Загружаем {LOCAL_FILE_PATH} → {REMOTE_PATH} в B2...")
        s3.put_object(
            Bucket=B2_BUCKET_NAME,
            Key=REMOTE_PATH,
            Body=file_data,
            ContentType='application/json'
        )
        print(f"✅ Файл {LOCAL_FILE_PATH} успешно загружен в B2 как {REMOTE_PATH}.")
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")


if __name__ == "__main__":
    upload_local_file_to_b2()
