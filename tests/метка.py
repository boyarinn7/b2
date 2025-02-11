import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

REMOTE_PATH = "data/topics_tracker.json"  # Путь в B2


def get_file_from_b2():
    """Загружает содержимое файла из Backblaze B2."""
    if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
        print("❌ Ошибка: не заданы переменные окружения B2.")
        return None

    # Создаём клиент B2
    s3 = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY
    )

    try:
        # Загружаем файл из B2
        print(f"🔄 Загружаем {REMOTE_PATH} из B2...")
        response = s3.get_object(Bucket=B2_BUCKET_NAME, Key=REMOTE_PATH)
        file_data = response['Body'].read().decode('utf-8')
        print(f"✅ Файл {REMOTE_PATH} успешно загружен из B2.")
        return file_data
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")
        return None


if __name__ == "__main__":
    content = get_file_from_b2()
    if content:
        print(content)
