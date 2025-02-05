import os
import boto3
import json

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

REMOTE_PATH = "data/topics_tracker.json"  # Файл в B2
EMPTY_JSON = {}  # Пустой JSON

def upload_empty_json_to_b2():
    """Создаёт пустой JSON-файл в Backblaze B2."""
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
        # Загружаем пустой JSON в B2
        print(f"🔄 Создаём {REMOTE_PATH} в B2...")
        s3.put_object(
            Bucket=B2_BUCKET_NAME,
            Key=REMOTE_PATH,
            Body=json.dumps(EMPTY_JSON, indent=4).encode('utf-8'),
            ContentType='application/json'
        )
        print(f"✅ Файл {REMOTE_PATH} успешно создан в B2.")
    except Exception as e:
        print(f"❌ Ошибка при создании файла: {e}")

if __name__ == "__main__":
    upload_empty_json_to_b2()
