import os
import boto3
import json

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Пути
LOCAL_FILE_PATH = r"C:\Users\boyar\777\config_midjourney.json"
REMOTE_FILE_PATH = "config/config_midjourney.json"

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# Клиент B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def reset_midjourney_task():
    try:
        # Шаг 1: Скачивание файла
        print(f"⬇️ Скачиваем {REMOTE_FILE_PATH} → {LOCAL_FILE_PATH}")
        s3.download_file(B2_BUCKET_NAME, REMOTE_FILE_PATH, LOCAL_FILE_PATH)

        # Шаг 2: Затираем содержимое
        with open(LOCAL_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump({"midjourney_task": None}, f, ensure_ascii=False, indent=2)
        print("🧹 Содержимое очищено и перезаписано.")

        # Шаг 3: Загрузка обратно
        print(f"🔼 Загружаем обратно в {REMOTE_FILE_PATH}")
        s3.upload_file(LOCAL_FILE_PATH, B2_BUCKET_NAME, REMOTE_FILE_PATH)
        print("✅ Готово.")

    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    reset_midjourney_task()
