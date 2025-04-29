import os
import sys
import logging
import boto3

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Переменные окружения для B2
B2_ACCESS_KEY  = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY  = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT    = os.getenv("B2_ENDPOINT")

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    logger.error("❌ Не заданы переменные окружения для B2")
    sys.exit(1)

# Инициализация клиента B2/S3
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

# Параметры
PREFIX = "666/"
SYSTEM_FILE = f"{PREFIX}placeholder.bzEmpty"
LOCAL_DIR = r"C:\Users\boyar\777\555"

def sync_and_delete():
    # Убедимся, что локальная папка существует
    os.makedirs(LOCAL_DIR, exist_ok=True)

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=B2_BUCKET_NAME, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # пропускаем саму папку и системный файл
            if key == PREFIX or key == SYSTEM_FILE:
                continue

            filename = os.path.basename(key)
            local_path = os.path.join(LOCAL_DIR, filename)

            try:
                # скачиваем
                logger.info(f"⬇️ Скачиваем {key} → {local_path}")
                s3.download_file(B2_BUCKET_NAME, key, local_path)

                # удаляем из бакета
                logger.info(f"⬆️ Удаляем {key} из бакета")
                s3.delete_object(Bucket=B2_BUCKET_NAME, Key=key)

                logger.info(f"✅ Обработан {key}")
            except Exception as e:
                logger.error(f"❌ Ошибка с {key}: {e}")

if __name__ == "__main__":
    sync_and_delete()
