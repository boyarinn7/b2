import os
import sys
import logging
import boto3

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Переменные окружения для B2
B2_ACCESS_KEY   = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY   = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME  = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT     = os.getenv("B2_ENDPOINT")

# Проверяем, что все нужные переменные заданы
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    logger.error("❌ Не заданы переменные окружения для B2")
    sys.exit(1)

# Инициализация клиента B2
b2 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

def download_file():
    storage_key = "666/20250427-1348.json"
    local_dir   = r"C:\Users\boyar\777\555"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, os.path.basename(storage_key))

    try:
        b2.download_file(B2_BUCKET_NAME, storage_key, local_path)
        logger.info(f"✅ Загружен {storage_key} → {local_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка при скачивании {storage_key}: {e}")

if __name__ == "__main__":
    download_file()
