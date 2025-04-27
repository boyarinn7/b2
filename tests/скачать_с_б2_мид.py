import os
import sys
import logging
import boto3

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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

# Инициализация клиента B2
b2 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

def delete_files():
    keys = [
        "666/20250426-2038.json",
        "666/20250427-173222.json",
        "666/20250427-173222.mp4",
        "666/20250427-173222.png",
    ]
    for key in keys:
        try:
            b2.delete_object(Bucket=B2_BUCKET_NAME, Key=key)
            logger.info(f"✅ Удалён {key}")
        except Exception as e:
            logger.error(f"❌ Ошибка удаления {key}: {e}")

if __name__ == "__main__":
    delete_files()
