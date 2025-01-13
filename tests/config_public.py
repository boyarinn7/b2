import os
import json
import boto3
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("b2_debug_script")

# Константы для доступа к B2
B2_BUCKET_NAME = "boyarinnbotbucket"  # Имя вашего бакета
B2_ENDPOINT = "https://s3.us-east-005.backblazeb2.com"  # Endpoint B2
B2_ACCESS_KEY = "00577030c4f964a0000000001"  # Ваш access key
B2_SECRET_KEY = "K005jbqS4BAIdtXF9vE5nXJgsV4NHVI"  # Ваш secret key
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"  # Путь к файлу в B2
CONFIG_PUBLIC_LOCAL_PATH = os.path.abspath("config_public.json")  # Локальный временный путь

# Функция для создания клиента B2
def get_b2_client():
    try:
        return boto3.client(
            "s3",
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_ACCESS_KEY,
            aws_secret_access_key=B2_SECRET_KEY,
        )
    except Exception as e:
        logger.error(f"❌ Ошибка при создании клиента B2: {e}")
        raise

# Функция для загрузки файла из B2
def download_file_from_b2(client, remote_path, local_path):
    try:
        client.download_file(B2_BUCKET_NAME, remote_path, local_path)
        logger.info(f"✅ Файл '{remote_path}' успешно загружен из B2 в {local_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке файла из B2: {e}")
        raise

# Основной процесс
def main():
    try:
        logger.info("🔄 Начинаю загрузку config_public.json из B2...")
        b2_client = get_b2_client()
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)

        # Чтение содержимого файла
        with open(CONFIG_PUBLIC_LOCAL_PATH, "r", encoding="utf-8") as file:
            config_public = json.load(file)
        logger.info(f"✅ Содержимое config_public.json: {json.dumps(config_public, ensure_ascii=False, indent=4)}")

        # Удаление временного файла (опционально)
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
        logger.info("🗑️ Временный файл config_public.json удалён.")
    except Exception as e:
        logger.error(f"❌ Ошибка в процессе: {e}")

if __name__ == "__main__":
    main()
