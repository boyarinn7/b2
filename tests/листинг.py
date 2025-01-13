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
CONFIG_GEN_LOCAL_PATH = os.path.abspath("core/config/config_gen.json")  # Путь к локальному файлу

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

# Функция для получения списка файлов в папках
def list_b2_folder_contents(client, folder_names):
    try:
        for folder in folder_names:
            logger.info(f"📂 Листинг содержимого папки: {folder}")
            response = client.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder)
            if 'Contents' in response:
                for obj in response['Contents']:
                    logger.info(f"📄 Найден файл: {obj['Key']}")
            else:
                logger.warning(f"⚠️ Папка '{folder}' пуста или не существует.")
    except Exception as e:
        logger.error(f"❌ Ошибка при листинге папок: {e}")
        raise

# Функция для чтения локального файла config_gen.json
def read_local_config_gen():
    try:
        logger.info(f"🔄 Чтение файла {CONFIG_GEN_LOCAL_PATH}...")
        with open(CONFIG_GEN_LOCAL_PATH, "r", encoding="utf-8") as file:
            config_gen = json.load(file)
        logger.info(f"✅ Содержимое config_gen.json: {json.dumps(config_gen, ensure_ascii=False, indent=4)}")
        return config_gen
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении config_gen.json: {e}")
        raise

# Основной процесс
def main():
    try:
        logger.info("🔄 Начинаю выполнение процесса...")
        b2_client = get_b2_client()

        # Листинг папок 444, 555, 666
        list_b2_folder_contents(b2_client, ["444/", "555/", "666/"])

        # Загрузка config_public.json из B2
        logger.info("🔄 Загрузка config_public.json из B2...")
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)

        # Чтение содержимого config_public.json
        with open(CONFIG_PUBLIC_LOCAL_PATH, "r", encoding="utf-8") as file:
            config_public = json.load(file)
        logger.info(f"✅ Содержимое config_public.json: {json.dumps(config_public, ensure_ascii=False, indent=4)}")

        # Удаление временного файла (опционально)
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
        logger.info("🗑️ Временный файл config_public.json удалён.")

        # Чтение содержимого config_gen.json
        read_local_config_gen()
    except Exception as e:
        logger.error(f"❌ Ошибка в процессе: {e}")

if __name__ == "__main__":
    main()
