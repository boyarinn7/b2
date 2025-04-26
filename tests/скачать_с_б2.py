import os
import boto3
import json
import tempfile
import logging
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

# Переменные окружения B2
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

CONFIG_KEY = "config/config_public.json"

# Проверка наличия env vars
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    logger.error("Не заданы переменные окружения для B2.")
    sys.exit(1)

# Инициализация клиента
s3 = boto3.client(
    's3',
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def main():
    # Создаём временный файл
    with tempfile.NamedTemporaryFile('w+', delete=False, suffix='.json') as tmp:
        tmp_path = tmp.name
    try:
        # Скачиваем из B2
        logger.info(f"⬇️ Загрузка {CONFIG_KEY} в {tmp_path}")
        s3.download_file(B2_BUCKET_NAME, CONFIG_KEY, tmp_path)

        # Изменяем JSON
        with open(tmp_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        data['generation_id'] = []
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info("✏️ Поле 'generation_id' очищено.")

        # Загружаем обратно
        logger.info(f"⬆️ Загрузка изменённого файла в {CONFIG_KEY}")
        s3.upload_file(tmp_path, B2_BUCKET_NAME, CONFIG_KEY)
        logger.info("✅ Обновление config_public.json завершено.")
    except Exception as e:
        logger.error(f"Ошибка процесса: {e}")
    finally:
        # Удаляем временный файл
        try:
            os.remove(tmp_path)
            logger.debug(f"Удалён временный файл {tmp_path}")
        except OSError:
            pass

if __name__ == '__main__':
    main()
