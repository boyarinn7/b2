import os
import logging
from b2sdk.v2 import InMemoryAccountInfo, B2Api

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
B2_ACCESS_KEY = "00577030c4f964a0000000002"
B2_SECRET_KEY = "K005i/y4ymXbsv4nrAkBIqgFBIGR5RE"
B2_BUCKET_NAME = "boyarinnbotbucket"
SOURCE_B2_FILE = "data/topics_tracker.json"  # Исходный файл в B2
DEST_B2_FILE = "config/topics_tracker.json"  # Целевой файл в B2
LOCAL_DOWNLOAD_FILE = os.path.abspath("downloaded_topics_tracker.json")  # Скачанный файл
LOCAL_UPLOAD_FILE = os.path.abspath("topics_tracker.json")  # Локальный файл для загрузки (в корне)

# Инициализация клиента B2
info = InMemoryAccountInfo()
b2_api = B2Api(info)
try:
    b2_api.authorize_account("production", B2_ACCESS_KEY, B2_SECRET_KEY)
    logger.info("✅ Авторизация успешна")
except Exception as e:
    logger.error(f"❌ Ошибка авторизации: {e}")
    exit(1)

# Получение бакета
try:
    bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)
    logger.info(f"✅ Бакет {B2_BUCKET_NAME} найден")
except Exception as e:
    logger.error(f"❌ Ошибка получения бакета: {e}")
    exit(1)

# Шаг 1: Скачивание data/topics_tracker.json из B2
file_id = None
try:
    file_info = bucket.get_file_info_by_name(SOURCE_B2_FILE)
    file_id = file_info.id_
    file_size = file_info.size
    logger.info(f"✅ Файл {SOURCE_B2_FILE} существует в B2, file_id: {file_id}, размер: {file_size} байт")
except Exception as e:
    logger.error(f"❌ Файл {SOURCE_B2_FILE} не найден в B2: {e}")
    exit(1)

try:
    logger.info(f"Попытка скачать {SOURCE_B2_FILE} в {LOCAL_DOWNLOAD_FILE}")
    os.makedirs(os.path.dirname(LOCAL_DOWNLOAD_FILE), exist_ok=True)
    if os.path.exists(LOCAL_DOWNLOAD_FILE):
        os.remove(LOCAL_DOWNLOAD_FILE)
    download_dest = b2_api.download_file_by_id(file_id)
    download_dest.save_to(LOCAL_DOWNLOAD_FILE)
    if os.path.exists(LOCAL_DOWNLOAD_FILE):
        local_size = os.path.getsize(LOCAL_DOWNLOAD_FILE)
        logger.info(f"✅ Файл {LOCAL_DOWNLOAD_FILE} скачан, размер: {local_size} байт")
        if local_size == file_size:
            logger.info("✅ Размер совпадает с B2")
        else:
            logger.warning(f"⚠️ Размер не совпадает: B2={file_size}, локально={local_size}")
    else:
        logger.error(f"❌ Файл {LOCAL_DOWNLOAD_FILE} не найден после скачивания")
except Exception as e:
    logger.error(f"❌ Ошибка при скачивании: {e}")

# Шаг 2: Загрузка локального topics_tracker.json в B2
if os.path.exists(LOCAL_UPLOAD_FILE):
    try:
        upload_size = os.path.getsize(LOCAL_UPLOAD_FILE)
        logger.info(f"Попытка загрузить {LOCAL_UPLOAD_FILE} (размер: {upload_size} байт) в {DEST_B2_FILE}")
        bucket.upload_local_file(local_file=LOCAL_UPLOAD_FILE, file_name=DEST_B2_FILE)
        logger.info(f"✅ Файл {DEST_B2_FILE} успешно загружен в B2")
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке: {e}")
else:
    logger.error(f"❌ Локальный файл {LOCAL_UPLOAD_FILE} не найден, загрузка невозможна")
    exit(1)