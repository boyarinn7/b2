import json
import os
from modules.api_clients import get_b2_client
from modules.logger import get_logger

# === Настройки ===
B2_BUCKET_NAME = "boyarinnbotbucket"
CONFIG_PUBLIC_PATH = "config/config_public.json"

# === Логгер ===
logger = get_logger("b2_publish_marker")

def load_config_public(s3):
    """Загружает config_public.json из B2."""
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки config_public.json: {e}")
        return {}

def save_config_public(s3, data):
    """Сохраняет config_public.json обратно в B2."""
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        with open(local_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(local_path, B2_BUCKET_NAME, CONFIG_PUBLIC_PATH)
        logger.info(f"✅ config_public.json обновлён: {data}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_public.json: {e}")

def mark_as_published(folder):
    """Добавляет метку о публикации в config_public.json."""
    s3 = get_b2_client()
    config_data = load_config_public(s3)

    # Добавляем метку "publish"
    config_data["publish"] = folder
    save_config_public(s3, config_data)

    logger.info(f"✅ Установлена метка публикации: {folder}")

# === Запуск скрипта ===
if __name__ == "__main__":
    folder_to_publish = "444/"  # Укажите папку, которая должна быть опубликована
    mark_as_published(folder_to_publish)
