import os
import hashlib
import json
import base64
import inspect

from modules.error_handler import handle_error
from modules.logger import get_logger

logger = get_logger("utils")

CONFIG_PATH = "config/config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()

def load_topics_tracker():
    tracker_path = config["FILE_PATHS"]["topics_tracker"]
    if os.path.exists(tracker_path):
        try:
            with open(tracker_path, "r", encoding="utf-8") as file:
                return json.load(file)
        except json.JSONDecodeError:
            return {}
    return {}

def save_topics_tracker(tracker):
    tracker_path = config["FILE_PATHS"]["topics_tracker"]
    os.makedirs(os.path.dirname(tracker_path), exist_ok=True)
    with open(tracker_path, "w", encoding="utf-8") as file:
        json.dump(tracker, file, ensure_ascii=False, indent=4)

def calculate_file_hash(file_path):
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        handle_error("File Hash Calculation Error", f"Файл не найден: {file_path}")
    except Exception as e:
        handle_error("File Hash Calculation Error", e)

def validate_json_structure(data, required_keys):
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        handle_error("JSON Validation Error", f"Отсутствуют ключи: {missing_keys}")

def ensure_directory_exists(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        handle_error("Directory Creation Error", e)

def encode_image_to_base64(image_path: str) -> str:
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except FileNotFoundError:
        handle_error("Image Encoding Error", f"Файл изображения не найден: {image_path}")
    except Exception as e:
        handle_error("Image Encoding Error", e)

def list_files_in_folder(s3, folder):
    try:
        objects = s3.list_objects_v2(Bucket="boyarinnbotbucket", Prefix=folder)
        return [obj["Key"] for obj in objects.get("Contents", [])]
    except Exception as e:
        logger.error(f"❌ Ошибка при получении списка файлов в {folder}: {e}")
        return []

def is_folder_empty(s3, bucket_name, folder_prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        return "Contents" not in response
    except Exception as e:
        handle_error("B2 Folder Check Error", e)

def load_config_public(config_path):
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    return {}

def save_config_public(config_path, config_data):
    with open(config_path, 'w', encoding='utf-8') as file:
        json.dump(config_data, file, indent=4, ensure_ascii=False)

def move_to_archive(s3, bucket_name, generation_id, logger):
    logger.info(f"🛠 Проверка s3 в {__file__}, строка {inspect.currentframe().f_lineno}: {type(s3)}")
    logger.info(f"🛠 Перед вызовом move_to_archive(): s3={type(s3)}")
    archive_folder = f"archive/{generation_id}/"
    source_folder = f"generated/{generation_id}/"
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_folder)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                old_key = obj["Key"]
                new_key = old_key.replace(source_folder, archive_folder, 1)
                s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": old_key}, Key=new_key)
                s3.delete_object(Bucket=bucket_name, Key=old_key)
                logger.info(f"📁 Файл {old_key} перемещён в {new_key}")
    except Exception as e:
        handle_error(logger, "Ошибка перемещения в архив", e)
    config_data = load_config_public(s3)  # Ошибка, нужно исправить позже
    if "generation_id" in config_data and generation_id in config_data["generation_id"]:
        config_data["generation_id"].remove(generation_id)
        save_config_public(s3, config_data)  # Ошибка, нужно исправить позже
        logger.info(f"🗑️ Удалён generation_id {generation_id} из config_public.json")

def load_from_b2(b2_client, b2_path, local_path):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        b2_client.download_file(bucket_name, b2_path, local_path)
        with open(local_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"✅ Загружен файл из B2: {b2_path} -> {local_path}, содержимое: {json.dumps(data, ensure_ascii=False)}")
        return data
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки из B2 {b2_path}: {e}")
        raise

def save_to_b2(b2_client, data, b2_path, local_path):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        b2_client.upload_file(local_path, bucket_name, b2_path)
        logger.info(f"✅ Сохранено в B2: {b2_path}, содержимое: {json.dumps(data, ensure_ascii=False)}")
        os.remove(local_path)
        logger.info(f"🗑️ Удален временный файл: {local_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения в B2 {b2_path}: {e}")
        raise