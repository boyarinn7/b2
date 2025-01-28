import os
import hashlib
import json
import base64
from modules.error_handler import handle_error


def calculate_file_hash(file_path):
    """
    Вычисляет хэш файла.
    """
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        handle_error("File Hash Calculation Error", f"Файл не найден: {file_path}")
    except Exception as e:
        handle_error("File Hash Calculation Error", e)


def validate_json_structure(data, required_keys):
    """
    Валидирует структуру JSON-данных.
    """
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        handle_error("JSON Validation Error", f"Отсутствуют ключи: {missing_keys}")


def ensure_directory_exists(path):
    """
    Проверяет существование директории, создаёт при необходимости.
    """
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        handle_error("Directory Creation Error", e)


def encode_image_to_base64(image_path: str) -> str:
    """
    Кодирует изображение в строку base64.
    """
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except FileNotFoundError:
        handle_error("Image Encoding Error", f"Файл изображения не найден: {image_path}")
    except Exception as e:
        handle_error("Image Encoding Error", e)


def list_files_in_folder(s3, folder):
    """Возвращает список файлов в указанной папке B2."""
    try:
        objects = s3.list_objects_v2(Bucket="boyarinnbotbucket", Prefix=folder)
        return [obj["Key"] for obj in objects.get("Contents", [])]
    except Exception as e:
        print(f"❌ Ошибка при получении списка файлов в {folder}: {e}")
        return []

