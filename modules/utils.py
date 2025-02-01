import os
import hashlib
import json
import base64
import inspect

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

def is_folder_empty(s3, bucket_name, folder_prefix):
    """
    Проверяет, пустая ли папка в B2.
    """
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        return "Contents" not in response  # Если нет содержимого, папка пустая
    except Exception as e:
        handle_error("B2 Folder Check Error", e)


def load_config_public(config_path):
    """
    Загружает config_public.json.
    """
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    return {}

def save_config_public(config_path, config_data):
    """
    Сохраняет config_public.json.
    """
    with open(config_path, 'w', encoding='utf-8') as file:
        json.dump(config_data, file, indent=4, ensure_ascii=False)



def move_to_archive(s3, bucket_name, generation_id, logger):
    """
    Перемещает файлы, относящиеся к generation_id, в архив.
    """

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

                # Копируем файл
                s3.copy_object(Bucket=bucket_name, CopySource={"Bucket": bucket_name, "Key": old_key}, Key=new_key)

                # Удаляем оригинал
                s3.delete_object(Bucket=bucket_name, Key=old_key)

                logger.info(f"📁 Файл {old_key} перемещён в {new_key}")

    except Exception as e:
        handle_error(logger, "Ошибка перемещения в архив", e)

    # Удаляем generation_id из config_public.json
    config_data = load_config_public(s3)
    if "generation_id" in config_data and generation_id in config_data["generation_id"]:
        config_data["generation_id"].remove(generation_id)  # Удаляем ID группы
        save_config_public(s3, config_data)  # Сохраняем обновлённый config_public.json
        logger.info(f"🗑️ Удалён generation_id {generation_id} из config_public.json")



