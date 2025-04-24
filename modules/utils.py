# -*- coding: utf-8 -*-
# В файле modules/utils.py

import os
import json
import logging
import time
from pathlib import Path
import requests
import shutil
from datetime import datetime, timezone

# --- Получение логгера ---
try:
    from .logger import get_logger
    logger = get_logger(__name__)
except ImportError:
     try:
         from logger import get_logger
         logger = get_logger(__name__)
     except ImportError:
        logger = logging.getLogger(__name__)
        if not logger.hasHandlers():
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            logger.warning("Кастомный логгер не найден, используется стандартный logging.")

# --- Исключения BotoCore ---
try:
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    logger.warning("BotoCore не найден. Функции B2 могут быть недоступны.")
    ClientError = Exception # Fallback
    NoCredentialsError = Exception # Fallback

# --- Функции ---

def ensure_directory_exists(file_path_str):
    """Убеждается, что директория для указанного пути файла существует."""
    try:
        path_obj = Path(file_path_str)
        directory = path_obj.parent
        if not directory.exists():
            logger.info(f"Создание директории: {directory}")
            directory.mkdir(parents=True, exist_ok=True)
        elif not directory.is_dir():
             logger.error(f"Путь {directory} существует, но не является директорией!")
             raise NotADirectoryError(f"Path exists but is not a directory: {directory}")
    except Exception as e:
        logger.error(f"Ошибка при создании директории для {file_path_str}: {e}", exc_info=True)
        raise

def load_json_config(file_path):
    """Загружает JSON из локального файла."""
    path_obj = Path(file_path)
    if not path_obj.is_file():
        logger.warning(f"Файл конфигурации не найден: {file_path}")
        return None
    try:
        with open(path_obj, 'r', encoding='utf-8') as f:
            # Проверяем, не пустой ли файл
            content = f.read()
            if not content.strip():
                logger.warning(f"Файл {file_path} пуст.")
                return None
            # Если не пустой, пытаемся загрузить JSON
            data = json.loads(content)
        # logger.debug(f"Конфигурация успешно загружена из {file_path}")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON в файле {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Не удалось прочитать файл {file_path}: {e}", exc_info=True)
        return None

def save_local_json(file_path_str, data):
    """Сохраняет данные в локальный JSON файл."""
    try:
        ensure_directory_exists(file_path_str)
        path_obj = Path(file_path_str)
        with open(path_obj, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Данные успешно сохранены в локальный файл: {path_obj}")
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения данных в {file_path_str}: {e}", exc_info=True)
        return False

def load_b2_json(s3_client, bucket_name, remote_path, local_temp_path, default_value=None):
    """Загружает JSON из B2, сохраняя во временный локальный файл."""
    try:
        ensure_directory_exists(local_temp_path)
        logger.debug(f"Попытка загрузки {remote_path} из B2 в {local_temp_path}...")
        s3_client.download_file(bucket_name, remote_path, local_temp_path)
        logger.info(f"Успешно загружен {remote_path} из B2.")
        # Загружаем уже из локального temp файла с проверкой на пустоту/валидность
        data = load_json_config(local_temp_path)
        if data is not None:
             logger.info(f"Успешно распарсен JSON из {local_temp_path}.")
             return data
        else:
             # Если load_json_config вернул None (файл пуст или невалидный JSON)
             logger.warning(f"Файл {local_temp_path} (скачанный из {remote_path}) пуст или содержит невалидный JSON. Возвращаем default_value.")
             return default_value
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'NoSuchKey' or '404' in str(e):
            logger.warning(f"Файл {remote_path} не найден в B2. Возвращаем default_value.")
            return default_value
        else:
            logger.error(f"Ошибка Boto3 при загрузке {remote_path}: {e}", exc_info=True)
            return default_value
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке {remote_path} из B2: {e}", exc_info=True)
        return default_value
    finally:
        if Path(local_temp_path).exists():
            try: os.remove(local_temp_path); logger.debug(f"Удален временный файл: {local_temp_path}")
            except OSError as remove_err: logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")


def save_b2_json(s3_client, bucket_name, remote_path, local_temp_path, data):
    """Сохраняет данные в JSON файл в B2 через временный локальный файл."""
    try:
        if not save_local_json(local_temp_path, data):
             raise IOError(f"Не удалось сохранить данные локально в {local_temp_path}")

        logger.debug(f"Загрузка {local_temp_path} в B2 как {remote_path}...")
        s3_client.upload_file(local_temp_path, bucket_name, remote_path)
        # Логируем только начало данных для краткости
        data_preview = json.dumps(data, ensure_ascii=False)[:100]
        logger.info(f"Данные успешно сохранены в {remote_path} в B2: {data_preview}...")
        return True
    except (IOError, ClientError, NoCredentialsError, Exception) as e:
        logger.error(f"Ошибка при сохранении {remote_path} в B2: {e}", exc_info=True)
        return False
    finally:
         if Path(local_temp_path).exists():
             try: os.remove(local_temp_path); logger.debug(f"Удален временный файл: {local_temp_path}")
             except OSError as remove_err: logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")

def download_file(url, local_path_str, stream=False, timeout=30):
    """Скачивает файл по URL."""
    logger.info(f"Загрузка файла с {url} в {local_path_str}...")
    try:
        ensure_directory_exists(local_path_str)
        with requests.get(url, stream=stream, timeout=timeout) as r:
            r.raise_for_status()
            with open(local_path_str, 'wb') as f:
                chunk_size = 8192 if stream else None
                for chunk in r.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
        logger.info(f"✅ Файл успешно сохранен: {local_path_str}")
        return True
    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут ({timeout} сек) при скачивании {url}")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка скачивания {url}: {e}")
        if e.response is not None:
             logger.error(f"    Статус код: {e.response.status_code}, Ответ: {e.response.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при скачивании {url}: {e}", exc_info=True)
        return False

def download_image(url, local_path_str, timeout=30):
    """Скачивает изображение."""
    logger.info(f"Загрузка изображения с {url} в {local_path_str}...")
    return download_file(url, local_path_str, stream=True, timeout=timeout)

def download_video(url, local_path_str, timeout=120):
    """Скачивает видео."""
    logger.info(f"Загрузка видео с {url} в {local_path_str}...")
    return download_file(url, local_path_str, stream=True, timeout=timeout)

# --- ИЗМЕНЕННАЯ ФУНКЦИЯ upload_to_b2 ---
def upload_to_b2(s3_client, bucket_name, target_folder, local_file_path_str, b2_filename_with_ext):
    """
    Загружает локальный файл в указанную папку B2 и проверяет его наличие.
    Использует переданное имя файла с расширением для ключа объекта B2.
    """
    local_path = Path(local_file_path_str)
    if not local_path.is_file():
        logger.error(f"Локальный файл для загрузки не найден: {local_path}")
        return False

    # Формируем ключ объекта B2
    b2_object_key = f"{target_folder.rstrip('/')}/{b2_filename_with_ext}"

    logger.info(f"Загрузка {local_path} в B2 как {b2_object_key}...")
    try:
        # Шаг 1: Попытка загрузки
        s3_client.upload_file(str(local_path), bucket_name, b2_object_key)
        logger.info(f"Вызов upload_file для {b2_object_key} завершен.")

        # Шаг 2: Проверка наличия файла в B2
        logger.info(f"Проверка наличия {b2_object_key} в B2...")
        time.sleep(1) # Небольшая пауза на всякий случай перед проверкой
        try:
            s3_client.head_object(Bucket=bucket_name, Key=b2_object_key)
            logger.info(f"✅ ПРОВЕРКА УСПЕШНА: Файл {b2_object_key} найден в B2.")
            return True # Загрузка и проверка успешны
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404' or 'NotFound' in str(e):
                 logger.error(f"❌ ПРОВЕРКА НЕУДАЧНА: Файл {b2_object_key} НЕ НАЙДЕН в B2 после upload_file!")
            else:
                 logger.error(f"❌ ОШИБКА ПРОВЕРКИ (head_object) для {b2_object_key}: {e}")
            return False # Ошибка проверки

    except ClientError as e:
        logger.error(f"Ошибка Boto3 при вызове upload_file для {b2_object_key}: {e}", exc_info=True)
        return False
    except NoCredentialsError:
        logger.error(f"Ошибка учетных данных B2 при загрузке {local_path}.")
        return False
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке {local_path} в B2: {e}", exc_info=True)
        return False
# --- КОНЕЦ ИЗМЕНЕННОЙ ФУНКЦИИ ---

# --- Функция list_b2_folder_contents (добавлена ранее) ---
def list_b2_folder_contents(s3_client, bucket_name, folder_prefix):
    """
    Возвращает список объектов (словарей с 'Key' и 'Size') в указанной папке B2.
    Игнорирует саму папку и placeholder'ы .bzEmpty.
    """
    contents = []
    try:
        logger_list = logging.getLogger(__name__) # Используем существующий логгер
    except NameError:
        logger_list = logging.getLogger("utils_list_fallback")
        if not logger_list.hasHandlers():
            logging.basicConfig(level=logging.INFO)
            logger_list.warning("Используется fallback логгер для list_b2_folder_contents.")

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'
        logger_list.debug(f"Листинг B2 папки: {bucket_name}/{prefix}")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
            if 'Contents' in page:
                for obj in page.get('Contents', []):
                    key = obj.get('Key')
                    size_bytes = obj.get('Size', 0)
                    if key == prefix or key.endswith('.bzEmpty'):
                         continue
                    contents.append({'Key': key, 'Size': size_bytes})
                    # logger_list.debug(f"Найден файл: {key}, Размер: {size_bytes}")

    except ClientError as e:
        logger_list.error(f"Ошибка Boto3 при листинге папки {folder_prefix}: {e}", exc_info=True)
    except Exception as e:
        logger_list.error(f"Неизвестная ошибка при листинге папки {folder_prefix}: {e}", exc_info=True)

    logger_list.debug(f"Содержимое папки {folder_prefix}: {len(contents)} объектов.")
    return contents
# --- Конец list_b2_folder_contents ---

def move_b2_object(s3_client, bucket_name, source_key, dest_key):
    """Перемещает объект в B2 (копирование + удаление)."""
    try:
        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        logger.debug(f"Копирование {source_key} -> {dest_key}...")
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=dest_key)
        logger.debug(f"Удаление {source_key}...")
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        logger.info(f"✅ Успешно перемещен: {source_key} -> {dest_key}")
        return True
    except ClientError as e:
        logger.error(f"Ошибка Boto3 при перемещении {source_key} -> {dest_key}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Неизвестная ошибка при перемещении {source_key} -> {dest_key}: {e}", exc_info=True)
        return False

def delete_b2_object(s3_client, bucket_name, key):
    """Удаляет объект из B2."""
    try:
        logger.debug(f"Удаление объекта B2: {key}...")
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        logger.info(f"✅ Объект удален из B2: {key}")
        return True
    except ClientError as e:
        logger.error(f"Ошибка Boto3 при удалении {key}: {e}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Неизвестная ошибка при удалении {key}: {e}", exc_info=True)
        return False

def is_folder_empty(s3_client, bucket_name, folder_prefix):
    """
    Проверяет, пуста ли папка в B2 (игнорируя placeholder).
    Возвращает True, если папка пуста (или содержит только placeholder), иначе False.
    """
    logger.debug(f"Проверка на пустоту папки: {bucket_name}/{folder_prefix}")
    try:
        contents = list_b2_folder_contents(s3_client, bucket_name, folder_prefix)
        # Проверяем, есть ли хоть один файл
        if contents:
             logger.debug(f"Папка {folder_prefix} не пуста, найдены файлы: {[item.get('Key') for item in contents]}")
             return False
        else:
             logger.debug(f"Папка {folder_prefix} пуста (или содержит только placeholder).")
             return True
    except Exception as e:
        logger.error(f"Ошибка при проверке пустоты папки {folder_prefix}: {e}", exc_info=True)
        return False # В случае ошибки считаем, что не пуста

def generate_file_id():
    """Генерирует уникальный ID на основе текущей даты и времени UTC."""
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")

