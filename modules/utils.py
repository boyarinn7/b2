# -*- coding: utf-8 -*-
# В файле modules/utils.py

import os
import json
import logging
import time
from pathlib import Path
import requests
import shutil
# --- ДОБАВЛЕН ИМПОРТ DATETIME ---
from datetime import datetime, timezone
# --- КОНЕЦ ДОБАВЛЕНИЯ ---


# --- Получение логгера (предполагается, что он уже настроен где-то) ---
# Используем стандартный logging, если кастомный недоступен на раннем этапе
try:
    # Абсолютный импорт, если logger.py в той же папке modules
    from .logger import get_logger
    logger = get_logger(__name__) # Используем имя модуля
except ImportError:
     # Относительный импорт, если структура другая (например, при прямом запуске utils.py)
     # Или если logger.py не найден через абсолютный импорт
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
        raise # Пробрасываем исключение дальше

def load_json_config(file_path):
    """Загружает JSON из локального файла."""
    path_obj = Path(file_path)
    if not path_obj.is_file():
        logger.warning(f"Файл конфигурации не найден: {file_path}")
        return None
    try:
        with open(path_obj, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # logger.debug(f"Конфигурация успешно загружена из {file_path}") # Убрано избыточное логирование
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
        ensure_directory_exists(local_temp_path) # Убедимся, что папка для temp файла существует
        logger.debug(f"Попытка загрузки {remote_path} из B2 в {local_temp_path}...")
        s3_client.download_file(bucket_name, remote_path, local_temp_path)
        logger.info(f"Успешно загружен и распарсен {remote_path} из B2.")
        return load_json_config(local_temp_path) # Загружаем уже из локального temp файла
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'NoSuchKey' or '404' in str(e):
            logger.warning(f"Файл {remote_path} не найден в B2. Возвращаем default_value.")
            return default_value
        else:
            logger.error(f"Ошибка Boto3 при загрузке {remote_path}: {e}", exc_info=True)
            return default_value # Возвращаем дефолт и при других ошибках Boto3
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке {remote_path} из B2: {e}", exc_info=True)
        return default_value
    finally: # Очищаем временный файл после использования
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
        logger.info(f"Данные успешно сохранены в {remote_path} в B2: {json.dumps(data, ensure_ascii=False)[:100]}...") # Логируем начало данных
        return True
    except (IOError, ClientError, NoCredentialsError, Exception) as e:
        logger.error(f"Ошибка при сохранении {remote_path} в B2: {e}", exc_info=True)
        return False
    finally:
         # Удаляем временный локальный файл после попытки загрузки
         if Path(local_temp_path).exists():
             try: os.remove(local_temp_path); logger.debug(f"Удален временный файл: {local_temp_path}")
             except OSError as remove_err: logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")

def download_file(url, local_path_str, stream=False, timeout=30):
    """Скачивает файл по URL."""
    logger.info(f"Загрузка файла с {url} в {local_path_str}...")
    try:
        ensure_directory_exists(local_path_str)
        with requests.get(url, stream=stream, timeout=timeout) as r:
            r.raise_for_status() # Проверка на HTTP ошибки
            with open(local_path_str, 'wb') as f:
                # Используем iter_content для потоковой записи, если stream=True
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
        # Дополнительно логируем статус код, если он есть
        if e.response is not None:
             logger.error(f"    Статус код: {e.response.status_code}, Ответ: {e.response.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при скачивании {url}: {e}", exc_info=True)
        return False

def download_image(url, local_path_str, timeout=30):
    """Скачивает изображение."""
    logger.info(f"Загрузка изображения с {url} в {local_path_str}...")
    return download_file(url, local_path_str, stream=True, timeout=timeout) # Используем stream для изображений

def download_video(url, local_path_str, timeout=120): # Увеличен таймаут для видео
    """Скачивает видео."""
    logger.info(f"Загрузка видео с {url} в {local_path_str}...")
    return download_file(url, local_path_str, stream=True, timeout=timeout) # Используем stream для видео

def upload_to_b2(s3_client, bucket_name, target_folder, local_file_path_str, b2_filename_with_ext):
    """
    Загружает локальный файл в указанную папку B2.
    Использует переданное имя файла с расширением для ключа объекта B2.
    """
    local_path = Path(local_file_path_str)
    if not local_path.is_file():
        logger.error(f"Локальный файл для загрузки не найден: {local_path}")
        return False

    # Формируем ключ объекта, используя переданное полное имя файла
    b2_object_key = f"{target_folder.rstrip('/')}/{b2_filename_with_ext}"
    # Убрано добавление расширения, так как оно уже есть в b2_filename_with_ext

    logger.info(f"Загрузка {local_path} в B2 как {b2_object_key}...")
    try:
        s3_client.upload_file(str(local_path), bucket_name, b2_object_key)
        logger.info(f"✅ Файл успешно загружен в B2: {b2_object_key}")
        return True
    except ClientError as e:
        logger.error(f"Ошибка Boto3 при загрузке {local_path} в {b2_object_key}: {e}", exc_info=True)
        return False
    except NoCredentialsError:
        logger.error(f"Ошибка учетных данных B2 при загрузке {local_path}.")
        return False
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке {local_path} в B2: {e}", exc_info=True)
        return False

def list_b2_folder_contents(s3_client, bucket_name, folder_prefix):
    """Возвращает список объектов и их размеров в указанной папке B2."""
    contents = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        # Убедимся, что префикс заканчивается на /
        prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'
        # logger.debug(f"Листинг B2 папки: {bucket_name}/{prefix}")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'): # Используем Delimiter для имитации папок
            # Обработка 'CommonPrefixes' (подпапки) - сейчас игнорируем
            # if 'CommonPrefixes' in page:
            #     for subdir in page.get('CommonPrefixes', []):
            #         # logger.debug(f"Найдена подпапка: {subdir.get('Prefix')}")
            #         pass # Пока не обрабатываем подпапки

            # Обработка 'Contents' (файлы)
            if 'Contents' in page:
                for obj in page.get('Contents', []):
                    key = obj.get('Key')
                    size_bytes = obj.get('Size', 0)
                    # Пропускаем сам префикс (пустой файл, обозначающий папку)
                    if key == prefix and size_bytes == 0:
                         continue
                    # Пропускаем placeholder, если он есть
                    # if key.endswith('placeholder.bzEmpty'):
                    #      continue
                    contents.append({'Key': key, 'Size': size_bytes})
            # Добавляем проверку, если папка пуста (кроме placeholder)
            if not page.get('Contents') and not page.get('CommonPrefixes'):
                 # logger.debug(f"Папка {prefix} пуста в B2.")
                 pass

    except ClientError as e:
        logger.error(f"Ошибка Boto3 при листинге папки {folder_prefix}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Неизвестная ошибка при листинге папки {folder_prefix}: {e}", exc_info=True)
    return contents

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

# --- ДОБАВЛЕНА ФУНКЦИЯ is_folder_empty ---
def is_folder_empty(s3_client, bucket_name, folder_prefix):
    """
    Проверяет, пуста ли папка в B2 (игнорируя placeholder).
    Возвращает True, если папка пуста (или содержит только placeholder), иначе False.
    """
    logger.debug(f"Проверка на пустоту папки: {bucket_name}/{folder_prefix}")
    try:
        contents = list_b2_folder_contents(s3_client, bucket_name, folder_prefix)
        # Проверяем, есть ли хоть один файл, НЕ являющийся placeholder'ом
        for item in contents:
            if not item.get('Key', '').endswith('placeholder.bzEmpty'):
                logger.debug(f"Папка {folder_prefix} не пуста, найден файл: {item.get('Key')}")
                return False # Нашли реальный файл, папка не пуста

        # Если прошли по всем файлам и не нашли ничего, кроме плейсхолдера (или вообще ничего)
        logger.debug(f"Папка {folder_prefix} пуста (или содержит только placeholder).")
        return True
    except Exception as e:
        logger.error(f"Ошибка при проверке пустоты папки {folder_prefix}: {e}", exc_info=True)
        return False # В случае ошибки считаем, что не пуста (безопаснее)
# --- КОНЕЦ ДОБАВЛЕННОЙ ФУНКЦИИ ---

# --- Добавлена функция generate_file_id (если ее нет) ---
def generate_file_id():
    """Генерирует уникальный ID на основе текущей даты и времени UTC."""
    # Убедимся, что datetime импортирован в начале файла
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
# --- Конец функции generate_file_id ---
