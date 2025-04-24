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
# --- ДОБАВЛЕН ИМПОРТ RE ---
import re
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

# --- Глобальная переменная для паттерна ID ---
# Определяем здесь, чтобы была доступна везде в модуле
# Используем re, который теперь импортирован
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}$")

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
    temp_path_obj = Path(local_temp_path) # Используем Path для работы с путем
    try:
        ensure_directory_exists(str(temp_path_obj)) # Убедимся, что папка для temp файла существует
        logger.debug(f"Попытка загрузки {remote_path} из B2 в {local_temp_path}...")
        s3_client.download_file(bucket_name, remote_path, str(temp_path_obj)) # Передаем строку пути
        logger.info(f"Успешно загружен {remote_path} из B2.")
        # Загружаем уже из локального temp файла
        loaded_data = load_json_config(str(temp_path_obj))
        if loaded_data is None:
             logger.warning(f"Не удалось распарсить JSON из временного файла {local_temp_path}. Возвращаем default_value.")
             # Не удаляем временный файл в этом случае, чтобы можно было посмотреть
             return default_value
        logger.info(f"Успешно распарсен JSON из {local_temp_path}.")
        return loaded_data
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
    finally: # Очищаем временный файл после использования, если он существует
        if temp_path_obj.exists():
            try:
                os.remove(temp_path_obj)
                logger.debug(f"Удален временный файл: {local_temp_path}")
            except OSError as remove_err:
                logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")


def save_b2_json(s3_client, bucket_name, remote_path, local_temp_path, data):
    """Сохраняет данные в JSON файл в B2 через временный локальный файл."""
    temp_path_obj = Path(local_temp_path) # Используем Path
    try:
        if not save_local_json(str(temp_path_obj), data): # Передаем строку пути
             raise IOError(f"Не удалось сохранить данные локально в {local_temp_path}")

        logger.debug(f"Загрузка {local_temp_path} в B2 как {remote_path}...")
        s3_client.upload_file(str(temp_path_obj), bucket_name, remote_path) # Передаем строку пути
        # Логируем начало данных для отладки
        # Преобразуем в строку безопасно, даже если data не словарь
        try:
            data_preview = json.dumps(data, ensure_ascii=False)[:100]
        except TypeError:
            data_preview = str(data)[:100]
        logger.info(f"Данные успешно сохранены в {remote_path} в B2: {data_preview}...")
        return True
    except (IOError, ClientError, NoCredentialsError, Exception) as e:
        logger.error(f"Ошибка при сохранении {remote_path} в B2: {e}", exc_info=True)
        return False
    finally:
         # Удаляем временный локальный файл после попытки загрузки
         if temp_path_obj.exists():
             try:
                 os.remove(temp_path_obj)
                 logger.debug(f"Удален временный файл: {local_temp_path}")
             except OSError as remove_err:
                 logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")

def download_file(url, local_path_str, stream=False, timeout=30):
    """Скачивает файл по URL."""
    logger.info(f"Загрузка файла с {url} в {local_path_str}...")
    try:
        ensure_directory_exists(local_path_str)
        # Используем User-Agent, чтобы избежать блокировок на некоторых хостингах
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        with requests.get(url, stream=stream, timeout=timeout, headers=headers) as r:
            r.raise_for_status() # Проверка на HTTP ошибки
            with open(local_path_str, 'wb') as f:
                # Используем iter_content для потоковой записи, если stream=True
                chunk_size = 8192 if stream else None
                for chunk in r.iter_content(chunk_size=chunk_size):
                    # if chunk: # filter out keep-alive new chunks
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
    # Убираем возможный начальный слэш из target_folder и b2_filename_with_ext
    target_folder_clean = target_folder.strip('/')
    b2_filename_clean = b2_filename_with_ext.strip('/')
    # Собираем ключ, гарантируя один слэш между папкой и файлом
    b2_object_key = f"{target_folder_clean}/{b2_filename_clean}"

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

def list_files_in_folder(s3_client, bucket_name, folder_prefix):
    """
    Возвращает список ключей файлов (не папок) в указанной папке B2,
    удовлетворяющих паттерну FILE_NAME_PATTERN и не являющихся .bzEmpty.
    """
    contents = []
    # Убедимся, что префикс папки заканчивается на /
    prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        logger.debug(f"Листинг B2 папки: {bucket_name}/{prefix}")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix): # Убрали Delimiter, чтобы получить все ключи
            if 'Contents' in page:
                for obj in page.get('Contents', []):
                    key = obj.get('Key')
                    # Пропускаем саму папку (ключ равен префиксу)
                    if key == prefix:
                        continue
                    # Пропускаем placeholder
                    if key.endswith('.bzEmpty'):
                        continue

                    # Извлекаем имя файла из ключа
                    base_name = os.path.basename(key)
                    # Извлекаем ID (часть имени до первого расширения)
                    # Используем FILE_NAME_PATTERN для извлечения ID
                    match = FILE_NAME_PATTERN.match(base_name)
                    if match:
                         group_id = match.group(0)
                         # Проверяем, что после ID идет расширение или точка
                         if len(base_name) > len(group_id) and base_name[len(group_id)] == '.':
                              contents.append(key)
                         # elif len(base_name) == len(group_id): # Случай без расширения (не должен быть)
                         #      contents.append(key)
                         else:
                              logger.debug(f"Файл {key} соответствует паттерну ID, но не имеет стандартного расширения? Пропуск.")
                    else:
                         # Логируем файлы, которые не соответствуют паттерну, если нужно
                         logger.debug(f"Файл {key} пропущен (не соответствует паттерну ID).")


        logger.debug(f"Найдено файлов в {prefix}, соответствующих паттерну: {len(contents)}")
    except ClientError as e:
        logger.error(f"Ошибка Boto3 при листинге папки {folder_prefix}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Неизвестная ошибка при листинге папки {folder_prefix}: {e}", exc_info=True)
    return contents

def move_b2_object(s3_client, bucket_name, source_key, dest_key):
    """Перемещает объект в B2 (копирование + удаление)."""
    try:
        # Проверяем, существует ли исходный объект
        try:
            s3_client.head_object(Bucket=bucket_name, Key=source_key)
            logger.debug(f"Исходный объект {source_key} найден.")
        except ClientError as e:
            if e.response['Error']['Code'] == '404' or 'NoSuchKey' in str(e): # Добавили NoSuchKey
                logger.warning(f"Исходный файл {source_key} не найден. Перемещение невозможно.")
                return False
            else:
                logger.error(f"Ошибка при проверке {source_key}: {e}")
                return False

        copy_source = {'Bucket': bucket_name, 'Key': source_key}
        logger.debug(f"Копирование {source_key} -> {dest_key}...")
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=dest_key)

        logger.debug(f"Удаление {source_key}...")
        s3_client.delete_object(Bucket=bucket_name, Key=source_key)

        logger.info(f"✅ Успешно перемещен: {source_key} -> {dest_key}")
        return True
    except ClientError as e:
        # Логируем специфичные ошибки Boto3
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
    Проверяет, пуста ли папка в B2 (игнорируя placeholder .bzEmpty).
    Возвращает True, если папка пуста, иначе False.
    """
    # Убедимся, что префикс папки заканчивается на /
    prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'
    logger.debug(f"Проверка на пустоту папки: {bucket_name}/{prefix}")
    try:
        # Запрашиваем только один объект для проверки
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=2) # MaxKeys=2 чтобы увидеть папку и файл

        # Проверяем наличие 'Contents' и количество ключей
        if 'Contents' in response:
            keys_in_folder = [obj['Key'] for obj in response['Contents']]
            # Проверяем, есть ли что-то кроме самой папки и/или .bzEmpty файла
            for key in keys_in_folder:
                if key != prefix and not key.endswith('.bzEmpty'):
                    logger.debug(f"Папка {prefix} не пуста, найден файл: {key}")
                    return False # Нашли реальный файл

        # Если 'Contents' нет или там только папка/placeholder
        logger.debug(f"Папка {prefix} пуста (или содержит только placeholder).")
        return True
    except ClientError as e:
        # Если папка не найдена, считаем ее пустой (хотя это странно)
        if e.response['Error']['Code'] == 'NoSuchKey':
             logger.warning(f"Папка {prefix} не найдена при проверке на пустоту (NoSuchKey). Считаем пустой.")
             return True
        logger.error(f"Ошибка Boto3 при проверке пустоты папки {prefix}: {e}", exc_info=True)
        return False # В случае других ошибок считаем, что не пуста (безопаснее)
    except Exception as e:
        logger.error(f"Неизвестная ошибка при проверке пустоты папки {prefix}: {e}", exc_info=True)
        return False

def generate_file_id():
    """Генерирует уникальный ID на основе текущей даты и времени UTC."""
    # Убедимся, что datetime импортирован в начале файла
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")

