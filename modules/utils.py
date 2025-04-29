# -*- coding: utf-8 -*-
# В файле modules/utils.py
import io
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
    # Попытка относительного импорта, если используется как часть пакета
    from .logger import get_logger
    logger = get_logger(__name__)
except ImportError:
     # Фоллбэк, если запускается напрямую или структура иная
     try:
         from logger import get_logger
         logger = get_logger(__name__)
     except ImportError:
        # Крайний случай: используем стандартный logging
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

# --- Pillow для обработки изображений ---
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    # Логируем ошибку, если Pillow не найден
    try:
        # Попытка получить существующий логгер utils
        logger_utils = logging.getLogger("utils") # Предполагаем, что логгер utils уже есть
        if not logger_utils.hasHandlers(): # Настроить, если нет
             logging.basicConfig(level=logging.INFO)
             logger_utils = logging.getLogger("utils_fallback")
        logger_utils.error("!!! Библиотека Pillow (PIL) не найдена. Функция add_text_to_image не будет работать. Установите: pip install Pillow !!!")
    except Exception:
        print("!!! ОШИБКА: Библиотека Pillow (PIL) не найдена И не удалось получить логгер. Установите: pip install Pillow !!!")
    # Определяем заглушки, чтобы код ниже не падал при импорте
    Image = None
    ImageDraw = None
    ImageFont = None
    ImageFilter = None


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
            # *** ИЗМЕНЕНИЕ: Добавлен ensure_ascii=False ***
            json.dump(data, f, ensure_ascii=False, indent=4)
            # *** КОНЕЦ ИЗМЕНЕНИЯ ***
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

def list_b2_folder_contents(s3_client, bucket_name, folder_prefix):
    """
    Возвращает список объектов (словарей с 'Key', 'Size', 'LastModified') в указанной папке B2.
    Игнорирует саму папку и placeholder'ы .bzEmpty.
    """
    contents = []
    logger_list = logging.getLogger(__name__) # Используем существующий логгер

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'
        logger_list.debug(f"Листинг B2 папки: {bucket_name}/{prefix}")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/'):
            if 'Contents' in page:
                for obj in page.get('Contents', []):
                    key = obj.get('Key')
                    size_bytes = obj.get('Size', 0)
                    last_modified = obj.get('LastModified') # <<< ПОЛУЧАЕМ ДАТУ МОДИФИКАЦИИ
                    if key == prefix or key.endswith('.bzEmpty'):
                         continue
                    # <<< ДОБАВЛЯЕМ LastModified В СЛОВАРЬ >>>
                    contents.append({'Key': key, 'Size': size_bytes, 'LastModified': last_modified})
                    # logger_list.debug(f"Найден файл: {key}, Размер: {size_bytes}, Дата: {last_modified}")

    except ClientError as e:
        logger_list.error(f"Ошибка Boto3 при листинге папки {folder_prefix}: {e}", exc_info=True)
    except Exception as e:
        logger_list.error(f"Неизвестная ошибка при листинге папки {folder_prefix}: {e}", exc_info=True)

    logger_list.debug(f"Содержимое папки {folder_prefix}: {len(contents)} объектов.")
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

def is_folder_empty(s3_client, bucket_name, folder_prefix):
    """
    Проверяет, пуста ли папка в B2 (игнорируя placeholder).
    Возвращает True, если папка пуста (или содержит только placeholder), иначе False.
    """
    logger.debug(f"Проверка на пустоту папки: {bucket_name}/{folder_prefix}")
    try:
        # Используем обновленную функцию, которая возвращает список словарей
        contents = list_b2_folder_contents(s3_client, bucket_name, folder_prefix)
        if contents:
             logger.debug(f"Папка {folder_prefix} не пуста, найдены файлы: {[item.get('Key') for item in contents]}")
             return False
        else:
             logger.debug(f"Папка {folder_prefix} пуста (или содержит только placeholder).")
             return True
    except Exception as e:
        logger.error(f"Ошибка при проверке пустоты папки {folder_prefix}: {e}", exc_info=True)
        return False # В случае ошибки считаем, что не пуста

# --- ИЗМЕНЕНИЕ: Возвращаем формат ID к ГГГГММДД-ЧЧММ ---
def generate_file_id():
    """Генерирует уникальный ID на основе текущей даты и времени UTC."""
    # Возвращаем исходный формат без секунд
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
# --- КОНЕЦ ИЗМЕНЕНИЯ ---

def save_error_to_b2(s3_client, bucket_name, error_folder, local_file_path_str, error_data_dict, max_error_files=20):
    """
    Сохраняет данные об ошибке (словарь) в JSON файл в папку ошибок B2 (`error_folder`, например '000/'),
    реализуя ротацию (удаление самого старого файла, если превышен лимит `max_error_files`).

    Args:
        s3_client: Инициализированный клиент Boto3 S3.
        bucket_name: Имя бакета B2.
        error_folder: Путь к папке ошибок в B2 (например, '000/').
        local_file_path_str: Путь к локальному файлу, который будет создан для временного хранения данных ошибки.
        error_data_dict: Словарь с данными об ошибке для сохранения в JSON.
        max_error_files: Максимальное количество файлов в папке ошибок.

    Returns:
        True, если сохранение (и возможная ротация) прошли успешно, иначе False.
    """
    error_folder_norm = error_folder.rstrip('/') + '/'
    local_path = Path(local_file_path_str)
    # Имя файла в B2 будет таким же, как у локального временного файла
    b2_filename = local_path.name
    b2_object_key = f"{error_folder_norm}{b2_filename}"

    logger.info(f"Сохранение файла ошибки {b2_filename} в папку {error_folder_norm}...")

    try:
        # 1. Проверка и ротация папки ошибок
        logger.debug(f"Проверка количества файлов в {error_folder_norm}...")
        # Используем обновленную list_b2_folder_contents, которая возвращает LastModified
        error_files = list_b2_folder_contents(s3_client, bucket_name, error_folder_norm)

        if len(error_files) >= max_error_files:
            logger.warning(f"В папке {error_folder_norm} {len(error_files)} файлов (лимит: {max_error_files}). Удаление самого старого...")
            # Сортируем файлы по дате модификации (от старых к новым)
            # Убедимся, что LastModified существует и является datetime объектом
            valid_files_with_date = [f for f in error_files if isinstance(f.get('LastModified'), datetime)]
            if not valid_files_with_date:
                 logger.error("Не удалось получить дату модификации для файлов в папке ошибок. Ротация невозможна.")
            else:
                valid_files_with_date.sort(key=lambda x: x['LastModified'])
                oldest_file_key = valid_files_with_date[0]['Key']
                logger.info(f"Самый старый файл для удаления: {oldest_file_key}")
                if not delete_b2_object(s3_client, bucket_name, oldest_file_key):
                    logger.error(f"Не удалось удалить старый файл {oldest_file_key}.")
                else:
                    logger.info(f"Старый файл {oldest_file_key} успешно удален.")

        # 2. Сохранение данных ошибки в локальный временный файл
        logger.debug(f"Сохранение данных ошибки в локальный файл: {local_path}...")
        if not save_local_json(str(local_path), error_data_dict):
            logger.error(f"Не удалось сохранить данные ошибки локально в {local_path}")
            return False

        # 3. Загрузка локального файла в папку ошибок B2
        logger.debug(f"Загрузка {local_path} в B2 как {b2_object_key}...")
        s3_client.upload_file(str(local_path), bucket_name, b2_object_key)
        logger.info(f"✅ Файл ошибки {b2_filename} успешно сохранен в {error_folder_norm}")
        return True

    except (IOError, ClientError, NoCredentialsError, Exception) as e:
        logger.error(f"Ошибка при сохранении файла ошибки {b2_filename} в {error_folder_norm}: {e}", exc_info=True)
        return False
    finally:
        # Удаление временного локального файла
        if local_path.exists():
            try:
                os.remove(local_path)
                logger.debug(f"Удален временный файл ошибки: {local_path}")
            except OSError as remove_err:
                logger.warning(f"Не удалось удалить временный файл ошибки {local_path}: {remove_err}")

# --- Функции для обработки изображений (Pillow) ---
# ВАЖНО: Эта функция оставлена БЕЗ ИЗМЕНЕНИЙ по сравнению с вашим файлом

def hex_to_rgba(hex_color, alpha=255):
    """Конвертирует HEX цвет (#RRGGBB) в кортеж RGBA."""
    # Получаем логгер (если logger не глобальный)
    local_logger = logging.getLogger("utils_hex_converter")
    if not local_logger.hasHandlers(): logging.basicConfig(level=logging.INFO)

    hex_color = hex_color.lstrip('#')
    default_color = (0, 0, 0, alpha)  # Черный по умолчанию
    if len(hex_color) != 6:
        local_logger.warning(f"Некорректный HEX цвет '{hex_color}'. Используется черный.")
        return default_color
    try:
        rgb = tuple(int(hex_color[i:i + 2], 16) for i in (0, 2, 4))
        return rgb + (alpha,)
    except ValueError:
        local_logger.warning(f"Не удалось сконвертировать HEX '{hex_color}'. Используется черный.")
        return default_color


# Основная функция с логированием параметра цвета
def add_text_to_image(
        image_path_str: str,
        text: str,  # Текст с переносами \n от ИИ
        font_path_str: str,
        output_path_str: str,
        text_color_hex: str = "#000000",  # Цвет по умолчанию черный (или ваш темно-серый, если меняли)
        position: tuple = ('center', 'center'),  # Позиция текста (ожидаем ('center','center'))
        padding: int = 50,  # Отступы (меньше используются при center)
        haze_opacity: int = 100,  # Прозрачность белой дымки (0-255)
        bg_blur_radius: float = 0,
        bg_opacity: int = 0,
        logger_instance=None,
        stroke_width: int = 2,
        stroke_color_hex: str = "#404040",
        target_width_fraction: float = 0.92,
        initial_font_size: int = 140,
        min_font_size: int = 50,
        min_font_size_multiline: int = 60
):
    """
    Наносит текст на изображение с автоподбором размера шрифта,
    добавляя белую "дымку" и обводку текста.
    Добавлено логирование получаемого цвета.
    """
    # Получаем логгер
    log = logger_instance if logger_instance else logging.getLogger(
        "utils_add_text")  # Используем стандартный, если logger не передан
    if not log.hasHandlers(): logging.basicConfig(level=logging.INFO)
    # Установим DEBUG уровень, если нужно больше деталей
    if log.level > logging.DEBUG: log.setLevel(logging.DEBUG)

    log.debug(">>> Вход в add_text_to_image (автоподбор v4 - крупнее, с логом цвета)")
    # --- ДОБАВЛЕНО ЛОГИРОВАНИЕ ПАРАМЕТРА ---
    log.info(f"Получен параметр text_color_hex: {text_color_hex}")
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---

    # Проверка доступности Pillow
    if not PIL_AVAILABLE or Image is None:
        log.error("Библиотека Pillow недоступна. Невозможно добавить текст.")
        return False

    try:
        base_image_path = Path(image_path_str)
        font_path = Path(font_path_str)
        output_path = Path(output_path_str)
        log.debug(f"Пути: image={base_image_path}, font={font_path}, output={output_path}")

        if not base_image_path.is_file(): log.error(f"Исходное изображение не найдено: {base_image_path}"); return False
        if not font_path.is_file(): log.error(f"Файл шрифта не найден: {font_path}"); return False
        log.debug("Файлы изображения и шрифта найдены.")

        log.info(f"Открытие изображения: {base_image_path.name}")
        img = Image.open(base_image_path).convert("RGBA")
        img_width, img_height = img.size
        log.debug(f"Изображение открыто: {img_width}x{img_height}, режим={img.mode}")

        # Добавление белой "дымки" (haze)
        if haze_opacity > 0:
            log.info(f"Добавление белой дымки (прозрачность: {haze_opacity})...")
            haze_layer = Image.new('RGBA', img.size, (255, 255, 255, haze_opacity))
            img = Image.alpha_composite(img, haze_layer)
            log.debug("Белая дымка добавлена.")
        else:
            log.debug("Дымка отключена (haze_opacity=0).")

        draw = ImageDraw.Draw(img)
        log.debug("Объект ImageDraw создан/обновлен.")

        # Автоподбор размера шрифта
        num_lines = text.count('\n') + 1
        log.info(f"Начало автоподбора размера шрифта (старт: {initial_font_size}, строк: {num_lines})...")
        current_min_font_size = min_font_size_multiline if num_lines >= 3 else min_font_size
        log.debug(f"Минимальный размер для {num_lines} строк: {current_min_font_size}")

        font = None
        current_font_size = initial_font_size
        text_width = img_width * 2
        max_text_width = img_width * target_width_fraction
        log.debug(f"Целевая ширина текста: {max_text_width:.0f}")

        font_bytes = None
        try:
            log.debug(f"Чтение файла шрифта в память: {font_path.name}")
            with open(font_path, 'rb') as f_font:
                font_bytes = f_font.read()
            log.debug(f"Файл шрифта прочитан, размер: {len(font_bytes)} байт.")
        except Exception as read_font_err:
            log.error(f"Ошибка чтения файла шрифта '{font_path}': {read_font_err}", exc_info=True);
            return False

        while current_font_size >= current_min_font_size:
            try:
                log.debug(f"Пробуем размер шрифта: {current_font_size}")
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_font_size)
                log.debug("Шрифт загружен для текущего размера.")
                bbox_multiline = draw.textbbox((0, 0), text, font=font)
                text_width = bbox_multiline[2] - bbox_multiline[0]
                text_height = bbox_multiline[3] - bbox_multiline[1]
                log.debug(f"Размер {current_font_size}: Ширина={text_width:.0f}, Высота={text_height:.0f}")
                if text_width <= max_text_width:
                    log.info(f"Найден подходящий размер шрифта: {current_font_size}")
                    break
                current_font_size -= 2
            except Exception as size_calc_err:
                log.error(f"Ошибка при расчете размера для шрифта {current_font_size}: {size_calc_err}", exc_info=True)
                current_font_size -= 2
                if current_font_size < current_min_font_size:
                    log.error("Не удалось подобрать размер шрифта.");
                    return False

        else:
            log.warning(
                f"Не удалось вместить текст в {target_width_fraction * 100:.0f}% ширины. Используется минимальный размер {current_min_font_size}.")
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_min_font_size)
                bbox_multiline = draw.textbbox((0, 0), text, font=font)
                text_width = bbox_multiline[2] - bbox_multiline[0]
                text_height = bbox_multiline[3] - bbox_multiline[1]
                current_font_size = current_min_font_size
            except Exception as min_font_err:
                log.error(f"Ошибка при загрузке/расчете минимального шрифта {current_min_font_size}: {min_font_err}",
                          exc_info=True);
                return False

        final_font_size = current_font_size
        log.debug(f"Финальный размер шрифта: {final_font_size}")

        # Расчет финальных размеров и позиции
        log.debug("Расчет финальных размеров и позиции...")
        try:
            bbox_multiline = draw.textbbox((0, 0), text, font=font)
            text_width = bbox_multiline[2] - bbox_multiline[0]
            text_height = bbox_multiline[3] - bbox_multiline[1]
            log.debug(f"Финальные размеры текста: Ширина={text_width:.0f}, Высота={text_height:.0f}")
        except Exception as final_size_err:
            log.error(f"Ошибка при расчете финального размера текста: {final_size_err}", exc_info=True);
            return False
        if text_width <= 0 or text_height <= 0:
            log.error(f"Рассчитаны некорректные финальные размеры текста: {text_width}x{text_height}");
            return False

        log.debug("Расчет X координаты...")
        x = (img_width - text_width) / 2
        log.debug(f"X = {x}")
        log.debug("Расчет Y координаты...")
        y = (img_height - text_height) / 2
        log.debug(f"Y = {y}")
        text_position = (int(x), int(y))
        log.info(f"Позиция текста (левый верхний угол): {text_position}")

        # Добавление подложки/размытия (если нужно)
        if bg_blur_radius > 0 or bg_opacity > 0:
            # ... (код подложки остается прежним) ...
            log.info("Добавление эффектов фона под текстом...")
            log.debug(f"Параметры фона: blur={bg_blur_radius}, opacity={bg_opacity}")
            background_layer = Image.new('RGBA', img.size, (0, 0, 0, 0))
            draw_bg = ImageDraw.Draw(background_layer)
            bg_padding = 20
            bg_left = max(0, text_position[0] - bg_padding)
            bg_top = max(0, text_position[1] - bg_padding)
            log.debug("Расчет bbox для фона под текстом...")
            try:
                bbox_multiline_at_pos = draw.textbbox(text_position, text, font=font)
                log.debug(f"bbox_multiline_at_pos: {bbox_multiline_at_pos}")
                bg_right = min(img_width,
                               text_position[0] + (bbox_multiline_at_pos[2] - bbox_multiline_at_pos[0]) + bg_padding)
                bg_bottom = min(img_height,
                                text_position[1] + (bbox_multiline_at_pos[3] - bbox_multiline_at_pos[1]) + bg_padding)
            except Exception as bbox_bg_err:
                log.error(f"Ошибка расчета bbox для фона: {bbox_bg_err}", exc_info=True)
                bg_right = min(img_width, text_position[0] + text_width + bg_padding)
                bg_bottom = min(img_height, text_position[1] + text_height + bg_padding)
                log.warning("Используются приблизительные размеры для фона.")
            bg_rect_coords = [(int(bg_left), int(bg_top)), (int(bg_right), int(bg_bottom))]
            bg_crop_box = (int(bg_left), int(bg_top), int(bg_right), int(bg_bottom))
            log.debug(f"Координаты фона: rect={bg_rect_coords}, crop={bg_crop_box}")
            if bg_blur_radius > 0:
                log.info(f"Применение размытия фона под текстом (радиус: {bg_blur_radius}) в области {bg_crop_box}")
                try:
                    log.debug("Обрезка области для размытия...")
                    crop = img.crop(bg_crop_box)
                    log.debug("Применение фильтра GaussianBlur...")
                    blurred_crop = crop.filter(ImageFilter.GaussianBlur(bg_blur_radius))
                    log.debug("Вставка размытой области...")
                    background_layer.paste(blurred_crop, bg_crop_box)
                    log.debug("Размытие применено.")
                except Exception as blur_err:
                    log.warning(f"Не удалось применить размытие под текстом: {blur_err}")
            if bg_opacity > 0:
                log.info(
                    f"Создание полупрозрачной подложки под текстом (opacity: {bg_opacity}) в области {bg_rect_coords}")
                overlay_color = (0, 0, 0, bg_opacity)  # Черный с заданной прозрачностью
                log.debug(f"Рисование прямоугольника подложки цветом {overlay_color}...")
                draw_bg.rectangle(bg_rect_coords, fill=overlay_color)
                log.debug("Подложка нарисована.")
            log.debug("Наложение слоя фона на основное изображение...")
            img = Image.alpha_composite(img, background_layer)
            draw = ImageDraw.Draw(img)  # Обновляем draw для финального изображения
            log.debug("Слой фона наложен, ImageDraw обновлен.")
        else:
            log.debug("Эффекты фона под текстом отключены.")

        # Нанесение текста
        log.debug(f"Конвертация HEX цвета текста: {text_color_hex}")
        final_text_color = hex_to_rgba(text_color_hex, alpha=240)
        log.debug(f"Финальный цвет текста (RGBA): {final_text_color}")
        final_stroke_color = hex_to_rgba(stroke_color_hex, alpha=240)
        log.debug(f"Цвет обводки (RGBA): {final_stroke_color}, Ширина: {stroke_width}")

        log_text_preview = text[:50].replace('\n', '\\n')
        # --- ИЗМЕНЕНИЕ: Логируем цвет, который БУДЕТ использоваться ---
        log.info(
            f"Нанесение текста '{log_text_preview}...' цветом {final_text_color} (из HEX: {text_color_hex}) с обводкой (размер: {final_font_size})")
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

        align_option = 'center' if position[0] == 'center' else 'left'
        log.debug(f"Выравнивание текста: {align_option}")
        try:
            log.debug(f"Вызов draw.text с позицией {text_position}...")
            draw.text(
                text_position, text, font=font, fill=final_text_color,
                align=align_option, stroke_width=stroke_width, stroke_fill=final_stroke_color
            )
            log.debug("draw.text выполнен (с обводкой).")
        except Exception as draw_err:
            log.error(f"Ошибка при вызове draw.text: {draw_err}", exc_info=True);
            return False

        # Сохранение результата
        log.debug("Подготовка к сохранению...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f"Сохранение изображения в {output_path}...")
        img.save(output_path, format='PNG')
        log.info(f"✅ Изображение с текстом сохранено: {output_path.name}")
        log.debug("<<< Выход из add_text_to_image (Успех)")
        return True

    except FileNotFoundError as fnf_err:
        log.error(f"Ошибка FileNotFoundError при обработке изображения: {fnf_err}")
        log.debug("<<< Выход из add_text_to_image (Ошибка FileNotFoundError)")
        return False
    except Exception as e:
        log.error(f"Ошибка при добавлении текста на изображение: {e}", exc_info=True)
        log.debug(f"<<< Выход из add_text_to_image (Ошибка Exception: {e})")
        return False
    
# +++ КОНЕЦ ФУНКЦИИ add_text_to_image +++

# Пример использования (можно закомментировать или удалить в финальной версии)
# if __name__ == '__main__':
#     # Нужен Pillow: pip install Pillow
#     if PIL_AVAILABLE:
#         # Создаем тестовое изображение
#         test_img_path = "test_image.png"
#         img = Image.new('RGB', (800, 200), color = (73, 109, 137))
#         img.save(test_img_path)
#
#         # Нужен файл шрифта, например, скачанный Roboto-Regular.ttf
#         # Поместите его рядом со скриптом или укажите полный путь
#         test_font_path = "fonts/Roboto-Regular.ttf" # Пример пути
#         output_file = "test_output.png"
#         test_text = "Пример текста для проверки функции"
#
#         if Path(test_font_path).is_file():
#             success = add_text_to_image(
#                 test_img_path,
#                 test_text,
#                 test_font_path,
#                 output_file,
#                 font_size=40,
#                 position=('center', 'center'),
#                 bg_blur_radius=5,
#                 bg_opacity=120
#             )
#             if success:
#                 print(f"Тестовое изображение сохранено как {output_file}")
#             else:
#                 print("Не удалось создать тестовое изображение.")
#         else:
#             print(f"Тестовый шрифт не найден по пути: {test_font_path}")
#     else:
#         print("Pillow не установлен. Тест не может быть выполнен.")

