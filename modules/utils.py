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

# Убедитесь, что эти импорты есть в начале файла modules/utils.py

from pathlib import Path
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


# --- Вставьте эту функцию в modules/utils.py ---

# +++ НАЧАЛО ОБНОВЛЕННОЙ ФУНКЦИИ add_text_to_image +++
def hex_to_rgba(hex_color, alpha=255):
    """Конвертирует HEX цвет (#RRGGBB) в кортеж RGBA."""
    hex_color = hex_color.lstrip('#')
    if len(hex_color) != 6:
        # Возвращаем белый цвет по умолчанию при ошибке
        logger.warning(f"Некорректный HEX цвет '{hex_color}'. Используется белый.")
        return (255, 255, 255, alpha)
    try:
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        return rgb + (alpha,)
    except ValueError:
        logger.warning(f"Не удалось сконвертировать HEX '{hex_color}'. Используется белый.")
        return (255, 255, 255, alpha)

def add_text_to_image(
    image_path_str: str,
    text: str,
    font_path_str: str,
    output_path_str: str,
    font_size: int = 70, # Обновлено значение по умолчанию
    text_color_hex: str = "#FFFFFF", # *** НОВОЕ: Цвет текста в HEX ***
    position: tuple = ('center', 'center'), # Позиция текста
    padding: int = 50, # Отступы для позиций left/right/top/bottom (меньше используется при center)
    haze_opacity: int = 100, # *** НОВОЕ: Прозрачность белой дымки (0-255) ***
    bg_blur_radius: float = 0, # Размытие под текстом (отключено по умолчанию)
    bg_opacity: int = 0, # Прозрачность подложки под текстом (отключено по умолчанию)
    logger_instance=None
    ):
    """
    Наносит текст на изображение с использованием Pillow, добавляя белую "дымку".

    Args:
        image_path_str: Путь к исходному изображению.
        text: Текст для нанесения (может содержать '\n').
        font_path_str: Путь к файлу шрифта (.ttf или .otf).
        output_path_str: Путь для сохранения итогового изображения PNG.
        font_size: Размер шрифта.
        text_color_hex: Цвет текста в формате HEX (e.g., "#333333").
        position: Позиция текста ('center', 'left', 'right', 'top', 'bottom').
        padding: Отступы от краев для позиций 'left'/'right'/'top'/'bottom'.
        haze_opacity: Прозрачность белой дымки на все изображение (0-255).
        bg_blur_radius: Радиус размытия фона ПОД текстом (0 - отключить).
        bg_opacity: Прозрачность прямоугольной подложки ПОД текстом (0-255).
        logger_instance: Экземпляр логгера для использования.

    Returns:
        bool: True в случае успеха, False в случае ошибки.
    """
    # Получаем логгер
    log = logger_instance if logger_instance else logger # Используем глобальный логгер utils по умолчанию

    # Проверка доступности Pillow
    if not PIL_AVAILABLE or Image is None:
        log.error("Библиотека Pillow недоступна. Невозможно добавить текст.")
        return False

    try:
        base_image_path = Path(image_path_str)
        font_path = Path(font_path_str)
        output_path = Path(output_path_str)

        # Проверка существования файлов
        if not base_image_path.is_file():
            log.error(f"Исходное изображение не найдено: {base_image_path}")
            return False
        if not font_path.is_file():
            log.error(f"Файл шрифта не найден: {font_path}")
            return False

        log.info(f"Открытие изображения: {base_image_path.name}")
        # Открываем с конвертацией в RGBA для поддержки прозрачности
        img = Image.open(base_image_path).convert("RGBA")
        img_width, img_height = img.size

        # *** НОВОЕ: Добавление белой "дымки" (haze) ***
        if haze_opacity > 0:
            log.info(f"Добавление белой дымки (прозрачность: {haze_opacity})...")
            # Создаем слой для дымки
            haze_layer = Image.new('RGBA', img.size, (255, 255, 255, haze_opacity))
            # Накладываем дымку на изображение
            img = Image.alpha_composite(img, haze_layer)
            log.debug("Белая дымка добавлена.")
        # *** КОНЕЦ НОВОГО БЛОКА ***

        # Обновляем объект Draw после возможного добавления дымки
        draw = ImageDraw.Draw(img)

        log.info(f"Загрузка шрифта: {font_path.name} (размер: {font_size})")
        font = ImageFont.truetype(str(font_path), font_size)

        # --- Расчет размеров и позиции текста ---
        try:
            # Используем getbbox для Pillow >= 9.2.0 для многострочного текста
            # Важно: textbbox ожидает текст без переносов для расчета общей рамки
            # Поэтому считаем рамку для текста без переносов, а рисуем с переносами
            bbox = draw.textbbox((0, 0), text.replace('\n', ' '), font=font, anchor="lt")
            text_width = bbox[2] - bbox[0]
            # Для высоты считаем bbox для текста С переносами
            bbox_multiline = draw.textbbox((0, 0), text, font=font, anchor="lt")
            text_height = bbox_multiline[3] - bbox_multiline[1]

        except AttributeError:
            # Fallback для старых версий Pillow (менее точен для многострочного)
            log.warning("Метод textbbox не найден, используем textsize (менее точный).")
            text_width, _ = draw.textsize(text.replace('\n', ' '), font=font) # Ширина по тексту без переносов
            _, text_height = draw.textsize(text, font=font) # Высота по тексту с переносами

        log.debug(f"Рассчитанные размеры текста: Ширина (max)={text_width}, Высота={text_height}")

        # Расчет координат X
        if position[0] == 'center':
            x = (img_width - text_width) / 2
        elif position[0] == 'left':
            x = padding
        elif position[0] == 'right':
            x = img_width - text_width - padding
        else: # По умолчанию центр
            x = (img_width - text_width) / 2

        # Расчет координат Y
        if position[1] == 'center':
            y = (img_height - text_height) / 2
        elif position[1] == 'top':
            y = padding
        elif position[1] == 'bottom':
            y = img_height - text_height - padding
        else: # По умолчанию центр по вертикали
            y = (img_height - text_height) / 2

        # Итоговая позиция для textdraw (левый верхний угол текста)
        text_position = (int(x), int(y))
        log.info(f"Позиция текста (левый верхний угол): {text_position}")

        # --- Добавление подложки/размытия под текстом (опционально) ---
        # Эта логика остается, но теперь она будет поверх "дымки"
        if bg_blur_radius > 0 or bg_opacity > 0:
            log.info("Добавление эффектов фона под текстом...")
            background_layer = Image.new('RGBA', img.size, (0, 0, 0, 0))
            draw_bg = ImageDraw.Draw(background_layer)

            bg_padding = 20
            bg_left = max(0, text_position[0] - bg_padding)
            bg_top = max(0, text_position[1] - bg_padding)
            # Используем размеры текста С ПЕРЕНОСАМИ для правильного охвата подложки
            bbox_multiline_at_pos = draw.textbbox(text_position, text, font=font, anchor="lt")
            bg_right = min(img_width, bbox_multiline_at_pos[2] + bg_padding)
            bg_bottom = min(img_height, bbox_multiline_at_pos[3] + bg_padding)

            bg_rect_coords = [(int(bg_left), int(bg_top)), (int(bg_right), int(bg_bottom))]
            bg_crop_box = (int(bg_left), int(bg_top), int(bg_right), int(bg_bottom))

            if bg_blur_radius > 0:
                log.info(f"Применение размытия фона под текстом (радиус: {bg_blur_radius}) в области {bg_crop_box}")
                try:
                    crop = img.crop(bg_crop_box)
                    blurred_crop = crop.filter(ImageFilter.GaussianBlur(bg_blur_radius))
                    background_layer.paste(blurred_crop, bg_crop_box)
                except Exception as blur_err:
                    log.warning(f"Не удалось применить размытие под текстом: {blur_err}")

            if bg_opacity > 0:
                 log.info(f"Создание полупрозрачной подложки под текстом (opacity: {bg_opacity}) в области {bg_rect_coords}")
                 overlay_color = (0, 0, 0, bg_opacity) # Черный с заданной прозрачностью
                 draw_bg.rectangle(bg_rect_coords, fill=overlay_color)

            img = Image.alpha_composite(img, background_layer)
            draw = ImageDraw.Draw(img) # Обновляем draw для финального изображения

        # --- Нанесение текста ---
        # *** НОВОЕ: Используем text_color_hex ***
        final_text_color = hex_to_rgba(text_color_hex, alpha=240) # Получаем RGBA из HEX

        # *** ИСПРАВЛЕНИЕ ОШИБКИ f-string ***
        # Создаем переменную для лога
        log_text_preview = text[:50].replace('\n', '\\n') # Заменяем переносы для лога
        # Используем переменную в f-строке
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {final_text_color} (HEX: {text_color_hex})")
        # *** КОНЕЦ ИСПРАВЛЕНИЯ ***

        # Используем align='center' для многострочного текста, если позиция по горизонтали 'center'
        align_option = 'center' if position[0] == 'center' else 'left'
        # Используем anchor='lt' для Pillow >= 9.3.0 для более предсказуемого позиционирования
        try:
             # Рисуем текст с возможными переносами \n
             draw.text(text_position, text, font=font, fill=final_text_color, anchor="lt", align=align_option)
        except TypeError: # Fallback для старых версий, где anchor/align нет
             log.warning("Параметры 'anchor'/'align' не поддерживаются, используем старый метод позиционирования.")
             # Старый метод может не очень хорошо центрировать многострочный текст
             draw.text(text_position, text, font=font, fill=final_text_color)


        # --- Сохранение результата ---
        # Убедимся, что директория для сохранения существует
        output_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(output_path, format='PNG') # Сохраняем в PNG
        log.info(f"✅ Изображение с текстом сохранено: {output_path.name}")
        return True

    except FileNotFoundError as fnf_err:
         log.error(f"Ошибка FileNotFoundError при обработке изображения: {fnf_err}")
         return False
    except Exception as e:
        log.error(f"Ошибка при добавлении текста на изображение: {e}", exc_info=True)
        return False

# +++ КОНЕЦ ОБНОВЛЕННОЙ ФУНКЦИИ +++

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



