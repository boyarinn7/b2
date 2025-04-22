import os
import hashlib
import json
import base64
import inspect
import requests # <--- Добавлен импорт
import shutil   # <--- Добавлен импорт
import logging  # <--- Добавлен импорт (для логгера по умолчанию в download*)
import sys

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Warning: boto3 library not found. B2 functionality will be limited.")
    boto3 = None
    ClientError = None

from datetime import datetime
# Относительные импорты для модулей в том же пакете (modules)
try:
    # Добавлен BASE_DIR для надежного импорта модулей
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if BASE_DIR not in sys.path:
        sys.path.append(BASE_DIR)
    from modules.error_handler import handle_error
    from modules.logger import get_logger
except ImportError:
    # Фоллбэк, если запускается не как часть пакета
    # Попытка импорта из стандартного места, если относительный не сработал
    try:
        from modules.error_handler import handle_error
        from modules.logger import get_logger
    except ImportError:
        # Если и это не сработало, используем базовый логгер
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger("utils_fallback")
        logger.warning("Could not import custom logger or error handler.")
        def handle_error(context, message, exception=None): # Dummy function
             logger.error(f"Error in {context}: {message} | Details: {exception}")

from pathlib import Path
logger = get_logger("utils") # Используем ваш стандартный логгер

# --- Существующие функции из вашего файла ---
CONFIG_PATH = "config/config.json" # Используется в load_config, но сама load_config не вызывается в других скриптах?

def generate_file_id():
    """Создает уникальный ID генерации в формате ГГГГММДД-ЧЧММ (БЕЗ .json)."""
    now = datetime.utcnow()
    date_part = now.strftime("%Y%m%d")
    time_part = now.strftime("%H%M")
    return f"{date_part}-{time_part}"

# Эта функция, похоже, не используется другими модулями напрямую,
# т.к. они используют ConfigManager. Оставляем на всякий случай.
def load_config():
    try:
        # Используем BASE_DIR для построения пути к конфигу
        config_full_path = os.path.join(BASE_DIR, CONFIG_PATH)
        with open(config_full_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Файл конфигурации {config_full_path} не найден в load_config()")
        return {} # Возвращаем пустой словарь при ошибке
    except json.JSONDecodeError:
         logger.error(f"Ошибка декодирования JSON в {config_full_path}")
         return {}
    except Exception as e:
         logger.error(f"Неизвестная ошибка в load_config: {e}")
         return {}

# Добавьте этот код в конец файла modules/utils.py

def load_json_config(file_path: str): # Принимает строку, а не Path
    """Загружает JSON конфиг из файла по строковому пути."""
    # Используем logger, который уже должен быть определен в utils.py
    # Если логгера нет, можно добавить базовый logging.info/error
    # или передавать logger как аргумент
    logger_utils = logging.getLogger("utils") # Получаем логгер utils

    # Преобразуем строку в Path для проверки файла
    file_path_obj = Path(file_path)
    if not file_path_obj.is_file():
        logger_utils.error(f"Файл конфигурации не найден: {file_path}")
        return None
    try:
        with open(file_path_obj, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as json_err:
         logger_utils.error(f"Ошибка декодирования JSON из {file_path}: {json_err}")
         return None
    except Exception as e:
        logger_utils.error(f"Ошибка загрузки JSON из {file_path}: {e}")
        return None

# Функции для работы с topics_tracker.json (если они еще нужны)
def load_topics_tracker():
    # Используем ConfigManager для получения пути
    try:
        # Нужен экземпляр ConfigManager или передача пути
        # Временное решение: жестко задаем путь или читаем из load_config()
        temp_config = load_config() # Не лучший вариант
        tracker_path_rel = temp_config.get("FILE_PATHS", {}).get("topics_tracker", "data/topics_tracker.json")
        tracker_path_abs = os.path.join(BASE_DIR, tracker_path_rel) # Используем BASE_DIR
        if os.path.exists(tracker_path_abs):
            try:
                with open(tracker_path_abs, "r", encoding="utf-8") as file:
                    return json.load(file)
            except json.JSONDecodeError:
                logger.error(f"Ошибка JSON в файле трекера: {tracker_path_abs}")
                return {}
        else:
             logger.warning(f"Файл трекера не найден: {tracker_path_abs}")
             return {}
    except Exception as e:
        logger.error(f"Ошибка загрузки трекера: {e}")
        return {}


def save_topics_tracker(tracker):
     # Используем ConfigManager для получения пути
    try:
        temp_config = load_config() # Не лучший вариант
        tracker_path_rel = temp_config.get("FILE_PATHS", {}).get("topics_tracker", "data/topics_tracker.json")
        tracker_path_abs = os.path.join(BASE_DIR, tracker_path_rel) # Используем BASE_DIR
        os.makedirs(os.path.dirname(tracker_path_abs), exist_ok=True) # Создаем папку перед записью
        with open(tracker_path_abs, "w", encoding="utf-8") as file:
            json.dump(tracker, file, ensure_ascii=False, indent=4)
    except Exception as e:
         logger.error(f"Ошибка сохранения трекера: {e}")


def calculate_file_hash(file_path):
    try:
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
    except FileNotFoundError:
        handle_error("File Hash Calculation Error", f"Файл не найден: {file_path}")
        return None # Возвращаем None при ошибке
    except Exception as e:
        handle_error("File Hash Calculation Error", str(e), e)
        return None # Возвращаем None при ошибке


def validate_json_structure(data, required_keys):
    missing_keys = [key for key in required_keys if key not in data]
    if missing_keys:
        handle_error("JSON Validation Error", f"Отсутствуют ключи: {missing_keys}")
        return False # Возвращаем False при ошибке
    return True # Возвращаем True, если все ключи на месте


def ensure_directory_exists(path):
    """Создает директорию для указанного пути к файлу, если она не существует."""
    # --- ИСПРАВЛЕНИЕ ЗДЕСЬ ---
    # Получаем имя директории из полного пути
    directory = os.path.dirname(path)
    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

    # Проверяем, что имя директории не пустое (например, если path был просто именем файла в текущей папке)
    if not directory:
        # logger.debug(f"Путь '{path}' не содержит директории, создание не требуется.")
        return # Ничего не делаем, если нет директории для создания

    try:
        # Создаем директорию, если она не существует
        os.makedirs(directory, exist_ok=True)
        # logger.debug(f"Директория '{directory}' проверена/создана.")
    except Exception as e:
        logger.error(f"Ошибка создания директории {directory}: {e}")
        # Можно пробросить исключение дальше, если это критично
        # raise


def encode_image_to_base64(image_path: str) -> str | None: # Добавил None в тип возврата
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode("utf-8")
    except FileNotFoundError:
        logger.error(f"Файл изображения не найден для Base64: {image_path}")
        return None
    except Exception as e:
        logger.error(f"Ошибка кодирования изображения {image_path} в Base64: {e}")
        return None


def list_files_in_folder(s3, bucket_name, folder): # Добавил bucket_name
    if not s3:
        logger.error("Boto3 S3 client is not available in list_files_in_folder.")
        return []
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder) # Используем переданный bucket_name
        return [obj["Key"] for obj in objects.get("Contents", [])]
    except Exception as e:
        logger.error(f"❌ Ошибка при получении списка файлов в {folder}: {e}")
        return []


def is_folder_empty(s3, bucket_name, folder_prefix):
    if not s3:
        logger.error("Boto3 S3 client is not available in is_folder_empty.")
        return False # Считаем не пустой при ошибке клиента
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        # Папка считается пустой, если нет 'Contents' или единственный элемент - сама папка (Key=folder_prefix)
        contents = response.get('Contents')
        if not contents:
            return True
        # Проверяем, есть ли что-то кроме самой папки и .bzEmpty
        non_placeholder_files = [
            obj for obj in contents
            if not obj['Key'].endswith('.bzEmpty') and obj['Key'] != folder_prefix.rstrip('/')+'/'
            ]
        return not non_placeholder_files

    except Exception as e:
        logger.error(f"Ошибка проверки папки {folder_prefix} на пустоту: {e}")
        return False # В случае ошибки считаем, что не пустая


# --- Функции load/save JSON в B2 (из вашего оригинального utils.py) ---
def load_b2_json(client, bucket, remote_path, local_path, default_value=None):
    """Загружает JSON из B2, возвращает default_value при ошибке или отсутствии."""
    if not client:
        logger.error("Boto3 S3 client is not available in load_b2_json.")
        return default_value
    try:
        logger.debug(f"Загрузка {remote_path} из B2 в {local_path}")
        # Вызываем ensure_directory_exists ПЕРЕД download_file, чтобы создать папку для local_path
        ensure_directory_exists(local_path) # <-- ИСПРАВЛЕНО: Вызов до download_file
        client.download_file(bucket, remote_path, local_path)
        content = default_value
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            with open(local_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
        elif os.path.exists(local_path): # Файл есть, но пустой
            logger.warning(f"Загруженный файл {local_path} ({remote_path}) пуст, используем значение по умолчанию.")
        # Если файл не скачался, content останется default_value
        if content != default_value:
             logger.info(f"Успешно загружен и распарсен {remote_path} из B2.")
        return content
    except ClientError as e:
        # Проверяем код ошибки B2
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'NoSuchKey' or '404' in str(e):
             logger.warning(f"{remote_path} не найден в B2. Используем значение по умолчанию.")
        else:
             logger.error(f"Ошибка B2 при загрузке {remote_path}: {e} (Code: {error_code})")
        return default_value
    except json.JSONDecodeError as json_err:
        logger.error(f"Ошибка парсинга JSON из {local_path} ({remote_path}): {json_err}. Используем значение по умолчанию.")
        return default_value
    except Exception as e:
        logger.error(f"Критическая ошибка загрузки {remote_path}: {e}", exc_info=True)
        return default_value
    finally:
        # Удаляем временный локальный файл в любом случае
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                # logger.debug(f"Временный файл {local_path} удален.")
            except OSError as remove_err:
                 logger.warning(f"Не удалось удалить временный файл {local_path}: {remove_err}")

def save_b2_json(client, bucket, remote_path, local_path, data):
    """Сохраняет словарь data как JSON в B2."""
    if not client:
        logger.error("Boto3 S3 client is not available in save_b2_json.")
        return False
    try:
        logger.debug(f"Сохранение данных в {remote_path} в B2 через {local_path}")
        # Вызываем ensure_directory_exists ПЕРЕД открытием файла на запись
        ensure_directory_exists(local_path) # <-- ИСПРАВЛЕНО: Вызов до open()
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        client.upload_file(local_path, bucket, remote_path)
        # Логируем только часть данных для краткости, если они большие
        log_data_str = json.dumps(data, ensure_ascii=False)
        if len(log_data_str) > 200: log_data_str = log_data_str[:200] + "..."
        logger.info(f"Данные успешно сохранены в {remote_path} в B2: {log_data_str}")
        return True
    except Exception as e:
        logger.error(f"Критическая ошибка сохранения {remote_path}: {e}", exc_info=True)
        return False
    finally:
        # Удаляем временный локальный файл в любом случае
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                # logger.debug(f"Временный файл {local_path} удален.")
            except OSError as remove_err:
                 logger.warning(f"Не удалось удалить временный файл {local_path}: {remove_err}")

# --- Функция upload_to_b2 (из вашего оригинального utils.py) ---
def upload_to_b2(client, bucket_name, target_folder, local_file_path, base_id):
    """
    Загружает локальный файл в указанную папку B2,
    формируя имя файла на основе base_id и расширения локального файла.
    Возвращает True при успехе, False при ошибке.
    """
    if not client:
        logger.error("Boto3 S3 client is not available in upload_to_b2.")
        return False
    if not os.path.exists(local_file_path):
        logger.error(f"Локальный файл {local_file_path} не найден для загрузки в B2.")
        return False
    clean_base_id = base_id.replace(".json", "")
    file_extension = os.path.splitext(local_file_path)[1]
    if not file_extension:
         logger.error(f"Не удалось определить расширение для файла {local_file_path}")
         return False
    s3_key = f"{target_folder.rstrip('/')}/{clean_base_id}{file_extension}"
    logger.info(f"Загрузка {local_file_path} в B2 как {s3_key}...")
    try:
        client.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"✅ Файл успешно загружен в B2: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Не удалось загрузить {local_file_path} в B2 как {s3_key}: {e}", exc_info=True) # Добавил exc_info
        return False

# --- ДОБАВЛЕНЫ НОВЫЕ ФУНКЦИИ ---

def download_image(url: str, local_path: str, logger_instance=None) -> bool:
    """Downloads an image from a URL to a local path."""
    logger = logger_instance if logger_instance else logging.getLogger("utils_download")
    logger.info(f"Загрузка изображения с {url} в {local_path}...")
    try:
        ensure_directory_exists(local_path) # Создаем папку перед скачиванием
        with requests.get(url, stream=True, timeout=60) as r: # Таймаут 60 сек для изображений
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        logger.info(f"✅ Изображение успешно сохранено: {local_path}")
        return True
    except requests.exceptions.Timeout:
         logger.error(f"❌ Таймаут при загрузке изображения с {url}")
         return False
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети при загрузке изображения {url}: {e}")
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except OSError: pass
        return False
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при загрузке изображения {url}: {e}", exc_info=True)
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except OSError: pass
        return False

def download_video(url: str, local_path: str, logger_instance=None) -> bool:
    """Downloads a video from a URL to a local path."""
    logger = logger_instance if logger_instance else logging.getLogger("utils_download")
    logger.info(f"Загрузка видео с {url} в {local_path}...")
    try:
        ensure_directory_exists(local_path) # Создаем папку перед скачиванием
        with requests.get(url, stream=True, timeout=300) as r: # Таймаут 5 минут для видео
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        logger.info(f"✅ Видео успешно сохранено: {local_path}")
        return True
    except requests.exceptions.Timeout:
         logger.error(f"❌ Таймаут при загрузке видео с {url}")
         return False
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети при загрузке видео {url}: {e}")
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except OSError: pass
        return False
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при загрузке видео {url}: {e}", exc_info=True)
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except OSError: pass
        return False

