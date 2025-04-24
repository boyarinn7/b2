#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Отладочный вывод для проверки старта скрипта в GitHub Actions
print("--- SCRIPT START (generate_media.py) ---", flush=True)

# --- Стандартные библиотеки ---
import os
import json
import sys
import time
import argparse
import requests
import shutil
import base64
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path # Используем pathlib
import logging # Добавляем logging
import httpx # <-- ДОБАВЛЕН ИМПОРТ httpx

# --- Предварительная инициализация базового логгера (на случай ошибок до основного) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
temp_logger = logging.getLogger("generate_media_init") # Используем этот логгер для ранних сообщений

# --- Ваши модули (попытка импорта ДО инициализации основного логгера) ---
try:
    BASE_DIR = Path(__file__).resolve().parent.parent # Используем pathlib
    if str(BASE_DIR) not in sys.path:
        sys.path.append(str(BASE_DIR))

    from modules.config_manager import ConfigManager
    from modules.logger import get_logger # Импортируем функцию, но не вызываем пока
    from modules.utils import (
        ensure_directory_exists, load_b2_json, save_b2_json,
        download_image, download_video, upload_to_b2, load_json_config
    )
    from modules.api_clients import get_b2_client
    from modules.error_handler import handle_error

except ModuleNotFoundError as import_err:
    temp_logger.error(f"Критическая Ошибка: Не найдены модули проекта: {import_err}", exc_info=True)
    sys.exit(1)
except ImportError as import_err:
     temp_logger.error(f"Критическая Ошибка импорта модулей проекта: {import_err}", exc_info=True)
     sys.exit(1)

# === Инициализация конфигурации и ОСНОВНОГО логгера ===
try:
    config = ConfigManager()
    print("--- CONFIG MANAGER INIT DONE ---", flush=True)
    # Инициализируем основной логгер ЗДЕСЬ
    logger = get_logger("generate_media")
    print("--- LOGGER INIT DONE ---", flush=True)
    logger.info("Logger generate_media is now active.")
except Exception as init_err:
    # Используем temp_logger, так как основной мог не создаться
    temp_logger.error(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}", exc_info=True)
    sys.exit(1) # Выход с ошибкой


# --- Сторонние библиотеки (теперь основной логгер доступен в except) ---
RunwayML = None
RunwayError = None
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    from PIL import Image
    from moviepy.editor import ImageClip
    import openai

    # --- ИМПОРТ RUNWAYML С УТОЧНЕННОЙ ОБРАБОТКОЙ ОШИБОК ---
    try:
        from runwayml import RunwayML
        logger.info("Основной модуль runwayml импортирован.")
        # Пытаемся импортировать специфичное исключение, но не падаем, если его нет
        try:
            from runwayml.exceptions import RunwayError
            logger.info("runwayml.exceptions.RunwayError импортирован.")
        except ImportError:
            logger.warning("Не удалось импортировать runwayml.exceptions.RunwayError. Будут использоваться общие исключения.")
            try:
                from runwayml.exceptions import RunwayError as BaseRunwayError
                RunwayError = BaseRunwayError
                logger.info("Используется базовый runwayml.exceptions.RunwayError.")
            except ImportError:
                 RunwayError = requests.HTTPError # Fallback на HTTPError
                 logger.warning("Не удалось импортировать базовый RunwayError. Fallback на requests.HTTPError.")

    except ImportError as e:
        if 'runwayml' in str(e).lower():
             logger.warning(f"Не удалось импортировать основную библиотеку runwayml: {e}. Функционал Runway будет недоступен.")
             RunwayML = None
             RunwayError = None
        else:
             logger.error(f"Ошибка импорта сторонней библиотеки (не runwayml): {e}", exc_info=True)
             raise e

except ImportError as e:
    logger.warning(f"Необходимая основная библиотека не найдена: {e}. Некоторые функции могут быть недоступны.")
    if 'PIL' in str(e): Image = None
    if 'moviepy' in str(e): ImageClip = None
    if 'openai' in str(e): openai = None

print("--- IMPORTS DONE ---", flush=True)


# === Глобальная переменная для клиента OpenAI ===
openai_client_instance = None

# === Константы и Настройки ===
try:
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))
    if not B2_BUCKET_NAME: raise ValueError("B2_BUCKET_NAME не определен в конфиге или переменных окружения")

    CONFIG_MJ_REMOTE_PATH = config.get('FILE_PATHS.config_midjourney', "config/config_midjourney.json") # Добавлен дефолт

    MIDJOURNEY_ENDPOINT = config.get("API_KEYS.midjourney.endpoint")
    MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
    RUNWAY_API_KEY = os.getenv("RUNWAY_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") # Ключ нужен для инициализации ниже

    # --- ИЗМЕНЕНО: Убедимся, что формат без точки ---
    IMAGE_FORMAT = config.get("FILE_PATHS.output_image_format", "png").lstrip('.')
    VIDEO_FORMAT = "mp4" # Обычно не меняется
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    MAX_ATTEMPTS = int(config.get("GENERATE.max_attempts", 1))
    OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4o")

    # Получаем размеры из конфига
    output_size_str = config.get("IMAGE_GENERATION.output_size", "1792x1024")
    delimiter = None
    if '×' in output_size_str: delimiter = '×'
    elif 'x' in output_size_str: delimiter = 'x'
    elif ':' in output_size_str: delimiter = ':'

    if delimiter:
        try:
            width_str, height_str = output_size_str.split(delimiter)
            PLACEHOLDER_WIDTH = int(width_str.strip())
            PLACEHOLDER_HEIGHT = int(height_str.strip())
            logger.info(f"Размеры изображения/плейсхолдера: {PLACEHOLDER_WIDTH}x{PLACEHOLDER_HEIGHT}")
        except ValueError as e:
            logger.error(f"Ошибка преобразования размеров '{output_size_str}': {e}. Используем 1792x1024.")
            PLACEHOLDER_WIDTH = 1792; PLACEHOLDER_HEIGHT = 1024
    else:
        logger.error(f"Не удалось определить разделитель в IMAGE_GENERATION.output_size: '{output_size_str}'. Используем 1792x1024.")
        PLACEHOLDER_WIDTH = 1792; PLACEHOLDER_HEIGHT = 1024

    PLACEHOLDER_BG_COLOR = config.get("VIDEO.placeholder_bg_color", "cccccc")
    PLACEHOLDER_TEXT_COLOR = config.get("VIDEO.placeholder_text_color", "333333")

    # Используем значение из конфига или дефолт 60
    TASK_REQUEST_TIMEOUT = int(config.get("WORKFLOW.task_request_timeout", 60))
    logger.info(f"Таймаут запроса к API (task_request_timeout): {TASK_REQUEST_TIMEOUT} сек.")

except Exception as config_err:
     logger.error(f"Критическая ошибка при чтении настроек: {config_err}", exc_info=True)
     sys.exit(1)

# === Вспомогательные Функции ===

def _initialize_openai_client():
    """Инициализирует глобальный клиент OpenAI, если он еще не создан."""
    global openai_client_instance
    if openai_client_instance:
        return True

    if openai is None:
        logger.error("❌ Модуль openai не был импортирован.")
        return False

    api_key_local = os.getenv("OPENAI_API_KEY")
    if not api_key_local:
        logger.error("❌ Переменная окружения OPENAI_API_KEY не задана для generate_media!")
        return False

    try:
        if hasattr(openai, 'OpenAI'):
            http_proxy = os.getenv("HTTP_PROXY")
            https_proxy = os.getenv("HTTPS_PROXY")
            proxies_dict = {}
            if http_proxy: proxies_dict["http://"] = http_proxy
            if https_proxy: proxies_dict["https://"] = https_proxy

            if proxies_dict:
                logger.info(f"Обнаружены настройки прокси для OpenAI (generate_media): {proxies_dict}")
                http_client = httpx.Client(proxies=proxies_dict)
            else:
                logger.info("Прокси не обнаружены (generate_media), создаем httpx.Client без аргумента proxies.")
                http_client = httpx.Client()

            openai_client_instance = openai.OpenAI(api_key=api_key_local, http_client=http_client)
            logger.info("✅ Клиент OpenAI (>1.0) инициализирован (generate_media).")
            return True
        else:
            logger.error("❌ Класс openai.OpenAI не найден в generate_media.")
            return False
    except Exception as init_err:
        logger.error(f"❌ Ошибка инициализации клиента OpenAI (generate_media): {init_err}", exc_info=True)
        return False


def select_best_image(image_urls, prompt_text, prompt_settings: dict) -> int | None:
    """
    Выбирает лучшее изображение из списка URL с помощью OpenAI Vision API.
    Возвращает индекс лучшего изображения (0-3) или None при ошибке.
    """
    global openai_client_instance
    logger.info("Выбор индекса лучшего изображения...")
    if not image_urls: logger.warning("Список image_urls пуст."); return None
    if not isinstance(image_urls, list) or len(image_urls) != 4:
        logger.warning(f"Ожидался список из 4 URL, получено: {type(image_urls)} (длина: {len(image_urls) if isinstance(image_urls, list) else 'N/A'}).")
        return None

    if not openai_client_instance:
        if not _initialize_openai_client():
            logger.error("Клиент OpenAI недоступен для выбора изображения.")
            return None

    creative_config_path_str = config.get('FILE_PATHS.creative_config')
    creative_config_data = {}
    if creative_config_path_str:
         creative_config_path = BASE_DIR / creative_config_path_str
         creative_config_data = load_json_config(str(creative_config_path)) or {}
    criteria = creative_config_data.get("visual_analysis_criteria", [])

    selection_prompt_template = prompt_settings.get("template_index")
    if not selection_prompt_template:
        logger.warning("Шаблон 'template_index' не найден в prompts_config.json -> visual_analysis -> image_selection. Используем fallback.")
        selection_prompt_template = """
Analyze the following 4 images based on the original prompt and the criteria provided.
Respond ONLY with the number (1, 2, 3, or 4) of the image that best fits the criteria and prompt. Do not add any other text.

Original Prompt Context: {prompt}
Evaluation Criteria: {criteria}
"""
    # --- ИЗМЕНЕНО: Используем max_tokens из конфига или дефолт ---
    max_tokens = int(prompt_settings.get("max_tokens", 500)) # Вернули 500, если не задано
    logger.info(f"Max tokens для Vision: {max_tokens}")
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    if not criteria:
         logger.warning("Критерии для выбора изображения не найдены. Выбор невозможен.")
         return None

    criteria_text = ", ".join([f"{c['name']} (weight: {c['weight']})" for c in criteria])
    full_prompt = selection_prompt_template.format(prompt=(prompt_text or "Image analysis"), criteria=criteria_text)
    messages_content = [{"type": "text", "text": full_prompt}]
    valid_image_urls = []

    for i, url in enumerate(image_urls):
        if isinstance(url, str) and re.match(r"^(https?|data:image)", url):
            messages_content.append({"type": "text", "text": f"Image {i+1}:"})
            messages_content.append({"type": "image_url", "image_url": {"url": url}})
            valid_image_urls.append(url)
        else: logger.warning(f"Некорректный URL #{i+1}: {url}. Пропуск.")

    if len(valid_image_urls) != 4: logger.warning("Количество валидных URL не равно 4."); return None
    if len(messages_content) <= 1: logger.warning("Нет контента для Vision API (только текст)."); return None

    for attempt in range(MAX_ATTEMPTS):
        try:
            logger.info(f"Попытка {attempt + 1}/{MAX_ATTEMPTS} выбора индекса лучшего изображения (max_tokens={max_tokens})...")
            gpt_response = openai_client_instance.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": messages_content}],
                max_tokens=max_tokens,
                temperature=0.2
            )
            if gpt_response.choices and gpt_response.choices[0].message:
                answer = gpt_response.choices[0].message.content.strip()
                if not answer:
                    logger.warning(f"OpenAI Vision вернул пустой ответ на попытке {attempt + 1}.")
                    continue

                logger.info(f"Ответ OpenAI Vision (ожидается индекс): '{answer}'")
                match = re.search(r'\b([1-4])\b', answer)
                if match:
                    try:
                        best_index_one_based = int(match.group(1))
                        best_index_zero_based = best_index_one_based - 1
                        logger.info(f"Выбран индекс изображения: {best_index_zero_based} (ответ: {best_index_one_based})")
                        return best_index_zero_based
                    except ValueError:
                        logger.warning(f"Не удалось преобразовать найденную цифру '{match.group(1)}' в индекс.")
                        continue
                else:
                    logger.warning(f"Не удалось найти индекс (1-4) в ответе: '{answer}'.")
                    continue

            else:
                 logger.warning(f"OpenAI Vision вернул некорректный ответ на попытке {attempt + 1}: {gpt_response}")
                 continue

        except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI: {e}"); return None
        except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI: {e}"); return None
        except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI: {e}"); return None
        except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}"); return None
        except openai.BadRequestError as e: logger.exception(f"Ошибка неверного запроса OpenAI: {e}"); return None
        except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI: {e}"); return None
        except Exception as e:
            logger.error(f"Неизвестная ошибка OpenAI API (Vision Index, попытка {attempt + 1}): {e}", exc_info=True)
            if attempt < MAX_ATTEMPTS - 1:
                logger.info(f"Ожидание 5 секунд перед повторной попыткой...")
                time.sleep(5)
            else:
                logger.error("Превышено количество попыток OpenAI Vision для выбора индекса.");
                return None
    logger.error("Не удалось получить ответ от OpenAI Vision для выбора индекса после всех попыток.")
    return None


def resize_existing_image(image_path_str: str) -> bool:
    """Изменяет размер существующего изображения."""
    if Image is None: logger.warning("Pillow не импортирован."); return True # Не ошибка, просто пропускаем
    image_path = Path(image_path_str)
    if not image_path.is_file(): logger.error(f"Ошибка ресайза: Файл не найден {image_path}"); return False
    try:
        target_width, target_height = PLACEHOLDER_WIDTH, PLACEHOLDER_HEIGHT
        logger.info(f"Ресайз {image_path} до {target_width}x{target_height}...")
        with Image.open(image_path) as img:
            # Сохраняем исходный формат
            img_format = img.format or IMAGE_FORMAT.upper() # Используем IMAGE_FORMAT как fallback
            if img.mode != 'RGB': img = img.convert('RGB') # Конвертируем в RGB для совместимости
            resample_filter = Image.Resampling.LANCZOS if hasattr(Image, 'Resampling') else Image.LANCZOS
            img = img.resize((target_width, target_height), resample_filter)
            # Сохраняем в том же формате, в котором открыли
            img.save(image_path, format=img_format)
        logger.info(f"✅ Ресайз до {target_width}x{target_height} завершен.")
        return True
    except Exception as e: logger.error(f"Ошибка ресайза {image_path}: {e}", exc_info=True); return False

def clean_script_text(script_text_param):
    """Очищает текст скрипта (убирает лишние пробелы и переносы)."""
    logger.debug("Очистка текста скрипта...");
    return ' '.join(script_text_param.replace('\n', ' ').replace('\r', ' ').split()) if script_text_param else ""

def generate_runway_video(image_path: str, script: str, config: ConfigManager, api_key: str) -> str | None:
    """Генерирует видео с помощью Runway ML SDK."""
    logger.info(f"Запуск генерации видео Runway для: {image_path}")

    if RunwayML is None: logger.error("❌ Класс RunwayML не доступен."); return None
    if not api_key: logger.error("❌ API ключ Runway не предоставлен."); return None
    if not Path(image_path).is_file(): logger.error(f"❌ Файл изображения не найден: {image_path}"); return None
    if not script: logger.warning("⚠️ Промпт для Runway пуст. Попытка генерации без текста."); script = "" # Позволяем пустой промпт

    try:
        model_name = config.get('API_KEYS.runwayml.model_name', 'gen-2')
        duration = int(config.get('VIDEO.runway_duration', 5))
        # Получаем ratio из конфига, проверяем на допустимость
        ratio_str = config.get('VIDEO.runway_ratio', f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}")
        supported_ratios = ["1280:720", "720:1280", "1104:832", "832:1104", "960:960", "1584:672"]
        # Проверяем, соответствует ли ratio_str формату Ш:В
        if not re.match(r"^\d+:\d+$", ratio_str) or ratio_str not in supported_ratios:
            logger.warning(f"Неподдерживаемое или некорректное соотношение сторон '{ratio_str}' в конфиге. Используется '1280:720'.")
            ratio_str = "1280:720" # Fallback на стандартный

        poll_timeout = int(config.get('WORKFLOW.runway_polling_timeout', 300))
        poll_interval = int(config.get('WORKFLOW.runway_polling_interval', 15))
        logger.info(f"Параметры Runway: model='{model_name}', duration={duration}, ratio='{ratio_str}'")
    except Exception as cfg_err:
        logger.error(f"Ошибка чтения параметров Runway из конфига: {cfg_err}. Используются значения по умолчанию.")
        model_name="gen-2"; duration=5; ratio_str="1280:720"; poll_timeout=300; poll_interval=15

    # Кодирование изображения в Base64
    try:
        with open(image_path, "rb") as f:
            base64_image = base64.b64encode(f.read()).decode("utf-8")
        ext = Path(image_path).suffix.lower().lstrip('.') # Убираем точку
        mime_type = f"image/{'jpeg' if ext == 'jpg' else ext}"
        image_data_uri = f"data:{mime_type};base64,{base64_image}"
        logger.info(f"Изображение {image_path} успешно кодировано в Base64 (MIME: {mime_type}).")
    except Exception as e:
        logger.error(f"❌ Ошибка кодирования изображения в Base64: {e}", exc_info=True)
        return None

    client = None; task_id = 'N/A'
    try:
        logger.info("Инициализация клиента RunwayML SDK...")
        client = RunwayML(api_key=api_key)
        logger.info("✅ Клиент RunwayML SDK инициализирован.")

        generation_params = {
            "model": model_name,
            "prompt_image": image_data_uri,
            "prompt_text": script, # Передаем очищенный или пустой скрипт
            "duration": duration,
            "ratio": ratio_str
        }
        logger.info("🚀 Создание задачи RunwayML Image-to-Video...")
        log_params = {k: (v[:50] + '...' if isinstance(v, str) and len(v) > 50 else v) for k, v in generation_params.items()}
        logger.debug(f"Параметры Runway: {json.dumps(log_params, indent=2)}")

        task = client.image_to_video.create(**generation_params)
        task_id = getattr(task, 'id', 'N/A')
        logger.info(f"✅ Задача Runway создана! ID: {task_id}")

        logger.info(f"⏳ Начало опроса статуса задачи Runway {task_id}...")
        start_time = time.time(); final_output_url = None

        while time.time() - start_time < poll_timeout:
            try:
                task_status = client.tasks.retrieve(task_id)
                current_status = getattr(task_status, 'status', 'UNKNOWN').upper()
                logger.info(f"Статус Runway {task_id}: {current_status}")

                if current_status == "SUCCEEDED":
                    logger.info(f"✅ Задача Runway {task_id} успешно завершена!")
                    task_output = getattr(task_status, 'output', None)
                    # Извлекаем URL
                    if isinstance(task_output, list) and len(task_output) > 0 and isinstance(task_output[0], str): final_output_url = task_output[0]
                    elif isinstance(task_output, dict) and task_output.get('url'): final_output_url = task_output['url']
                    elif isinstance(task_output, str) and task_output.startswith('http'): final_output_url = task_output
                    if final_output_url: logger.info(f"Получен URL видео: {final_output_url}"); return final_output_url
                    else: logger.warning(f"Статус SUCCEEDED, но URL видео не найден: {task_output}")
                    break

                elif current_status == "FAILED":
                    logger.error(f"❌ Задача Runway {task_id} завершилась с ошибкой (FAILED)!")
                    error_details = getattr(task_status, 'error_message', 'Детали ошибки отсутствуют.')
                    logger.error(f"Детали ошибки Runway: {error_details}")
                    break

                elif current_status in ["PENDING", "PROCESSING", "QUEUED", "WAITING", "RUNNING"]: time.sleep(poll_interval)
                else: logger.warning(f"Неизвестный статус Runway: {current_status}. Прерывание."); break

            except requests.HTTPError as http_err: logger.error(f"❌ Ошибка HTTP при опросе Runway {task_id}: {http_err.response.status_code} - {http_err.response.text}", exc_info=False); break
            except Exception as poll_err:
                if RunwayError and isinstance(poll_err, RunwayError): logger.error(f"❌ Ошибка SDK Runway при опросе {task_id}: {poll_err}", exc_info=True)
                else: logger.error(f"❌ Общая ошибка при опросе статуса Runway {task_id}: {poll_err}", exc_info=True)
                break
        else: logger.warning(f"⏰ Таймаут ({poll_timeout} сек) ожидания задачи Runway {task_id}.")
        return None

    except requests.HTTPError as http_err: logger.error(f"❌ Ошибка HTTP при создании задачи Runway: {http_err.response.status_code} - {http_err.response.text}", exc_info=False); return None
    except Exception as e:
         if RunwayError and isinstance(e, RunwayError): logger.error(f"❌ Ошибка SDK Runway при создании задачи: {e}", exc_info=True)
         else: logger.error(f"❌ Общая ошибка при взаимодействии с Runway: {e}", exc_info=True)
         return None


def create_mock_video(image_path_str: str) -> str | None:
    """Создает mock-видео из изображения."""
    if ImageClip is None: logger.error("MoviePy не импортирован."); return None
    logger.info(f"Создание mock видео для {image_path_str}...")
    image_path_obj = Path(image_path_str)
    if not image_path_obj.is_file(): logger.error(f"{image_path_obj} не найден или не файл."); return None

    clip = None
    # --- ИЗМЕНЕНО: Используем ID из имени файла, если он соответствует паттерну ---
    base_name = image_path_obj.stem
    match = re.match(r"(\d{8}-\d{4})", base_name) # Ищем паттерн ID в начале имени
    if match:
        file_id = match.group(1)
        logger.debug(f"Извлечен ID '{file_id}' из имени файла для mock видео.")
    else:
        # Если ID не найден, используем stem как есть (менее надежно)
        file_id = base_name
        logger.warning(f"Не удалось извлечь ID из '{base_name}'. Используется как есть.")
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    output_path = str(image_path_obj.parent / f"{file_id}.{VIDEO_FORMAT}")
    try:
        duration = int(config.get("VIDEO.mock_duration", 10)); fps = int(config.get("VIDEO.mock_fps", 24)); codec = config.get("VIDEO.mock_codec", "libx264")
        logger.debug(f"Параметры mock: output={output_path}, duration={duration}, fps={fps}, codec={codec}")
        clip = ImageClip(str(image_path_obj), duration=duration); clip.fps = fps
        # Добавляем параметры для подавления вывода ffmpeg, кроме ошибок
        clip.write_videofile(output_path, codec=codec, fps=fps, audio=False, logger=None, ffmpeg_params=["-loglevel", "error"])
        logger.info(f"✅ Mock видео создано: {output_path}"); return output_path
    except Exception as e: logger.error(f"❌ Ошибка создания mock: {e}", exc_info=True); return None
    finally:
        if clip:
            try: clip.close(); logger.debug("MoviePy clip closed.")
            except Exception as close_err: logger.warning(f"Ошибка закрытия clip: {close_err}")

def initiate_midjourney_task(prompt: str, config: ConfigManager, api_key: str, endpoint: str, ref_id: str = "") -> dict | None:
    """Инициирует задачу Midjourney /imagine."""
    if not api_key: logger.error("Нет MIDJOURNEY_API_KEY."); return None
    if not endpoint: logger.error("Нет API_KEYS.midjourney.endpoint."); return None
    if not prompt: logger.error("Промпт MJ пуст."); return None
    logger.info(f"Используется финальный промпт MJ: {prompt[:100]}...")
    if "--ar" not in prompt: logger.warning("Промпт MJ не содержит --ar?")
    if "--v" not in prompt: logger.warning("Промпт MJ не содержит --v?")
    cleaned_prompt = " ".join(prompt.split())
    payload = {"model": "midjourney", "task_type": "imagine", "input": {"prompt": cleaned_prompt}}
    if ref_id: payload["ref"] = ref_id
    headers = { 'X-API-Key': api_key, 'Content-Type': 'application/json' }
    request_time = datetime.now(timezone.utc)
    logger.info(f"Инициация задачи MJ /imagine..."); logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
    response = None
    try:
        logger.info(f"Отправка запроса на {endpoint}...")
        response = requests.post(endpoint, headers=headers, json=payload, timeout=TASK_REQUEST_TIMEOUT)
        logger.debug(f"Ответ PiAPI MJ Init: Status={response.status_code}, Body={response.text[:200]}")
        response.raise_for_status(); result = response.json()
        task_id = result.get("result", {}).get("task_id") or result.get("data", {}).get("task_id") or result.get("task_id")
        if task_id: logger.info(f"✅ Получен task_id MJ /imagine: {task_id} (запрошено в {request_time.isoformat()})"); return {"task_id": str(task_id), "requested_at_utc": request_time.isoformat()}
        else: logger.error(f"❌ Ответ MJ API не содержит task_id: {result}"); return None
    except requests.exceptions.Timeout: logger.error(f"❌ Таймаут MJ API ({TASK_REQUEST_TIMEOUT} сек): {endpoint}"); return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса MJ API: {e}")
        if e.response is not None: logger.error(f"    Status: {e.response.status_code}, Body: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        response_text = response.text[:500] if response else "Ответ не получен"
        logger.error(f"❌ Ошибка JSON MJ API: {e}. Ответ: {response_text}"); return None
    except Exception as e: logger.error(f"❌ Неизвестная ошибка MJ: {e}", exc_info=True); return None

def trigger_piapi_action(original_task_id: str, action: str, api_key: str, endpoint: str) -> dict | None:
    """Запускает действие (например, upscale) для задачи Midjourney через PiAPI."""
    if not api_key or not endpoint or not original_task_id or not action:
        logger.error("Недостаточно данных для запуска действия PiAPI (trigger_piapi_action).")
        return None

    task_type = None
    if action.startswith("upscale"): task_type = "upscale"
    elif action.startswith("variation"): task_type = "variation"
    else: logger.error(f"Неизвестный тип действия в '{action}'."); return None

    index_match = re.search(r'\d+$', action)
    if not index_match: logger.error(f"Не удалось извлечь индекс из '{action}'."); return None
    index_str = index_match.group(0)

    payload = {"model": "midjourney", "task_type": task_type, "input": {"origin_task_id": original_task_id, "index": index_str}}
    headers = {'X-API-Key': api_key, 'Content-Type': 'application/json'}
    request_time = datetime.now(timezone.utc)

    logger.info(f"Отправка запроса на действие '{action}' (тип: {task_type}) для задачи {original_task_id} на {endpoint}...")
    logger.debug(f"Payload действия PiAPI: {json.dumps(payload, indent=2)}")
    response = None
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=TASK_REQUEST_TIMEOUT)
        logger.debug(f"Ответ от PiAPI Action Trigger: Status={response.status_code}, Body={response.text[:500]}")
        response.raise_for_status(); result = response.json()
        new_task_id = result.get("result", {}).get("task_id") or result.get("data", {}).get("task_id") or result.get("task_id")
        if new_task_id:
            timestamp_str = request_time.isoformat()
            logger.info(f"✅ Получен НОВЫЙ task_id для действия '{action}': {new_task_id} (запрошено в {timestamp_str})")
            return {"task_id": str(new_task_id), "requested_at_utc": timestamp_str}
        else: logger.warning(f"Ответ API на действие '{action}' не содержит нового task_id. Ответ: {result}"); return None
    except requests.exceptions.Timeout: logger.error(f"❌ Таймаут при запросе на действие '{action}' к PiAPI: {endpoint}"); return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса на действие '{action}' к PiAPI: {e}")
        if e.response is not None: logger.error(f"    Статус: {e.response.status_code}, Тело: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        response_text = response.text[:500] if response else "Ответ не получен"
        logger.error(f"❌ Ошибка JSON ответа на действие '{action}' от PiAPI: {e}. Ответ: {response_text}"); return None
    except Exception as e: logger.exception(f"❌ Неизвестная ошибка при запуске действия '{action}' PiAPI: {e}"); return None


# === Основная Функция ===
def main():
    parser = argparse.ArgumentParser(description='Generate media or initiate Midjourney task.')
    parser.add_argument('--generation_id', type=str, required=True, help='The generation ID.')
    parser.add_argument('--use-mock', action='store_true', default=False, help='Force generation of a mock video.')
    args = parser.parse_args()
    generation_id = args.generation_id
    use_mock_flag = args.use_mock

    # Очистка ID на всякий случай (если передали с .json)
    if isinstance(generation_id, str) and generation_id.endswith(".json"):
        generation_id = generation_id[:-5]
        logger.warning(f"Удалено .json из generation_id. Используется: {generation_id}")

    logger.info(f"--- Запуск generate_media для ID: {generation_id} (Use Mock: {use_mock_flag}) ---")

    b2_client = None; content_data = None; config_mj = None; prompts_config_data = None
    first_frame_description = ""; final_mj_prompt = ""; final_runway_prompt = ""

    # Уникальные временные пути
    timestamp_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    pid_suffix = os.getpid()
    content_local_path = f"{generation_id}_content_temp_{pid_suffix}_{timestamp_suffix}.json"
    config_mj_local_path = f"config_midjourney_{generation_id}_temp_{pid_suffix}_{timestamp_suffix}.json"
    temp_dir_path = Path(f"temp_{generation_id}_{pid_suffix}_{timestamp_suffix}")

    try:
        b2_client = get_b2_client();
        if not b2_client: raise ConnectionError("Не удалось создать клиент B2.")

        # Загрузка prompts_config.json
        prompts_config_path_str = config.get('FILE_PATHS.prompts_config')
        if not prompts_config_path_str: logger.error("❌ Путь к prompts_config не найден!"); prompts_config_data = {}
        else:
            prompts_config_path = BASE_DIR / prompts_config_path_str
            prompts_config_data = load_json_config(str(prompts_config_path))
            if not prompts_config_data: logger.error(f"❌ Не загрузить prompts_config из {prompts_config_path}!"); prompts_config_data = {}
            else: logger.info("✅ Конфигурация промптов загружена.")

        logger.info("Пауза (3 сек) перед загрузкой контента..."); time.sleep(3)
        content_remote_path = f"666/{generation_id}.json"
        logger.info(f"Загрузка контента: {content_remote_path}...")
        ensure_directory_exists(content_local_path)
        content_data = load_b2_json(b2_client, B2_BUCKET_NAME, content_remote_path, content_local_path, default_value=None)
        if content_data is None: logger.error(f"❌ Не удалось загрузить {content_remote_path}."); sys.exit(1)

        # Извлекаем данные
        first_frame_description = content_data.get("first_frame_description", "")
        final_mj_prompt = content_data.get("final_mj_prompt", "")
        final_runway_prompt = content_data.get("final_runway_prompt", "")
        logger.info("Данные из контента:"); logger.info(f"  - Описание: '{first_frame_description[:100]}...'"); logger.info(f"  - MJ Промпт: '{final_mj_prompt[:100]}...'"); logger.info(f"  - Runway Промпт: '{final_runway_prompt[:100]}...'")
        if not first_frame_description: logger.warning("Описание отсутствует!")

        logger.info(f"Загрузка состояния: {CONFIG_MJ_REMOTE_PATH}...")
        ensure_directory_exists(config_mj_local_path)
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, default_value=None)
        if config_mj is None: logger.warning(f"Не загрузить {CONFIG_MJ_REMOTE_PATH}."); config_mj = {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None}
        else: config_mj.setdefault("midjourney_task", None); config_mj.setdefault("midjourney_results", {}); config_mj.setdefault("generation", False); config_mj.setdefault("status", None)
        logger.info("✅ Данные и конфиги загружены.")

        ensure_directory_exists(str(temp_dir_path))
        local_image_path = None; video_path = None
        final_upscaled_image_url = None
        is_imagine_result = False
        is_upscale_result = False

        # Определяем тип результата в конфиге
        mj_results = config_mj.get("midjourney_results", {})
        task_result_data = mj_results.get("task_result")

        if isinstance(task_result_data, dict):
            if task_result_data.get("actions") and isinstance(task_result_data.get("temporary_image_urls"), list):
                is_imagine_result = True; logger.info("Обнаружен результат задачи /imagine.")
            elif isinstance(task_result_data.get("image_url"), str) and task_result_data.get("image_url").startswith("http") and not task_result_data.get("actions"):
                 is_upscale_result = True; final_upscaled_image_url = task_result_data.get("image_url")
                 logger.info(f"Обнаружен результат задачи /upscale: {final_upscaled_image_url}")
            else: logger.warning(f"Не удалось определить тип результата MJ в task_result: {json.dumps(task_result_data, indent=2)[:500]}...")
        elif mj_results: logger.warning(f"Поле 'task_result' в midjourney_results не словарь: {mj_results}")

        # --- Основной блок обработки ---
        try:
            if use_mock_flag:
                logger.warning(f"⚠️ Принудительный mock для ID: {generation_id}")
                placeholder_text = f"MJ/Upscale Timeout\n{first_frame_description[:60]}" if first_frame_description else "MJ/Upscale Timeout"
                encoded_text = urllib.parse.quote(placeholder_text)
                placeholder_url = f"https://placehold.co/{PLACEHOLDER_WIDTH}x{PLACEHOLDER_HEIGHT}/{PLACEHOLDER_BG_COLOR}/{PLACEHOLDER_TEXT_COLOR}?text={encoded_text}"
                # --- ИЗМЕНЕНО: Используем IMAGE_FORMAT ---
                local_image_path = temp_dir_path / f"{generation_id}_placeholder.{IMAGE_FORMAT}"
                logger.info(f"Создание плейсхолдера: {placeholder_url}")
                if not download_image(placeholder_url, str(local_image_path)): raise Exception("Не скачать плейсхолдер")
                logger.info(f"Плейсхолдер сохранен: {local_image_path}")
                video_path_str = create_mock_video(str(local_image_path))
                if not video_path_str: raise Exception("Не создать mock видео.")
                video_path = Path(video_path_str)
                logger.info("Сброс состояния MJ..."); config_mj['midjourney_task'] = None; config_mj['midjourney_results'] = {}; config_mj['generation'] = False; config_mj['status'] = None

            elif is_upscale_result and final_upscaled_image_url:
                logger.info(f"Обработка результата /upscale. Генерация видео Runway...")
                # --- ИЗМЕНЕНО: Используем IMAGE_FORMAT ---
                local_image_path = temp_dir_path / f"{generation_id}_upscaled.{IMAGE_FORMAT}"
                if not download_image(final_upscaled_image_url, str(local_image_path)): raise Exception(f"Не скачать апскейл {final_upscaled_image_url}")
                logger.info(f"Апскейл сохранен: {local_image_path}")
                resize_existing_image(str(local_image_path)) # Ресайз апскейла

                video_path_str = None
                if not final_runway_prompt: logger.error("❌ Промпт Runway отсутствует! Создание Mock."); video_path_str = create_mock_video(str(local_image_path))
                else:
                     video_url_or_path = generate_runway_video(str(local_image_path), final_runway_prompt, config, RUNWAY_API_KEY)
                     if video_url_or_path:
                         if video_url_or_path.startswith("http"):
                             # --- ИЗМЕНЕНО: Используем VIDEO_FORMAT ---
                             video_path_temp = temp_dir_path / f"{generation_id}_downloaded.{VIDEO_FORMAT}"
                             if download_video(video_url_or_path, str(video_path_temp)): video_path = video_path_temp
                             else: logger.error(f"Не скачать видео {video_url_or_path}. Mock."); video_path_str = create_mock_video(str(local_image_path))
                         else: video_path = Path(video_url_or_path) # Если вернулся локальный путь
                     else: logger.error("Runway не удалась. Mock."); video_path_str = create_mock_video(str(local_image_path))
                     if not video_path and video_path_str: video_path = Path(video_path_str)

                if not video_path: raise Exception("Не сгенерировать/скачать видео.")
                logger.info("Очистка состояния MJ (после Runway)...");
                config_mj['midjourney_results'] = {} # Очищаем результаты апскейла
                config_mj['generation'] = False
                config_mj['midjourney_task'] = None
                config_mj['status'] = None # Сбрасываем статус

            elif is_imagine_result:
                logger.info(f"Обработка результата /imagine. Выбор лучшего и запуск /upscale...")
                imagine_task_id = mj_results.get("task_id")
                image_urls = task_result_data.get("temporary_image_urls", [])

                if not image_urls or len(image_urls) != 4: raise ValueError(f"Некорректные temporary_image_urls: {task_result_data}")
                if not imagine_task_id: raise ValueError(f"Отсутствует ID исходной задачи /imagine: {mj_results}")

                visual_analysis_settings = prompts_config_data.get("visual_analysis", {}).get("image_selection", {})
                best_index = select_best_image(image_urls, first_frame_description or " ", visual_analysis_settings)
                if best_index is None or not (0 <= best_index <= 3):
                    logger.warning(f"Не удалось выбрать индекс (результат: {best_index}). Используем индекс 0.")
                    best_index = 0

                action_to_trigger = f"upscale{best_index + 1}"
                available_actions = task_result_data.get("actions", [])
                logger.info(f"Выбран индекс {best_index}. Требуемое действие: {action_to_trigger}. Доступные: {available_actions}")

                if action_to_trigger not in available_actions:
                    logger.warning(f"Действие {action_to_trigger} недоступно! Попытка найти другое upscale...")
                    found_upscale = False
                    for action in available_actions:
                        if action.startswith("upscale"): action_to_trigger = action; logger.info(f"Используем первое доступное: {action_to_trigger}"); found_upscale = True; break
                    if not found_upscale: raise ValueError("Нет доступных upscale действий")

                upscale_task_info = trigger_piapi_action(imagine_task_id, action_to_trigger, MIDJOURNEY_API_KEY, MIDJOURNEY_ENDPOINT)
                if upscale_task_info and upscale_task_info.get("task_id"):
                    logger.info(f"Задача /upscale запущена. Новый ID: {upscale_task_info['task_id']}")
                    config_mj['midjourney_task'] = upscale_task_info
                    config_mj['midjourney_results'] = {} # Очищаем старые результаты imagine
                    config_mj['generation'] = False
                    config_mj['status'] = "waiting_for_upscale"
                    logger.info("Состояние обновлено для ожидания /upscale.")
                else:
                    logger.error(f"Не удалось запустить задачу /upscale для {action_to_trigger}.")
                    config_mj['status'] = "upscale_trigger_failed"; config_mj['midjourney_task'] = None; config_mj['midjourney_results'] = {}

            elif config_mj.get("generation") is True:
                logger.info(f"Нет результатов MJ, но установлен флаг generation. Запуск /imagine...")
                if not final_mj_prompt: logger.error("❌ Промпт MJ отсутствует! Невозможно запустить /imagine."); config_mj['generation'] = False
                else:
                    imagine_task_info = initiate_midjourney_task(final_mj_prompt, config, MIDJOURNEY_API_KEY, MIDJOURNEY_ENDPOINT, generation_id)
                    if imagine_task_info and imagine_task_info.get("task_id"):
                        logger.info(f"Задача /imagine запущена. ID: {imagine_task_info['task_id']}")
                        config_mj['midjourney_task'] = imagine_task_info; config_mj['generation'] = False; config_mj['midjourney_results'] = {}; config_mj['status'] = "waiting_for_imagine"
                    else: logger.warning("Не удалось получить task_id для /imagine."); config_mj['midjourney_task'] = None; config_mj['generation'] = False

            else: logger.warning("Не найдено активной задачи MJ, результатов или флага 'generation'. Пропуск шагов MJ/Runway.")

            # --- ИЗМЕНЕНО: Явное формирование имени файла для B2 ---
            target_folder_b2 = "666/"
            upload_success_img = False
            upload_success_vid = False

            # Загрузка изображения (плейсхолдер или апскейл)
            if local_image_path and isinstance(local_image_path, Path) and local_image_path.is_file():
                 # Убираем любые возможные суффиксы из ID и добавляем нужный
                 clean_generation_id = generation_id.split('.')[0] # На всякий случай
                 b2_image_filename = f"{clean_generation_id}.{IMAGE_FORMAT}" # Используем IMAGE_FORMAT
                 logger.info(f"Подготовлено имя для загрузки изображения: {b2_image_filename}")
                 upload_success_img = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(local_image_path), b2_image_filename)
            elif local_image_path:
                 logger.warning(f"Изображение {local_image_path} не найдено или не Path для загрузки.")

            # Загрузка видео (Runway или Mock)
            if video_path and isinstance(video_path, Path) and video_path.is_file():
                 clean_generation_id = generation_id.split('.')[0] # На всякий случай
                 b2_video_filename = f"{clean_generation_id}.{VIDEO_FORMAT}" # Используем VIDEO_FORMAT
                 logger.info(f"Подготовлено имя для загрузки видео: {b2_video_filename}")
                 upload_success_vid = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(video_path), b2_video_filename)
            elif video_path:
                 logger.error(f"Видео {video_path} не найдено или не Path для загрузки!")
            elif is_upscale_result or use_mock_flag: # Логируем отсутствие видео только если оно ожидалось
                 logger.warning("Видео не сгенерировано/не найдено для загрузки.")
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---

            # Логирование результата загрузки
            if local_image_path and video_path:
                if upload_success_img and upload_success_vid: logger.info("✅ Изображение и видео успешно загружены в B2.")
                else: logger.warning("⚠️ Не все медиа файлы были успешно загружены в B2.")
            elif local_image_path and upload_success_img: logger.info("✅ Изображение успешно загружено в B2.")
            elif video_path and upload_success_vid: logger.info("✅ Видео успешно загружено в B2.")

        finally:
             # Очистка временной папки
             if temp_dir_path.exists():
                 try: shutil.rmtree(temp_dir_path); logger.debug(f"Удалена папка: {temp_dir_path}")
                 except OSError as e: logger.warning(f"Не удалить {temp_dir_path}: {e}")

        # Сохранение финального состояния config_mj
        logger.info(f"Сохранение config_midjourney.json в B2...")
        if not isinstance(config_mj, dict): logger.error("config_mj не словарь!")
        elif not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, config_mj): logger.error("Не сохранить config_midjourney.json.")
        else: logger.info("✅ config_midjourney.json сохранен.")

        logger.info("✅ Работа generate_media.py успешно завершена.")

    except ConnectionError as conn_err: logger.error(f"❌ Ошибка соединения B2: {conn_err}"); sys.exit(1)
    except Exception as e: logger.error(f"❌ Критическая ошибка в generate_media.py: {e}", exc_info=True); sys.exit(1)
    finally:
        # Очистка временных файлов конфигов
        content_temp_path = Path(content_local_path)
        if content_temp_path.exists():
            try: os.remove(content_temp_path); logger.debug(f"Удален temp контент: {content_temp_path}")
            except OSError as e: logger.warning(f"Не удалить {content_temp_path}: {e}")
        config_mj_temp_path = Path(config_mj_local_path)
        if config_mj_temp_path.exists():
            try: os.remove(config_mj_temp_path); logger.debug(f"Удален temp конфиг MJ: {config_mj_temp_path}")
            except OSError as e: logger.warning(f"Не удалить {config_mj_temp_path}: {e}")

# === Точка входа ===
if __name__ == "__main__":
    exit_code_main = 1
    try:
        main()
        exit_code_main = 0
    except KeyboardInterrupt: logger.info("🛑 Остановлено пользователем."); exit_code_main = 130
    except SystemExit as e:
        if e.code != 0: logger.error(f"Завершение с кодом ошибки: {e.code}")
        else: logger.info(f"Завершение с кодом {e.code}")
        exit_code_main = e.code
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}")
        try: logger.error(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}", exc_info=True)
        except NameError: pass
        exit_code_main = 1
    finally: sys.exit(exit_code_main)
