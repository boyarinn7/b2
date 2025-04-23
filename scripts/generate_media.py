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

# --- Сторонние библиотеки ---
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    from PIL import Image
    # --- ИМПОРТ RUNWAYML БЕЗ TRY...EXCEPT ---
    # Попытка импорта напрямую. Если не сработает, скрипт упадет здесь.
    from runwayml import RunwayML
    from runwayml.exceptions import RunwayError
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    from moviepy.editor import ImageClip
    import openai
except ImportError as e:
    # Логируем предупреждение, если *другая* библиотека не найдена
    # Ошибка импорта runwayml теперь приведет к падению скрипта выше
    print(f"Предупреждение: Необходимая библиотека не найдена (кроме runwayml?): {e}. Некоторые функции могут быть недоступны.")
    # Устанавливаем флаги/переменные в None для проверки в коде
    if 'PIL' in str(e): Image = None
    if 'moviepy' in str(e): ImageClip = None
    if 'openai' in str(e): openai = None
    # Если ошибка была именно с runwayml, она уже должна была произойти выше
    if 'runwayml' not in str(e):
        # Если ошибка не связана с runwayml, позволяем продолжить,
        # но Runway все равно не будет доступен, если импорт выше не удался.
        RunwayML = None
        RunwayError = Exception

# --- Ваши модули ---
try:
    # Добавлен BASE_DIR для надежного импорта модулей
    BASE_DIR = Path(__file__).resolve().parent.parent # Используем pathlib
    if str(BASE_DIR) not in sys.path:
        sys.path.append(str(BASE_DIR))

    from modules.utils import (
        ensure_directory_exists, load_b2_json, save_b2_json,
        download_image, download_video, upload_to_b2, load_json_config # Убедимся, что load_json_config здесь есть
    )
    from modules.api_clients import get_b2_client
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.config_manager import ConfigManager
except ModuleNotFoundError as import_err:
    print(f"Критическая Ошибка: Не найдены модули проекта: {import_err}", file=sys.stderr)
    sys.exit(1)
except ImportError as import_err:
     # Проверяем, не ошибка ли это импорта load_json_config
     if 'load_json_config' in str(import_err):
         print(f"Критическая Ошибка: Функция 'load_json_config' не найдена в 'modules.utils'. Убедитесь, что она добавлена.", file=sys.stderr)
     else:
          print(f"Критическая Ошибка: Не найдена функция/класс в модулях: {import_err}", file=sys.stderr)
     sys.exit(1)

print("--- IMPORTS DONE ---", flush=True)

# === Инициализация конфигурации и логирования ===
try:
    config = ConfigManager()
    print("--- CONFIG MANAGER INIT DONE ---", flush=True)
    logger = get_logger("generate_media")
    print("--- LOGGER INIT DONE ---", flush=True)
    logger.info("Logger generate_media is now active.")
except Exception as init_err:
    # Используем print, так как логгер мог не инициализироваться
    print(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}", file=sys.stderr)
    sys.exit(1) # Выход с ошибкой

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

    IMAGE_FORMAT = config.get("FILE_PATHS.output_image_format", "png")
    VIDEO_FORMAT = "mp4"
    MAX_ATTEMPTS = int(config.get("GENERATE.max_attempts", 1))
    OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4o")

    # Получаем размеры из конфига
    output_size_str = config.get("IMAGE_GENERATION.output_size", "1792x1024")
    delimiter = None
    if '×' in output_size_str: delimiter = '×'
    elif 'x' in output_size_str: delimiter = 'x'

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

    TASK_REQUEST_TIMEOUT = int(config.get("WORKFLOW.task_request_timeout", 60))

except Exception as config_err:
     logger.error(f"Критическая ошибка при чтении настроек: {config_err}", exc_info=True)
     sys.exit(1)

# === Вспомогательные Функции ===

def _initialize_openai_client():
    """Инициализирует глобальный клиент OpenAI, если он еще не создан."""
    global openai_client_instance
    if openai_client_instance:
        return True # Уже инициализирован

    api_key_local = os.getenv("OPENAI_API_KEY")
    if not api_key_local:
        logger.error("❌ Переменная окружения OPENAI_API_KEY не задана для generate_media!")
        return False # Не можем инициализировать

    try:
        if openai and hasattr(openai, 'OpenAI'):
            # Проверяем наличие прокси
            http_proxy = os.getenv("HTTP_PROXY")
            https_proxy = os.getenv("HTTPS_PROXY")
            proxies_dict = {}
            if http_proxy: proxies_dict["http://"] = http_proxy
            if https_proxy: proxies_dict["https://"] = https_proxy

            # Создаем httpx_client
            if proxies_dict:
                logger.info(f"Обнаружены настройки прокси для OpenAI (generate_media): {proxies_dict}")
                http_client = httpx.Client(proxies=proxies_dict)
            else:
                logger.info("Прокси не обнаружены (generate_media), создаем httpx.Client без аргумента proxies.")
                http_client = httpx.Client()

            # Передаем http_client в OpenAI
            openai_client_instance = openai.OpenAI(api_key=api_key_local, http_client=http_client)
            logger.info("✅ Клиент OpenAI (>1.0) инициализирован (generate_media).")
            return True
        else:
            logger.error("❌ Модуль/класс openai.OpenAI не найден в generate_media.")
            return False
    except Exception as init_err:
        logger.error(f"❌ Ошибка инициализации клиента OpenAI (generate_media): {init_err}", exc_info=True)
        if "got an unexpected keyword argument 'proxies'" in str(init_err):
             logger.error("!!! Повторная ошибка 'unexpected keyword argument proxies' в generate_media.")
        return False


def select_best_image(image_urls, prompt_text, prompt_settings: dict):
    """
    Выбирает лучшее изображение из списка URL с помощью OpenAI Vision API.
    """
    global openai_client_instance
    logger.info("Выбор лучшего изображения...")
    if not image_urls: logger.warning("Список image_urls пуст."); return None
    if not isinstance(image_urls, list):
        logger.warning(f"image_urls не список ({type(image_urls)}).");
        return image_urls if isinstance(image_urls, str) and image_urls.startswith('http') else None

    # --- Инициализация клиента OpenAI при необходимости ---
    if not openai_client_instance:
        if not _initialize_openai_client():
            logger.warning("Клиент OpenAI недоступен. Возвращаем первый URL.")
            return image_urls[0] if image_urls else None
    # --- Конец инициализации ---


    # Получаем критерии из creative_config
    creative_config_path_str = config.get('FILE_PATHS.creative_config')
    creative_config_data = {}
    if creative_config_path_str:
         creative_config_path = BASE_DIR / creative_config_path_str
         creative_config_data = load_json_config(str(creative_config_path)) or {}
    criteria = creative_config_data.get("visual_analysis_criteria", [])

    # Получаем шаблон промпта и max_tokens из настроек
    selection_prompt_template = prompt_settings.get("template")
    max_tokens = int(prompt_settings.get("max_tokens", 500))

    if not criteria or not selection_prompt_template:
         logger.warning("Критерии/промпт для выбора изображения не найдены. Возвращаем первый URL.")
         return image_urls[0] if image_urls else None

    criteria_text = ", ".join([f"{c['name']} (weight: {c['weight']})" for c in criteria])
    full_prompt = selection_prompt_template.format(prompt=(prompt_text or "Image analysis"), criteria=criteria_text)
    messages_content = [{"type": "text", "text": full_prompt}]
    valid_image_urls = []

    for i, url in enumerate(image_urls):
        if isinstance(url, str) and re.match(r"^(https?|data:image)", url):
            messages_content.append({"type": "image_url", "image_url": {"url": url}})
            valid_image_urls.append(url)
        else: logger.warning(f"Некорректный URL #{i+1}: {url}. Пропуск.")

    if not valid_image_urls: logger.warning("Нет валидных URL для Vision API."); return None
    if len(messages_content) <= 1: logger.warning("Нет контента для Vision API (только текст)."); return valid_image_urls[0]

    for attempt in range(MAX_ATTEMPTS):
        try:
            logger.info(f"Попытка {attempt + 1}/{MAX_ATTEMPTS} выбора лучшего изображения (max_tokens={max_tokens})...")
            gpt_response = openai_client_instance.chat.completions.create( # Используем глобальный клиент
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": messages_content}],
                max_tokens=max_tokens
            )
            if gpt_response.choices and gpt_response.choices[0].message:
                answer = gpt_response.choices[0].message.content
                if not answer:
                    logger.warning(f"OpenAI Vision вернул пустой ответ на попытке {attempt + 1}.")
                    continue

                logger.info(f"Ответ OpenAI Vision: {answer[:100]}...")
                matches = re.findall(r'\b([1-4])\b', answer) # Ищем цифры 1-4
                best_index = -1 # Инициализируем

                # Пытаемся найти номер изображения в ответе
                # Сначала ищем явное указание типа "Image 2" или "Image #3"
                explicit_match = re.search(r'[Ii]mage\s*#?(\d+)', answer)
                if explicit_match:
                    try:
                        best_index = int(explicit_match.group(1)) - 1
                        logger.debug(f"Найдено явное указание: Image {best_index + 1}")
                    except ValueError:
                        logger.warning(f"Не удалось извлечь число из явного указания: {explicit_match.group(1)}")
                        best_index = -1

                # Если явного указания нет, ищем просто цифру
                if best_index == -1 and matches:
                    best_index_str = matches[-1] # Берем последнюю найденную цифру
                    try:
                        best_index = int(best_index_str) - 1
                        logger.debug(f"Найдена цифра (последняя): {best_index + 1}")
                    except ValueError:
                         logger.warning(f"Не удалось преобразовать индекс '{best_index_str}' в число.")
                         best_index = -1

                # Проверяем валидность индекса и возвращаем URL
                if 0 <= best_index < len(valid_image_urls):
                    logger.info(f"Выбрано изображение #{best_index + 1} на основе ответа.")
                    return valid_image_urls[best_index]
                else:
                    logger.warning(f"Не удалось извлечь валидный индекс (0-{len(valid_image_urls)-1}) из ответа: '{answer}'. Выбираем первое.")
                    return valid_image_urls[0] # Возвращаем первый как fallback

            else:
                 logger.warning(f"OpenAI Vision вернул некорректный ответ на попытке {attempt + 1}: {gpt_response}")
                 continue

        # Обработка ошибок OpenAI
        except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI: {e}"); return valid_image_urls[0]
        except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI: {e}"); return valid_image_urls[0]
        except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI: {e}"); return valid_image_urls[0]
        except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}"); return valid_image_urls[0]
        except openai.BadRequestError as e: logger.exception(f"Ошибка неверного запроса OpenAI: {e}"); return valid_image_urls[0]
        except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI: {e}"); return valid_image_urls[0]
        except Exception as e:
            logger.error(f"Неизвестная ошибка OpenAI API (Vision, попытка {attempt + 1}): {e}", exc_info=True)
            if attempt < MAX_ATTEMPTS - 1:
                logger.info(f"Ожидание 5 секунд перед повторной попыткой...")
                time.sleep(5)
            else:
                logger.error("Превышено количество попыток OpenAI Vision.");
                return valid_image_urls[0] # Fallback на первый URL
    logger.error("Не удалось получить ответ от OpenAI Vision после всех попыток.")
    return valid_image_urls[0] # Fallback на первый URL


def resize_existing_image(image_path_str: str) -> bool:
    """Изменяет размер существующего изображения."""
    if Image is None: logger.warning("Pillow не импортирован."); return True
    image_path = Path(image_path_str)
    if not image_path.is_file(): logger.error(f"Ошибка ресайза: Файл не найден {image_path}"); return False
    try:
        target_width, target_height = PLACEHOLDER_WIDTH, PLACEHOLDER_HEIGHT
        logger.info(f"Ресайз {image_path} до {target_width}x{target_height}...")
        with Image.open(image_path) as img:
            img_format = img.format or IMAGE_FORMAT.upper()
            if img.mode != 'RGB': img = img.convert('RGB')
            resample_filter = Image.Resampling.LANCZOS if hasattr(Image, 'Resampling') else Image.LANCZOS
            img = img.resize((target_width, target_height), resample_filter)
            img.save(image_path, format=img_format)
        logger.info(f"✅ Ресайз до {target_width}x{target_height} завершен.")
        return True
    except Exception as e: logger.error(f"Ошибка ресайза {image_path}: {e}", exc_info=True); return False

def clean_script_text(script_text_param):
    """Очищает текст скрипта (может быть не нужна)."""
    logger.info("Очистка текста скрипта...");
    return ' '.join(script_text_param.replace('\n', ' ').replace('\r', ' ').split()) if script_text_param else ""

def generate_runway_video(image_path: str, script: str, config: ConfigManager, api_key: str) -> str | None:
    """Генерирует видео с помощью Runway ML SDK."""
    logger.info(f"Запуск генерации видео Runway для: {image_path}")

    # --- УБРАН ЛЕНИВЫЙ ИМПОРТ, Т.К. ИМПОРТ ЕСТЬ В НАЧАЛЕ ФАЙЛА ---
    # Если импорт в начале файла не сработал, скрипт уже должен был упасть.
    # Если он дошел сюда, значит RunwayML должен быть доступен.
    if RunwayML is None:
        logger.error("❌ Класс RunwayML не доступен (ошибка импорта в начале файла?).")
        return None
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    if not api_key:
        logger.error("❌ API ключ Runway не предоставлен.")
        return None
    if not Path(image_path).is_file():
        logger.error(f"❌ Файл изображения не найден: {image_path}")
        return None
    if not script:
        logger.error("❌ Промпт для Runway пуст.")
        return None

    try:
        model_name = config.get('API_KEYS.runwayml.model_name', 'gen-2')
        duration = int(config.get('VIDEO.runway_duration', 5))
        ratio_str = config.get('VIDEO.runway_ratio', f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}")
        poll_timeout = int(config.get('WORKFLOW.runway_polling_timeout', 300))
        poll_interval = int(config.get('WORKFLOW.runway_polling_interval', 15))
        logger.info(f"Параметры Runway: model='{model_name}', duration={duration}, ratio='{ratio_str}'")
    except Exception as cfg_err:
        logger.error(f"Ошибка чтения параметров Runway из конфига: {cfg_err}. Используются значения по умолчанию.")
        model_name="gen-2"; duration=5; ratio_str="16:9"; poll_timeout=300; poll_interval=15

    # Кодирование изображения в Base64
    try:
        with open(image_path, "rb") as f:
            base64_image = base64.b64encode(f.read()).decode("utf-8")
        ext = Path(image_path).suffix.lower()
        mime_type = f"image/{'jpeg' if ext == '.jpg' else ext[1:]}"
        image_data_uri = f"data:{mime_type};base64,{base64_image}"
        logger.info(f"Изображение {image_path} успешно кодировано в Base64.")
    except Exception as e:
        logger.error(f"❌ Ошибка кодирования изображения в Base64: {e}", exc_info=True)
        return None

    client = None
    task_id = 'N/A' # Инициализируем task_id
    try:
        logger.info("Инициализация клиента RunwayML SDK...")
        client = RunwayML(api_key=api_key)
        logger.info("✅ Клиент RunwayML SDK инициализирован.")

        generation_params = {
            "model": model_name,
            "prompt_image": image_data_uri,
            "prompt_text": script,
            "duration": duration,
            "ratio": ratio_str
        }
        logger.info("🚀 Создание задачи RunwayML Image-to-Video...")
        # Логируем параметры, обрезая длинные строки
        log_params = {k: (v[:50] + '...' if isinstance(v, str) and len(v) > 50 else v) for k, v in generation_params.items()}
        logger.debug(f"Параметры Runway: {json.dumps(log_params, indent=2)}")

        task = client.image_to_video.create(**generation_params)
        task_id = getattr(task, 'id', 'N/A') # Получаем ID задачи
        logger.info(f"✅ Задача Runway создана! ID: {task_id}")

        logger.info(f"⏳ Начало опроса статуса задачи Runway {task_id}...")
        start_time = time.time()
        final_output_url = None

        while time.time() - start_time < poll_timeout:
            try:
                task_status = client.tasks.retrieve(task_id)
                current_status = getattr(task_status, 'status', 'UNKNOWN').upper()
                logger.info(f"Статус Runway {task_id}: {current_status}")

                if current_status == "SUCCEEDED":
                    logger.info(f"✅ Задача Runway {task_id} успешно завершена!")
                    task_output = getattr(task_status, 'output', None)
                    # Пытаемся извлечь URL из разных возможных структур ответа
                    if isinstance(task_output, list) and len(task_output) > 0 and isinstance(task_output[0], str):
                        final_output_url = task_output[0]
                    elif isinstance(task_output, dict) and task_output.get('url'):
                        final_output_url = task_output['url']
                    elif isinstance(task_output, str) and task_output.startswith('http'):
                         final_output_url = task_output

                    if final_output_url:
                        logger.info(f"Получен URL видео: {final_output_url}")
                        return final_output_url
                    else:
                        logger.warning(f"Статус SUCCEEDED, но URL видео не найден в ответе: {task_output}")
                    break # Выходим из цикла опроса

                elif current_status == "FAILED":
                    logger.error(f"❌ Задача Runway {task_id} завершилась с ошибкой (FAILED)!")
                    error_details = getattr(task_status, 'error_message', 'Детали ошибки отсутствуют в ответе API.')
                    logger.error(f"Детали ошибки Runway: {error_details}")
                    break # Выходим из цикла опроса

                elif current_status in ["PENDING", "PROCESSING", "QUEUED", "WAITING"]:
                    # Статус промежуточный, продолжаем опрос
                    time.sleep(poll_interval)
                else:
                    logger.warning(f"Неизвестный или неожиданный статус Runway: {current_status}. Прерывание опроса.")
                    break # Выходим при неизвестном статусе

            except RunwayError as poll_sdk_err:
                 logger.error(f"❌ Ошибка SDK Runway при опросе задачи {task_id}: {poll_sdk_err}", exc_info=True)
                 break # Прерываем опрос при ошибке SDK
            except Exception as poll_err:
                logger.error(f"❌ Ошибка при опросе статуса Runway {task_id}: {poll_err}", exc_info=True)
                break # Прерываем опрос при других ошибках
        else:
            # Цикл завершился по таймауту
            logger.warning(f"⏰ Таймаут ({poll_timeout} сек) ожидания завершения задачи Runway {task_id}.")

        return None # Возвращаем None, если видео не было получено

    except RunwayError as r_err: # Ошибка SDK при создании задачи
        logger.error(f"❌ Ошибка Runway SDK при создании задачи: {r_err}", exc_info=True)
        return None
    except Exception as e: # Другие ошибки
        logger.error(f"❌ Общая ошибка при взаимодействии с Runway: {e}", exc_info=True)
        return None


def create_mock_video(image_path_str: str) -> str | None:
    """Создает mock-видео из изображения."""
    if ImageClip is None: logger.error("MoviePy не импортирован."); return None
    logger.info(f"Создание mock видео для {image_path_str}...")
    image_path_obj = Path(image_path_str)
    if not image_path_obj.is_file(): logger.error(f"{image_path_obj} не найден или не файл."); return None

    clip = None; base_name = image_path_obj.stem
    suffixes_to_remove = ["_mj_final", "_placeholder", "_best", "_temp"]
    for suffix in suffixes_to_remove:
        if base_name.endswith(suffix): base_name = base_name[:-len(suffix)]; break
    output_path = str(image_path_obj.parent / f"{base_name}.{VIDEO_FORMAT}")
    try:
        duration = int(config.get("VIDEO.mock_duration", 10)); fps = int(config.get("VIDEO.mock_fps", 24)); codec = config.get("VIDEO.mock_codec", "libx264")
        logger.debug(f"Параметры mock: output={output_path}, duration={duration}, fps={fps}, codec={codec}")
        clip = ImageClip(str(image_path_obj), duration=duration); clip.fps = fps
        clip.write_videofile(output_path, codec=codec, fps=fps, audio=False, logger=None, ffmpeg_params=["-loglevel", "error"])
        logger.info(f"✅ Mock видео создано: {output_path}"); return output_path
    except Exception as e: logger.error(f"❌ Ошибка создания mock: {e}", exc_info=True); return None
    finally:
        if clip:
            try:
                clip.close()
                logger.debug("MoviePy clip closed.")
            except Exception as close_err:
                 logger.warning(f"Ошибка закрытия clip: {close_err}")

def initiate_midjourney_task(prompt: str, config: ConfigManager, api_key: str, endpoint: str, ref_id: str = "") -> dict | None:
    """Инициирует задачу Midjourney, используя переданный финальный промпт."""
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
    logger.info(f"Инициация задачи MJ..."); logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
    response = None
    try:
        logger.info(f"Отправка запроса на {endpoint}...")
        response = requests.post(endpoint, headers=headers, json=payload, timeout=TASK_REQUEST_TIMEOUT)
        logger.debug(f"Ответ PiAPI MJ Init: Status={response.status_code}, Body={response.text[:200]}")
        response.raise_for_status(); result = response.json()
        task_id = result.get("result", {}).get("task_id") or result.get("data", {}).get("task_id") or result.get("task_id")
        if task_id: logger.info(f"✅ Получен task_id MJ: {task_id} (запрошено в {request_time.isoformat()})"); return {"task_id": str(task_id), "requested_at_utc": request_time.isoformat()}
        else: logger.error(f"❌ Ответ MJ API не содержит task_id: {result}"); return None
    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут MJ API ({TASK_REQUEST_TIMEOUT} сек): {endpoint}");
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса MJ API: {e}")
        if e.response is not None:
            logger.error(f"    Status: {e.response.status_code}, Body: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        response_text = response.text[:500] if response else "Ответ не получен"
        logger.error(f"❌ Ошибка JSON MJ API: {e}. Ответ: {response_text}");
        return None
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка MJ: {e}", exc_info=True);
        return None

# === Основная Функция ===
def main():
    parser = argparse.ArgumentParser(description='Generate media or initiate Midjourney task.')
    parser.add_argument('--generation_id', type=str, required=True, help='The generation ID.')
    parser.add_argument('--use-mock', action='store_true', default=False, help='Force generation of a mock video.')
    args = parser.parse_args()
    generation_id = args.generation_id
    use_mock_flag = args.use_mock

    if isinstance(generation_id, str) and generation_id.endswith(".json"):
        generation_id = generation_id[:-5]
    logger.info(f"--- Запуск generate_media для ID: {generation_id} (Use Mock: {use_mock_flag}) ---")

    b2_client = None; content_data = None; config_mj = None; prompts_config_data = None
    first_frame_description = ""; final_mj_prompt = ""; final_runway_prompt = ""

    timestamp_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    content_local_path = f"{generation_id}_content_temp_{timestamp_suffix}.json"
    config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"

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
        ensure_directory_exists(content_local_path) # Папка для temp
        content_data = load_b2_json(b2_client, B2_BUCKET_NAME, content_remote_path, content_local_path, default_value=None)
        if content_data is None: logger.error(f"❌ Не удалось загрузить {content_remote_path}."); sys.exit(1)

        # Извлекаем данные
        first_frame_description = content_data.get("first_frame_description", "")
        final_mj_prompt = content_data.get("final_mj_prompt", "")
        final_runway_prompt = content_data.get("final_runway_prompt", "")
        logger.info("Данные из контента:"); logger.info(f"  - Описание: '{first_frame_description[:100]}...'"); logger.info(f"  - MJ Промпт: '{final_mj_prompt[:100]}...'"); logger.info(f"  - Runway Промпт: '{final_runway_prompt[:100]}...'")
        if not first_frame_description: logger.warning("Описание отсутствует!")

        logger.info(f"Загрузка состояния: {CONFIG_MJ_REMOTE_PATH}...")
        ensure_directory_exists(config_mj_local_path) # Папка для temp
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, default_value=None)
        if config_mj is None: logger.warning(f"Не загрузить {CONFIG_MJ_REMOTE_PATH}."); config_mj = {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None}
        else: config_mj.setdefault("midjourney_task", None); config_mj.setdefault("midjourney_results", {}); config_mj.setdefault("generation", False); config_mj.setdefault("status", None)
        logger.info("✅ Данные и конфиги загружены.")

        temp_dir_path = Path(f"temp_{generation_id}_{timestamp_suffix}")
        ensure_directory_exists(str(temp_dir_path))
        local_image_path = None; video_path = None

        try:
            if use_mock_flag:
                logger.warning(f"⚠️ Принудительный mock для ID: {generation_id}")
                placeholder_text = f"MJ Timeout\n{first_frame_description[:60]}" if first_frame_description else "MJ Timeout"
                encoded_text = urllib.parse.quote(placeholder_text)
                placeholder_url = f"https://placehold.co/{PLACEHOLDER_WIDTH}x{PLACEHOLDER_HEIGHT}/{PLACEHOLDER_BG_COLOR}/{PLACEHOLDER_TEXT_COLOR}?text={encoded_text}"
                local_image_path = temp_dir_path / f"{generation_id}_placeholder.{IMAGE_FORMAT}"
                logger.info(f"Создание плейсхолдера: {placeholder_url}")
                if not download_image(placeholder_url, str(local_image_path)): raise Exception("Не скачать плейсхолдер")
                logger.info(f"Плейсхолдер сохранен: {local_image_path}")
                video_path_str = create_mock_video(str(local_image_path))
                if not video_path_str: raise Exception("Не создать mock видео.")
                video_path = Path(video_path_str)
                logger.info("Сброс состояния MJ..."); config_mj['midjourney_task'] = None; config_mj['midjourney_results'] = {}; config_mj['generation'] = False; config_mj['status'] = None
            else:
                mj_results_data = config_mj.get("midjourney_results", {}); image_urls_from_results = None
                if isinstance(mj_results_data.get("task_result"), dict):
                     task_result = mj_results_data["task_result"]; possible_url_keys = ["temporary_image_urls", "image_urls", "image_url"]
                     for key in possible_url_keys:
                         urls = task_result.get(key)
                         if isinstance(urls, list) and urls: image_urls_from_results = urls; logger.debug(f"URL MJ из '{key}'."); break
                         elif isinstance(urls, str) and urls.startswith('http'): image_urls_from_results = [urls]; logger.debug(f"URL MJ из '{key}'."); break
                     if not image_urls_from_results: logger.warning(f"URL не найдены в task_result: {task_result}")

                if image_urls_from_results:
                    logger.info(f"Обнаружены результаты MJ. Генерация медиа...")
                    image_urls = image_urls_from_results; logger.info(f"Используем {len(image_urls)} URL.")
                    if not first_frame_description: logger.warning("Нет описания для выбора!")
                    # Получаем настройки для промпта анализа
                    visual_analysis_settings = prompts_config_data.get("visual_analysis", {}).get("image_selection", {})
                    best_image_url = select_best_image(image_urls, first_frame_description or " ", visual_analysis_settings)
                    if not best_image_url: raise ValueError("Не выбрать лучшее изображение.")
                    logger.info(f"Выбрано: {best_image_url}")
                    local_image_path = temp_dir_path / f"{generation_id}_best.{IMAGE_FORMAT}"
                    if not download_image(best_image_url, str(local_image_path)): raise Exception(f"Не скачать {best_image_url}")
                    logger.info(f"Изображение сохранено: {local_image_path}"); resize_existing_image(str(local_image_path))

                    video_path_str = None
                    if not final_runway_prompt: logger.error("❌ Промпт Runway отсутствует! Mock."); video_path_str = create_mock_video(str(local_image_path))
                    else:
                         # Передаем необходимые параметры в generate_runway_video
                         video_url_or_path = generate_runway_video(
                             image_path=str(local_image_path),
                             script=final_runway_prompt,
                             config=config, # Передаем экземпляр ConfigManager
                             api_key=RUNWAY_API_KEY # Передаем ключ Runway
                         )
                         if video_url_or_path:
                             if video_url_or_path.startswith("http"):
                                 video_path_temp = temp_dir_path / f"{generation_id}_downloaded.{VIDEO_FORMAT}"
                                 if download_video(video_url_or_path, str(video_path_temp)): video_path = video_path_temp
                                 else: logger.error(f"Не скачать видео {video_url_or_path}. Mock."); video_path_str = create_mock_video(str(local_image_path))
                             else: video_path = Path(video_url_or_path)
                         else: logger.error("Runway не удалась. Mock."); video_path_str = create_mock_video(str(local_image_path))
                         if not video_path and video_path_str: video_path = Path(video_path_str)

                    if not video_path: raise Exception("Не сгенерировать видео.")
                    logger.info("Очистка состояния MJ..."); config_mj['midjourney_results'] = {}; config_mj['generation'] = False; config_mj['midjourney_task'] = None; config_mj['status'] = None
                else:
                    logger.info(f"Результаты MJ не найдены. Запуск новой задачи MJ...")
                    if not final_mj_prompt: logger.error("❌ Промпт MJ отсутствует!"); config_mj['generation'] = False; config_mj['midjourney_task'] = None; config_mj['status'] = None
                    else:
                        # Передаем необходимые параметры в initiate_midjourney_task
                        task_result = initiate_midjourney_task(
                            prompt=final_mj_prompt,
                            config=config, # Передаем экземпляр ConfigManager
                            api_key=MIDJOURNEY_API_KEY, # Передаем ключ MJ
                            endpoint=MIDJOURNEY_ENDPOINT, # Передаем эндпоинт MJ
                            ref_id=generation_id
                        )
                        if task_result and isinstance(task_result, dict) and task_result.get("task_id"):
                            logger.info(f"Обновление config_mj: task={task_result}, generation=False.")
                            config_mj['midjourney_task'] = task_result; config_mj['generation'] = False; config_mj['midjourney_results'] = {}; config_mj['status'] = None
                        else: logger.warning("Не получить task_id MJ."); config_mj['midjourney_task'] = None; config_mj['generation'] = False; config_mj['midjourney_results'] = {}; config_mj['status'] = None

            # Загрузка файлов в B2
            target_folder_b2 = "666/"; upload_success_img = False; upload_success_vid = False
            if local_image_path and isinstance(local_image_path, Path) and local_image_path.is_file():
                 upload_success_img = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(local_image_path), generation_id)
            else:
                if local_image_path: # Если путь был, но файл не найден
                    logger.warning(f"Изображение {local_image_path} не найдено или не Path для загрузки.")
                # Если local_image_path изначально None (например, при инициации MJ), ничего не логируем

            if video_path and isinstance(video_path, Path) and video_path.is_file():
                 upload_success_vid = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(video_path), generation_id)
            elif video_path: logger.error(f"Видео {video_path} не найдено или не Path для загрузки!")
            elif image_urls_from_results or use_mock_flag: logger.warning("Видео не сгенерировано/не найдено для загрузки.")

            if (local_image_path and video_path):
                if upload_success_img and upload_success_vid: logger.info("✅ Изображение и видео загружены.")
                else: logger.warning("⚠️ Не все медиа файлы загружены.")
            elif local_image_path and upload_success_img: logger.info("✅ Изображение загружено.")
            elif video_path and upload_success_vid: logger.info("✅ Видео загружено.")

        finally:
             if temp_dir_path.exists():
                 try: shutil.rmtree(temp_dir_path); logger.debug(f"Удалена папка: {temp_dir_path}")
                 except OSError as e: logger.warning(f"Не удалить {temp_dir_path}: {e}")

        # Сохранение config_mj
        logger.info(f"Сохранение config_midjourney.json в B2...")
        if not isinstance(config_mj, dict): logger.error("config_mj не словарь!")
        elif not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, config_mj): logger.error("Не сохранить config_midjourney.json.")
        else: logger.info("✅ config_midjourney.json сохранен.")

        logger.info("✅ Работа generate_media.py успешно завершена.")

    except ConnectionError as conn_err: logger.error(f"❌ Ошибка соединения B2: {conn_err}"); sys.exit(1)
    except Exception as e: logger.error(f"❌ Критическая ошибка в generate_media.py: {e}", exc_info=True); sys.exit(1)
    finally:
        # Очистка временных файлов
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
    exit_code_main = 1 # По умолчанию ошибка
    try:
        main()
        exit_code_main = 0 # Успех, если main() завершился без исключений
    except KeyboardInterrupt:
        logger.info("🛑 Остановлено пользователем.")
        exit_code_main = 130 # Стандартный код для Ctrl+C
    except SystemExit as e:
        # Логируем код выхода, если он не 0
        if e.code != 0: logger.error(f"Завершение с кодом ошибки: {e.code}")
        else: logger.info(f"Завершение с кодом {e.code}")
        exit_code_main = e.code # Пробрасываем код выхода
    except Exception as e:
        # Логируем неперехваченные ошибки
        print(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}")
        try: logger.error(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}", exc_info=True)
        except NameError: pass # Логгер может быть недоступен
        exit_code_main = 1 # Общий код ошибки
    finally:
        # Выходим с финальным кодом
        sys.exit(exit_code_main)
