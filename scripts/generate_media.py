#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Отладочный вывод для проверки старта скрипта в GitHub Actions
print("--- SCRIPT START (generate_media.py) ---", flush=True)

# --- Стандартные библиотеки ---
import os
import json
import sys
import subprocess
import time # <--- Импорт time для sleep
import argparse
import requests
import shutil
import base64
import re
import urllib.parse
from datetime import datetime, timezone

# --- Сторонние библиотеки ---
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    from PIL import Image
    from runwayml import RunwayML
    RUNWAY_SDK_AVAILABLE = True
    RunwayError = Exception # Используем базовый Exception, если RunwayError не импортируется
    from moviepy.editor import ImageClip
    import openai
except ImportError as e:
    print(f"Предупреждение: Необходимая библиотека не найдена: {e}. Некоторые функции могут быть недоступны.")
    if 'PIL' in str(e): Image = None
    if 'runwayml' in str(e):
        RunwayML = None; RunwayError = Exception
        RUNWAY_SDK_AVAILABLE = False
    if 'moviepy' in str(e): ImageClip = None
    if 'openai' in str(e): openai = None

# --- Ваши модули ---
try:
    # Добавлен BASE_DIR для надежного импорта модулей
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if BASE_DIR not in sys.path:
        sys.path.append(BASE_DIR)

    from modules.utils import (
        ensure_directory_exists, load_b2_json, save_b2_json,
        download_image, download_video, upload_to_b2
    )
    from modules.api_clients import get_b2_client
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.config_manager import ConfigManager
except ModuleNotFoundError as import_err:
    print(f"Критическая Ошибка: Не найдены модули проекта: {import_err}", file=sys.stderr)
    sys.exit(1)
except ImportError as import_err:
     print(f"Критическая Ошибка: Не найдена функция в модулях: {import_err}", file=sys.stderr)
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
    print(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}", file=sys.stderr)
    sys.exit(1)

# === Константы и Настройки ===
try:
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))
    if not B2_BUCKET_NAME: raise ValueError("B2_BUCKET_NAME не определен")

    CONFIG_MJ_REMOTE_PATH = "config/config_midjourney.json"

    MIDJOURNEY_ENDPOINT = config.get("API_KEYS.midjourney.endpoint")
    MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
    RUNWAY_API_KEY = os.getenv("RUNWAY_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    if OPENAI_API_KEY:
        if openai and hasattr(openai, 'api_key'): openai.api_key = OPENAI_API_KEY # Проверка наличия атрибута
        elif openai: logger.warning("Атрибут openai.api_key не найден. Проверьте версию библиотеки.")
        else: logger.warning("Модуль openai не импортирован, ключ API не установлен.")
    else: logger.warning("API-ключ OpenAI не найден в переменной окружения OPENAI_API_KEY")

    IMAGE_FORMAT = config.get("FILE_PATHS.output_image_format", "png")
    VIDEO_FORMAT = "mp4"
    MAX_ATTEMPTS = config.get("GENERATE.max_attempts", 1)
    OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4o")

    output_size_str = config.get("IMAGE_GENERATION.output_size", "1792x1024")
    if '×' in output_size_str: delimiter = '×'
    elif 'x' in output_size_str: delimiter = 'x'
    else:
        logger.error(f"Не удалось определить разделитель в IMAGE_GENERATION.output_size: '{output_size_str}'. Используем 1792x1024.")
        output_size_str = "1792x1024"; delimiter = 'x'
    try:
        width_str, height_str = output_size_str.split(delimiter)
        PLACEHOLDER_WIDTH = int(width_str.strip())
        PLACEHOLDER_HEIGHT = int(height_str.strip())
        logger.info(f"Размеры изображения/плейсхолдера установлены: {PLACEHOLDER_WIDTH}x{PLACEHOLDER_HEIGHT}")
    except ValueError as e:
        logger.error(f"Ошибка преобразования размеров '{output_size_str}' в числа: {e}. Используем 1792x1024.")
        PLACEHOLDER_WIDTH = 1792; PLACEHOLDER_HEIGHT = 1024

    PLACEHOLDER_BG_COLOR = config.get("VIDEO.placeholder_bg_color", "cccccc")
    PLACEHOLDER_TEXT_COLOR = config.get("VIDEO.placeholder_text_color", "333333")

    TASK_REQUEST_TIMEOUT = 60 # Секунд

except Exception as config_err:
     logger.error(f"Критическая ошибка при чтении настроек: {config_err}", exc_info=True)
     sys.exit(1)

# === Вспомогательные Функции ===
def select_best_image(b2_client, image_urls, prompt_text):
    """Выбирает лучшее изображение из списка URL с помощью OpenAI Vision API."""
    logger.info("Выбор лучшего изображения...")
    if not image_urls: logger.warning("Список image_urls пуст для select_best_image."); return None
    if not isinstance(image_urls, list):
        logger.warning(f"image_urls не является списком ({type(image_urls)}). Используем как есть, если строка.")
        if isinstance(image_urls, str) and image_urls.startswith('http'): return image_urls
        else: return None

    # Проверка доступности OpenAI и ключа
    openai_available = False
    if openai and hasattr(openai, 'OpenAI') and OPENAI_API_KEY:
        try:
            # Попытка инициализировать клиента, если он еще не создан глобально (маловероятно здесь)
            # Обычно клиент должен быть уже доступен
            if not hasattr(openai, '_is_legacy_openai') or not openai._is_legacy_openai: # Проверка на новую версию
                 openai_client_vision = openai.OpenAI(api_key=OPENAI_API_KEY)
                 openai_available = True
            else:
                 # Для старой версии библиотеки
                 openai.api_key = OPENAI_API_KEY
                 openai_available = True
        except Exception as init_err:
            logger.warning(f"Не удалось инициализировать OpenAI для Vision: {init_err}")
    elif openai and hasattr(openai, 'api_key') and openai.api_key: # Старая версия, ключ установлен
         openai_available = True

    if not openai_available:
        logger.warning("OpenAI недоступен или нет ключа API. Возвращаем первый URL из списка.")
        return image_urls[0] if image_urls else None

    criteria = config.get("VISUAL_ANALYSIS.image_selection_criteria", [])
    selection_prompt_template = config.get("VISUAL_ANALYSIS.image_selection_prompt")
    max_tokens = config.get("VISUAL_ANALYSIS.image_selection_max_tokens", 500)

    if not criteria or not selection_prompt_template:
         logger.warning("Критерии/промпт для выбора изображения не найдены. Возвращаем первый URL.")
         return image_urls[0] if image_urls else None

    criteria_text = ", ".join([f"{c['name']} (weight: {c['weight']})" for c in criteria])
    full_prompt = selection_prompt_template.format(prompt=(prompt_text or "Image analysis"), criteria=criteria_text)
    messages_content = [{"type": "text", "text": full_prompt}]
    valid_image_urls = []

    for i, url in enumerate(image_urls):
        if isinstance(url, str) and url.startswith(('http://', 'https://', 'data:image')):
            messages_content.append({"type": "image_url", "image_url": {"url": url}})
            valid_image_urls.append(url)
        else: logger.warning(f"Некорректный URL #{i+1} в списке: {url}. Пропуск.")

    if not valid_image_urls: logger.warning("Нет валидных URL для Vision API."); return None
    if len(messages_content) <= 1: logger.warning("Нет контента для Vision API."); return valid_image_urls[0]

    for attempt in range(MAX_ATTEMPTS):
        try:
            logger.info(f"Попытка {attempt + 1}/{MAX_ATTEMPTS} выбора лучшего изображения через OpenAI Vision...")
            # Используем новый синтаксис, если доступен
            if 'openai_client_vision' in locals() and openai_client_vision:
                 gpt_response = openai_client_vision.chat.completions.create(model=OPENAI_MODEL, messages=[{"role": "user", "content": messages_content}], max_tokens=max_tokens)
                 answer = gpt_response.choices[0].message.content
            elif hasattr(openai, "chat") and hasattr(openai.chat, "completions"): # Проверка на новый API, если клиент не был создан локально
                 gpt_response = openai.chat.completions.create(model=OPENAI_MODEL, messages=[{"role": "user", "content": messages_content}], max_tokens=max_tokens)
                 answer = gpt_response.choices[0].message.content
            else: # Старый API
                 gpt_response = openai.ChatCompletion.create(model=OPENAI_MODEL, messages=[{"role": "user", "content": messages_content}], max_tokens=max_tokens)
                 answer = gpt_response.choices[0].message.content

            logger.info(f"Ответ OpenAI Vision: {answer[:100]}...")
            matches = re.findall(r'(?<!\d)(\d+)(?!\d)', answer)
            if matches:
                for match in matches:
                    try:
                        best_index = int(match) - 1
                        if 0 <= best_index < len(valid_image_urls):
                            logger.info(f"Выбрано изображение #{best_index + 1} на основе ответа: '{answer}'")
                            return valid_image_urls[best_index]
                    except ValueError: continue
                logger.warning(f"Не удалось найти подходящий индекс (1-{len(valid_image_urls)}) в ответе: '{answer}'. Выбираем первое изображение.")
            else: logger.warning(f"Не удалось извлечь числовой индекс из ответа: '{answer}'. Выбираем первое изображение.")
            return valid_image_urls[0]
        except AttributeError as ae:
             logger.error(f"Ошибка атрибута OpenAI API (возможно, несовместимость версии библиотеки): {ae}")
             logger.warning("Проверьте версию библиотеки OpenAI (pip show openai). Используем первый URL.")
             return valid_image_urls[0]
        except Exception as e:
            error_details = ""
            if hasattr(e, 'response') and e.response:
                 try: error_details = e.response.json()
                 except: error_details = str(e.response.content)
            logger.error(f"Ошибка OpenAI API (Vision, попытка {attempt + 1}): {e}. Детали: {error_details}")
            if attempt < MAX_ATTEMPTS - 1: time.sleep(5)
            else: logger.error("Превышено количество попыток OpenAI Vision."); return valid_image_urls[0]
    return valid_image_urls[0]

def resize_existing_image(image_path):
    """Изменяет размер существующего изображения до заданных в конфиге размеров."""
    if Image is None: logger.warning("Pillow не импортирован. Пропуск ресайза."); return True
    try:
        target_width, target_height = PLACEHOLDER_WIDTH, PLACEHOLDER_HEIGHT
        logger.info(f"Изменение размера {image_path} до {target_width}x{target_height}...")
        with Image.open(image_path) as img:
            img_format = img.format or IMAGE_FORMAT.upper()
            if img.mode != 'RGB':
                logger.debug(f"Конвертация изображения из {img.mode} в RGB перед ресайзом.")
                img = img.convert('RGB')
            img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
            img.save(image_path, format=img_format)
        logger.info(f"✅ Размер изображения изменен до {target_width}x{target_height}")
        return True
    except FileNotFoundError: logger.error(f"Ошибка ресайза: Файл не найден {image_path}"); return False
    except Exception as e: logger.error(f"Ошибка изменения размера {image_path}: {e}", exc_info=True); return False

def clean_script_text(script_text_param):
    """Очищает текст скрипта, убирая переносы строк."""
    logger.info("Очистка текста скрипта (убирает переносы)...")
    if not script_text_param: return ""
    cleaned = script_text_param.replace('\n', ' ').replace('\r', ' ')
    cleaned = ' '.join(cleaned.split())
    return cleaned

def generate_runway_video(image_path: str, runway_prompt: str) -> str | None:
    """Генерирует видео с помощью Runway ML."""
    logger.info(f"Попытка генерации видео Runway для {image_path}...")
    if not RUNWAY_SDK_AVAILABLE: logger.error("❌ SDK Runway недоступен."); return None
    if not RUNWAY_API_KEY: logger.error("❌ RUNWAY_API_KEY не найден."); return None
    if not os.path.exists(image_path): logger.error(f"❌ Файл изображения {image_path} не найден."); return None
    if not runway_prompt: logger.error("❌ Промпт для Runway пуст."); return None # Добавлена проверка на пустой промпт

    try:
        model_name = config.get('API_KEYS.runwayml.model_name', 'gen-2') # Используем gen-2 как дефолт, если не указано
        duration = int(config.get('VIDEO.runway_duration', 5))
        ratio_str = config.get('VIDEO.runway_ratio', f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}") # Используем размеры из конфига
        poll_timeout = int(config.get('WORKFLOW.runway_polling_timeout', 300))
        poll_interval = int(config.get('WORKFLOW.runway_polling_interval', 15))
        logger.info(f"Параметры Runway из конфига: model='{model_name}', duration={duration}, ratio='{ratio_str}'")
    except Exception as cfg_err:
        logger.error(f"Ошибка чтения параметров Runway из конфига: {cfg_err}. Используем дефолты.")
        model_name = "gen-2"; duration = 5; ratio_str = "16:9"; poll_timeout = 300; poll_interval = 15

    try:
        with open(image_path, "rb") as image_file: base64_image = base64.b64encode(image_file.read()).decode("utf-8")
        ext = os.path.splitext(image_path)[1].lower()
        mime_type = f"image/{'jpeg' if ext == '.jpg' else ext[1:]}"
        image_data_uri = f"data:{mime_type};base64,{base64_image}"
        logger.info(f"Изображение {image_path} успешно преобразовано в Base64.")
    except Exception as e: logger.error(f"❌ Ошибка кодирования изображения для Runway: {e}", exc_info=True); return None

    client = None; task_id = 'N/A'
    try:
        logger.info(f"Инициализация клиента Runway...")
        client = RunwayML(api_key=RUNWAY_API_KEY)
        logger.info("✅ Клиент Runway инициализирован.")
        generation_params = {
            "model": model_name,
            "prompt_image": image_data_uri,
            "prompt_text": runway_prompt, # <-- ИСПОЛЬЗУЕМ runway_prompt
            "duration": duration,
            "ratio": ratio_str
        }
        logger.info(f"🚀 Создание задачи Runway...")
        logger.debug(f"Параметры: {json.dumps({k: v[:50] + '...' if isinstance(v, str) and len(v) > 50 else v for k, v in generation_params.items()}, indent=2)}")
        task = client.image_to_video.create(**generation_params)
        task_id = getattr(task, 'id', 'N/A')
        logger.info(f"✅ Задача Runway создана! ID: {task_id}")
        logger.info(f"⏳ Начало опроса статуса задачи {task_id}...")
        start_time = time.time()
        final_output_url = None
        while time.time() - start_time < poll_timeout:
            try:
                task_status = client.tasks.retrieve(task_id)
                current_status = getattr(task_status, 'status', 'UNKNOWN').upper()
                logger.info(f"Статус задачи Runway {task_id}: {current_status}")
                if current_status == "SUCCEEDED":
                    logger.info(f"✅ Задача Runway {task_id} успешно завершена!")
                    # Проверяем наличие и корректность output
                    task_output = getattr(task_status, 'output', None)
                    if isinstance(task_output, list) and len(task_output) > 0 and isinstance(task_output[0], str):
                        final_output_url = task_output[0]
                        logger.info(f"Получен URL видео: {final_output_url}")
                        return final_output_url
                    else:
                        logger.warning(f"Статус SUCCEEDED, но результат (output) не найден или некорректен: {task_output}")
                    break # Выходим из цикла в любом случае при SUCCEEDED
                elif current_status == "FAILED":
                    logger.error(f"❌ Задача Runway {task_id} завершилась с ошибкой!")
                    error_details = getattr(task_status, 'error_message', 'Нет деталей')
                    logger.error(f"Детали ошибки Runway: {error_details}")
                    break # Выходим из цикла при ошибке
                # Добавляем другие возможные статусы ожидания/обработки
                elif current_status in ["PENDING", "PROCESSING", "QUEUED", "WAITING"]:
                    time.sleep(poll_interval) # Ждем перед следующим запросом
                else:
                    logger.warning(f"Неизвестный или неожиданный статус Runway: {current_status}. Прекращаем опрос.")
                    break # Выходим при неизвестном статусе
            except Exception as poll_err:
                logger.error(f"❌ Ошибка во время опроса статуса Runway {task_id}: {poll_err}", exc_info=True)
                break # Выходим при ошибке опроса
        else: # Сработает, если цикл завершился по таймауту
            logger.warning(f"⏰ Превышен таймаут ожидания ({poll_timeout} сек) результата от Runway для задачи {task_id}.")
        return None # Возвращаем None, если видео не получено
    except RunwayError as r_err: # Ловим специфичную ошибку Runway, если она определена
        logger.error(f"❌ Ошибка Runway SDK при создании или обработке задачи {task_id}: {r_err}", exc_info=True)
        return None
    except Exception as e: # Ловим остальные ошибки
        logger.error(f"❌ Общая ошибка при создании или обработке задачи Runway {task_id}: {e}", exc_info=True)
        return None

def create_mock_video(image_path):
    """Создает mock-видео из изображения."""
    if ImageClip is None: logger.error("MoviePy не импортирован."); return None
    logger.info(f"Создание mock видео для {image_path}...")
    if not os.path.exists(image_path): logger.error(f"{image_path} не найден."); return None
    clip = None
    # Очистка имени файла для выходного пути
    base_name_path = Path(image_path)
    base_name = base_name_path.stem
    suffixes_to_remove = ["_mj_final", "_placeholder", "_best", "_temp"]
    for suffix in suffixes_to_remove:
        if base_name.endswith(suffix):
            base_name = base_name[:-len(suffix)]
            break # Удаляем только один суффикс
    output_path = str(base_name_path.parent / f"{base_name}.{VIDEO_FORMAT}")

    try:
        duration = int(config.get("VIDEO.mock_duration", 10))
        fps = int(config.get("VIDEO.mock_fps", 24))
        codec = config.get("VIDEO.mock_codec", "libx264")
        logger.debug(f"Параметры mock видео: output={output_path}, duration={duration}, fps={fps}, codec={codec}")
        clip = ImageClip(image_path, duration=duration)
        clip.fps = fps
        # Убрали ffmpeg_logfile, используем logger=None для тишины
        clip.write_videofile(output_path, codec=codec, fps=fps, audio=False, logger=None, ffmpeg_params=["-loglevel", "error"])
        logger.info(f"✅ Mock видео успешно создано: {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"❌ Ошибка при создании mock видео: {e}", exc_info=True)
        return None
    finally:
        if clip:
            try: clip.close(); logger.debug("MoviePy clip closed.")
            except Exception as close_err: logger.warning(f"Ошибка при закрытии MoviePy clip: {close_err}")

def initiate_midjourney_task(mj_prompt, ref_id=""):
    """Инициирует задачу Midjourney, используя переданный финальный промпт."""
    if not MIDJOURNEY_API_KEY: logger.error("Нет MIDJOURNEY_API_KEY."); return None
    if not MIDJOURNEY_ENDPOINT: logger.error("Нет API_KEYS.midjourney.endpoint."); return None
    if not mj_prompt: logger.error("Промпт для Midjourney пуст."); return None # Добавлена проверка

    # Параметры AR и Version теперь должны быть частью mj_prompt, сгенерированного на Шаге 6a
    # Убедимся, что промпт содержит --ar и --v
    if "--ar" not in mj_prompt: logger.warning("Промпт MJ не содержит --ar. Используется AR по умолчанию?")
    if "--v" not in mj_prompt: logger.warning("Промпт MJ не содержит --v. Используется версия по умолчанию?")

    # Убираем лишние пробелы
    cleaned_prompt = " ".join(mj_prompt.split())

    # Собираем payload (только prompt, т.к. параметры уже в нем)
    payload = {
        "model": "midjourney",
        "task_type": "imagine",
        "input": {
            "prompt": cleaned_prompt
        }
    }
    if ref_id: payload["ref"] = ref_id # Добавляем ref, если он есть

    headers = { 'X-API-Key': MIDJOURNEY_API_KEY, 'Content-Type': 'application/json' }
    request_time = datetime.now(timezone.utc)
    logger.info(f"Инициация задачи Midjourney с финальным промптом...")
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}") # Логируем payload

    try:
        logger.info(f"Отправка запроса на {MIDJOURNEY_ENDPOINT}...")
        response = requests.post(MIDJOURNEY_ENDPOINT, headers=headers, json=payload, timeout=TASK_REQUEST_TIMEOUT)
        logger.debug(f"Ответ от PiAPI MJ Init: Status={response.status_code}, Body={response.text[:200]}")
        response.raise_for_status()
        result = response.json()
        # Ищем task_id в возможных местах ответа
        task_id = result.get("result", {}).get("task_id") # Новый формат ответа?
        if not task_id: task_id = result.get("data", {}).get("task_id") # Старый формат ответа?
        if not task_id: task_id = result.get("task_id") # Еще один возможный формат?

        if task_id:
            timestamp_str = request_time.isoformat()
            logger.info(f"✅ Получен task_id от Midjourney API: {task_id} (запрошено в {timestamp_str})")
            return {"task_id": str(task_id), "requested_at_utc": timestamp_str} # Возвращаем строку task_id
        else:
            logger.error(f"❌ Ответ Midjourney API не содержит task_id: {result}")
            return None
    except requests.exceptions.Timeout: logger.error(f"❌ Таймаут ({TASK_REQUEST_TIMEOUT} сек) при запросе к API: {MIDJOURNEY_ENDPOINT}"); return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса MJ API: {e}")
        if e.response is not None: logger.error(f"    Статус ответа: {e.response.status_code}\n    Тело ответа: {e.response.text}")
        return None
    except json.JSONDecodeError as e: logger.error(f"❌ Ошибка JSON ответа MJ API: {e}. Ответ: {response.text[:500]}"); return None
    except Exception as e: logger.error(f"❌ Неизвестная ошибка инициации MJ: {e}", exc_info=True); return None


# === Основная Функция ===
def main():
    parser = argparse.ArgumentParser(description='Generate media or initiate Midjourney task.')
    parser.add_argument('--generation_id', type=str, required=True, help='The generation ID.')
    parser.add_argument('--use-mock', action='store_true', default=False, help='Force generation of a mock video.')
    args = parser.parse_args()
    generation_id = args.generation_id
    use_mock_flag = args.use_mock

    if generation_id.endswith(".json"): generation_id = generation_id.replace(".json", "")
    logger.info(f"--- Запуск generate_media для ID: {generation_id} (Use Mock: {use_mock_flag}) ---")

    b2_client = None
    content_data = None
    config_mj = None
    # --- ИЗМЕНЕНО: Добавляем переменные для новых промптов ---
    script_text = "" # Старый сценарий, возможно, больше не нужен для Runway
    first_frame_description = "" # Описание кадра для select_best_image
    final_mj_prompt = "" # Новый промпт для MJ
    final_runway_prompt = "" # Новый промпт для Runway
    # ---------------------------------------------------------

    timestamp_suffix = datetime.now().strftime("%Y%m%d%H%M%S%f")
    content_local_path = f"{generation_id}_content_temp_{timestamp_suffix}.json"
    config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"

    try:
        b2_client = get_b2_client()
        if not b2_client: raise ConnectionError("Не удалось создать клиент B2.")

        logger.info("Небольшая пауза (3 сек) перед загрузкой контента из B2...")
        time.sleep(3)

        content_remote_path = f"666/{generation_id}.json"
        logger.info(f"Загрузка файла контента: {content_remote_path}...")
        content_data = load_b2_json(b2_client, B2_BUCKET_NAME, content_remote_path, content_local_path, default_value=None)
        if content_data is None: logger.error(f"❌ Не удалось загрузить {content_remote_path}."); sys.exit(1)

        # --- ИЗМЕНЕНО: Извлекаем новые промпты и старое описание ---
        script_text = content_data.get("script", "") # Старый сценарий (на всякий случай)
        first_frame_description = content_data.get("first_frame_description", "") # Описание для выбора изображения
        final_mj_prompt = content_data.get("final_mj_prompt", "") # Новый промпт MJ
        final_runway_prompt = content_data.get("final_runway_prompt", "") # Новый промпт Runway

        # Логируем извлеченные значения
        logger.info("Данные из файла контента:")
        logger.info(f"  - Описание 1-го кадра (для select_best_image): '{first_frame_description[:100]}...'")
        logger.info(f"  - Финальный промпт MJ: '{final_mj_prompt[:100]}...'")
        logger.info(f"  - Финальный промпт Runway: '{final_runway_prompt[:100]}...'")

        # Проверка наличия ключевых промптов
        if not first_frame_description: logger.warning("Описание первого кадра (first_frame_description) отсутствует в JSON!")
        if not final_mj_prompt: logger.warning("Финальный промпт Midjourney (final_mj_prompt) отсутствует в JSON!")
        if not final_runway_prompt: logger.warning("Финальный промпт Runway (final_runway_prompt) отсутствует в JSON!")
        # -------------------------------------------------------------

        logger.info(f"Загрузка файла состояния: {CONFIG_MJ_REMOTE_PATH}...")
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, default_value=None)
        if config_mj is None: logger.warning(f"Не удалось загрузить {CONFIG_MJ_REMOTE_PATH}, используем дефолт."); config_mj = {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None}
        else: config_mj.setdefault("midjourney_task", None); config_mj.setdefault("midjourney_results", {}); config_mj.setdefault("generation", False); config_mj.setdefault("status", None)
        logger.info("✅ Необходимые данные и конфиги загружены.")

        temp_dir = f"temp_{generation_id}_{timestamp_suffix}"
        ensure_directory_exists(temp_dir)
        local_image_path = None
        video_path = None

        try:
            if use_mock_flag:
                # --- Логика mock видео (без изменений) ---
                logger.warning(f"⚠️ Принудительная генерация имитации (mock) видео для ID: {generation_id}")
                placeholder_text = f"MJ Timeout\n{first_frame_description[:60]}" if first_frame_description else "Midjourney Timeout"
                encoded_text = urllib.parse.quote(placeholder_text)
                placeholder_url = f"https://placehold.co/{PLACEHOLDER_WIDTH}x{PLACEHOLDER_HEIGHT}/{PLACEHOLDER_BG_COLOR}/{PLACEHOLDER_TEXT_COLOR}?text={encoded_text}"
                local_image_path = os.path.join(temp_dir, f"{generation_id}_placeholder.{IMAGE_FORMAT}")
                logger.info(f"Создание плейсхолдера: {placeholder_url}")
                if not download_image(placeholder_url, local_image_path): raise Exception(f"Не удалось скачать плейсхолдер {placeholder_url}")
                logger.info(f"Плейсхолдер сохранен: {local_image_path}")
                video_path = create_mock_video(local_image_path)
                if not video_path: raise Exception("Не удалось создать mock видео из плейсхолдера.")
                logger.info("Сброс статуса таймаута и очистка состояния MJ в config_mj...")
                config_mj['midjourney_task'] = None; config_mj['midjourney_results'] = {}; config_mj['generation'] = False; config_mj['status'] = None
                # --- Конец логики mock видео ---
            else:
                # --- Основная логика генерации ---
                mj_results_data = config_mj.get("midjourney_results", {})
                image_urls_from_results = None
                # (Логика извлечения URL из результатов MJ без изменений)
                if isinstance(mj_results_data.get("task_result"), dict):
                     task_result = mj_results_data["task_result"]
                     if isinstance(task_result.get("temporary_image_urls"), list) and task_result["temporary_image_urls"]: image_urls_from_results = task_result["temporary_image_urls"]
                     elif isinstance(task_result.get("image_urls"), list) and task_result["image_urls"]: image_urls_from_results = task_result["image_urls"]
                     elif task_result.get("image_url"): image_urls_from_results = [task_result.get("image_url")]

                if image_urls_from_results:
                    # --- Обработка готовых результатов MJ ---
                    logger.info(f"Обнаружены результаты Midjourney. Запуск генерации медиа...")
                    image_urls = image_urls_from_results
                    logger.info(f"Используем {len(image_urls)} URL изображений из 'task_result'.")
                    if not first_frame_description: logger.warning("Нет описания кадра для выбора лучшего изображения!")
                    # Используем first_frame_description для выбора
                    best_image_url = select_best_image(b2_client, image_urls, first_frame_description or " ")
                    if not best_image_url: raise ValueError("Не удалось выбрать лучшее изображение.")
                    logger.info(f"Выбрано лучшее изображение: {best_image_url}")
                    local_image_path = os.path.join(temp_dir, f"{generation_id}_best.{IMAGE_FORMAT}")
                    if not download_image(best_image_url, local_image_path): raise Exception(f"Не удалось скачать {best_image_url}")
                    logger.info(f"Изображение сохранено: {local_image_path}")
                    resize_existing_image(local_image_path)

                    # --- ИЗМЕНЕНО: Используем final_runway_prompt для Runway ---
                    if not final_runway_prompt:
                         logger.error("❌ Финальный промпт Runway отсутствует! Невозможно запустить генерацию видео.")
                         video_path = create_mock_video(local_image_path) # Создаем mock как fallback
                    else:
                         video_url_or_path = generate_runway_video(local_image_path, final_runway_prompt)
                         # (Остальная логика обработки Runway и fallback на mock без изменений)
                         if video_url_or_path:
                             if video_url_or_path.startswith("http"):
                                 video_path_temp = os.path.join(temp_dir, f"{generation_id}_downloaded.{VIDEO_FORMAT}")
                                 if download_video(video_url_or_path, video_path_temp): video_path = video_path_temp
                                 else: logger.error(f"Не удалось скачать видео с {video_url_or_path}. Используем mock."); video_path = create_mock_video(local_image_path)
                             else: video_path = video_url_or_path # Если функция вернула локальный путь
                         else: logger.error("Генерация Runway не удалась. Используем mock."); video_path = create_mock_video(local_image_path)
                    # ---------------------------------------------------------

                    if not video_path: raise Exception("Не удалось сгенерировать ни Runway, ни Mock видео.")
                    logger.info("Очистка результатов MJ и флага генерации в config_mj...")
                    config_mj['midjourney_results'] = {}; config_mj['generation'] = False; config_mj['midjourney_task'] = None; config_mj['status'] = None
                    # --- Конец обработки готовых результатов MJ ---
                else:
                    # --- Инициация новой задачи MJ ---
                    logger.info(f"Результаты Midjourney не найдены. Запуск новой задачи Midjourney...")
                    # --- ИЗМЕНЕНО: Используем final_mj_prompt ---
                    if not final_mj_prompt:
                        logger.error("❌ Финальный промпт Midjourney отсутствует! Невозможно запустить задачу.")
                        config_mj['generation'] = False; config_mj['midjourney_task'] = None; config_mj['status'] = None
                    else:
                        task_result = initiate_midjourney_task(final_mj_prompt, generation_id)
                        # (Остальная логика обработки task_result без изменений)
                        if task_result and isinstance(task_result, dict) and task_result.get("task_id"):
                            logger.info(f"Словарь config_mj будет обновлен: task={task_result}, generation=False.")
                            config_mj['midjourney_task'] = task_result; config_mj['generation'] = False; config_mj['midjourney_results'] = {}; config_mj['status'] = None
                        else:
                            logger.warning("Не удалось получить task_id от Midjourney.")
                            config_mj['midjourney_task'] = None; config_mj['generation'] = False; config_mj['midjourney_results'] = {}; config_mj['status'] = None
                    # -----------------------------------------
                    # --- Конец инициации новой задачи MJ ---

            # --- Загрузка файлов в B2 (без изменений) ---
            target_folder_b2 = "666/"
            upload_success_img = False
            upload_success_vid = False
            if local_image_path and os.path.exists(local_image_path):
                 upload_success_img = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, local_image_path, generation_id)
            else: logger.warning(f"Финальное изображение {local_image_path} не найдено для загрузки.")
            if video_path and os.path.exists(video_path):
                upload_success_vid = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, video_path, generation_id)
            elif video_path: logger.error(f"Финальное видео {video_path} не найдено для загрузки!")
            else: logger.warning("Финальное видео не было сгенерировано.")
            if not upload_success_img or not upload_success_vid: logger.warning("Не все финальные медиа файлы были успешно загружены в B2.")
            # --- Конец загрузки файлов ---

        finally:
             # --- Очистка временной папки (без изменений) ---
             if os.path.exists(temp_dir):
                try: shutil.rmtree(temp_dir); logger.debug(f"Удалена временная папка: {temp_dir}")
                except OSError as e: logger.warning(f"Не удалось удалить временную папку {temp_dir}: {e}")

        # --- Сохранение config_mj (без изменений) ---
        logger.info(f"Сохранение итогового состояния config_midjourney.json в B2...")
        if not isinstance(config_mj, dict): logger.error("Переменная config_mj не словарь!")
        elif not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, config_mj): logger.error("Не удалось сохранить config_midjourney.json в B2.")

        logger.info("✅ Работа generate_media.py успешно завершена.")

    except ConnectionError as conn_err: logger.error(f"❌ Ошибка соединения B2: {conn_err}"); sys.exit(1)
    except Exception as e: logger.error(f"❌ Критическая ошибка в generate_media.py: {e}", exc_info=True); sys.exit(1)
    finally:
        # --- Очистка временных файлов (без изменений) ---
        if os.path.exists(content_local_path):
            try: os.remove(content_local_path); logger.debug(f"Удален временный файл контента: {content_local_path}")
            except OSError as e: logger.warning(f"Не удалось удалить {content_local_path}: {e}")
        if os.path.exists(config_mj_local_path):
            try: os.remove(config_mj_local_path); logger.debug(f"Удален временный файл конфига MJ: {config_mj_local_path}")
            except OSError as e: logger.warning(f"Не удалось удалить {config_mj_local_path}: {e}")

# === Точка входа ===
if __name__ == "__main__":
    try: main()
    except KeyboardInterrupt: logger.info("🛑 Программа остановлена пользователем.")
    except SystemExit as e: logger.info(f"Завершение работы generate_media.py с кодом {e.code}"); sys.exit(e.code)
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА generate_media.py: {e}")
        try: logger.error(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА generate_media.py: {e}", exc_info=True)
        except NameError: pass # Логгер может быть недоступен
        sys.exit(1)
