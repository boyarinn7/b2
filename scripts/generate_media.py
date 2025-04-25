#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Отладочный вывод для проверки старта скрипта в GitHub Actions
print("--- SCRIPT START (generate_media.py) ---", flush=True)

# В файле scripts/generate_media.py

# --- Убедитесь, что все необходимые импорты присутствуют в начале файла ---
import os, json, sys, time, argparse, requests, shutil, base64, re, urllib.parse, logging, httpx
from datetime import datetime, timezone
from pathlib import Path
# --- Импорт кастомных модулей ---
# Попытка абсолютного импорта
try:
    from modules.config_manager import ConfigManager
    from modules.logger import get_logger
    from modules.utils import (
        ensure_directory_exists, load_b2_json, save_b2_json,
        download_image, download_video, upload_to_b2, load_json_config,
        add_text_to_image # <--- Функция для текста ИЗ utils.py
        # Функции resize_existing_image и create_mock_video НЕ импортируются отсюда
    )
    from modules.api_clients import get_b2_client
    # from modules.error_handler import handle_error # Если используется
except ModuleNotFoundError:
    # Попытка относительного импорта, если запускается из папки scripts
    # или если абсолютный не сработал
    try:
        # Добавляем родительскую директорию в sys.path
        _BASE_DIR_FOR_IMPORT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if _BASE_DIR_FOR_IMPORT not in sys.path:
            sys.path.insert(0, _BASE_DIR_FOR_IMPORT) # Добавляем в начало для приоритета

        from modules.config_manager import ConfigManager
        from modules.logger import get_logger
        from modules.utils import (
            ensure_directory_exists, load_b2_json, save_b2_json,
            download_image, download_video, upload_to_b2, load_json_config,
            add_text_to_image
        )
        from modules.api_clients import get_b2_client
        # from modules.error_handler import handle_error # Если используется
        del _BASE_DIR_FOR_IMPORT # Очищаем временную переменную
    except ModuleNotFoundError as import_err:
        print(f"Критическая Ошибка: Не найдены модули проекта: {import_err}", file=sys.stderr)
        sys.exit(1)
    except ImportError as import_err_rel:
        print(f"Критическая Ошибка импорта (относительный): {import_err_rel}", file=sys.stderr)
        sys.exit(1)
# --------------------------------------------
# --- Импорт сторонних библиотек ---
try:
    from runwayml import RunwayML
    RUNWAY_SDK_AVAILABLE = True
    # Пытаемся импортировать специфичное исключение RunwayError
    try:
        from runwayml.exceptions import RunwayError
    except ImportError:
        # Используем базовый класс ошибок Runway, если он доступен, иначе requests.HTTPError
        try:
            from runwayml.exceptions import RunwayError as BaseRunwayError
            RunwayError = BaseRunwayError
        except ImportError:
             RunwayError = requests.HTTPError # Fallback на HTTPError
except ImportError:
    RUNWAY_SDK_AVAILABLE = False; RunwayML = None; RunwayError = requests.HTTPError
try:
    from PIL import Image, ImageFilter, ImageFont, ImageDraw
except ImportError:
    Image = None; ImageFilter = None; ImageFont = None; ImageDraw = None
try:
    from moviepy.editor import ImageClip
except ImportError:
    ImageClip = None
try:
    import openai
except ImportError:
    openai = None
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    # Логирование ошибки будет позже, когда logger будет инициализирован
    pass
# ---------------------------------------------------------------------------

# === Инициализация конфигурации и логгера ===
# Этот блок ДОЛЖЕН идти СРАЗУ ПОСЛЕ импортов и ПЕРЕД использованием config или logger
try:
    config = ConfigManager()
    # Теперь, когда config создан, можно инициализировать логгер
    logger = get_logger("generate_media") # Используем имя скрипта для логгера
    logger.info("ConfigManager и Logger для generate_media инициализированы.")
except Exception as init_err:
    # Если что-то пойдет не так на этом раннем этапе,
    # используем стандартный logging для вывода критической ошибки.
    # Кастомный логгер может быть еще недоступен.
    import logging
    logging.critical(f"Критическая ошибка инициализации ConfigManager или Logger в generate_media: {init_err}", exc_info=True)
    # Выход из скрипта, так как без конфига или логгера работа невозможна
    import sys
    sys.exit(1)
# === Конец блока инициализации ===

# --- Определение BASE_DIR ---
# Этот блок идет ПОСЛЕ инициализации config и logger
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
     BASE_DIR = Path.cwd()
     # Используем logger, т.к. он уже должен быть инициализирован
     logger.warning(f"Переменная __file__ не определена, BASE_DIR установлен как {BASE_DIR}")
# -----------------------------

# --- Глобальные константы из конфига ---
# Этот блок идет ПОСЛЕ инициализации config и logger и определения BASE_DIR
try:
    # Используем УЖЕ созданный объект config
    CONFIG_MJ_REMOTE_PATH = config.get('FILE_PATHS.config_midjourney', "config/config_midjourney.json")
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))
    IMAGE_FORMAT = config.get("FILE_PATHS.output_image_format", "png")
    VIDEO_FORMAT = "mp4"

    # Получаем и парсим размер изображения
    output_size_str = config.get("IMAGE_GENERATION.output_size", "1792x1024")
    delimiter = next((d for d in ['x', '×', ':'] if d in output_size_str), 'x')
    try:
        width_str, height_str = output_size_str.split(delimiter)
        PLACEHOLDER_WIDTH = int(width_str.strip())
        PLACEHOLDER_HEIGHT = int(height_str.strip())
    except ValueError:
        # Используем logger для записи ошибки
        logger.error(f"Ошибка парсинга размеров '{output_size_str}'. Используем 1792x1024.")
        PLACEHOLDER_WIDTH, PLACEHOLDER_HEIGHT = 1792, 1024

    PLACEHOLDER_BG_COLOR = config.get("VIDEO.placeholder_bg_color", "cccccc")
    PLACEHOLDER_TEXT_COLOR = config.get("VIDEO.placeholder_text_color", "333333")
    MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
    RUNWAY_API_KEY = os.getenv("RUNWAY_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    MJ_IMAGINE_ENDPOINT = config.get("API_KEYS.midjourney.endpoint")
    MJ_FETCH_ENDPOINT = config.get("API_KEYS.midjourney.task_endpoint")

    # Добавляем константы, которые были в удаленном блоке, если они нужны
    MAX_ATTEMPTS = int(config.get("GENERATE.max_attempts", 1))
    OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4o")
    TASK_REQUEST_TIMEOUT = int(config.get("WORKFLOW.task_request_timeout", 60))

    # Проверка наличия ключей API
    if not B2_BUCKET_NAME: logger.warning("B2_BUCKET_NAME не определен.")
    if not MIDJOURNEY_API_KEY: logger.warning("MIDJOURNEY_API_KEY не найден в переменных окружения.")
    if not RUNWAY_API_KEY: logger.warning("RUNWAY_API_KEY не найден в переменных окружения.")
    if not OPENAI_API_KEY: logger.warning("OPENAI_API_KEY не найден в переменных окружения.")
    if not MJ_IMAGINE_ENDPOINT: logger.warning("API_KEYS.midjourney.endpoint не найден в конфиге.")
    if not MJ_FETCH_ENDPOINT: logger.warning("API_KEYS.midjourney.task_endpoint не найден в конфиге.")

except Exception as _cfg_err:
    # Используем logger для записи ошибки
    logger.critical(f"Критическая ошибка при загрузке констант из конфига: {_cfg_err}", exc_info=True)
    sys.exit(1)
# ------------------------------------

# --- Проверка доступности сторонних библиотек (после инициализации logger) ---
if not RUNWAY_SDK_AVAILABLE: logger.warning("RunwayML SDK недоступен.")
if Image is None: logger.warning("Библиотека Pillow (PIL) недоступна.")
if ImageClip is None: logger.warning("Библиотека MoviePy недоступна.")
if openai is None: logger.warning("Библиотека OpenAI недоступна.")
# ---------------------------------------------------------------------------

# === Глобальная переменная для клиента OpenAI ===
openai_client_instance = None

# === Вспомогательные Функции ===

def _initialize_openai_client():
    """Инициализирует глобальный клиент OpenAI, если он еще не создан."""
    global openai_client_instance
    if openai_client_instance:
        return True # Уже инициализирован

    # Проверяем, доступен ли модуль openai вообще
    if openai is None:
        logger.error("❌ Модуль openai не был импортирован.")
        return False

    # Используем OPENAI_API_KEY из констант
    if not OPENAI_API_KEY:
        logger.error("❌ Переменная окружения OPENAI_API_KEY не задана для generate_media!")
        return False # Не можем инициализировать

    try:
        if hasattr(openai, 'OpenAI'):
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
            openai_client_instance = openai.OpenAI(api_key=OPENAI_API_KEY, http_client=http_client)
            logger.info("✅ Клиент OpenAI (>1.0) инициализирован (generate_media).")
            return True
        else:
            logger.error("❌ Класс openai.OpenAI не найден в generate_media.")
            return False
    except Exception as init_err:
        logger.error(f"❌ Ошибка инициализации клиента OpenAI (generate_media): {init_err}", exc_info=True)
        if "got an unexpected keyword argument 'proxies'" in str(init_err):
             logger.error("!!! Повторная ошибка 'unexpected keyword argument proxies' в generate_media.")
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
        return None # Не можем выбрать индекс из некорректного списка

    # --- Инициализация клиента OpenAI при необходимости ---
    if not openai_client_instance:
        if not _initialize_openai_client():
            logger.error("Клиент OpenAI недоступен для выбора изображения.")
            return None # Не можем выбрать без OpenAI
    # --- Конец инициализации ---

    # Получаем критерии из creative_config
    creative_config_path_str = config.get('FILE_PATHS.creative_config')
    creative_config_data = {}
    if creative_config_path_str:
         # Убедимся, что путь абсолютный
         creative_config_path = Path(creative_config_path_str)
         if not creative_config_path.is_absolute():
             # BASE_DIR уже должен быть определен
             creative_config_path = BASE_DIR / creative_config_path
         creative_config_data = load_json_config(str(creative_config_path)) or {}
    criteria = creative_config_data.get("visual_analysis_criteria", [])

    # Получаем шаблон промпта и max_tokens из настроек
    selection_prompt_template = prompt_settings.get("template_index") # Ищем ключ template_index
    if not selection_prompt_template:
        logger.warning("Шаблон 'template_index' не найден в prompts_config.json -> visual_analysis -> image_selection. Используем fallback.")
        selection_prompt_template = """
Analyze the following 4 images based on the original prompt and the criteria provided.
Respond ONLY with the number (1, 2, 3, or 4) of the image that best fits the criteria and prompt. Do not add any other text.

Original Prompt Context: {prompt}
Evaluation Criteria: {criteria}
"""
    # Используем max_tokens из конфига, если есть, иначе дефолт
    max_tokens = int(prompt_settings.get("max_tokens", 500)) # Увеличил дефолт для анализа

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
            gpt_response = openai_client_instance.chat.completions.create( # Используем глобальный клиент
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": messages_content}],
                max_tokens=max_tokens,
                temperature=0.2 # Низкая температура для более детерминированного ответа
            )
            if gpt_response.choices and gpt_response.choices[0].message:
                answer = gpt_response.choices[0].message.content.strip()
                if not answer:
                    logger.warning(f"OpenAI Vision вернул пустой ответ на попытке {attempt + 1}.")
                    continue

                logger.info(f"Ответ OpenAI Vision (ожидается индекс): '{answer}'")
                # Ищем первую цифру от 1 до 4 в ответе
                match = re.search(r'\b([1-4])\b', answer)
                if match:
                    try:
                        best_index_one_based = int(match.group(1))
                        best_index_zero_based = best_index_one_based - 1
                        logger.info(f"Выбран индекс изображения: {best_index_zero_based} (ответ: {best_index_one_based})")
                        return best_index_zero_based # Возвращаем индекс 0-3
                    except ValueError:
                        logger.warning(f"Не удалось преобразовать найденную цифру '{match.group(1)}' в индекс.")
                        continue # Попробуем еще раз, если есть попытки
                else:
                    logger.warning(f"Не удалось найти индекс (1-4) в ответе: '{answer}'.")
                    continue # Попробуем еще раз

            else:
                 logger.warning(f"OpenAI Vision вернул некорректный ответ на попытке {attempt + 1}: {gpt_response}")
                 continue

        # Обработка ошибок OpenAI (остается без изменений)
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
                return None # Не удалось выбрать
    logger.error("Не удалось получить ответ от OpenAI Vision для выбора индекса после всех попыток.")
    return None # Не удалось выбрать


def resize_existing_image(image_path_str: str) -> bool:
    """Изменяет размер существующего изображения."""
    if Image is None: logger.warning("Pillow не импортирован."); return True # Не ошибка, просто пропускаем
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

    if RunwayML is None:
        logger.error("❌ Класс RunwayML не доступен (ошибка импорта в начале файла?).")
        return None

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
        # Используем размеры из констант для ratio
        ratio_str = f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}"
        logger.info(f"Используется ratio: {ratio_str}")
        poll_timeout = int(config.get('WORKFLOW.runway_polling_timeout', 300))
        poll_interval = int(config.get('WORKFLOW.runway_polling_interval', 15))
        logger.info(f"Параметры Runway: model='{model_name}', duration={duration}, ratio='{ratio_str}'")
    except Exception as cfg_err:
        logger.error(f"Ошибка чтения параметров Runway из конфига: {cfg_err}. Используются значения по умолчанию.")
        model_name="gen-2"; duration=5; ratio_str=f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}"; poll_timeout=300; poll_interval=15

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

                elif current_status in ["PENDING", "PROCESSING", "QUEUED", "WAITING", "RUNNING"]:
                    # Статус промежуточный, продолжаем опрос
                    time.sleep(poll_interval)
                else:
                    logger.warning(f"Неизвестный или неожиданный статус Runway: {current_status}. Прерывание опроса.")
                    break # Выходим при неизвестном статусе

            except requests.HTTPError as http_err: # Ловим HTTP ошибки от requests (которые использует SDK)
                 logger.error(f"❌ Ошибка HTTP при опросе задачи Runway {task_id}: {http_err.response.status_code} - {http_err.response.text}", exc_info=False) # Не выводим полный traceback для HTTP ошибок
                 break
            except Exception as poll_err: # Ловим остальные ошибки (включая возможные ошибки SDK, если RunwayError не определен)
                if RunwayError and isinstance(poll_err, RunwayError):
                     logger.error(f"❌ Ошибка SDK Runway при опросе задачи {task_id}: {poll_err}", exc_info=True)
                else:
                     logger.error(f"❌ Общая ошибка при опросе статуса Runway {task_id}: {poll_err}", exc_info=True)
                break # Прерываем опрос при других ошибках
        else:
            # Цикл завершился по таймауту
            logger.warning(f"⏰ Таймаут ({poll_timeout} сек) ожидания завершения задачи Runway {task_id}.")

        return None # Возвращаем None, если видео не было получено

    except requests.HTTPError as http_err: # Ловим HTTP ошибки от requests при создании задачи
        logger.error(f"❌ Ошибка HTTP при создании задачи Runway: {http_err.response.status_code} - {http_err.response.text}", exc_info=False)
        return None
    except Exception as e: # Ловим остальные ошибки (включая возможные ошибки SDK)
         if RunwayError and isinstance(e, RunwayError):
              logger.error(f"❌ Ошибка SDK Runway при создании задачи: {e}", exc_info=True)
         else:
              logger.error(f"❌ Общая ошибка при взаимодействии с Runway: {e}", exc_info=True)
         return None


def create_mock_video(image_path_str: str) -> str | None:
    """Создает mock-видео из изображения."""
    if ImageClip is None: logger.error("MoviePy не импортирован."); return None
    logger.info(f"Создание mock видео для {image_path_str}...")
    image_path_obj = Path(image_path_str)
    if not image_path_obj.is_file(): logger.error(f"{image_path_obj} не найден или не файл."); return None

    clip = None; base_name = image_path_obj.stem
    suffixes_to_remove = ["_mj_final", "_placeholder", "_best", "_temp", "_upscaled"] # Добавлено _upscaled
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

def trigger_piapi_action(original_task_id: str, action: str, api_key: str, endpoint: str) -> dict | None:
    """Запускает действие (например, upscale) для задачи Midjourney через PiAPI."""
    if not api_key or not endpoint or not original_task_id or not action:
        logger.error("Недостаточно данных для запуска действия PiAPI (trigger_piapi_action).")
        return None

    task_type = None; index_str = None
    if action.startswith("upscale"): task_type = "upscale"
    elif action.startswith("variation"): task_type = "variation"
    else: logger.error(f"Неизвестный тип действия в '{action}'."); return None

    index_match = re.search(r'\d+$', action)
    if not index_match: logger.error(f"Не удалось извлечь индекс из '{action}'."); return None
    index_str = index_match.group(0)

    payload = { "model": "midjourney", "task_type": task_type, "input": { "origin_task_id": original_task_id, "index": index_str } }
    headers = {'X-API-Key': api_key, 'Content-Type': 'application/json'}
    request_time = datetime.now(timezone.utc)
    logger.info(f"Отправка запроса на действие '{action}' для {original_task_id} на {endpoint}...")
    logger.debug(f"Payload действия PiAPI: {json.dumps(payload, indent=2)}")
    response = None
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=TASK_REQUEST_TIMEOUT)
        logger.debug(f"Ответ от PiAPI Action Trigger: Status={response.status_code}, Body={response.text[:500]}")
        response.raise_for_status(); result = response.json()
        new_task_id = result.get("result", {}).get("task_id") or result.get("data", {}).get("task_id") or result.get("task_id")
        if new_task_id:
            timestamp_str = request_time.isoformat()
            logger.info(f"✅ Получен НОВЫЙ task_id для '{action}': {new_task_id} (запрошено в {timestamp_str})")
            return {"task_id": str(new_task_id), "requested_at_utc": timestamp_str}
        else: logger.warning(f"Ответ API на '{action}' не содержит нового task_id. Ответ: {result}"); return None
    except requests.exceptions.Timeout: logger.error(f"❌ Таймаут при запросе '{action}' к PiAPI: {endpoint}"); return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса '{action}' к PiAPI: {e}")
        if e.response is not None: logger.error(f"    Статус: {e.response.status_code}, Тело: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        response_text = response.text[:500] if response else "Ответ не получен"
        logger.error(f"❌ Ошибка JSON ответа '{action}' от PiAPI: {e}. Ответ: {response_text}"); return None
    except Exception as e: logger.exception(f"❌ Неизвестная ошибка при запуске '{action}' PiAPI: {e}"); return None


# === Основная Функция ===
def main():
    """
    Основная функция скрипта generate_media.py.
    Обрабатывает разные состояния задачи, генерирует заголовок с текстом,
    запускает апскейл и генерацию видео.
    """
    # --- Инициализация переменных перед try блоком ---
    # config и logger уже должны быть инициализированы ГЛОБАЛЬНО выше
    b2_client = None
    # openai_client_instance инициализируется в _initialize_openai_client()
    # --- ИСПРАВЛЕНИЕ: Инициализация переменных, используемых в finally ---
    generation_id = None # Будет переопределен из args
    timestamp_suffix = None
    config_mj_local_path = None
    temp_dir_path = None
    # -------------------------------------------------------------------

    # --- Инициализация B2 клиента и проверка глобальных config/logger ---
    try:
        # Проверка, что глобальные config и logger доступны
        if 'config' not in globals() or config is None:
             raise RuntimeError("Глобальный объект 'config' не инициализирован.")
        if 'logger' not in globals() or logger is None:
             raise RuntimeError("Глобальный объект 'logger' не инициализирован.")

        b2_client = get_b2_client()
        if not b2_client: raise ConnectionError("Не удалось создать клиент B2.")

        # --- Инициализация клиента OpenAI (вынесена в функцию _initialize_openai_client) ---
        # Вызов _initialize_openai_client() будет происходить по мере необходимости,
        # например, перед вызовом select_best_image.
        # -------------------------------------------------
    except (RuntimeError, ConnectionError) as init_err:
        # Используем стандартный logging, так как кастомный мог не создаться
        logging.critical(f"Критическая ошибка инициализации в main(): {init_err}", exc_info=True)
        sys.exit(1)
    # -----------------------------------------

    parser = argparse.ArgumentParser(description='Generate media or initiate Midjourney task.')
    parser.add_argument('--generation_id', type=str, required=True, help='The generation ID.')
    parser.add_argument('--use-mock', action='store_true', default=False, help='Force generation of a mock video.')
    args = parser.parse_args()
    # --- ИСПРАВЛЕНИЕ: Присваиваем generation_id здесь ---
    generation_id = args.generation_id
    # ----------------------------------------------------
    use_mock_flag = args.use_mock

    if isinstance(generation_id, str) and generation_id.endswith(".json"):
        generation_id = generation_id[:-5]
    logger.info(f"--- Запуск generate_media для ID: {generation_id} (Use Mock: {use_mock_flag}) ---")

    # --- Переменные для путей и состояния ---
    content_data = None
    config_mj = None
    local_image_path = None # Путь к финальному PNG для загрузки
    video_path = None # Путь к финальному MP4 для загрузки

    # --- ИСПРАВЛЕНИЕ: Определяем timestamp_suffix и пути здесь, чтобы они были доступны в finally ---
    timestamp_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    temp_dir_path = Path(f"temp_{generation_id}_{timestamp_suffix}")
    config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"
    ensure_directory_exists(config_mj_local_path) # Убедимся, что папка для temp файла есть
    # ----------------------------------------------------------------------------------------

    try:
        # --- Загрузка content_data ---
        logger.info("Загрузка данных контента...")
        content_remote_path = f"666/{generation_id}.json"
        content_local_temp_path = f"{generation_id}_content_temp_{timestamp_suffix}.json"
        ensure_directory_exists(content_local_temp_path)
        content_data = load_b2_json(b2_client, B2_BUCKET_NAME, content_remote_path, content_local_temp_path, default_value=None)
        if content_data is None:
            logger.error(f"❌ Не удалось загрузить {content_remote_path}.");
            sys.exit(1)
        else:
            logger.info("✅ Данные контента загружены.")
            # Удаляем временный файл контента СРАЗУ после успешной загрузки
            if Path(content_local_temp_path).exists():
                try: os.remove(content_local_temp_path); logger.debug(f"Удален temp контент: {content_local_temp_path}")
                except OSError as e: logger.warning(f"Не удалить {content_local_temp_path}: {e}")
        # -----------------------------

        # --- Извлечение полей из content_data ---
        topic = content_data.get("topic", "Нет темы")
        selected_focus = content_data.get("selected_focus")
        first_frame_description = content_data.get("first_frame_description", "")
        final_mj_prompt = content_data.get("final_mj_prompt", "")
        final_runway_prompt = content_data.get("final_runway_prompt", "")
        logger.info(f"Тема: '{topic[:100]}...'")
        logger.info(f"Выбранный фокус: {selected_focus}")
        # ------------------------------------

        # --- Загрузка config_mj ---
        logger.info(f"Загрузка состояния: {CONFIG_MJ_REMOTE_PATH}...")
        # Используем config_mj_local_path, определенный ранее
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, default_value=None)
        if config_mj is None:
            logger.warning(f"Не загрузить {CONFIG_MJ_REMOTE_PATH}. Создание структуры по умолчанию.");
            config_mj = {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None}
        else:
            # Гарантируем наличие ключей
            config_mj.setdefault("midjourney_task", None)
            config_mj.setdefault("midjourney_results", {})
            config_mj.setdefault("generation", False)
            config_mj.setdefault("status", None)
        logger.info("✅ Конфиг MJ загружен.")
        # --------------------------

        # --- Определение типа результата MJ ---
        mj_results = config_mj.get("midjourney_results", {})
        task_result_data = mj_results.get("task_result")
        task_meta_data = mj_results.get("meta")
        is_imagine_result = False
        is_upscale_result = False
        imagine_urls = []
        final_upscaled_image_url = None

        if isinstance(task_result_data, dict):
            # Проверка на результат /imagine
            if isinstance(task_result_data.get("temporary_image_urls"), list) and \
               len(task_result_data["temporary_image_urls"]) == 4 and \
               isinstance(task_result_data.get("actions"), list):
                is_imagine_result = True
                imagine_urls = task_result_data["temporary_image_urls"]
                logger.info("Обнаружен результат задачи /imagine (сетка 2x2).")
            # Проверка на результат /upscale
            elif isinstance(task_result_data.get("image_url"), str) and \
                 task_result_data["image_url"].startswith("http") and \
                 isinstance(task_meta_data, dict) and \
                 task_meta_data.get("task_type") == "upscale":
                 is_upscale_result = True
                 final_upscaled_image_url = task_result_data.get("image_url")
                 logger.info(f"Обнаружен результат задачи /upscale: {final_upscaled_image_url[:60]}...")
            # Если есть результаты, но не опознаны
            else:
                 if mj_results and not is_imagine_result and not is_upscale_result:
                      logger.warning(f"Не удалось определить тип результата MJ. task_result: {json.dumps(task_result_data, indent=2)[:500]}... meta: {task_meta_data}")
        elif mj_results: # Если midjourney_results есть, но task_result не словарь
             logger.warning(f"Поле 'task_result' в midjourney_results не является словарем: {mj_results}")
        # ------------------------------------

        # --- Основной блок обработки сценариев ---
        # --- ИСПРАВЛЕНИЕ: Вложенный try...finally для очистки temp_dir_path ---
        try:
            # --- Создание временной директории ---
            # temp_dir_path уже определен выше
            ensure_directory_exists(str(temp_dir_path))
            logger.info(f"Создана временная папка: {temp_dir_path}")
            # ------------------------------------

            if use_mock_flag:
                # --- Сценарий 0: Принудительный Mock ---
                logger.warning(f"⚠️ Принудительный mock для ID: {generation_id}")
                placeholder_text = f"MJ Timeout\n{topic[:60]}"
                encoded_text = urllib.parse.quote(placeholder_text)
                placeholder_url = f"https://placehold.co/{PLACEHOLDER_WIDTH}x{PLACEHOLDER_HEIGHT}/{PLACEHOLDER_BG_COLOR}/{PLACEHOLDER_TEXT_COLOR}?text={encoded_text}"
                local_image_path = temp_dir_path / f"{generation_id}.{IMAGE_FORMAT}"
                logger.info(f"Создание плейсхолдера: {placeholder_url}")
                if not download_image(placeholder_url, str(local_image_path)):
                    logger.error("Не удалось скачать плейсхолдер.")
                    local_image_path = None # Устанавливаем в None, если скачивание не удалось
                else:
                     logger.info(f"Плейсхолдер сохранен как финальный PNG: {local_image_path}")

                video_path_str = None
                # Проверяем наличие ImageClip и функции create_mock_video
                if ImageClip and callable(create_mock_video) and local_image_path and local_image_path.is_file():
                     video_path_str = create_mock_video(str(local_image_path)) # Вызываем локальную функцию
                     if not video_path_str: logger.warning("Не удалось создать mock видео.")
                     else: video_path = Path(video_path_str)
                elif not ImageClip:
                     logger.warning("MoviePy не найден, mock видео не создано.")
                elif not local_image_path or not local_image_path.is_file():
                     logger.warning("Базовое изображение для mock не найдено, mock видео не создано.")
                else: # Если create_mock_video не найдена
                     # --- ИСПРАВЛЕНИЕ: Используем callable() для проверки ---
                     if not callable(create_mock_video):
                         logger.error("Функция create_mock_video не найдена!")
                     else: # Другая причина, почему не создалось видео
                         logger.error("Неизвестная ошибка при вызове create_mock_video.")


                logger.info("Сброс состояния MJ...");
                config_mj['midjourney_task'] = None; config_mj['midjourney_results'] = {}
                config_mj['generation'] = False; config_mj['status'] = None
                # --- Конец сценария Mock ---

            elif is_upscale_result and final_upscaled_image_url:
                # --- Сценарий 3: Есть результат апскейла -> Генерируем Runway ---
                logger.info(f"Обработка результата /upscale для ID {generation_id}. Генерация видео Runway...")
                runway_base_image_path = temp_dir_path / f"{generation_id}_upscaled_for_runway.{IMAGE_FORMAT}"
                if not download_image(final_upscaled_image_url, str(runway_base_image_path)):
                    raise Exception(f"Не скачать апскейл {final_upscaled_image_url}")
                logger.info(f"Апскейл для Runway сохранен: {runway_base_image_path}")

                # Проверяем наличие Image и функции resize_existing_image
                if Image and callable(resize_existing_image): # Вызываем локальную функцию
                    if not resize_existing_image(str(runway_base_image_path)):
                        logger.warning(f"Не удалось выполнить ресайз для {runway_base_image_path}, но продолжаем.")
                elif not Image:
                     logger.warning("Pillow не найден, ресайз не выполнен.")
                else:
                     # --- ИСПРАВЛЕНИЕ: Используем callable() для проверки ---
                     if not callable(resize_existing_image):
                         logger.error("Функция resize_existing_image не найдена!")
                     else:
                         logger.error("Неизвестная ошибка при вызове resize_existing_image.")

                video_path_str = None
                if not final_runway_prompt:
                    logger.error("❌ Промпт Runway отсутствует! Создание mock видео.")
                    # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                    if ImageClip and callable(create_mock_video): video_path_str = create_mock_video(str(runway_base_image_path))
                    else: logger.warning("MoviePy или create_mock_video не найдены, mock видео не создано.")
                else:
                     if not RUNWAY_SDK_AVAILABLE:
                         logger.error("SDK RunwayML недоступен. Создание mock видео.")
                         # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                         if ImageClip and callable(create_mock_video): video_path_str = create_mock_video(str(runway_base_image_path))
                         else: logger.warning("MoviePy или create_mock_video не найдены, mock видео не создано.")
                     elif not RUNWAY_API_KEY:
                          logger.error("RUNWAY_API_KEY не найден. Создание mock видео.")
                          # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                          if ImageClip and callable(create_mock_video): video_path_str = create_mock_video(str(runway_base_image_path))
                          else: logger.warning("MoviePy или create_mock_video не найдены, mock видео не создано.")
                     else:
                         # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                         if callable(generate_runway_video):
                             video_url_or_path = generate_runway_video(
                                 image_path=str(runway_base_image_path),
                                 script=final_runway_prompt,
                                 config=config, # Передаем глобальный config
                                 api_key=RUNWAY_API_KEY
                             )
                             if video_url_or_path:
                                 if video_url_or_path.startswith("http"):
                                     video_path_temp = temp_dir_path / f"{generation_id}_runway_final.{VIDEO_FORMAT}"
                                     if download_video(video_url_or_path, str(video_path_temp)):
                                         video_path = video_path_temp
                                         logger.info(f"Видео Runway скачано: {video_path}")
                                     else:
                                         logger.error(f"Не удалось скачать видео Runway {video_url_or_path}. Создание mock.")
                                         # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                                         if ImageClip and callable(create_mock_video): video_path_str = create_mock_video(str(runway_base_image_path))
                                         else: logger.warning("MoviePy или create_mock_video не найдены, mock видео не создано.")
                                 else:
                                     video_path = Path(video_url_or_path)
                                     logger.info(f"Получен локальный путь к видео Runway: {video_path}")
                             else:
                                 logger.error("Генерация видео Runway не удалась. Создание mock.")
                                 # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                                 if ImageClip and callable(create_mock_video): video_path_str = create_mock_video(str(runway_base_image_path))
                                 else: logger.warning("MoviePy или create_mock_video не найдены, mock видео не создано.")
                         else:
                              logger.error("Функция generate_runway_video не найдена! Создание mock видео.")
                              # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                              if ImageClip and callable(create_mock_video): video_path_str = create_mock_video(str(runway_base_image_path))
                              else: logger.warning("MoviePy или create_mock_video не найдены, mock видео не создано.")

                     # Если mock был создан, присваиваем путь
                     if not video_path and video_path_str:
                         video_path = Path(video_path_str)

                if not video_path or not video_path.is_file():
                    logger.warning("Не удалось получить финальное видео (Runway или mock).")

                # Финальное изображение для этого сценария - это апскейл, который не сохраняется как отдельный артефакт
                # Поэтому local_image_path остается None
                local_image_path = None

                logger.info("Очистка состояния MJ (после Runway)...");
                config_mj['midjourney_results'] = {}
                config_mj['generation'] = False
                config_mj['midjourney_task'] = None
                config_mj['status'] = None
                # --- Конец Сценария 3 ---

            elif is_imagine_result:
                # --- Сценарий 2: Есть результат imagine -> Выбираем картинки, создаем заголовок, запускаем upscale ---
                logger.info(f"Обработка результата /imagine для ID {generation_id}.")
                # Получаем ID исходной задачи /imagine из сохраненных результатов
                imagine_task_id = mj_results.get("task_id") # Ищем ID в корне результатов
                if not imagine_task_id and isinstance(task_meta_data, dict): # Ищем в meta
                     imagine_task_id = task_meta_data.get("task_id")

                if not imagine_urls or len(imagine_urls) != 4:
                    logger.error("Не найдены URL сетки /imagine (4 шт.)."); raise ValueError("Некорректные результаты /imagine")
                if not imagine_task_id:
                     logger.error(f"Не найден task_id исходной задачи /imagine в результатах: {mj_results}."); raise ValueError("Отсутствует ID исходной задачи /imagine")

                # --- Выбор картинки для Runway ---
                logger.info("Выбор лучшего изображения для Runway...")
                prompts_config_path_str = config.get('FILE_PATHS.prompts_config')
                prompts_config_data = {}
                if prompts_config_path_str:
                    # BASE_DIR уже должен быть определен глобально
                    prompts_config_path = BASE_DIR / prompts_config_path_str
                    prompts_config_data = load_json_config(str(prompts_config_path)) or {}
                else: logger.error("Путь к prompts_config не найден!")

                visual_analysis_settings = prompts_config_data.get("visual_analysis", {}).get("image_selection", {})
                best_index_runway = 0 # Индекс по умолчанию
                # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                if callable(select_best_image):
                    if _initialize_openai_client(): # Инициализируем/проверяем клиента
                        best_index_runway = select_best_image(imagine_urls, first_frame_description or " ", visual_analysis_settings)
                    elif openai is None:
                        logger.warning("Модуль OpenAI недоступен. Используется индекс 0 для Runway.")
                    else:
                         logger.warning("Клиент OpenAI не инициализирован. Используется индекс 0 для Runway.")
                else:
                     logger.error("Функция select_best_image не найдена! Используется индекс 0 для Runway.")

                if best_index_runway is None or not (0 <= best_index_runway <= 3):
                    logger.warning(f"Не удалось выбрать индекс для Runway (результат: {best_index_runway}). Используем индекс 0.")
                    best_index_runway = 0
                image_for_runway_url = imagine_urls[best_index_runway]
                logger.info(f"Индекс для Runway: {best_index_runway}, URL: {image_for_runway_url[:60]}...")

                # --- Выбор картинки для заголовка ---
                title_index = (best_index_runway + 1) % 4
                image_for_title_url = imagine_urls[title_index]
                logger.info(f"Индекс для заголовка: {title_index}, URL: {image_for_title_url[:60]}...")

                # --- Определение шрифта ---
                final_font_path = None
                if selected_focus:
                    logger.info(f"Определение шрифта для фокуса: '{selected_focus}'")
                    creative_config_path_str = config.get('FILE_PATHS.creative_config')
                    creative_config_data = {}
                    if creative_config_path_str:
                        # BASE_DIR уже должен быть определен
                        creative_config_path = BASE_DIR / creative_config_path_str
                        creative_config_data = load_json_config(str(creative_config_path)) or {}
                    else: logger.error("Путь к creative_config не найден!")

                    fonts_mapping = creative_config_data.get("FOCUS_FONT_MAPPING", {})
                    fonts_folder_rel = config.get("FILE_PATHS.fonts_folder", "fonts/")

                    default_font_rel_path = fonts_mapping.get("__default__")
                    if not default_font_rel_path:
                         logger.error("Критическая ошибка: Шрифт по умолчанию '__default__' не задан!")
                         default_font_rel_path = "fonts/Roboto-Regular.ttf" # Жестко заданный fallback
                         logger.warning(f"Используется жестко заданный шрифт по умолчанию: {default_font_rel_path}")

                    font_rel_path = fonts_mapping.get(selected_focus)
                    final_rel_path = font_rel_path if font_rel_path else default_font_rel_path

                    font_path_abs = BASE_DIR / final_rel_path

                    if font_path_abs.is_file():
                        final_font_path = str(font_path_abs)
                        logger.info(f"Выбран шрифт: {final_font_path}")
                    else:
                        logger.error(f"Файл шрифта не найден: {font_path_abs}")
                        # Попытка использовать шрифт по умолчанию, если кастомный не найден
                        if font_rel_path and font_rel_path != default_font_rel_path:
                             logger.warning(f"Попытка использовать шрифт по умолчанию: {default_font_rel_path}")
                             font_path_abs_default = BASE_DIR / default_font_rel_path
                             if font_path_abs_default.is_file():
                                 final_font_path = str(font_path_abs_default)
                                 logger.info(f"Используется шрифт по умолчанию: {final_font_path}")
                             else: logger.error(f"Файл шрифта по умолчанию также не найден: {font_path_abs_default}")
                        # Если и по умолчанию нет - ошибка
                        if not final_font_path: raise FileNotFoundError("Не удалось найти файл шрифта.")
                else:
                    logger.error("Не удалось получить 'selected_focus'. Невозможно выбрать шрифт.")
                    raise ValueError("selected_focus не найден")

                # --- Создание картинки-заголовка ---
                logger.info("Создание изображения-заголовка...")
                title_base_path = temp_dir_path / f"{generation_id}_title_base.{IMAGE_FORMAT}"
                final_title_image_path = temp_dir_path / f"{generation_id}.{IMAGE_FORMAT}" # Финальное имя PNG

                if download_image(image_for_title_url, str(title_base_path)):
                    logger.info(f"Базовое изображение для заголовка скачано: {title_base_path.name}")

                    title_font_size = 70
                    title_text_color = (255, 255, 255, 240)
                    title_position = ('center', 'center')
                    title_padding = 60
                    title_bg_blur_radius = 5.0
                    title_bg_opacity = 150

                    # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                    if callable(add_text_to_image):
                        if add_text_to_image(
                            image_path_str=str(title_base_path), text=topic,
                            font_path_str=final_font_path, output_path_str=str(final_title_image_path),
                            font_size=title_font_size, text_color=title_text_color,
                            position=title_position, padding=title_padding,
                            bg_blur_radius=title_bg_blur_radius, bg_opacity=title_bg_opacity,
                            logger_instance=logger # Передаем инициализированный logger
                        ):
                            logger.info(f"✅ Изображение-заголовок с текстом создано: {final_title_image_path.name}")
                            local_image_path = final_title_image_path # Это финальный PNG для загрузки
                        else:
                            logger.error("Не удалось создать изображение-заголовок.")
                            local_image_path = title_base_path # Используем базовое изображение
                            logger.warning("В качестве финального PNG будет использовано базовое изображение без текста.")
                    else:
                         logger.error("Функция add_text_to_image не найдена/импортирована!")
                         local_image_path = title_base_path # Используем базовое изображение
                         logger.warning("В качестве финального PNG будет использовано базовое изображение без текста.")
                else:
                    logger.error(f"Не удалось скачать базовое изображение для заголовка: {image_for_title_url}")
                    local_image_path = None # Финальное изображение не создано

                # --- Запуск Upscale для картинки Runway ---
                action_to_trigger = f"upscale{best_index_runway + 1}"
                available_actions = task_result_data.get("actions", [])
                logger.info(f"Запуск Upscale для картинки Runway (индекс {best_index_runway}). Действие: {action_to_trigger}.")

                if action_to_trigger not in available_actions:
                    logger.warning(f"Действие {action_to_trigger} недоступно! Поиск другого upscale...")
                    action_to_trigger = next((a for a in available_actions if a.startswith("upscale")), None)
                    if action_to_trigger: logger.info(f"Используем первое доступное upscale: {action_to_trigger}")
                    else: logger.error("Не найдено доступных upscale действий!")

                upscale_task_info = None
                # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                if action_to_trigger and callable(trigger_piapi_action):
                    if not MIDJOURNEY_API_KEY: logger.error("MIDJOURNEY_API_KEY не найден для trigger_piapi_action.")
                    elif not MJ_IMAGINE_ENDPOINT: logger.error("MJ_IMAGINE_ENDPOINT не найден для trigger_piapi_action.")
                    else:
                         upscale_task_info = trigger_piapi_action(
                             original_task_id=imagine_task_id, action=action_to_trigger,
                             api_key=MIDJOURNEY_API_KEY, endpoint=MJ_IMAGINE_ENDPOINT
                         )
                elif not action_to_trigger:
                     logger.warning("Нет действия upscale для запуска.")
                else:
                     logger.error("Функция trigger_piapi_action не найдена!")

                if upscale_task_info and upscale_task_info.get("task_id"):
                    logger.info(f"Задача Upscale ({action_to_trigger}) запущена. ID: {upscale_task_info['task_id']}")
                    config_mj['midjourney_task'] = upscale_task_info
                    config_mj['midjourney_results'] = {} # Очищаем старые результаты /imagine
                    config_mj['generation'] = False
                    config_mj['status'] = "waiting_for_upscale"
                    logger.info("Состояние обновлено для ожидания /upscale.")
                else:
                    logger.error(f"Не удалось запустить задачу Upscale ({action_to_trigger}).")
                    config_mj['status'] = "upscale_trigger_failed"
                    config_mj['midjourney_task'] = None
                    config_mj['midjourney_results'] = {} # Очищаем старые результаты /imagine

                video_path = None # Видео еще не создано
                # --- Конец Сценария 2 ---

            elif config_mj.get("generation") is True:
                # --- Сценарий 1: Запускаем imagine ---
                logger.info(f"Нет результатов MJ, флаг generation=true. Запуск /imagine для ID {generation_id}...")
                if not final_mj_prompt:
                    logger.error("❌ Промпт MJ отсутствует!"); config_mj['generation'] = False
                else:
                    # --- ИСПРАВЛЕНИЕ: Проверка callable() ---
                    if callable(initiate_midjourney_task):
                        if not MIDJOURNEY_API_KEY: logger.error("MIDJOURNEY_API_KEY не найден для initiate_midjourney_task.")
                        elif not MJ_IMAGINE_ENDPOINT: logger.error("MJ_IMAGINE_ENDPOINT не найден для initiate_midjourney_task.")
                        else:
                            imagine_task_info = initiate_midjourney_task(
                                prompt=final_mj_prompt, config=config, api_key=MIDJOURNEY_API_KEY,
                                endpoint=MJ_IMAGINE_ENDPOINT, ref_id=generation_id
                            )
                            if imagine_task_info and imagine_task_info.get("task_id"):
                                logger.info(f"Задача /imagine запущена. ID: {imagine_task_info['task_id']}")
                                config_mj['midjourney_task'] = imagine_task_info
                                config_mj['generation'] = False
                                config_mj['midjourney_results'] = {}
                                config_mj['status'] = "waiting_for_imagine"
                            else:
                                logger.warning("Не удалось получить task_id для /imagine.")
                                config_mj['midjourney_task'] = None; config_mj['generation'] = False
                    else:
                         logger.error("Функция initiate_midjourney_task не найдена!")
                         config_mj['midjourney_task'] = None; config_mj['generation'] = False
                local_image_path = None; video_path = None # Артефакты еще не созданы
                # --- Конец Сценария 1 ---

            else:
                # Неожиданное состояние
                logger.warning("Нет активной задачи MJ, результатов или флага 'generation'. Пропуск.")
                local_image_path = None; video_path = None

            # --- Загрузка файлов в B2 ---
            target_folder_b2 = "666/"; upload_success_img = False; upload_success_vid = False

            # Загружаем ИЗОБРАЖЕНИЕ, если оно было создано (Сценарий 2 или 0)
            if local_image_path and isinstance(local_image_path, Path) and local_image_path.is_file():
                 b2_image_filename = f"{generation_id}.png" # Всегда PNG
                 upload_success_img = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(local_image_path), b2_image_filename)
                 if not upload_success_img: logger.error(f"!!! ОШИБКА ЗАГРУЗКИ ИЗОБРАЖЕНИЯ {b2_image_filename} !!!")
            elif local_image_path: # Если путь есть, но это не файл
                 logger.warning(f"Финальное изображение {local_image_path} не найдено для загрузки.")
            # Не логируем отсутствие изображения в сценариях 1 и 3, т.к. оно там и не создается

            # Загружаем ВИДЕО, если оно было создано (Сценарий 3 или 0)
            if video_path and isinstance(video_path, Path) and video_path.is_file():
                 b2_video_filename = f"{generation_id}.mp4" # Всегда MP4
                 upload_success_vid = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(video_path), b2_video_filename)
                 if not upload_success_vid: logger.error(f"!!! ОШИБКА ЗАГРУЗКИ ВИДЕО {b2_video_filename} !!!")
            elif video_path: # Если путь есть, но это не файл
                 logger.error(f"Видео {video_path} не найдено для загрузки!")
            # Не логируем отсутствие видео в сценариях 1 и 2, т.к. оно там и не создается

            # Финальное логирование загрузок
            if upload_success_img and upload_success_vid: logger.info("✅ Изображение и видео успешно загружены.")
            elif upload_success_img: logger.info("✅ Изображение успешно загружено.")
            elif upload_success_vid: logger.info("✅ Видео успешно загружено.")
            elif local_image_path or video_path: # Если что-то должно было загрузиться, но не загрузилось
                 logger.warning("⚠️ Не все созданные медиа файлы были успешно загружены.")

        # --- ИСПРАВЛЕНИЕ: finally для очистки temp_dir_path ---
        finally:
             # Очистка временной папки
             if temp_dir_path and temp_dir_path.exists(): # Проверяем, что temp_dir_path не None
                 try:
                     shutil.rmtree(temp_dir_path)
                     logger.debug(f"Удалена временная папка: {temp_dir_path}")
                 except Exception as e:
                     logger.warning(f"Не удалить {temp_dir_path}: {e}")
        # --- Конец вложенного finally ---


        # --- Сохранение финального состояния config_mj ---
        logger.info(f"Сохранение config_midjourney.json в B2...")
        # Удаляем временный файл перед сохранением нового
        # config_mj_local_path уже определен выше
        if config_mj_local_path and Path(config_mj_local_path).exists():
            try: os.remove(config_mj_local_path); logger.debug(f"Удален старый temp конфиг MJ: {config_mj_local_path}")
            except OSError as e: logger.warning(f"Не удалить старый temp конфиг MJ {config_mj_local_path}: {e}")

        if not isinstance(config_mj, dict):
            logger.error("config_mj не словарь! Невозможно сохранить.")
        elif not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, config_mj):
            logger.error("!!! Не удалось сохранить config_midjourney.json в B2!")
        else:
            logger.info("✅ config_midjourney.json сохранен в B2.")
            # Удаляем временный файл и после успешного сохранения
            if config_mj_local_path and Path(config_mj_local_path).exists():
                try: os.remove(config_mj_local_path); logger.debug(f"Удален temp конфиг MJ после сохранения: {config_mj_local_path}")
                except OSError as e: logger.warning(f"Не удалить temp конфиг MJ {config_mj_local_path} после сохранения: {e}")

        logger.info(f"✅ Работа generate_media.py успешно завершена для ID {generation_id}.")

    # --- Обработка исключений верхнего уровня ---
    except ConnectionError as conn_err:
        # Используем logger, если он доступен
        if 'logger' in globals() and logger: logger.error(f"❌ Ошибка соединения B2: {conn_err}")
        else: print(f"ERROR: Ошибка соединения B2: {conn_err}")
        sys.exit(1)
    except FileNotFoundError as fnf_err:
         if 'logger' in globals() and logger: logger.error(f"❌ Ошибка: Файл не найден: {fnf_err}", exc_info=True)
         else: print(f"ERROR: Файл не найден: {fnf_err}")
         sys.exit(1)
    except ValueError as val_err:
         if 'logger' in globals() and logger: logger.error(f"❌ Ошибка значения (например, не найден фокус или ID): {val_err}", exc_info=True)
         else: print(f"ERROR: Ошибка значения: {val_err}")
         sys.exit(1)
    except Exception as e:
        if 'logger' in globals() and logger: logger.error(f"❌ Критическая ошибка в generate_media.py: {e}", exc_info=True)
        else: print(f"ERROR: Критическая ошибка в generate_media.py: {e}")
        sys.exit(1)
    # --- ИСПРАВЛЕНИЕ: Внешний finally для очистки временных файлов конфигов ---
    finally:
        # Очистка временных файлов конфигов
        # Используем try-except для каждой операции удаления
        if 'generation_id' in locals() and generation_id and 'timestamp_suffix' in locals() and timestamp_suffix:
            content_temp_path_str = f"{generation_id}_content_temp_{timestamp_suffix}.json"
            content_temp_path = Path(content_temp_path_str)
            if content_temp_path.exists():
                try:
                    os.remove(content_temp_path)
                    if 'logger' in globals() and logger: logger.debug(f"Удален temp контент (в finally): {content_temp_path}")
                except OSError as e:
                     if 'logger' in globals() and logger: logger.warning(f"Не удалить {content_temp_path} (в finally): {e}")

        # config_mj_local_path уже определен и инициализирован
        if config_mj_local_path:
            config_mj_temp_path = Path(config_mj_local_path)
            if config_mj_temp_path.exists():
                try:
                    os.remove(config_mj_temp_path)
                    if 'logger' in globals() and logger: logger.debug(f"Удален temp конфиг MJ (в finally): {config_mj_temp_path}")
                except OSError as e:
                     if 'logger' in globals() and logger: logger.warning(f"Не удалить {config_mj_temp_path} (в finally): {e}")

        # Очистка временной папки (temp_dir_path) уже обрабатывается во вложенном finally

# === Точка входа ===
if __name__ == "__main__":
    exit_code_main = 1 # По умолчанию ошибка
    try:
        main()
        exit_code_main = 0 # Успех, если main() завершился без исключений
    except KeyboardInterrupt:
        # Используем logger, если он доступен
        if 'logger' in globals() and logger: logger.info("🛑 Остановлено пользователем.")
        else: print("🛑 Остановлено пользователем.")
        exit_code_main = 130 # Стандартный код для Ctrl+C
    except SystemExit as e:
        # Логируем код выхода, если он не 0
        exit_code_main = e.code if isinstance(e.code, int) else 1
        if exit_code_main != 0:
            # --- ИСПРАВЛЕНИЕ: Проверка logger ---
            if 'logger' in globals() and logger: logger.error(f"Завершение с кодом ошибки: {exit_code_main}")
            else: print(f"ERROR: Завершение с кодом ошибки: {exit_code_main}")
        else:
            # --- ИСПРАВЛЕНИЕ: Проверка logger ---
            if 'logger' in globals() and logger: logger.info(f"Завершение с кодом {exit_code_main}")
            else: print(f"INFO: Завершение с кодом {exit_code_main}")
    except Exception as e:
        # Логируем неперехваченные ошибки
        print(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}")
        # --- ИСПРАВЛЕНИЕ: Проверка logger ---
        if 'logger' in globals() and logger: logger.error(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}", exc_info=True)
        exit_code_main = 1 # Общий код ошибки
    finally:
        # Выходим с финальным кодом
        sys.exit(exit_code_main)

