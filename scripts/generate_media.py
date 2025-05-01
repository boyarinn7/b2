#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Отладочный вывод для проверки старта скрипта в GitHub Actions
print("--- SCRIPT START (generate_media.py v4 - sarcasm integrated - FULL) ---", flush=True)

# В файле scripts/generate_media.py

# --- Убедитесь, что все необходимые импорты присутствуют в начале файла ---
import os, json, sys, time, argparse, requests, shutil, base64, re, urllib.parse, logging, httpx
from datetime import datetime, timezone
from pathlib import Path
# --- Импорт кастомных модулей ---
try:
    # Абсолютный импорт
    BASE_DIR = Path(__file__).resolve().parent.parent
    if str(BASE_DIR) not in sys.path:
        sys.path.append(str(BASE_DIR))

    from modules.sarcasm_image_utils import add_text_to_image_sarcasm_openai_ready
    from modules.config_manager import ConfigManager
    from modules.logger import get_logger
    from modules.utils import (
        ensure_directory_exists, load_b2_json, save_b2_json,
        download_image, download_video, upload_to_b2, load_json_config,
        add_text_to_image # <-- Оригинальная функция для заголовков
    )
    # +++ НОВЫЙ ИМПОРТ +++
    from modules.sarcasm_image_utils import add_text_to_image_sarcasm
    # ++++++++++++++++++++
    from modules.api_clients import get_b2_client
    # from modules.error_handler import handle_error # Если используется
except ModuleNotFoundError as import_err:
    # Попытка относительного импорта
    try:
        _BASE_DIR_FOR_IMPORT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if _BASE_DIR_FOR_IMPORT not in sys.path:
            sys.path.insert(0, _BASE_DIR_FOR_IMPORT)

        from modules.config_manager import ConfigManager
        from modules.logger import get_logger
        from modules.utils import (
            ensure_directory_exists, load_b2_json, save_b2_json,
            download_image, download_video, upload_to_b2, load_json_config,
            add_text_to_image
        )
        # +++ НОВЫЙ ИМПОРТ +++
        from modules.sarcasm_image_utils import add_text_to_image_sarcasm
        # ++++++++++++++++++++
        from modules.api_clients import get_b2_client
        # from modules.error_handler import handle_error # Если используется
        del _BASE_DIR_FOR_IMPORT
    except ModuleNotFoundError as import_err_rel:
        print(f"Критическая Ошибка: Не найдены модули проекта: {import_err_rel}", file=sys.stderr)
        sys.exit(1)
    except ImportError as import_err_rel_imp:
        print(f"Критическая Ошибка импорта (относительный): {import_err_rel_imp}", file=sys.stderr)
        sys.exit(1)
# --------------------------------------------
# --- Импорт сторонних библиотек ---
try:
    from runwayml import RunwayML
    RUNWAY_SDK_AVAILABLE = True
    try: from runwayml.exceptions import RunwayError
    except ImportError:
        try: from runwayml.exceptions import RunwayError as BaseRunwayError; RunwayError = BaseRunwayError
        except ImportError: RunwayError = requests.HTTPError
except ImportError: RUNWAY_SDK_AVAILABLE = False; RunwayML = None; RunwayError = requests.HTTPError
try:
    from PIL import Image, ImageFilter, ImageFont, ImageDraw
    PIL_AVAILABLE = True # <-- Добавить эту строку
except ImportError:
    Image = None; ImageFilter = None; ImageFont = None; ImageDraw = None
    PIL_AVAILABLE = False # <-- Добавить эту строку
try:
    from moviepy.editor import ImageClip
except ImportError: ImageClip = None
try: import openai
except ImportError: openai = None
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError: pass
# ---------------------------------------------------------------------------

# === Инициализация конфигурации и логгера ===
try:
    config = ConfigManager()
    logger = get_logger("generate_media")
    logger.info("ConfigManager и Logger для generate_media инициализированы.")
except Exception as init_err:
    import logging
    logging.critical(f"Критическая ошибка инициализации ConfigManager или Logger в generate_media: {init_err}", exc_info=True)
    import sys
    sys.exit(1)
# === Конец блока инициализации ===

# --- Определение BASE_DIR (если не определен ранее) ---
try:
    if 'BASE_DIR' not in globals(): BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
     BASE_DIR = Path.cwd()
     logger.warning(f"Переменная __file__ не определена, BASE_DIR установлен как {BASE_DIR}")
# -----------------------------

# --- Глобальные константы из конфига ---
try:
    CONFIG_MJ_REMOTE_PATH = config.get('FILE_PATHS.config_midjourney', "config/config_midjourney.json")
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))
    IMAGE_FORMAT = config.get("FILE_PATHS.output_image_format", "png")
    VIDEO_FORMAT = "mp4"
    # +++ НОВЫЕ КОНСТАНТЫ +++
    SARCASM_BASE_IMAGE_REL_PATH = config.get("FILE_PATHS.sarcasm_baron_image", "assets/Барон.png")
    SARCASM_FONT_REL_PATH = config.get("FILE_PATHS.sarcasm_font", "assets/fonts/Kurale-Regular.ttf")
    SARCASM_IMAGE_SUFFIX = config.get("FILE_PATHS.sarcasm_image_suffix", "_sarcasm.png")
    # ++++++++++++++++++++++++

    output_size_str = config.get("IMAGE_GENERATION.output_size", "1792x1024")
    delimiter = next((d for d in ['x', '×', ':'] if d in output_size_str), 'x')
    try:
        width_str, height_str = output_size_str.split(delimiter)
        PLACEHOLDER_WIDTH = int(width_str.strip())
        PLACEHOLDER_HEIGHT = int(height_str.strip())
    except ValueError:
        logger.error(f"Ошибка парсинга размеров '{output_size_str}'. Используем 1792x1024.")
        PLACEHOLDER_WIDTH, PLACEHOLDER_HEIGHT = 1792, 1024

    PLACEHOLDER_BG_COLOR = config.get("VIDEO.placeholder_bg_color", "cccccc")
    PLACEHOLDER_TEXT_COLOR = config.get("VIDEO.placeholder_text_color", "333333")
    MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
    RUNWAY_API_KEY = os.getenv("RUNWAY_API_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    MJ_IMAGINE_ENDPOINT = config.get("API_KEYS.midjourney.endpoint")
    MJ_FETCH_ENDPOINT = config.get("API_KEYS.midjourney.task_endpoint")

    MAX_ATTEMPTS = int(config.get("GENERATE.max_attempts", 1))
    OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4o")
    OPENAI_VISION_MODEL = config.get("OPENAI_SETTINGS.vision_model", "gpt-4o")
    TASK_REQUEST_TIMEOUT = int(config.get("WORKFLOW.task_request_timeout", 60))
    HAZE_OPACITY_DEFAULT = int(config.get("VIDEO.title_haze_opacity", 128))

    if not B2_BUCKET_NAME: logger.warning("B2_BUCKET_NAME не определен.")
    if not MIDJOURNEY_API_KEY: logger.warning("MIDJOURNEY_API_KEY не найден.")
    if not RUNWAY_API_KEY: logger.warning("RUNWAY_API_KEY не найден.")
    if not OPENAI_API_KEY: logger.warning("OPENAI_API_KEY не найден.")
    if not MJ_IMAGINE_ENDPOINT: logger.warning("API_KEYS.midjourney.endpoint не найден.")
    if not MJ_FETCH_ENDPOINT: logger.warning("API_KEYS.midjourney.task_endpoint не найден.")
    # +++ ПРОВЕРКА НОВЫХ ПУТЕЙ +++
    if not SARCASM_BASE_IMAGE_REL_PATH: logger.warning("FILE_PATHS.sarcasm_baron_image не задан.")
    if not SARCASM_FONT_REL_PATH: logger.warning("FILE_PATHS.sarcasm_font не задан.")
    if not SARCASM_IMAGE_SUFFIX: logger.warning("FILE_PATHS.sarcasm_image_suffix не задан, используется '_sarcasm.png'.")
    # ++++++++++++++++++++++++++++

except Exception as _cfg_err:
    logger.critical(f"Критическая ошибка при загрузке констант из конфига: {_cfg_err}", exc_info=True)
    sys.exit(1)
# ------------------------------------

# --- Проверка доступности сторонних библиотек (после инициализации logger) ---
if not RUNWAY_SDK_AVAILABLE: logger.warning("RunwayML SDK недоступен.")
if not PIL_AVAILABLE: logger.warning("Библиотека Pillow (PIL) недоступна. Функции обработки изображений будут отключены.") # Добавили проверку
if ImageClip is None: logger.warning("Библиотека MoviePy недоступна.")
if openai is None: logger.warning("Библиотека OpenAI недоступна.")
# ---------------------------------------------------------------------------

# === Глобальная переменная для клиента OpenAI ===
openai_client_instance = None

# === Вспомогательные Функции (оставляем как есть, кроме добавления Pillow) ===

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


def get_text_placement_suggestions(image_url: str, text: str, image_width: int, image_height: int) -> dict:
    """
    Использует GPT-4o Vision для получения рекомендаций по размещению текста на изображении,
    загружая промпт и параметры из prompts_config.json.
    Возвращает темно-серый цвет по умолчанию при ошибке.

    Args:
        image_url: URL изображения для анализа.
        text: Текст, который нужно разместить (может включать доп. контекст).
        image_width: Ширина изображения.
        image_height: Высота изображения.

    Returns:
        Словарь с предложенными параметрами:
        {
            "position": tuple, # ('center', 'center') - фиксировано
            "font_size": int,
            "formatted_text": str, # Текст с переносами строк '\n'
            "text_color": str # Цвет текста в HEX
        }
        Или словарь со значениями по умолчанию при ошибке.
    """
    # Используем глобальные переменные, определенные в основном скрипте
    global openai_client_instance, config, logger, BASE_DIR, OPENAI_VISION_MODEL

    logger.info(f"Получение рекомендаций по размещению текста для URL: {image_url[:60]}...")

    # --- ИЗМЕНЕНИЕ: Цвет по умолчанию теперь ТЕМНО-СЕРЫЙ ---
    default_suggestions = {
        "position": ('center', 'center'), # Фиксировано
        "font_size": 70, # Начальный размер по умолчанию
        "formatted_text": text.split('\n')[0] if text else "Текст отсутствует", # Берем первую строку или дефолт
        "text_color": "#333333" # Темно-серый по умолчанию
    }
    # Извлекаем только сам текст заголовка для дефолта
    actual_text_for_default = text.split('\n')[0] if text else "Текст отсутствует"
    default_suggestions["formatted_text"] = actual_text_for_default
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    if not image_url or not text:
        logger.warning("URL изображения или текст отсутствуют. Возврат стандартных параметров (темно-серый текст).")
        return default_suggestions

    # Инициализация клиента OpenAI, если нужно (используем функцию из основного скрипта)
    # Предполагается, что _initialize_openai_client() существует и работает
    if not openai_client_instance:
        # Вызываем функцию инициализации, определенную глобально в generate_media.py
        # Она должна вернуть True при успехе или False при ошибке
        if '_initialize_openai_client' not in globals() or not _initialize_openai_client():
             logger.error("Клиент OpenAI недоступен для получения рекомендаций. Возврат стандартных параметров (темно-серый текст).")
             return default_suggestions

    # --- Загрузка промпта и параметров из prompts_config.json ---
    prompts_config_path_str = config.get('FILE_PATHS.prompts_config')
    prompts_config_data = {}
    if prompts_config_path_str:
        # Убедимся, что путь абсолютный
        prompts_config_path = Path(prompts_config_path_str)
        if not prompts_config_path.is_absolute():
            # Предполагаем, что BASE_DIR определен глобально
            if 'BASE_DIR' not in globals():
                 logger.error("Глобальная переменная BASE_DIR не найдена! Невозможно определить абсолютный путь к prompts_config.")
                 return default_suggestions
            prompts_config_path = BASE_DIR / prompts_config_path

        # Предполагаем, что load_json_config импортирована и доступна
        if 'load_json_config' not in globals():
            logger.error("Функция load_json_config не найдена! Невозможно загрузить промпт.")
            return default_suggestions
        prompts_config_data = load_json_config(str(prompts_config_path)) or {}
    else:
        logger.error("Путь к prompts_config не найден! Невозможно загрузить промпт. Возврат стандартных параметров (темно-серый текст).")
        return default_suggestions

    # Получаем настройки промпта (используем обновленный промпт из prompts_config_color_fix_v2)
    prompt_settings = prompts_config_data.get("text_placement", {}).get("suggestions", {})
    prompt_template = prompt_settings.get("template")
    max_tokens = int(prompt_settings.get("max_tokens", 300))
    temperature = float(prompt_settings.get("temperature", 0.5)) # Используем температуру из обновленного промпта

    if not prompt_template:
        logger.error("Промпт 'text_placement.suggestions.template' не найден. Возврат стандартных параметров (темно-серый текст).")
        return default_suggestions
    # --- Конец загрузки промпта ---

    # Формируем промпт с актуальными данными
    prompt = prompt_template.format(image_width=image_width, image_height=image_height, text=text) # Передаем весь текст с контекстом

    # Формируем контент для запроса к Vision API
    messages_content = [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": image_url}}
    ]

    try:
        # Предполагаем, что OPENAI_VISION_MODEL определена глобально
        if 'OPENAI_VISION_MODEL' not in globals():
             logger.error("Глобальная переменная OPENAI_VISION_MODEL не найдена!")
             return default_suggestions

        logger.info(f"Запрос к OpenAI Vision ({OPENAI_VISION_MODEL}) для рекомендаций по тексту (t={temperature}, max_tokens={max_tokens})...")
        response = openai_client_instance.chat.completions.create(
            model=OPENAI_VISION_MODEL, # Используем модель Vision
            messages=[{"role": "user", "content": messages_content}],
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"} # Просим JSON ответ
        )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            response_text = response.choices[0].message.content.strip()
            logger.debug(f"Сырой ответ от Vision (ожидается JSON): {response_text}")

            # Парсим JSON ответ
            try:
                suggestions = json.loads(response_text)
                # Валидация полученных данных
                pos_list = suggestions.get("position")
                size = suggestions.get("font_size")
                fmt_text = suggestions.get("formatted_text")
                color_hex = suggestions.get("text_color")

                # Проверка и корректировка позиции (должна быть ['center', 'center'])
                valid_pos = default_suggestions["position"] # По умолчанию
                if isinstance(pos_list, list) and pos_list == ['center', 'center']:
                    valid_pos = tuple(pos_list)
                else:
                    logger.warning(f"Получена некорректная позиция: {pos_list}. Используется {valid_pos}.")

                # Проверка и корректировка размера шрифта
                valid_size = default_suggestions["font_size"] # По умолчанию
                if isinstance(size, int) and 10 < size < 200:
                    valid_size = size
                else:
                    logger.warning(f"Получен некорректный размер шрифта: {size}. Используется {valid_size}.")


                # Проверка текста
                valid_text = default_suggestions["formatted_text"] # По умолчанию
                if isinstance(fmt_text, str) and fmt_text.strip():
                    valid_text = fmt_text
                    # Заменяем литерал \n на реальный символ переноса строки
                    valid_text = valid_text.replace('\\n', '\n')
                else:
                    logger.warning(f"Получен некорректный форматированный текст: {fmt_text}. Используется исходный.")

                # --- ИЗМЕНЕНИЕ: Fallback на темно-серый цвет ---
                valid_color = default_suggestions["text_color"] # Темно-серый по умолчанию
                if isinstance(color_hex, str) and re.match(r'^#[0-9a-fA-F]{6}$', color_hex):
                    valid_color = color_hex
                    logger.info(f"ИИ предложил цвет: {valid_color}")
                else:
                    logger.warning(f"Получен некорректный HEX цвет: {color_hex}. Используется {valid_color} (темно-серый по умолчанию).")
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---

                # Формируем превью текста для лога
                log_text_preview = valid_text[:50].replace('\n', '\\n')
                logger.info(f"Получены рекомендации: Позиция={valid_pos}, Размер={valid_size}, Цвет={valid_color}, Текст='{log_text_preview}...'")

                # Возвращаем валидированный результат
                return {
                    "position": valid_pos,
                    "font_size": valid_size,
                    "formatted_text": valid_text,
                    "text_color": valid_color
                }

            except json.JSONDecodeError as json_e:
                logger.error(f"Ошибка декодирования JSON из ответа Vision: {json_e}. Ответ: {response_text}. Возврат стандартных параметров (темно-серый текст).")
                return default_suggestions
            except Exception as parse_err:
                logger.error(f"Ошибка парсинга или валидации рекомендаций: {parse_err}", exc_info=True)
                return default_suggestions

        else:
            logger.error("OpenAI Vision вернул пустой или некорректный ответ. Возврат стандартных параметров (темно-серый текст).")
            return default_suggestions

    # Обработка специфичных ошибок OpenAI
    except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI Vision: {e}"); return default_suggestions
    except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI Vision: {e}"); return default_suggestions
    except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI Vision: {e}"); return default_suggestions
    except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI Vision: {e.status_code} - {e.response}"); return default_suggestions
    except openai.BadRequestError as e:
        # Особо логируем ошибку, если она связана с форматом JSON
        if "response_format" in str(e):
             logger.error(f"Ошибка OpenAI Vision: Модель {OPENAI_VISION_MODEL}, возможно, не поддерживает response_format=json_object. {e}")
        else:
             logger.exception(f"Ошибка неверного запроса OpenAI Vision: {e}");
        return default_suggestions
    except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI Vision: {e}"); return default_suggestions
    # Обработка других исключений
    except Exception as e:
        logger.error(f"Неизвестная ошибка при получении рекомендаций по тексту: {e}", exc_info=True)
        return default_suggestions

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
    # Ищем ключ template_index, если нет, то template
    selection_prompt_template = prompt_settings.get("template_index", prompt_settings.get("template"))
    if not selection_prompt_template:
        logger.warning("Шаблон 'template_index' или 'template' не найден в prompts_config.json -> visual_analysis -> image_selection. Используем fallback.")
        selection_prompt_template = """
Analyze the following 4 images based on the original prompt and the criteria provided.
Respond ONLY with the number (1, 2, 3, or 4) of the image that best fits the criteria and prompt. Do not add any other text.

Original Prompt Context: {prompt}
Evaluation Criteria: {criteria}
"""
    # Используем max_tokens из конфига, если есть, иначе дефолт
    max_tokens = int(prompt_settings.get("max_tokens", 50)) # Уменьшил дефолт, т.к. нужен только индекс

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
                model=OPENAI_VISION_MODEL, # Используем Vision модель
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
    if not PIL_AVAILABLE: logger.warning("Pillow недоступен, ресайз пропущен."); return True # Не ошибка, просто пропускаем
    image_path = Path(image_path_str)
    if not image_path.is_file(): logger.error(f"Ошибка ресайза: Файл не найден {image_path}"); return False
    try:
        target_width, target_height = PLACEHOLDER_WIDTH, PLACEHOLDER_HEIGHT
        logger.info(f"Ресайз {image_path} до {target_width}x{target_height}...")
        with Image.open(image_path) as img:
            img_format = img.format or IMAGE_FORMAT.upper()
            # --- ИСПРАВЛЕНИЕ: Конвертируем в RGBA для сохранения прозрачности, если она есть ---
            # if img.mode != 'RGB': img = img.convert('RGB')
            if img.mode == 'P': # Если палитра, конвертируем в RGBA
                 img = img.convert('RGBA')
            elif img.mode != 'RGBA' and img.mode != 'RGB': # Если не RGB и не RGBA, конвертируем в RGBA
                 img = img.convert('RGBA')
            # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
            resample_filter = Image.Resampling.LANCZOS if hasattr(Image, 'Resampling') else Image.LANCZOS
            img = img.resize((target_width, target_height), resample_filter)
            # --- ИСПРАВЛЕНИЕ: Сохраняем в PNG для поддержки прозрачности ---
            # img.save(image_path, format=img_format)
            img.save(image_path, format="PNG")
            # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
        logger.info(f"✅ Ресайз до {target_width}x{target_height} завершен (сохранено как PNG).")
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
        duration = int(config.get('VIDEO.runway_duration', 10)) # Увеличено до 10 по умолчанию
        # Используем размеры из констант для ratio
        ratio_str = f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}"
        logger.info(f"Используется ratio: {ratio_str}")
        poll_timeout = int(config.get('WORKFLOW.runway_polling_timeout', 300))
        poll_interval = int(config.get('WORKFLOW.runway_polling_interval', 15))
        logger.info(f"Параметры Runway: model='{model_name}', duration={duration}, ratio='{ratio_str}'")
    except Exception as cfg_err:
        logger.error(f"Ошибка чтения параметров Runway из конфига: {cfg_err}. Используются значения по умолчанию.")
        model_name="gen-2"; duration=10; ratio_str=f"{PLACEHOLDER_WIDTH}:{PLACEHOLDER_HEIGHT}"; poll_timeout=300; poll_interval=15

    # Кодирование изображения в Base64
    try:
        with open(image_path, "rb") as f:
            base64_image = base64.b64encode(f.read()).decode("utf-8")
        ext = Path(image_path).suffix.lower()
        mime_type = f"image/{'png' if ext == '.png' else ('jpeg' if ext in ['.jpg', '.jpeg'] else 'octet-stream')}" # Улучшенный вариант
        image_data_uri = f"data:{mime_type};base64,{base64_image}"
        logger.info(f"Изображение {image_path} успешно кодировано в Base64 (MIME: {mime_type}).")
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
    # +++ ШАГ 6: Добавляем суффикс сарказма в список для удаления +++
    suffixes_to_remove = ["_mj_final", "_placeholder", "_best", "_temp", "_upscaled", SARCASM_IMAGE_SUFFIX.replace('.png','')]
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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
    Включает вызов OpenAI для форматирования текста сарказма.
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
    content_local_temp_path = None # Добавлено для очистки
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
    # --- Добавляем переменные для данных от OpenAI ---
    prompts_config_data = {} # Инициализируем здесь, чтобы была доступна позже

    # --- ИСПРАВЛЕНИЕ: Определяем timestamp_suffix и пути здесь, чтобы они были доступны в finally ---
    timestamp_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    temp_dir_path = Path(f"temp_{generation_id}_{timestamp_suffix}")
    config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"
    content_local_temp_path = f"{generation_id}_content_temp_{timestamp_suffix}.json" # Путь к временному файлу контента
    ensure_directory_exists(config_mj_local_path) # Убедимся, что папка для temp файла есть
    # ----------------------------------------------------------------------------------------

    # +++ ИСПРАВЛЕНИЕ: Получение констант из конфига ВНУТРИ main +++
    try:
        # Убедимся, что config доступен
        if 'config' not in globals() or config is None:
            raise RuntimeError("Глобальный объект 'config' не инициализирован.")
        if 'BASE_DIR' not in globals() or BASE_DIR is None:
             raise RuntimeError("Глобальная переменная BASE_DIR не определена.")

        # Получаем нужные константы для сарказма и другие, если нужно
        SARCASM_IMAGE_SUFFIX = config.get("FILE_PATHS.sarcasm_image_suffix", "_sarcasm.png")
        SARCASM_BASE_IMAGE_REL_PATH = config.get("FILE_PATHS.sarcasm_baron_image", "assets/Барон.png")
        SARCASM_FONT_REL_PATH = config.get("FILE_PATHS.sarcasm_font", "assets/fonts/Kurale-Regular.ttf")
        # Другие константы, которые нужны только в main, можно получить здесь же
        # Например:
        # PLACEHOLDER_WIDTH = int(config.get("IMAGE_GENERATION.output_size", "1280x720").split('x')[0])
        # PLACEHOLDER_HEIGHT = int(config.get("IMAGE_GENERATION.output_size", "1280x720").split('x')[1])
        # ... и т.д.

        # Проверка, что значения получены (опционально, но полезно)
        if not SARCASM_IMAGE_SUFFIX: logger.warning("FILE_PATHS.sarcasm_image_suffix не найден в конфиге, используется '_sarcasm.png'.")
        if not SARCASM_BASE_IMAGE_REL_PATH: logger.error("FILE_PATHS.sarcasm_baron_image не найден в конфиге!")
        if not SARCASM_FONT_REL_PATH: logger.error("FILE_PATHS.sarcasm_font не найден в конфиге!")

    except Exception as config_err:
        logger.critical(f"Критическая ошибка при получении констант из конфига внутри main: {config_err}", exc_info=True)
        sys.exit(1)
    # +++ КОНЕЦ ПОЛУЧЕНИЯ КОНСТАНТ +++

    try:
        # --- Загрузка content_data ---
        logger.info("Загрузка данных контента...")
        content_remote_path = f"666/{generation_id}.json"
        # content_local_temp_path определен выше
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
        text_for_title = topic
        selected_focus = content_data.get("selected_focus") # Может быть None
        first_frame_description = content_data.get("first_frame_description", "")
        final_mj_prompt = content_data.get("final_mj_prompt", "")
        final_runway_prompt = content_data.get("final_runway_prompt", "")
        logger.info(f"Тема: '{topic[:100]}...'")
        if selected_focus:
            logger.info(f"Выбранный фокус: {selected_focus}")
        else:
            logger.warning("⚠️ Ключ 'selected_focus' отсутствует в данных контента.")
        # ------------------------------------

        # +++ ИЗВЛЕЧЕНИЕ ТЕКСТА САРКАЗМА +++
        sarcasm_comment_text = None
        sarcasm_data = content_data.get("sarcasm")
        if isinstance(sarcasm_data, dict):
            comment_value = sarcasm_data.get("comment") # Получаем значение ключа comment
            if isinstance(comment_value, str):
                # --- ИСПРАВЛЕННАЯ ЛОГИКА ПАРСИНГА ---
                parsed_comment_value = None
                is_parsed_as_dict = False
                try:
                    # Пытаемся распарсить строку как JSON
                    parsed_comment_value = json.loads(comment_value)
                    # Проверяем, является ли результат словарем
                    if isinstance(parsed_comment_value, dict):
                        is_parsed_as_dict = True
                        # Ищем ключ comment или комментарий
                        sarcasm_comment_text = parsed_comment_value.get("comment") or parsed_comment_value.get("комментарий")
                        if sarcasm_comment_text:
                             logger.info("Текст сарказма извлечен из JSON-строки.")
                        else:
                             logger.warning("В JSON-строке 'sarcasm.comment' не найден ключ 'comment'/'комментарий'.")
                             sarcasm_comment_text = None # Сбрасываем, если ключ не найден
                    else:
                        # json.loads вернул не словарь (например, строку)
                        if isinstance(parsed_comment_value, str):
                            logger.info("JSON-строка 'sarcasm.comment' содержит простую строку, используем ее.")
                            sarcasm_comment_text = parsed_comment_value
                        else:
                            logger.warning(f"JSON-строка 'sarcasm.comment' содержит не строку и не словарь: {type(parsed_comment_value)}. Игнорируем.")
                            sarcasm_comment_text = None
                except json.JSONDecodeError:
                    # Если парсинг как JSON не удался, считаем исходное значение простой строкой
                    logger.info("Значение 'sarcasm.comment' не JSON, используется как простая строка.")
                    sarcasm_comment_text = comment_value.strip('"') # Убираем лишние кавычки
                # --- КОНЕЦ ИСПРАВЛЕННОЙ ЛОГИКИ ---
            elif comment_value is not None:
                 logger.warning(f"Значение 'sarcasm.comment' не строка: {type(comment_value)}")
        if sarcasm_comment_text: logger.info(f"Текст для картинки сарказма: '{sarcasm_comment_text[:60]}...'")
        else: logger.info("Текст сарказма не найден в данных контента.")
        # +++++++++++++++++++++++++++++++++++++

        # +++ НОВЫЙ БЛОК: Получение форматирования от OpenAI +++
        formatted_sarcasm_text = None
        suggested_sarcasm_font_size = None
        default_sarcasm_font_size = 60 # Размер по умолчанию, если OpenAI не ответит

        if sarcasm_comment_text:
            logger.info("Запрос форматирования текста сарказма у OpenAI...")
            # Убедимся, что клиент OpenAI инициализирован
            if not openai_client_instance:
                # Предполагаем, что функция _initialize_openai_client() определена где-то выше
                if '_initialize_openai_client' in globals() and callable(globals()['_initialize_openai_client']):
                    if not _initialize_openai_client():
                        logger.error("Клиент OpenAI недоступен для форматирования сарказма.")
                    # Если инициализация не удалась, openai_client_instance останется None
                else:
                    logger.error("Функция _initialize_openai_client не найдена!")


            if openai_client_instance:
                # Загрузка конфигурации промптов (если еще не загружена)
                # Убедитесь, что переменная prompts_config_data доступна в этой области видимости
                if not prompts_config_data: # Проверяем, пуст ли словарь
                     prompts_config_path_str = config.get('FILE_PATHS.prompts_config')
                     if prompts_config_path_str:
                         prompts_config_path = Path(prompts_config_path_str)
                         if not prompts_config_path.is_absolute():
                             # Предполагаем, что BASE_DIR определена глобально
                             if 'BASE_DIR' not in globals() or BASE_DIR is None:
                                 logger.error("Глобальная переменная BASE_DIR не найдена!")
                                 # Обработка ошибки или выход
                             else:
                                 prompts_config_path = BASE_DIR / prompts_config_path

                         # Предполагаем, что load_json_config импортирована
                         if 'load_json_config' in globals() and callable(globals()['load_json_config']):
                             prompts_config_data = load_json_config(str(prompts_config_path)) or {}
                         else:
                              logger.error("Функция load_json_config не найдена!")
                              prompts_config_data = {}
                     else:
                         logger.error("Путь к prompts_config не найден!")
                         prompts_config_data = {} # Предотвращаем ошибку ниже

                # Получаем настройки промпта форматирования
                formatting_prompt_settings = prompts_config_data.get("sarcasm", {}).get("image_formatting", {})
                formatting_prompt_template = formatting_prompt_settings.get("template")
                formatting_max_tokens = int(formatting_prompt_settings.get("max_tokens", 300))
                formatting_temperature = float(formatting_prompt_settings.get("temperature", 0.5))

                if formatting_prompt_template:
                    prompt_text_for_formatting = formatting_prompt_template.format(
                        sarcasm_text_input=sarcasm_comment_text
                    )
                    try:
                        # Используем модель из конфига (например, gpt-4o)
                        # Убедитесь, что OPENAI_MODEL определена глобально или доступна
                        if 'OPENAI_MODEL' not in globals(): OPENAI_MODEL = "gpt-4o" # Fallback

                        response = openai_client_instance.chat.completions.create(
                            model=OPENAI_MODEL,
                            messages=[{"role": "user", "content": prompt_text_for_formatting}],
                            max_tokens=formatting_max_tokens,
                            temperature=formatting_temperature,
                            response_format={"type": "json_object"} # Ожидаем JSON
                        )

                        if response.choices and response.choices[0].message and response.choices[0].message.content:
                            response_json_str = response.choices[0].message.content.strip()
                            logger.debug(f"Ответ OpenAI (форматирование сарказма): {response_json_str}")
                            try:
                                formatting_result = json.loads(response_json_str)
                                fmt_text = formatting_result.get("formatted_text")
                                fnt_size = formatting_result.get("font_size")

                                # Валидация ответа
                                if isinstance(fmt_text, str) and fmt_text.strip():
                                    formatted_sarcasm_text = fmt_text
                                else:
                                    logger.warning("OpenAI вернул некорректный 'formatted_text'.")

                                if isinstance(fnt_size, int) and 10 < fnt_size < 200:
                                    suggested_sarcasm_font_size = fnt_size
                                else:
                                    logger.warning(f"OpenAI вернул некорректный 'font_size': {fnt_size}.")

                                # --- ИСПРАВЛЕНИЕ ОШИБКИ В ЛОГЕ ---
                                if formatted_sarcasm_text and suggested_sarcasm_font_size:
                                     # Создаем безопасную для f-строки версию текста
                                     log_text_preview = formatted_sarcasm_text[:50].replace('\n', '\\n')
                                     logger.info(f"Получены рекомендации от OpenAI: Размер={suggested_sarcasm_font_size}, Текст='{log_text_preview}...'")
                                else:
                                     logger.warning("Не удалось получить валидные данные форматирования от OpenAI.")
                                # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                            except json.JSONDecodeError:
                                logger.error(f"Ошибка декодирования JSON ответа OpenAI: {response_json_str}")
                            except Exception as parse_err:
                                logger.error(f"Ошибка парсинга ответа OpenAI: {parse_err}", exc_info=True)
                        else:
                            logger.error("OpenAI вернул пустой ответ на запрос форматирования.")

                    # Обработка ошибок OpenAI API
                    except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI: {e}")
                    except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI: {e}")
                    except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI: {e}")
                    except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}")
                    except openai.BadRequestError as e: logger.exception(f"Ошибка неверного запроса OpenAI: {e}")
                    except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI: {e}")
                    except Exception as e:
                        logger.error(f"Неизвестная ошибка при запросе форматирования к OpenAI: {e}", exc_info=True)
                else:
                    logger.error("Промпт 'sarcasm.image_formatting.template' не найден в prompts_config.json.")
            else:
                 logger.error("Клиент OpenAI не инициализирован, форматирование невозможно.")
        else:
            logger.info("Текст сарказма отсутствует, форматирование не требуется.")

        # Fallback, если OpenAI не сработал
        if not formatted_sarcasm_text:
            logger.warning("Используется исходный текст сарказма для отрисовки (без форматирования OpenAI).")
            formatted_sarcasm_text = sarcasm_comment_text # Используем исходный текст
            # Можно добавить логику для удаления переносов, если они есть в исходном
            if formatted_sarcasm_text: formatted_sarcasm_text = formatted_sarcasm_text.replace('\n', ' ')

        if not suggested_sarcasm_font_size:
            logger.warning(f"Используется размер шрифта по умолчанию: {default_sarcasm_font_size}")
            suggested_sarcasm_font_size = default_sarcasm_font_size

        # +++ КОНЕЦ НОВОГО БЛОКА +++

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
        try:
            # --- Создание временной директории ---
            # temp_dir_path уже определен выше
            ensure_directory_exists(str(temp_dir_path))
            logger.info(f"Создана временная папка: {temp_dir_path}")
            # ------------------------------------

            # ==================================================================
            # ||                                                              ||
            # ||   БЛОК ОБРАБОТКИ СЦЕНАРИЕВ (use-mock, upscale, imagine)      ||
            # ||   ОСТАЕТСЯ БЕЗ ИЗМЕНЕНИЙ                                     ||
            # ||   ... (весь ваш существующий код для этих сценариев) ...     ||
            # ||                                                              ||
            # ==================================================================
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
                # Убедимся, что create_mock_video доступна
                if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                    if ImageClip and local_image_path and local_image_path.is_file():
                         video_path_str = create_mock_video(str(local_image_path)) # Вызываем локальную функцию
                         if not video_path_str: logger.warning("Не удалось создать mock видео.")
                         else: video_path = Path(video_path_str)
                    elif not ImageClip:
                         logger.warning("MoviePy не найден, mock видео не создано.")
                    elif not local_image_path or not local_image_path.is_file():
                         logger.warning("Базовое изображение для mock не найдено, mock видео не создано.")
                else:
                     logger.error("Функция create_mock_video не найдена!")


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

                # Убедимся, что resize_existing_image доступна
                if 'resize_existing_image' in globals() and callable(globals()['resize_existing_image']):
                    if PIL_AVAILABLE:
                        if not resize_existing_image(str(runway_base_image_path)):
                            logger.warning(f"Не удалось выполнить ресайз для {runway_base_image_path}, но продолжаем.")
                    else:
                         logger.warning("Pillow не найден, ресайз не выполнен.")
                else:
                     logger.error("Функция resize_existing_image не найдена!")


                video_path_str = None
                if not final_runway_prompt:
                    logger.error("❌ Промпт Runway отсутствует! Создание mock видео.")
                    if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                        if ImageClip: video_path_str = create_mock_video(str(runway_base_image_path))
                        else: logger.warning("MoviePy не найден, mock видео не создано.")
                    else: logger.error("Функция create_mock_video не найдена!")
                else:
                     if not RUNWAY_SDK_AVAILABLE:
                         logger.error("SDK RunwayML недоступен. Создание mock видео.")
                         if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                             if ImageClip: video_path_str = create_mock_video(str(runway_base_image_path))
                             else: logger.warning("MoviePy не найден, mock видео не создано.")
                         else: logger.error("Функция create_mock_video не найдена!")
                     elif not RUNWAY_API_KEY:
                          logger.error("RUNWAY_API_KEY не найден. Создание mock видео.")
                          if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                              if ImageClip: video_path_str = create_mock_video(str(runway_base_image_path))
                              else: logger.warning("MoviePy не найден, mock видео не создано.")
                          else: logger.error("Функция create_mock_video не найдена!")
                     else:
                         # Убедимся, что generate_runway_video доступна
                         if 'generate_runway_video' in globals() and callable(globals()['generate_runway_video']):
                             video_url_or_path = generate_runway_video(
                                 image_path=str(runway_base_image_path),
                                 script=final_runway_prompt,
                                 config=config,
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
                                         if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                                             if ImageClip: video_path_str = create_mock_video(str(runway_base_image_path))
                                             else: logger.warning("MoviePy не найден, mock видео не создано.")
                                         else: logger.error("Функция create_mock_video не найдена!")
                                 else:
                                     video_path = Path(video_url_or_path)
                                     logger.info(f"Получен локальный путь к видео Runway: {video_path}")
                             else:
                                 logger.error("Генерация видео Runway не удалась. Создание mock.")
                                 if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                                     if ImageClip: video_path_str = create_mock_video(str(runway_base_image_path))
                                     else: logger.warning("MoviePy не найден, mock видео не создано.")
                                 else: logger.error("Функция create_mock_video не найдена!")
                         else:
                              logger.error("Функция generate_runway_video не найдена! Создание mock видео.")
                              if 'create_mock_video' in globals() and callable(globals()['create_mock_video']):
                                  if ImageClip: video_path_str = create_mock_video(str(runway_base_image_path))
                                  else: logger.warning("MoviePy не найден, mock видео не создано.")
                              else: logger.error("Функция create_mock_video не найдена!")

                     if not video_path and video_path_str:
                         video_path = Path(video_path_str)

                if not video_path or not video_path.is_file():
                    logger.warning("Не удалось получить финальное видео (Runway или mock).")

                local_image_path = None # Изображение не нужно сохранять на этом шаге

                logger.info("Очистка состояния MJ (после Runway)...");
                config_mj['midjourney_results'] = {}
                config_mj['generation'] = False
                config_mj['midjourney_task'] = None
                config_mj['status'] = None
                # --- Конец Сценария 3 ---

            elif is_imagine_result:
                # --- Сценарий 2: Есть результат imagine -> Выбираем картинки, создаем заголовок, запускаем upscale ---
                logger.info(f"Обработка результата /imagine для ID {generation_id}.")
                imagine_task_id = mj_results.get("task_id")
                if not imagine_task_id and isinstance(task_meta_data, dict):
                     imagine_task_id = task_meta_data.get("task_id")

                if not imagine_urls or len(imagine_urls) != 4:
                    logger.error("Не найдены URL сетки /imagine (4 шт.)."); raise ValueError("Некорректные результаты /imagine")
                if not imagine_task_id:
                     logger.error(f"Не найден task_id исходной задачи /imagine в результатах: {mj_results}."); raise ValueError("Отсутствует ID исходной задачи /imagine")

                # --- Выбор картинки для Runway ---
                logger.info("Выбор лучшего изображения для Runway...")
                # Загрузка prompts_config_data, если еще не загружен
                if not prompts_config_data:
                     prompts_config_path_str = config.get('FILE_PATHS.prompts_config')
                     if prompts_config_path_str:
                         prompts_config_path = Path(prompts_config_path_str)
                         if not prompts_config_path.is_absolute(): prompts_config_path = BASE_DIR / prompts_config_path
                         if 'load_json_config' in globals() and callable(globals()['load_json_config']):
                             prompts_config_data = load_json_config(str(prompts_config_path)) or {}
                         else: logger.error("Функция load_json_config не найдена!")
                     else: logger.error("Путь к prompts_config не найден!")

                visual_analysis_settings = prompts_config_data.get("visual_analysis", {}).get("image_selection", {})
                best_index_runway = 0
                # Убедимся, что select_best_image доступна
                if 'select_best_image' in globals() and callable(globals()['select_best_image']):
                    if not openai_client_instance: _initialize_openai_client() # Инициализация, если нужно
                    if openai_client_instance:
                        best_index_runway = select_best_image(imagine_urls, first_frame_description or " ", visual_analysis_settings)
                    elif openai is None: logger.warning("Модуль OpenAI недоступен. Используется индекс 0 для Runway.")
                    else: logger.warning("Клиент OpenAI не инициализирован. Используется индекс 0 для Runway.")
                else: logger.error("Функция select_best_image не найдена! Используется индекс 0 для Runway.")
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
                logger.info("Определение шрифта для заголовка...")
                creative_config_path_str = config.get('FILE_PATHS.creative_config')
                creative_config_data = {}
                if creative_config_path_str:
                    creative_config_path = Path(creative_config_path_str)
                    if not creative_config_path.is_absolute(): creative_config_path = BASE_DIR / creative_config_path
                    if 'load_json_config' in globals() and callable(globals()['load_json_config']):
                         creative_config_data = load_json_config(str(creative_config_path)) or {}
                    else: logger.error("Функция load_json_config не найдена!")
                else: logger.error("Путь к creative_config не найден!")

                fonts_mapping = creative_config_data.get("FOCUS_FONT_MAPPING", {})
                default_font_rel_path = fonts_mapping.get("__default__")

                if not default_font_rel_path:
                    logger.error("Критическая ошибка: Шрифт по умолчанию '__default__' не задан!")
                    raise ValueError("Шрифт по умолчанию не настроен")

                font_rel_path = None
                if selected_focus and isinstance(selected_focus, str):
                    font_rel_path = fonts_mapping.get(selected_focus)
                    if font_rel_path: logger.info(f"Найден шрифт для фокуса '{selected_focus}': {font_rel_path}")
                    else: logger.warning(f"Шрифт для фокуса '{selected_focus}' не найден. Используется дефолтный.")
                else: logger.warning(f"Ключ 'selected_focus' отсутствует/некорректен. Используется дефолтный.")

                final_rel_path = font_rel_path if font_rel_path else default_font_rel_path
                logger.info(f"Используемый относительный путь к шрифту: {final_rel_path}")

                font_path_abs = BASE_DIR / final_rel_path
                if font_path_abs.is_file():
                    final_font_path = str(font_path_abs)
                    logger.info(f"Финальный абсолютный путь к шрифту: {final_font_path}")
                else:
                    logger.error(f"Файл шрифта не найден: {font_path_abs}")
                    if final_rel_path == default_font_rel_path:
                         logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не найден файл шрифта по умолчанию: {font_path_abs}")
                         raise FileNotFoundError(f"Не найден файл шрифта по умолчанию: {font_path_abs}")
                    else:
                         logger.warning(f"Попытка использовать шрифт по умолчанию: {default_font_rel_path}")
                         font_path_abs_default = BASE_DIR / default_font_rel_path
                         if font_path_abs_default.is_file():
                             final_font_path = str(font_path_abs_default)
                             logger.info(f"Успешно использован шрифт по умолчанию: {final_font_path}")
                         else:
                             logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Файл шрифта по умолчанию также не найден: {font_path_abs_default}")
                             raise FileNotFoundError(f"Не найден файл шрифта по умолчанию: {font_path_abs_default}")

                if not final_font_path: raise RuntimeError("Не удалось определить финальный путь к шрифту")

                # --- Создание картинки-заголовка ---
                logger.info("Создание изображения-заголовка...")
                title_base_path = temp_dir_path / f"{generation_id}_title_base.{IMAGE_FORMAT}"
                final_title_image_path = temp_dir_path / f"{generation_id}.{IMAGE_FORMAT}" # Финальное имя PNG

                if download_image(image_for_title_url, str(title_base_path)):
                    logger.info(f"Базовое изображение для заголовка скачано: {title_base_path.name}")

                    # Получаем рекомендации от Vision, передаем ТОЛЬКО тему
                    logger.info(f"Запрос рекомендаций по размещению для текста (тема): '{text_for_title[:100]}...'")
                    if not openai_client_instance: _initialize_openai_client() # Инициализация, если нужно
                    # Убедимся, что get_text_placement_suggestions доступна
                    if 'get_text_placement_suggestions' in globals() and callable(globals()['get_text_placement_suggestions']):
                        placement_suggestions = get_text_placement_suggestions(
                            image_url=image_for_title_url,
                            text=text_for_title, # <<< Передаем только тему
                            image_width=PLACEHOLDER_WIDTH,
                            image_height=PLACEHOLDER_HEIGHT
                        )
                    else:
                        logger.error("Функция get_text_placement_suggestions не найдена!")
                        # Устанавливаем дефолтные значения
                        placement_suggestions = {
                            "position": ('center', 'center'),
                            "font_size": 70,
                            "formatted_text": text_for_title.split('\n')[0] if text_for_title else "Текст отсутствует",
                            "text_color": "#333333"
                        }


                    title_padding = 60; title_bg_blur_radius = 0; title_bg_opacity = 0

                    # Убедимся, что add_text_to_image доступна
                    if 'add_text_to_image' in globals() and callable(globals()['add_text_to_image']):
                        if PIL_AVAILABLE:
                            log_text_preview = placement_suggestions['formatted_text'].replace('\n', '\\n')
                            logger.info(f"Параметры для add_text_to_image: text='{log_text_preview}', color={placement_suggestions['text_color']}, pos={placement_suggestions['position']}")

                            if add_text_to_image(
                                image_path_str=str(title_base_path),
                                text=placement_suggestions["formatted_text"], # Текст с переносами
                                font_path_str=final_font_path, # <<< Используем определенный путь
                                output_path_str=str(final_title_image_path),
                                text_color_hex=placement_suggestions["text_color"], # Передаем HEX цвет
                                position=placement_suggestions["position"], # Рекомендуемая позиция
                                padding=title_padding,
                                haze_opacity=HAZE_OPACITY_DEFAULT, # Добавляем дымку
                                bg_blur_radius=title_bg_blur_radius,
                                bg_opacity=title_bg_opacity,
                                logger_instance=logger
                            ):
                                logger.info(f"✅ Изображение-заголовок с текстом создано: {final_title_image_path.name}")
                                local_image_path = final_title_image_path # Это финальный PNG для загрузки
                            else:
                                logger.error("Не удалось создать изображение-заголовок.")
                                local_image_path = title_base_path # Используем базовое изображение
                                logger.warning("В качестве финального PNG будет использовано базовое изображение без текста.")
                        else:
                             logger.warning("Pillow недоступен, текст на заголовок не добавлен.")
                             local_image_path = title_base_path
                    else:
                         logger.error("Функция add_text_to_image не найдена/импортирована!")
                         local_image_path = title_base_path
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
                # Убедимся, что trigger_piapi_action доступна
                if 'trigger_piapi_action' in globals() and callable(globals()['trigger_piapi_action']):
                    if action_to_trigger:
                        if not MIDJOURNEY_API_KEY: logger.error("MIDJOURNEY_API_KEY не найден для trigger_piapi_action.")
                        elif not MJ_IMAGINE_ENDPOINT: logger.error("MJ_IMAGINE_ENDPOINT не найден для trigger_piapi_action.")
                        else:
                             upscale_task_info = trigger_piapi_action(
                                 original_task_id=imagine_task_id, action=action_to_trigger,
                                 api_key=MIDJOURNEY_API_KEY, endpoint=MJ_IMAGINE_ENDPOINT
                             )
                    else:
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
                    # Убедимся, что initiate_midjourney_task доступна
                    if 'initiate_midjourney_task' in globals() and callable(globals()['initiate_midjourney_task']):
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

            # +++ ОБНОВЛЕННЫЙ БЛОК: Генерация картинки с сарказмом (используя данные OpenAI) +++
            sarcasm_image_path = None # Инициализируем здесь

            # Проверяем наличие ТЕПЕРЬ УЖЕ ОТФОРМАТИРОВАННОГО текста, размера и Pillow
            # Убедимся, что функция add_text_to_image_sarcasm_openai_ready импортирована и доступна
            if 'add_text_to_image_sarcasm_openai_ready' in globals() and callable(globals()['add_text_to_image_sarcasm_openai_ready']):
                if formatted_sarcasm_text and suggested_sarcasm_font_size and PIL_AVAILABLE:
                    logger.info("Генерация изображения с сарказмом (с форматированием OpenAI)...")
                    # Пути к ресурсам (как и раньше)
                    # Убедимся, что SARCASM_BASE_IMAGE_REL_PATH, SARCASM_FONT_REL_PATH, SARCASM_IMAGE_SUFFIX доступны
                    if 'SARCASM_BASE_IMAGE_REL_PATH' not in locals() or 'SARCASM_FONT_REL_PATH' not in locals() or 'SARCASM_IMAGE_SUFFIX' not in locals():
                         logger.error("Константы для сарказма не определены в области видимости main!")
                    else:
                        sarcasm_base_image_path_abs = BASE_DIR / SARCASM_BASE_IMAGE_REL_PATH
                        sarcasm_font_path_abs = BASE_DIR / SARCASM_FONT_REL_PATH
                        sarcasm_output_path_temp = temp_dir_path / f"{generation_id}{SARCASM_IMAGE_SUFFIX}"

                        # Проверка наличия базового изображения и шрифта
                        if not sarcasm_base_image_path_abs.is_file():
                            logger.error(f"Базовое изображение Барона не найдено: {sarcasm_base_image_path_abs}")
                        elif not sarcasm_font_path_abs.is_file():
                            logger.error(f"Шрифт для сарказма не найден: {sarcasm_font_path_abs}")
                        else:
                            # Вызываем НОВУЮ функцию отрисовки
                            sarcasm_success = add_text_to_image_sarcasm_openai_ready(
                                image_path_str=str(sarcasm_base_image_path_abs),
                                formatted_text=formatted_sarcasm_text, # <-- Текст от OpenAI
                                suggested_font_size=suggested_sarcasm_font_size, # <-- Размер от OpenAI
                                font_path_str=str(sarcasm_font_path_abs),
                                output_path_str=str(sarcasm_output_path_temp),
                                # Остальные параметры можно оставить по умолчанию или настроить
                                text_color_hex="#FFFFFF",
                                align='right',
                                padding_fraction=0.05,
                                stroke_width=2,
                                stroke_color_hex="#404040",
                                logger_instance=logger
                            )
                            if sarcasm_success:
                                sarcasm_image_path = sarcasm_output_path_temp # Запоминаем путь для загрузки
                                logger.info(f"✅ Изображение с сарказмом создано (OpenAI формат): {sarcasm_image_path.name}")
                            else:
                                logger.error("Не удалось создать изображение с сарказмом (OpenAI формат).")
                # Логирование причин пропуска (если что-то пошло не так ДО вызова отрисовки)
                elif not formatted_sarcasm_text:
                    logger.info("Пропуск генерации картинки с сарказмом (нет форматированного текста).")
                elif not suggested_sarcasm_font_size:
                     logger.info("Пропуск генерации картинки с сарказмом (нет предложенного размера шрифта).")
                elif not PIL_AVAILABLE:
                    logger.warning("Pillow недоступен, пропуск генерации картинки с сарказмом.")
            else:
                 logger.error("Функция add_text_to_image_sarcasm_openai_ready не найдена!")
            # +++ КОНЕЦ ОБНОВЛЕННОГО БЛОКА +++


            # --- Загрузка файлов в B2 ---
            target_folder_b2 = "666/"
            upload_success_img = False
            upload_success_vid = False
            upload_success_sarcasm = False # Инициализируем флаг загрузки сарказма

            # Загрузка основного изображения (заголовка)
            # Убедимся, что upload_to_b2 доступна
            if 'upload_to_b2' in globals() and callable(globals()['upload_to_b2']):
                if local_image_path and isinstance(local_image_path, Path) and local_image_path.is_file():
                     b2_image_filename = f"{generation_id}.png" # Всегда PNG
                     upload_success_img = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(local_image_path), b2_image_filename)
                     if not upload_success_img: logger.error(f"!!! ОШИБКА ЗАГРУЗКИ ИЗОБРАЖЕНИЯ {b2_image_filename} !!!")
                elif local_image_path:
                     logger.warning(f"Финальное изображение {local_image_path} не найдено для загрузки.")

                # Загрузка видео
                if video_path and isinstance(video_path, Path) and video_path.is_file():
                     b2_video_filename = f"{generation_id}.mp4" # Всегда MP4
                     upload_success_vid = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(video_path), b2_video_filename)
                     if not upload_success_vid: logger.error(f"!!! ОШИБКА ЗАГРУЗКИ ВИДЕО {b2_video_filename} !!!")
                elif video_path:
                     logger.error(f"Видео {video_path} не найдено для загрузки!")

                # +++ ЗАГРУЗКА КАРТИНКИ С САРКАЗМОМ +++
                if sarcasm_image_path and isinstance(sarcasm_image_path, Path) and sarcasm_image_path.is_file():
                     # Убедимся, что SARCASM_IMAGE_SUFFIX доступен
                     if 'SARCASM_IMAGE_SUFFIX' not in locals():
                         logger.error("Переменная SARCASM_IMAGE_SUFFIX не определена в области видимости main!")
                     else:
                         b2_sarcasm_filename = f"{generation_id}{SARCASM_IMAGE_SUFFIX}" # Используем суффикс из конфига
                         upload_success_sarcasm = upload_to_b2(b2_client, B2_BUCKET_NAME, target_folder_b2, str(sarcasm_image_path), b2_sarcasm_filename)
                         if not upload_success_sarcasm: logger.error(f"!!! ОШИБКА ЗАГРУЗКИ КАРТИНКИ С САРКАЗМОМ {b2_sarcasm_filename} !!!")
                elif sarcasm_image_path: # Если путь был, но файла нет
                     logger.warning(f"Картинка с сарказмом {sarcasm_image_path} не найдена для загрузки.")
                # ++++++++++++++++++++++++++++++++++++++
            else:
                logger.error("Функция upload_to_b2 не найдена! Загрузка в B2 невозможна.")


            # Логирование результатов загрузки
            uploaded_items = []
            if upload_success_img: uploaded_items.append("Изображение")
            if upload_success_vid: uploaded_items.append("Видео")
            if upload_success_sarcasm: uploaded_items.append("Сарказм") # <-- Добавлено
            if uploaded_items: logger.info(f"✅ Успешно загружены: {', '.join(uploaded_items)}.")

            # Проверяем все три флага
            # Убедимся, что local_image_path, video_path, sarcasm_image_path определены перед проверкой
            img_exists = local_image_path and local_image_path.is_file()
            vid_exists = video_path and video_path.is_file()
            sarc_exists = sarcasm_image_path and sarcasm_image_path.is_file()

            # Проверяем, были ли попытки создать файлы (пути не None)
            attempted_img = local_image_path is not None
            attempted_vid = video_path is not None
            attempted_sarc = sarcasm_image_path is not None

            # Логируем предупреждение, если хотя бы один файл пытались создать, но не загрузили
            if (attempted_img and not upload_success_img) or \
               (attempted_vid and not upload_success_vid) or \
               (attempted_sarc and not upload_success_sarcasm):
                 logger.warning("⚠️ Не все созданные медиа файлы были успешно загружены.")


        # --- finally для очистки temp_dir_path ---
        finally:
             if temp_dir_path and temp_dir_path.exists(): # Проверяем, что temp_dir_path не None
                 try:
                     shutil.rmtree(temp_dir_path)
                     logger.debug(f"Удалена временная папка: {temp_dir_path}")
                 except Exception as e:
                     logger.warning(f"Не удалить {temp_dir_path}: {e}")
        # --- Конец вложенного finally ---


        # --- Сохранение финального состояния config_mj ---
        logger.info(f"Сохранение config_midjourney.json в B2...")
        if config_mj_local_path and Path(config_mj_local_path).exists():
            try: os.remove(config_mj_local_path); logger.debug(f"Удален старый temp конфиг MJ: {config_mj_local_path}")
            except OSError as e: logger.warning(f"Не удалить старый temp конфиг MJ {config_mj_local_path}: {e}")

        # Убедимся, что save_b2_json доступна
        if 'save_b2_json' in globals() and callable(globals()['save_b2_json']):
            if not isinstance(config_mj, dict):
                logger.error("config_mj не словарь! Невозможно сохранить.")
            elif not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, config_mj_local_path, config_mj):
                logger.error("!!! Не удалось сохранить config_midjourney.json в B2!")
            else:
                logger.info("✅ config_midjourney.json сохранен в B2.")
                if config_mj_local_path and Path(config_mj_local_path).exists():
                    try: os.remove(config_mj_local_path); logger.debug(f"Удален temp конфиг MJ после сохранения: {config_mj_local_path}")
                    except OSError as e: logger.warning(f"Не удалить temp конфиг MJ {config_mj_local_path} после сохранения: {e}")
        else:
            logger.error("Функция save_b2_json не найдена! Невозможно сохранить config_mj.")


        logger.info(f"✅ Работа generate_media.py успешно завершена для ID {generation_id}.")

    # --- Обработка исключений верхнего уровня ---
    except ConnectionError as conn_err:
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
    # --- Внешний finally для очистки временных файлов конфигов ---
    finally:
        # Используем .locals() для безопасной проверки наличия переменных
        if 'generation_id' in locals() and generation_id and \
           'timestamp_suffix' in locals() and timestamp_suffix:
            if 'content_local_temp_path' in locals() and content_local_temp_path:
                content_temp_path_obj = Path(content_local_temp_path)
                if content_temp_path_obj.exists():
                    try:
                        os.remove(content_temp_path_obj)
                        if 'logger' in locals() and logger: logger.debug(f"Удален temp контент (в finally): {content_temp_path_obj}")
                    except OSError as e:
                         if 'logger' in locals() and logger: logger.warning(f"Не удалить {content_temp_path_obj} (в finally): {e}")

        if 'config_mj_local_path' in locals() and config_mj_local_path:
            config_mj_temp_path = Path(config_mj_local_path)
            if config_mj_temp_path.exists():
                try:
                    os.remove(config_mj_temp_path)
                    if 'logger' in locals() and logger: logger.debug(f"Удален temp конфиг MJ (в finally): {config_mj_temp_path}")
                except OSError as e:
                     if 'logger' in locals() and logger: logger.warning(f"Не удалить {config_mj_temp_path} (в finally): {e}")

# === Точка входа ===
if __name__ == "__main__":
    exit_code_main = 1
    try:
        main()
        exit_code_main = 0
    except KeyboardInterrupt:
        if 'logger' in globals() and logger: logger.info("🛑 Остановлено пользователем.")
        else: print("🛑 Остановлено пользователем.")
        exit_code_main = 130
    except SystemExit as e:
        exit_code_main = e.code if isinstance(e.code, int) else 1
        if exit_code_main != 0:
            if 'logger' in globals() and logger: logger.error(f"Завершение с кодом ошибки: {exit_code_main}")
            else: print(f"ERROR: Завершение с кодом ошибки: {exit_code_main}")
        else:
            if 'logger' in globals() and logger: logger.info(f"Завершение с кодом {exit_code_main}")
            else: print(f"INFO: Завершение с кодом {exit_code_main}")
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}")
        if 'logger' in globals() and logger: logger.error(f"❌ КРИТИЧЕСКАЯ НЕПЕРЕХВАЧЕННАЯ ОШИБКА: {e}", exc_info=True)
        exit_code_main = 1
    finally:
        sys.exit(exit_code_main)
