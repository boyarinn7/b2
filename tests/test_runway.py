import os
import json
import base64
import re
import logging
from pathlib import Path
import sys
import openai
import httpx

# --- Добавляем путь к модулям, если скрипт не в корне проекта ---
# Предполагаем, что скрипт находится в папке tests внутри вашего проекта b2
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
    MODULES_DIR = BASE_DIR / 'modules'
    if str(MODULES_DIR) not in sys.path:
        sys.path.insert(0, str(MODULES_DIR))
    # Импортируем нужную функцию ИЗ УЖЕ ИСПРАВЛЕННОГО utils.py
    from utils import add_text_to_image
except ImportError as e:
    print(f"Ошибка импорта 'add_text_to_image' из utils: {e}")
    print("Убедитесь, что скрипт находится в папке tests вашего проекта,")
    print("и файл modules/utils.py существует и доступен.")
    sys.exit(1)
except NameError:
    print("Ошибка: Не удалось определить путь к модулям. Запустите скрипт из папки tests.")
    sys.exit(1)


# --- Настройка Логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_title_generator")

# +++ ИНИЦИАЛИЗАЦИЯ ГЛОБАЛЬНОЙ ПЕРЕМЕННОЙ +++
openai_client_instance = None
# +++ КОНЕЦ ИНИЦИАЛИЗАЦИИ +++

# --- Константы и Пути ---
# Используйте необработанные строки (r"...") для путей Windows
IMAGE_PATH_STR = r"C:\Users\boyar\777\555\20250303-0855.png"
JSON_PATH_STR = r"C:\Users\boyar\777\555\20250303-0855.json"
# *** ИЗМЕНЕНИЕ: Используем Arial ***
FONT_PATH_STR = r"C:\Windows\Fonts\arial.ttf" # Проверьте этот путь!
# **********************************
OUTPUT_PATH_STR = r"C:\Users\boyar\777\555\1_arial_test.png" # Изменил имя для теста

# --- Встроенный Промпт для OpenAI Vision ---
# (Взят из prompts_config.json, который вы показывали)
VISION_PROMPT_TEMPLATE = """
Analyze the text provided below. Assume it will be placed centrally on an image ({image_width}x{image_height}) that has a semi-transparent white overlay (haze) for readability.

Text to place:
"{text}"

Your tasks:
1.  **Line Breaks:** If the text is long, insert newline characters ('\\n') to break it into 2-3 concise lines for better fit in a central position. Keep the original meaning.
2.  **Text Color:** Analyze the mood and theme of the text. Suggest an appropriate text color as a hex code string (e.g., "#FFFFFF", "#000000", "#FFD700") that would contrast well with a white haze and fit the text's content.

Respond ONLY with a valid JSON object containing the keys:
* `position`: MUST be `["center", "center"]`.
* `font_size`: MUST be `70`.
* `formatted_text`: The text with potential '\\n' line breaks.
* `text_color`: The suggested hex color code string.

Example JSON response:
{{
  "position": ["center", "center"],
  "font_size": 70,
  "formatted_text": "Тайные общества,\\\\nправящие миром:\\\\nиллюзия или реальность?",
  "text_color": "#333333"
}}
"""

# --- Параметры для add_text_to_image ---
HAZE_OPACITY = 128 # Прозрачность дымки (0-255), можно настроить
DEFAULT_FONT_SIZE = 70
DEFAULT_TEXT_COLOR = "#FFFFFF" # Белый по умолчанию

# === Вспомогательные Функции ===

def get_image_mime_type(image_path: Path) -> str:
    """Определяет MIME-тип изображения по расширению."""
    ext = image_path.suffix.lower()
    if ext == ".png":
        return "image/png"
    elif ext in [".jpg", ".jpeg"]:
        return "image/jpeg"
    elif ext == ".gif":
        return "image/gif"
    elif ext == ".webp":
        return "image/webp"
    else:
        logger.warning(f"Неизвестный тип изображения: {ext}. Используется 'image/png'.")
        return "image/png" # По умолчанию

def encode_image_to_data_uri(image_path: Path) -> str | None:
    """Кодирует локальное изображение в Base64 Data URI."""
    try:
        mime_type = get_image_mime_type(image_path)
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
        return f"data:{mime_type};base64,{encoded_string}"
    except FileNotFoundError:
        logger.error(f"Ошибка кодирования: Файл не найден - {image_path}")
        return None
    except Exception as e:
        logger.error(f"Ошибка кодирования изображения {image_path}: {e}", exc_info=True)
        return None

def get_local_text_placement_suggestions(
    image_path: Path,
    text: str,
    image_width: int,
    image_height: int
) -> dict:
    """
    Получает рекомендации по размещению текста для локального файла,
    используя встроенный промпт и кодирование в Base64.
    """
    global openai_client_instance # Используем глобальный клиент
    logger.info(f"Получение рекомендаций для локального файла: {image_path.name}")

    # Параметры по умолчанию
    default_suggestions = {
        "position": ('center', 'center'),
        "font_size": DEFAULT_FONT_SIZE,
        "formatted_text": text.split('\n')[0] if text else "Текст отсутствует",
        "text_color": DEFAULT_TEXT_COLOR
    }
    actual_text_for_default = text.split('\n')[0] if text else "Текст отсутствует"
    default_suggestions["formatted_text"] = actual_text_for_default

    if not image_path.is_file() or not text:
        logger.warning("Путь к изображению невалиден или текст отсутствует. Возврат стандартных параметров.")
        return default_suggestions

    # Инициализация клиента OpenAI, если нужно
    if not openai_client_instance:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("Переменная окружения OPENAI_API_KEY не задана!")
            return default_suggestions
        try:
            # Проверяем наличие прокси
            http_proxy = os.getenv("HTTP_PROXY")
            https_proxy = os.getenv("HTTPS_PROXY")
            proxies_dict = {}
            if http_proxy: proxies_dict["http://"] = http_proxy
            if https_proxy: proxies_dict["https://"] = https_proxy
            http_client = httpx.Client(proxies=proxies_dict) if proxies_dict else httpx.Client()

            openai_client_instance = openai.OpenAI(api_key=api_key, http_client=http_client)
            logger.info("Клиент OpenAI инициализирован для теста.")
        except Exception as init_err:
            logger.error(f"Ошибка инициализации клиента OpenAI: {init_err}", exc_info=True)
            return default_suggestions

    # Кодируем изображение
    data_uri = encode_image_to_data_uri(image_path)
    if not data_uri:
        logger.error("Не удалось закодировать изображение. Возврат стандартных параметров.")
        return default_suggestions

    # Формируем промпт
    prompt = VISION_PROMPT_TEMPLATE.format(
        image_width=image_width,
        image_height=image_height,
        text=text # Передаем текст с контекстом
    )

    messages_content = [
        {"type": "text", "text": prompt},
        {"type": "image_url", "image_url": {"url": data_uri}}
    ]

    try:
        logger.info("Запрос к OpenAI Vision для рекомендаций по тексту...")
        response = openai_client_instance.chat.completions.create(
            model="gpt-4o", # Используем gpt-4o или gpt-4-vision-preview
            messages=[{"role": "user", "content": messages_content}],
            max_tokens=300,
            temperature=0.4,
            response_format={"type": "json_object"}
        )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            response_text = response.choices[0].message.content.strip()
            logger.debug(f"Сырой ответ от Vision (JSON): {response_text}")

            try:
                suggestions = json.loads(response_text)
                # Валидация
                pos_list = suggestions.get("position")
                size = suggestions.get("font_size")
                fmt_text = suggestions.get("formatted_text")
                color_hex = suggestions.get("text_color")

                valid_pos = default_suggestions["position"]
                if isinstance(pos_list, list) and pos_list == ['center', 'center']:
                    valid_pos = tuple(pos_list)
                else:
                    logger.warning(f"Некорректная позиция: {pos_list}. Используется {valid_pos}.")

                valid_size = default_suggestions["font_size"]
                if isinstance(size, int) and 10 < size < 200:
                    valid_size = size
                else:
                    logger.warning(f"Некорректный размер шрифта: {size}. Используется {valid_size}.")

                valid_text = default_suggestions["formatted_text"]
                if isinstance(fmt_text, str) and fmt_text.strip():
                    valid_text = fmt_text.replace('\\n', '\n')
                else:
                    logger.warning(f"Некорректный форматированный текст: {fmt_text}. Используется исходный.")

                valid_color = default_suggestions["text_color"]
                if isinstance(color_hex, str) and re.match(r'^#[0-9a-fA-F]{6}$', color_hex):
                    valid_color = color_hex
                else:
                    logger.warning(f"Некорректный HEX цвет: {color_hex}. Используется {valid_color}.")

                log_text_preview = valid_text[:50].replace('\n', '\\n')
                logger.info(f"Получены рекомендации: Позиция={valid_pos}, Размер={valid_size}, Цвет={valid_color}, Текст='{log_text_preview}...'")
                return {
                    "position": valid_pos,
                    "font_size": valid_size,
                    "formatted_text": valid_text,
                    "text_color": valid_color
                }

            except json.JSONDecodeError as json_e:
                logger.error(f"Ошибка декодирования JSON: {json_e}. Ответ: {response_text}")
                return default_suggestions
            except Exception as parse_err:
                logger.error(f"Ошибка парсинга рекомендаций: {parse_err}", exc_info=True)
                return default_suggestions
        else:
            logger.error("OpenAI Vision вернул пустой ответ.")
            return default_suggestions

    except Exception as e:
        logger.error(f"Ошибка при вызове OpenAI Vision: {e}", exc_info=True)
        return default_suggestions


# === Основная Логика Теста ===
if __name__ == "__main__":
    logger.info("--- Запуск тестового скрипта генерации титульника ---")

    image_path = Path(IMAGE_PATH_STR)
    json_path = Path(JSON_PATH_STR)
    font_path = Path(FONT_PATH_STR)
    output_path = Path(OUTPUT_PATH_STR)

    # 1. Проверка наличия файлов
    if not image_path.is_file():
        logger.error(f"Ошибка: Исходное изображение не найдено - {image_path}")
        sys.exit(1)
    if not json_path.is_file():
        logger.error(f"Ошибка: JSON файл не найден - {json_path}")
        sys.exit(1)
    if not font_path.is_file():
        logger.error(f"Ошибка: Файл шрифта не найден - {font_path}")
        sys.exit(1)

    # 2. Загрузка текста из JSON
    topic_text = None
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            topic_text = data.get("topic") # Предполагаем, что ключ называется "topic"
            if not topic_text:
                logger.error(f"Ошибка: Ключ 'topic' не найден в {json_path}")
                sys.exit(1)
            logger.info(f"Загружен текст: '{topic_text}'")
            # Добавим немного контекста для анализа цвета, если есть поле content
            content_text = data.get("content", "")
            if content_text:
                text_for_analysis = f"{topic_text}\n\n{content_text[:200]}"
            else:
                text_for_analysis = topic_text

    except json.JSONDecodeError:
        logger.error(f"Ошибка: Некорректный JSON в файле {json_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Ошибка чтения JSON файла {json_path}: {e}", exc_info=True)
        sys.exit(1)

    # 3. Получение размеров изображения
    try:
        from PIL import Image
        with Image.open(image_path) as img:
            img_width, img_height = img.size
        logger.info(f"Размеры изображения: {img_width}x{img_height}")
    except ImportError:
        logger.error("Ошибка: Библиотека Pillow не найдена. Невозможно получить размеры.")
        # Устанавливаем размеры по умолчанию, чтобы тест мог продолжить работу
        # но без реального вызова Vision API
        img_width, img_height = 1280, 720 # Примерные размеры
        logger.warning("Используются размеры по умолчанию. Вызов Vision API будет пропущен.")
        placement_suggestions = {
            "position": ('center', 'center'),
            "font_size": DEFAULT_FONT_SIZE,
            "formatted_text": topic_text, # Используем исходный topic
            "text_color": DEFAULT_TEXT_COLOR
        }
    except Exception as e:
        logger.error(f"Ошибка получения размеров изображения: {e}", exc_info=True)
        sys.exit(1)

    # 4. Получение рекомендаций от ИИ (если Pillow доступен)
    if 'placement_suggestions' not in locals(): # Если не были установлены из-за ошибки Pillow
        placement_suggestions = get_local_text_placement_suggestions(
            image_path=image_path,
            text=text_for_analysis, # Передаем текст с контекстом
            image_width=img_width,
            image_height=img_height
        )

    # 5. Вызов функции add_text_to_image
    logger.info(f"Вызов add_text_to_image для сохранения в {output_path}...")
    success = add_text_to_image(
        image_path_str=str(image_path),
        text=placement_suggestions["formatted_text"],
        font_path_str=str(font_path),
        output_path_str=str(output_path),
        font_size=placement_suggestions["font_size"],
        text_color_hex=placement_suggestions["text_color"],
        position=placement_suggestions["position"], # Должно быть ('center', 'center')
        haze_opacity=HAZE_OPACITY,
        # Остальные параметры по умолчанию (без доп. фона/размытия)
        logger_instance=logger
    )

    if success:
        logger.info(f"✅ Тест завершен успешно! Результат сохранен в: {output_path}")
    else:
        logger.error("❌ Тест завершился с ошибкой при создании изображения.")

