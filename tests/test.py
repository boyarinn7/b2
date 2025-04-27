import os
import json
import base64
import re
import logging
from pathlib import Path
import sys
import openai
import httpx
import io
from datetime import datetime

# --- Импорт Pillow (нужен для работы скрипта) ---
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("!!! ОШИБКА: Библиотека Pillow (PIL) не найдена. Скрипт не может работать. !!!")
    print("!!! Установите: pip install Pillow !!!")
    sys.exit(1)

# --- Настройка Логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_title_standalone")

# === Глобальная переменная для клиента OpenAI ===
openai_client_instance = None

# --- Константы и Пути ---
# Используйте необработанные строки (r"...") для путей Windows
FONT_PATH_STR = r"C:\Users\boyar\b2\fonts\Alice-Regular.ttf"
OUTPUT_FOLDER_STR = r"C:\Users\boyar\777\555"

# Параметры изображения
IMG_WIDTH = 1280
IMG_HEIGHT = 720
BACKGROUND_COLOR = (255, 255, 255, 255) # Белый RGBA

# --- Встроенный Промпт для OpenAI Vision ---
# (Последняя версия, запрашивает переносы и цвет, размер НЕ запрашивает)
VISION_PROMPT_TEMPLATE = """
Analyze the text provided below (it's a post title). Assume it will be placed centrally on an image ({image_width}x{image_height}) that has a semi-transparent white overlay (haze) for readability.

Text to analyze (post title):
"{text}"

Your tasks:
1.  **Line Breaks:** Break the title into lines using '\\n'. Aim for roughly 3 significant words per line (ignore short prepositions/conjunctions like 'a', 'the', 'in', 'of', 'and'). If the title is short (e.g., 5-6 words or less), 1 or 2 lines is acceptable. If it's longer, strongly prefer 3 lines. Ensure the meaning is preserved.
2.  **Text Color:** Analyze the theme of the title (e.g., empires, sea, war, mystery). Suggest an appropriate, vibrant and saturated text color as a hex code string (e.g., rich gold like \"#DAA520\" for empires, deep sea blue like \"#00008B\" for sea, blood red like \"#8B0000\" for war, dark violet like \"#483D8B\" for mystery). The color should contrast well with a white haze and stand out clearly. Avoid overly bright or neon colors unless the theme strongly suggests it. If no specific theme color comes to mind, suggest black \"#000000\".

Respond ONLY with a valid JSON object containing the keys:
* `position`: MUST be `[\"center\", \"center\"]`.
* `formatted_text`: The title text with '\\n' line breaks applied according to the rules above.
* `text_color`: The suggested hex color code string.

Example JSON response (for a long title):
{{
  "position": ["center", "center"],
  "formatted_text": "Тайны великих империй:\\\\nнеизвестные главы\\\\nдревней истории?",
  "text_color": "#DAA520"
}}
Example JSON response (for a short title):
{{\n  \"position\": [\"center\", \"center\"],\n  \"formatted_text\": \"Морские\\\\nСражения\",\n  \"text_color\": \"#00008B\"\n}}\n
"""

# --- Параметры для add_text_to_image ---
HAZE_OPACITY = 128
DEFAULT_TEXT_COLOR = "#000000" # Черный по умолчанию

# === Скопированные Вспомогательные Функции ===

def hex_to_rgba(hex_color, alpha=255):
    """Конвертирует HEX цвет (#RRGGBB) в кортеж RGBA."""
    hex_color = hex_color.lstrip('#')
    default_color = (0, 0, 0, alpha) # Черный
    if len(hex_color) != 6:
        logger.warning(f"Некорректный HEX цвет '{hex_color}'. Используется черный.")
        return default_color
    try:
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        return rgb + (alpha,)
    except ValueError:
        logger.warning(f"Не удалось сконвертировать HEX '{hex_color}'. Используется черный.")
        return default_color

def add_text_to_image(
    image_path_str: str,
    text: str, # Текст с переносами \n от ИИ
    font_path_str: str,
    output_path_str: str,
    text_color_hex: str = "#000000", # Цвет по умолчанию черный
    position: tuple = ('center', 'center'),
    padding: int = 50,
    haze_opacity: int = 100,
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
    Наносит текст на изображение с автоподбором размера шрифта (с порогом для 3+ строк),
    добавляя белую "дымку" и обводку текста.
    """
    log = logger_instance if logger_instance else logger
    log.debug(">>> Вход в add_text_to_image (автоподбор v4 - крупнее)")

    if not PIL_AVAILABLE or Image is None:
        log.error("Библиотека Pillow недоступна. Невозможно добавить текст.")
        return False

    try:
        base_image_path = Path(image_path_str)
        font_path = Path(font_path_str)
        output_path = Path(output_path_str)
        log.debug(f"Пути: image={base_image_path}, font={font_path}, output={output_path}")

        if not base_image_path.is_file():
            log.error(f"Исходное изображение не найдено: {base_image_path}")
            return False
        if not font_path.is_file():
            log.error(f"Файл шрифта не найден: {font_path}")
            return False
        log.debug("Файлы изображения и шрифта найдены.")

        log.info(f"Открытие изображения: {base_image_path.name}")
        img = Image.open(base_image_path).convert("RGBA")
        img_width, img_height = img.size
        log.debug(f"Изображение открыто: {img_width}x{img_height}, режим={img.mode}")

        if haze_opacity > 0:
            log.info(f"Добавление белой дымки (прозрачность: {haze_opacity})...")
            haze_layer = Image.new('RGBA', img.size, (255, 255, 255, haze_opacity))
            log.debug("Наложение слоя дымки...")
            img = Image.alpha_composite(img, haze_layer)
            log.debug("Белая дымка добавлена.")
        else:
            log.debug("Дымка отключена (haze_opacity=0).")

        draw = ImageDraw.Draw(img)
        log.debug("Объект ImageDraw создан/обновлен.")

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
             log.error(f"Ошибка чтения файла шрифта '{font_path}': {read_font_err}", exc_info=True)
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
                    log.error("Не удалось подобрать размер шрифта.")
                    return False
        else:
            log.warning(f"Не удалось вместить текст в {target_width_fraction*100:.0f}% ширины даже с минимальным размером {current_min_font_size}. Используется минимальный размер.")
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_min_font_size)
                bbox_multiline = draw.textbbox((0, 0), text, font=font)
                text_width = bbox_multiline[2] - bbox_multiline[0]
                text_height = bbox_multiline[3] - bbox_multiline[1]
                current_font_size = current_min_font_size
            except Exception as min_font_err:
                log.error(f"Ошибка при загрузке/расчете минимального шрифта {current_min_font_size}: {min_font_err}", exc_info=True)
                return False

        final_font_size = current_font_size
        log.debug(f"Финальный размер шрифта: {final_font_size}")

        log.debug("Расчет финальных размеров и позиции...")
        try:
            bbox_multiline = draw.textbbox((0, 0), text, font=font)
            text_width = bbox_multiline[2] - bbox_multiline[0]
            text_height = bbox_multiline[3] - bbox_multiline[1]
            log.debug(f"Финальные размеры текста: Ширина={text_width:.0f}, Высота={text_height:.0f}")
        except Exception as final_size_err:
             log.error(f"Ошибка при расчете финального размера текста: {final_size_err}", exc_info=True)
             return False

        if text_width <= 0 or text_height <= 0:
            log.error(f"Рассчитаны некорректные финальные размеры текста: {text_width}x{text_height}")
            return False

        log.debug("Расчет X координаты...")
        x = (img_width - text_width) / 2
        log.debug(f"X = {x}")
        log.debug("Расчет Y координаты...")
        y = (img_height - text_height) / 2
        log.debug(f"Y = {y}")
        text_position = (int(x), int(y))
        log.info(f"Позиция текста (левый верхний угол): {text_position}")

        if bg_blur_radius > 0 or bg_opacity > 0:
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
                bg_right = min(img_width, text_position[0] + (bbox_multiline_at_pos[2] - bbox_multiline_at_pos[0]) + bg_padding)
                bg_bottom = min(img_height, text_position[1] + (bbox_multiline_at_pos[3] - bbox_multiline_at_pos[1]) + bg_padding)
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
                 log.info(f"Создание полупрозрачной подложки под текстом (opacity: {bg_opacity}) в области {bg_rect_coords}")
                 overlay_color = (0, 0, 0, bg_opacity)
                 log.debug(f"Рисование прямоугольника подложки цветом {overlay_color}...")
                 draw_bg.rectangle(bg_rect_coords, fill=overlay_color)
                 log.debug("Подложка нарисована.")
            log.debug("Наложение слоя фона на основное изображение...")
            img = Image.alpha_composite(img, background_layer)
            draw = ImageDraw.Draw(img)
            log.debug("Слой фона наложен, ImageDraw обновлен.")
        else:
            log.debug("Эффекты фона под текстом отключены.")

        log.debug(f"Конвертация HEX цвета текста: {text_color_hex}")
        final_text_color = hex_to_rgba(text_color_hex, alpha=240)
        log.debug(f"Финальный цвет текста (RGBA): {final_text_color}")

        final_stroke_color = hex_to_rgba(stroke_color_hex, alpha=240)
        log.debug(f"Цвет обводки (RGBA): {final_stroke_color}, Ширина: {stroke_width}")

        log_text_preview = text[:50].replace('\n', '\\n')
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {final_text_color} (HEX: {text_color_hex}) с обводкой (размер: {final_font_size})")

        align_option = 'center' if position[0] == 'center' else 'left'
        log.debug(f"Выравнивание текста: {align_option}")
        try:
             log.debug(f"Вызов draw.text с позицией {text_position}...")
             draw.text(
                 text_position,
                 text,
                 font=font,
                 fill=final_text_color,
                 align=align_option,
                 stroke_width=stroke_width,
                 stroke_fill=final_stroke_color
             )
             log.debug("draw.text выполнен (с обводкой).")
        except Exception as draw_err:
             log.error(f"Ошибка при вызове draw.text: {draw_err}", exc_info=True)
             return False

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

# Скопированная функция get_text_placement_suggestions (без изменений размера шрифта)
def get_text_placement_suggestions(
    # *** ИЗМЕНЕНИЕ: image_url больше не обязателен ***
    # image_url: str,
    text: str, # Текст для анализа (только тема)
    image_width: int,
    image_height: int
) -> dict:
    """
    Использует GPT-4o Vision для получения рекомендаций по переносам строк и цвету,
    загружая промпт из встроенной константы. Размер шрифта НЕ запрашивается.
    НЕ требует URL изображения.
    """
    global openai_client_instance
    logger.info(f"Получение рекомендаций (строки, цвет) для текста: {text[:50]}...")

    # Параметры по умолчанию
    default_suggestions = {
        "position": ('center', 'center'), # Фиксировано
        "formatted_text": text, # Исходный текст без переносов по умолчанию
        "text_color": DEFAULT_TEXT_COLOR # Черный по умолчанию
    }

    # *** ИЗМЕНЕНИЕ: Убрана проверка image_url ***
    if not text:
        logger.warning("Текст отсутствует. Возврат стандартных параметров.")
        return default_suggestions

    # Инициализация клиента OpenAI, если нужно
    if not openai_client_instance:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("Переменная окружения OPENAI_API_KEY не задана!")
            return default_suggestions
        try:
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

    # Формируем промпт
    prompt = VISION_PROMPT_TEMPLATE.format(
        image_width=image_width,
        image_height=image_height,
        text=text # Передаем только тему
    )

    # *** ИЗМЕНЕНИЕ: Отправляем только текст ***
    messages_content = [
        {"type": "text", "text": prompt}
        # {"type": "image_url", "image_url": {"url": image_url}} # <<< Закомментировано
    ]
    # *** КОНЕЦ ИЗМЕНЕНИЯ ***

    try:
        logger.info("Запрос к OpenAI (gpt-4o) для рекомендаций (строки, цвет)...") # Убрано "Vision"
        response = openai_client_instance.chat.completions.create(
            model="gpt-4o", # Можно использовать и не-Vision модель
            messages=[{"role": "user", "content": messages_content}], # Передаем только текст
            max_tokens=300,
            temperature=0.5,
            response_format={"type": "json_object"}
        )

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            response_text = response.choices[0].message.content.strip()
            logger.debug(f"Сырой ответ от OpenAI (JSON): {response_text}")

            try:
                suggestions = json.loads(response_text)
                # Валидация
                fmt_text = suggestions.get("formatted_text")
                color_hex = suggestions.get("text_color")

                # Проверка текста
                valid_text = default_suggestions["formatted_text"] # По умолчанию
                if isinstance(fmt_text, str) and fmt_text.strip():
                    valid_text = fmt_text.replace('\\n', '\n')
                else:
                    logger.warning(f"Получен некорректный форматированный текст: {fmt_text}. Используется исходный.")

                # Проверка цвета
                valid_color = default_suggestions["text_color"] # По умолчанию
                if isinstance(color_hex, str) and re.match(r'^#[0-9a-fA-F]{6}$', color_hex):
                    valid_color = color_hex
                else:
                    logger.warning(f"Некорректный HEX цвет: {color_hex}. Используется {valid_color}.")

                log_text_preview = valid_text[:50].replace('\n', '\\n')
                logger.info(f"Получены рекомендации от ИИ: Цвет={valid_color}, Текст='{log_text_preview}...'")
                # Возвращаем ТОЛЬКО текст и цвет
                return {
                    "position": ('center', 'center'), # Всегда центр
                    "formatted_text": valid_text,
                    "text_color": valid_color
                    # font_size больше не возвращаем
                }

            except json.JSONDecodeError as json_e:
                logger.error(f"Ошибка декодирования JSON: {json_e}. Ответ: {response_text}")
                return default_suggestions
            except Exception as parse_err:
                logger.error(f"Ошибка парсинга рекомендаций: {parse_err}", exc_info=True)
                return default_suggestions
        else:
            logger.error("OpenAI вернул пустой ответ.")
            return default_suggestions

    except Exception as e:
        logger.error(f"Ошибка при вызове OpenAI: {e}", exc_info=True)
        return default_suggestions


# === Основная Логика Теста ===
if __name__ == "__main__":
    logger.info("--- Запуск тестового скрипта генерации титульника ---")

    # Запрашиваем фразу у пользователя
    input_phrase = input("Введите фразу для титульника: ")
    if not input_phrase:
        logger.error("Ошибка: Фраза не может быть пустой.")
        sys.exit(1)

    font_path = Path(FONT_PATH_STR)
    output_dir = Path(OUTPUT_FOLDER_STR)
    output_dir.mkdir(parents=True, exist_ok=True) # Создаем папку, если ее нет

    # Генерируем имя файла на основе времени
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"title_test_{timestamp}.png"
    output_path = output_dir / output_filename

    # 1. Проверка наличия шрифта
    if not font_path.is_file():
        logger.error(f"Ошибка: Файл шрифта не найден - {font_path}")
        sys.exit(1)

    # 2. Создание белого фона
    try:
        logger.info(f"Создание белого фона {IMG_WIDTH}x{IMG_HEIGHT}...")
        white_bg = Image.new('RGBA', (IMG_WIDTH, IMG_HEIGHT), BACKGROUND_COLOR)
        # Сохраняем временно, т.к. add_text_to_image ожидает путь
        temp_bg_path = output_dir / f"temp_bg_{timestamp}.png"
        white_bg.save(temp_bg_path)
        logger.info(f"Временный фон сохранен: {temp_bg_path}")
    except Exception as e:
        logger.error(f"Ошибка создания белого фона: {e}", exc_info=True)
        sys.exit(1)


    # 3. Получение рекомендаций от ИИ (переносы и цвет)
    # *** ИЗМЕНЕНИЕ: URL больше не передаем ***
    placement_suggestions = get_text_placement_suggestions(
        # image_url=placeholder_url, # <<< УДАЛЕНО
        text=input_phrase,         # Передаем введенную фразу
        image_width=IMG_WIDTH,
        image_height=IMG_HEIGHT
    )
    # *** КОНЕЦ ИЗМЕНЕНИЯ ***

    # 4. Вызов функции add_text_to_image
    logger.info(f"Вызов add_text_to_image для сохранения в {output_path}...")
    success = add_text_to_image(
        image_path_str=str(temp_bg_path), # Используем временный белый фон
        text=placement_suggestions["formatted_text"], # Текст с переносами от ИИ
        font_path_str=str(font_path),
        output_path_str=str(output_path), # Путь для сохранения финального результата
        # font_size - удален, подбирается внутри
        text_color_hex=placement_suggestions["text_color"], # Цвет от ИИ
        position=placement_suggestions["position"], # ('center', 'center')
        haze_opacity=0, # Дымка не нужна на белом фоне
        # Остальные параметры по умолчанию
        logger_instance=logger
    )

    # 5. Очистка временного фона
    try:
        os.remove(temp_bg_path)
        logger.info(f"Временный фон удален: {temp_bg_path}")
    except OSError as e:
        logger.warning(f"Не удалось удалить временный фон {temp_bg_path}: {e}")

    if success:
        logger.info(f"✅ Тест завершен успешно! Результат сохранен в: {output_path}")
    else:
        logger.error("❌ Тест завершился с ошибкой при создании изображения.")

