import os
import json
import base64
import re
import logging
from pathlib import Path
import sys
import io # <-- Добавили для BytesIO
import argparse # <-- Добавили для аргументов командной строки

# --- Pillow ---
# Пытаемся импортировать Pillow, он будет нужен для скопированной функции
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    # Определяем заглушки, чтобы код ниже не падал
    Image = None
    ImageDraw = None
    ImageFont = None
    ImageFilter = None
    print("!!! ВНИМАНИЕ: Библиотека Pillow (PIL) не найдена. Установите: pip install Pillow !!!")
    # Можно либо прервать выполнение, либо продолжить без возможности рисования
    # sys.exit(1)

# --- Настройка Логирования ---
# Устанавливаем уровень INFO, чтобы убрать лишние DEBUG сообщения
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger("test_sarcasm_image")


# ===============================================================
# === КОПИЯ ФУНКЦИИ add_text_to_image ИЗ utils.py (ИСПРАВЛЕННАЯ) ===
# ===============================================================

# Вспомогательная функция (тоже скопирована)
def hex_to_rgba(hex_color, alpha=255):
    """Конвертирует HEX цвет (#RRGGBB) в кортеж RGBA."""
    hex_color = hex_color.lstrip('#')
    default_color = (0, 0, 0, alpha) # Черный по умолчанию
    if len(hex_color) != 6:
        logger.warning(f"Некорректный HEX цвет '{hex_color}'. Используется черный.")
        return default_color
    try:
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        return rgb + (alpha,)
    except ValueError:
        logger.warning(f"Не удалось сконвертировать HEX '{hex_color}'. Используется черный.")
        return default_color

# Основная скопированная функция (Исправлена ошибка с anchor)
def add_text_to_image_sarcasm(
    image_path_str: str,
    text: str,
    font_path_str: str,
    output_path_str: str,
    text_color_hex: str = "#FFFFFF",
    align: str = 'right', # <-- Выравнивание текста внутри блока
    vertical_align: str = 'center', # <-- Вертикальное выравнивание блока текста
    padding: int = 40, # Отступы от краев правой зоны
    stroke_width: int = 2,
    stroke_color_hex: str = "#404040", # Обводка для белого текста
    initial_font_size: int = 100, # Можно настроить стартовый размер
    min_font_size: int = 30,      # Минимальный размер
    logger_instance=None
    ):
    """
    (КОПИЯ ИЗ UTILS - ИСПРАВЛЕНА ОШИБКА ANCHOR)
    Наносит текст на ПРАВУЮ ПОЛОВИНУ изображения с автоподбором размера,
    выравниванием по правому краю и вертикальным центрированием.
    """
    log = logger_instance if logger_instance else logger
    log.info(">>> Запуск add_text_to_image_sarcasm")

    if not PIL_AVAILABLE:
        log.error("Pillow недоступна. Невозможно добавить текст.")
        return False

    try:
        base_image_path = Path(image_path_str)
        font_path = Path(font_path_str)
        output_path = Path(output_path_str)

        if not base_image_path.is_file(): log.error(f"Изображение не найдено: {base_image_path}"); return False
        if not font_path.is_file(): log.error(f"Шрифт не найден: {font_path}"); return False

        log.info(f"Открытие изображения: {base_image_path.name}")
        img = Image.open(base_image_path).convert("RGBA")
        img_width, img_height = img.size
        log.debug(f"Изображение: {img_width}x{img_height}, режим={img.mode}") # Оставим один debug для размера

        draw = ImageDraw.Draw(img)

        # 1. Определить область для текста (правая половина с отступами)
        text_area_x_start = img_width // 2 + padding
        text_area_y_start = padding
        text_area_width = img_width // 2 - 2 * padding
        text_area_height = img_height - 2 * padding
        log.info(f"Область для текста: X={text_area_x_start}, Y={text_area_y_start}, W={text_area_width}, H={text_area_height}")

        if text_area_width <= 0 or text_area_height <= 0:
            log.error("Рассчитана некорректная область для текста (слишком маленькая).")
            return False

        # 2. Автоподбор размера шрифта для вписывания в text_area_width (с переносами)
        font = None
        font_bytes = None
        try:
            # Читаем шрифт в память один раз
            with open(font_path, 'rb') as f_font:
                font_bytes = f_font.read()
        except Exception as read_font_err:
             log.error(f"Ошибка чтения шрифта '{font_path}': {read_font_err}"); return False

        current_font_size = initial_font_size
        best_font_size = -1
        best_wrapped_lines = []

        log.info(f"Начало подбора размера шрифта (старт: {initial_font_size}, мин: {min_font_size})")
        while current_font_size >= min_font_size:
            try:
                log.debug(f"Пробуем размер: {current_font_size}...") # Оставим debug для размера
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_font_size)

                # --- Логика разбиения на строки и проверки размеров ---
                max_line_width = 0
                current_lines = []
                words = text.split()
                if not words:
                    log.warning("Текст для нанесения пуст.")
                    current_lines = []
                    max_line_width = 0
                    break

                current_line = words[0]
                for word in words[1:]:
                    test_line = current_line + " " + word
                    line_width = draw.textlength(test_line, font=font)
                    if line_width <= text_area_width:
                        current_line = test_line
                    else:
                        current_lines.append(current_line)
                        current_line_width = draw.textlength(current_line, font=font)
                        if current_line_width > max_line_width: max_line_width = current_line_width
                        current_line = word
                        single_word_width = draw.textlength(current_line, font=font)
                        if single_word_width > text_area_width:
                             log.warning(f"Слово '{current_line}' шире области ({single_word_width} > {text_area_width}) при размере {current_font_size}. Уменьшаем шрифт.")
                             max_line_width = single_word_width
                             break
                if current_line:
                    current_lines.append(current_line)
                    current_line_width = draw.textlength(current_line, font=font)
                    if current_line_width > max_line_width: max_line_width = current_line_width

                if max_line_width > text_area_width:
                    current_font_size -= 2
                    continue

                if not current_lines: text_height = 0
                else:
                    multiline_text_check = "\n".join(current_lines)
                    # --- ИСПРАВЛЕНИЕ: Убираем anchor ---
                    bbox_check = draw.textbbox((0, 0), multiline_text_check, font=font, align=align)
                    text_height = bbox_check[3] - bbox_check[1]
                    log.debug(f"Размер {current_font_size}: Ширина={max_line_width:.0f}, Высота={text_height:.0f}")

                if max_line_width <= text_area_width and text_height <= text_area_height:
                    log.info(f"Найден подходящий размер шрифта: {current_font_size}")
                    best_font_size = current_font_size
                    best_wrapped_lines = current_lines
                    break
                else:
                    log.debug(f"Размер {current_font_size} не подходит (Ширина: {max_line_width} > {text_area_width} или Высота: {text_height} > {text_area_height}). Уменьшаем.")
                    current_font_size -= 2

            except Exception as size_err:
                 # Логируем ошибку, но не выводим полный traceback, если это ошибка Pillow
                 if "anchor not supported" in str(size_err):
                     log.error(f"Ошибка Pillow при расчете размера {current_font_size}: {size_err}")
                 else:
                     log.error(f"Ошибка при расчете размера {current_font_size}: {size_err}", exc_info=True)
                 current_font_size -= 2

        # --- Конец цикла подбора размера ---

        if best_font_size == -1:
            log.warning(f"Не удалось вместить текст в область даже с минимальным размером {min_font_size}. Используем минимальный.")
            best_font_size = min_font_size
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
                max_line_width = 0; current_lines = []; words = text.split(); current_line = words[0] if words else ""
                for word in words[1:]:
                    test_line = current_line + " " + word; line_width = draw.textlength(test_line, font=font)
                    if line_width <= text_area_width: current_line = test_line
                    else:
                        current_lines.append(current_line); current_line_width = draw.textlength(current_line, font=font)
                        if current_line_width > max_line_width: max_line_width = current_line_width
                        current_line = word
                if current_line: current_lines.append(current_line)
                best_wrapped_lines = current_lines
            except Exception as min_font_err:
                 log.error(f"Критическая ошибка при работе с минимальным шрифтом: {min_font_err}"); return False

        if not best_wrapped_lines:
            log.error("Не удалось подготовить текст для нанесения (возможно, исходный текст пуст).")
            return False

        final_text_string = "\n".join(best_wrapped_lines)
        font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
        log.info(f"Финальный размер шрифта: {best_font_size}")

        # 3. Расчет финальных координат X, Y
        try:
            # --- ИСПРАВЛЕНИЕ: Убираем anchor ---
            bbox_final = draw.textbbox((0, 0), final_text_string, font=font, align=align)
            final_text_width = bbox_final[2] - bbox_final[0]
            final_text_height = bbox_final[3] - bbox_final[1]
            log.debug(f"Финальные размеры текстового блока: W={final_text_width}, H={final_text_height}")
        except Exception as final_bbox_err:
             log.error(f"Ошибка финального расчета bbox: {final_bbox_err}"); return False

        x = text_area_x_start + text_area_width - final_text_width
        if vertical_align == 'center': y = text_area_y_start + (text_area_height - final_text_height) / 2
        elif vertical_align == 'top': y = text_area_y_start
        elif vertical_align == 'bottom': y = text_area_y_start + text_area_height - final_text_height
        else: y = text_area_y_start + (text_area_height - final_text_height) / 2
        text_position = (int(x), int(y))
        log.info(f"Финальная позиция текста (левый верх блока): {text_position}")

        # --- Нанесение текста ---
        final_text_color = hex_to_rgba(text_color_hex)
        final_stroke_color = hex_to_rgba(stroke_color_hex)
        log_text_preview = final_text_string[:50].replace('\n', '\\n')
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {final_text_color} (HEX: {text_color_hex}) с обводкой (размер: {best_font_size})")

        try:
             # --- ИСПРАВЛЕНИЕ: Убираем anchor ---
             draw.text(
                 text_position,
                 final_text_string,
                 font=font,
                 fill=final_text_color,
                 align=align,
                 stroke_width=stroke_width,
                 stroke_fill=final_stroke_color
                 # anchor убран
             )
             log.debug("draw.text выполнен.")
        except Exception as draw_err:
             log.error(f"Ошибка вызова draw.text: {draw_err}"); return False

        # --- Сохранение результата ---
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            img.save(output_path, format='PNG')
            log.info(f"✅ Изображение с текстом сохранено: {output_path.name}")
            log.info("<<< Выход из add_text_to_image_sarcasm (Успех)")
            return True
        except Exception as save_err:
             log.error(f"Ошибка сохранения изображения {output_path}: {save_err}"); return False

    except Exception as e:
        log.error(f"Критическая ошибка в add_text_to_image_sarcasm: {e}", exc_info=True)
        return False

# ===============================================================
# === КОНЕЦ КОПИИ ФУНКЦИИ ===
# ===============================================================


# === Константы и Пути (Обновленные) ===
# Путь к папке, где лежат файлы
BASE_FOLDER_PATH = r"C:\Users\boyar\777\555" # <-- Базовый путь

# Имена файлов по умолчанию (можно переопределить через аргументы, но оставим для простоты)
IMAGE_FILENAME = "Барон.png"
FONT_FILENAME = "Kurale-Regular.ttf"
OUTPUT_FILENAME_SUFFIX = "_с_комментарием_test.png" # Суффикс для имени выходного файла

# === Основная Логика Теста ===
if __name__ == "__main__":
    # --- Парсинг аргументов ---
    parser = argparse.ArgumentParser(description='Тестовый скрипт для наложения сарказма на изображение Барона.')
    parser.add_argument('--json-file', type=str, required=True, help='Имя JSON файла с данными (например, 20250427-1348.json)')
    args = parser.parse_args()
    json_filename = args.json_file
    # --- Конец парсинга ---

    logger.info(f"--- Запуск тестового скрипта для JSON: {json_filename} ---")

    # --- Формирование полных путей ---
    base_folder = Path(BASE_FOLDER_PATH)
    image_path = base_folder / IMAGE_FILENAME
    json_path = base_folder / json_filename # Используем имя из аргумента
    font_path = base_folder / FONT_FILENAME
    # Формируем имя выходного файла на основе входного JSON
    output_filename = Path(json_filename).stem + OUTPUT_FILENAME_SUFFIX
    output_path = base_folder / output_filename
    # --- Конец формирования путей ---

    # 1. Проверка наличия файлов
    if not image_path.is_file(): logger.error(f"Изображение не найдено: {image_path}"); sys.exit(1)
    if not json_path.is_file(): logger.error(f"JSON не найден: {json_path}"); sys.exit(1)
    if not font_path.is_file(): logger.error(f"Шрифт не найден: {font_path}"); sys.exit(1)
    if not PIL_AVAILABLE: logger.error("Библиотека Pillow не установлена, тест невозможен."); sys.exit(1)

    # 2. Загрузка и парсинг комментария из JSON (с обработкой двух форматов)
    comment_text = None
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            sarcasm_data = data.get("sarcasm")
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
                            # Ищем текст в словаре
                            comment_text = parsed_comment_value.get("comment") or parsed_comment_value.get("комментарий")
                            if comment_text:
                                logger.info("Комментарий извлечен из словаря внутри JSON-строки.")
                            else:
                                logger.warning("В словаре JSON-строки 'sarcasm.comment' не найден ключ 'comment'/'комментарий'.")
                                comment_text = None # Сбрасываем, если ключ не найден
                        else:
                            # json.loads вернул не словарь (например, строку)
                            if isinstance(parsed_comment_value, str):
                                logger.info("JSON-строка 'sarcasm.comment' содержит простую строку, используем ее.")
                                comment_text = parsed_comment_value
                            else:
                                logger.warning(f"JSON-строка 'sarcasm.comment' содержит не строку и не словарь: {type(parsed_comment_value)}. Игнорируем.")
                                comment_text = None
                    except json.JSONDecodeError:
                        # Если парсинг как JSON не удался, считаем исходное значение простой строкой
                        logger.info("Значение 'sarcasm.comment' не JSON, используется как простая строка.")
                        comment_text = comment_value.strip('"') # Убираем лишние кавычки
                    # --- КОНЕЦ ИСПРАВЛЕННОЙ ЛОГИКИ ---
                elif comment_value is not None:
                     logger.warning(f"Значение 'sarcasm.comment' не строка: {type(comment_value)}")
                else:
                     logger.warning("Ключ 'sarcasm.comment' отсутствует.")
            else:
                logger.warning("Ключ 'sarcasm' отсутствует или не словарь.")

            if not comment_text:
                logger.error(f"Не удалось извлечь текст комментария из {json_path}")
                sys.exit(1)
            logger.info(f"Загружен текст комментария: '{comment_text}'")

    except json.JSONDecodeError: logger.error(f"Некорректный JSON в {json_path}"); sys.exit(1)
    except Exception as e: logger.error(f"Ошибка чтения {json_path}: {e}"); sys.exit(1)

    # 3. Вызов функции add_text_to_image_sarcasm (которая теперь внутри этого скрипта)
    logger.info(f"Вызов add_text_to_image_sarcasm для сохранения в {output_path}...")
    # Вызываем доработанную функцию
    success = add_text_to_image_sarcasm(
        image_path_str=str(image_path),
        text=comment_text,
        font_path_str=str(font_path),
        output_path_str=str(output_path),
        text_color_hex="#FFFFFF", # Белый цвет
        align='right',            # Выравнивание по правому краю
        vertical_align='center',  # Вертикально по центру правой половины
        padding=40,               # Отступы (можно настроить)
        initial_font_size=80,    # Уменьшил стартовый размер для теста
        min_font_size=24         # Уменьшил минимальный размер для теста
    )

    if success:
        logger.info(f"✅ Тест завершен. Проверьте результат: {output_path}")
    else:
        logger.error("❌ Тест завершился с ошибкой при создании изображения.")

