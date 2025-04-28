# -*- coding: utf-8 -*-
# modules/sarcasm_image_utils.py
"""
Вспомогательные функции для создания изображения с саркастическим комментарием.
"""
import logging
from pathlib import Path
import io

# --- Pillow ---
try:
    from PIL import Image, ImageDraw, ImageFont, ImageFilter
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    Image = None
    ImageDraw = None
    ImageFont = None
    ImageFilter = None
    # Логирование ошибки будет в вызывающем коде, если PIL недоступен

# Получаем логгер для этого модуля
# Используем стандартный logging, если get_logger не доступен при прямом импорте
try:
    # Попытка импортировать кастомный логгер
    from modules.logger import get_logger
    logger = get_logger("sarcasm_image_utils")
except ImportError:
    # Fallback на стандартный logging
    logger = logging.getLogger("sarcasm_image_utils")
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        logger.warning("Кастомный логгер не найден в sarcasm_image_utils, используется стандартный logging.")


# Вспомогательная функция (копия из тестового скрипта)
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

# Основная функция (копия из тестового скрипта с исправлениями)
def add_text_to_image_sarcasm(
    image_path_str: str,
    text: str,
    font_path_str: str,
    output_path_str: str,
    text_color_hex: str = "#FFFFFF",
    align: str = 'right', # Выравнивание текста внутри блока
    vertical_align: str = 'center', # Вертикальное выравнивание блока текста
    padding: int = 40, # Отступы от краев правой зоны
    stroke_width: int = 2,
    stroke_color_hex: str = "#404040", # Обводка для белого текста
    initial_font_size: int = 100, # Стартовый размер шрифта
    min_font_size: int = 30,      # Минимальный размер шрифта
    logger_instance=None # Возможность передать логгер извне
    ):
    """
    Наносит текст на ПРАВУЮ ПОЛОВИНУ изображения с автоподбором размера,
    выравниванием по правому краю и вертикальным центрированием.
    """
    log = logger_instance if logger_instance else logger # Используем переданный или локальный логгер
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

        log.info(f"Открытие базового изображения: {base_image_path.name}")
        img = Image.open(base_image_path).convert("RGBA")
        img_width, img_height = img.size
        log.debug(f"Размеры изображения: {img_width}x{img_height}, режим={img.mode}")

        draw = ImageDraw.Draw(img)

        # 1. Определить область для текста (правая половина с отступами)
        text_area_x_start = img_width // 2 + padding
        text_area_y_start = padding
        text_area_width = img_width // 2 - 2 * padding
        text_area_height = img_height - 2 * padding
        log.info(f"Расчетная область для текста: X={text_area_x_start}, Y={text_area_y_start}, W={text_area_width}, H={text_area_height}")

        if text_area_width <= 0 or text_area_height <= 0:
            log.error("Рассчитана некорректная область для текста (слишком маленькая).")
            return False

        # 2. Автоподбор размера шрифта
        font = None
        font_bytes = None
        try:
            with open(font_path, 'rb') as f_font:
                font_bytes = f_font.read()
        except Exception as read_font_err:
             log.error(f"Ошибка чтения файла шрифта '{font_path}': {read_font_err}"); return False

        current_font_size = initial_font_size
        best_font_size = -1
        best_wrapped_lines = []

        log.info(f"Подбор размера шрифта (старт: {initial_font_size}, мин: {min_font_size}) для текста: '{text[:50]}...'")
        while current_font_size >= min_font_size:
            try:
                log.debug(f"Пробуем размер шрифта: {current_font_size}")
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_font_size)

                # Логика разбиения на строки
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
                             log.debug(f"Слово '{current_line}' шире области ({single_word_width:.0f} > {text_area_width}) при размере {current_font_size}.")
                             max_line_width = single_word_width
                             break
                if current_line:
                    current_lines.append(current_line)
                    current_line_width = draw.textlength(current_line, font=font)
                    if current_line_width > max_line_width: max_line_width = current_line_width

                if max_line_width > text_area_width:
                    log.debug(f"Размер {current_font_size} слишком велик по ширине ({max_line_width:.0f} > {text_area_width}). Уменьшаем.")
                    current_font_size -= 2
                    continue

                # Проверка высоты
                if not current_lines: text_height = 0
                else:
                    multiline_text_check = "\n".join(current_lines)
                    bbox_check = draw.textbbox((0, 0), multiline_text_check, font=font, align=align)
                    text_height = bbox_check[3] - bbox_check[1]

                if text_height <= text_area_height:
                    log.info(f"Найден подходящий размер шрифта: {current_font_size}")
                    best_font_size = current_font_size
                    best_wrapped_lines = current_lines
                    break
                else:
                    log.debug(f"Размер {current_font_size} слишком велик по высоте ({text_height:.0f} > {text_area_height}). Уменьшаем.")
                    current_font_size -= 2

            except Exception as size_err:
                 log.error(f"Ошибка при расчете размера {current_font_size}: {size_err}", exc_info=True)
                 current_font_size -= 2

        # --- Конец цикла подбора ---

        if best_font_size == -1:
            log.warning(f"Не удалось вместить текст в область. Используем минимальный размер {min_font_size}.")
            best_font_size = min_font_size
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
                # Повторная разбивка с минимальным размером
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
            log.error("Не удалось подготовить текст для нанесения.")
            return False

        final_text_string = "\n".join(best_wrapped_lines)
        font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
        log.info(f"Финальный размер шрифта: {best_font_size}")

        # 3. Расчет финальных координат X, Y
        try:
            bbox_final = draw.textbbox((0, 0), final_text_string, font=font, align=align)
            final_text_width = bbox_final[2] - bbox_final[0]
            final_text_height = bbox_final[3] - bbox_final[1]
        except Exception as final_bbox_err:
             log.error(f"Ошибка финального расчета bbox: {final_bbox_err}"); return False

        # Координата X для выравнивания по правому краю области
        x = text_area_x_start + text_area_width - final_text_width
        # Координата Y
        if vertical_align == 'center': y = text_area_y_start + (text_area_height - final_text_height) / 2
        elif vertical_align == 'top': y = text_area_y_start
        elif vertical_align == 'bottom': y = text_area_y_start + text_area_height - final_text_height
        else: y = text_area_y_start + (text_area_height - final_text_height) / 2
        text_position = (int(x), int(y))
        log.info(f"Финальная позиция текста (левый верх блока): {text_position}")

        # 4. Нанесение текста
        final_text_color = hex_to_rgba(text_color_hex)
        final_stroke_color = hex_to_rgba(stroke_color_hex)
        log_text_preview = final_text_string[:50].replace('\n', '\\n')
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {text_color_hex} с обводкой")

        try:
             draw.text(
                 text_position,
                 final_text_string,
                 font=font,
                 fill=final_text_color,
                 align=align,
                 stroke_width=stroke_width,
                 stroke_fill=final_stroke_color
             )
        except Exception as draw_err:
             log.error(f"Ошибка вызова draw.text: {draw_err}"); return False

        # 5. Сохранение результата
        try:
            # Убедимся, что папка для сохранения существует
            output_path.parent.mkdir(parents=True, exist_ok=True)
            img.save(output_path, format='PNG')
            log.info(f"✅ Изображение с текстом сарказма сохранено: {output_path.name}")
            return True
        except Exception as save_err:
             log.error(f"Ошибка сохранения изображения {output_path}: {save_err}"); return False

    except Exception as e:
        log.error(f"Критическая ошибка в add_text_to_image_sarcasm: {e}", exc_info=True)
        return False

