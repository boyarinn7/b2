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

# --- Логгер ---
# Используем стандартный logging, если get_logger не доступен
try:
    from modules.logger import get_logger
    logger = get_logger("sarcasm_image_utils")
except ImportError:
    logger = logging.getLogger("sarcasm_image_utils")
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        logger.warning("Кастомный логгер не найден, используется стандартный logging.")
    # Установим уровень DEBUG для отладки этой функции, если нужно
    if logger.level > logging.DEBUG:
         logger.setLevel(logging.DEBUG)

# Вспомогательная функция
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

# Основная функция (исправленная версия)
def add_text_to_image_sarcasm(
    image_path_str: str,
    text: str,
    font_path_str: str,
    output_path_str: str,
    text_color_hex: str = "#FFFFFF",
    align: str = 'right', # Выравнивание текста внутри блока
    vertical_align: str = 'center', # Используется для расчета смещения
    padding: int = 25, # Уменьшенный отступ
    stroke_width: int = 2,
    stroke_color_hex: str = "#404040", # Обводка для белого текста
    initial_font_size: int = 100, # Стартовый размер шрифта
    min_font_size: int = 40,      # Увеличенный минимальный размер шрифта
    logger_instance=None # Возможность передать логгер извне
    ):
    """
    Наносит текст на ПРАВУЮ ПОЛОВИНУ изображения с автоподбором размера,
    выравниванием по правому краю и смещением вверх относительно центра.
    Позиционирование по левому верхнему углу блока. Добавлено логирование.
    """
    log = logger_instance if logger_instance else logger
    if log.level > logging.DEBUG: log.setLevel(logging.DEBUG) # Убедимся, что DEBUG включен
    log.info(">>> Запуск add_text_to_image_sarcasm (исправление позиции v2 + логи)")

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
        log.debug(f"Размеры изображения: {img_width}x{img_height}")

        draw = ImageDraw.Draw(img)

        # 1. Определить область для текста
        text_area_x_start = img_width // 2 + padding
        text_area_y_start = padding
        text_area_width = img_width // 2 - 2 * padding
        text_area_height = img_height - 2 * padding
        log.info(f"Область текста: X={text_area_x_start}, Y={text_area_y_start}, W={text_area_width}, H={text_area_height}")

        if text_area_width <= 0 or text_area_height <= 0:
            log.error("Некорректная область текста."); return False

        # 2. Автоподбор размера шрифта
        font = None
        font_bytes = None
        try:
            with open(font_path, 'rb') as f_font: font_bytes = f_font.read()
            log.debug(f"Шрифт '{font_path.name}' прочитан ({len(font_bytes)} байт).")
        except Exception as read_font_err:
             log.error(f"Ошибка чтения шрифта '{font_path}': {read_font_err}"); return False

        current_font_size = initial_font_size
        best_font_size = -1
        best_wrapped_lines = []

        log.info(f"Подбор размера шрифта (старт: {initial_font_size}, мин: {min_font_size}) для текста: '{text[:50]}...'")
        # --- НАЧАЛО ЦИКЛА ПОДБОРА ---
        while current_font_size >= min_font_size:
            log.debug(f"--- Итерация цикла: Пробуем размер = {current_font_size} ---")
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_font_size)
                max_line_width_calc = 0
                current_lines = []
                words = text.split()
                if not words: log.warning("Текст пуст."); current_lines = []; break
                current_line = words[0]
                # --- Цикл разбивки на строки ---
                for word_idx, word in enumerate(words[1:], 1):
                    test_line = current_line + " " + word
                    line_width = font.getlength(test_line)
                    log.debug(f"  Проверка строки (слово {word_idx+1}): '{test_line[:40]}...' -> Ширина: {line_width:.1f} (лимит: {text_area_width})")
                    if line_width <= text_area_width:
                        current_line = test_line
                    else:
                        log.debug(f"  -> Перенос строки. Завершаем строку: '{current_line[:40]}...'")
                        current_lines.append(current_line)
                        current_line_width = font.getlength(current_line)
                        if current_line_width > max_line_width_calc: max_line_width_calc = current_line_width
                        current_line = word
                        single_word_width = font.getlength(current_line)
                        if single_word_width > text_area_width:
                             log.debug(f"  !!! Слово '{current_line}' [{single_word_width:.0f}px] шире области [{text_area_width}px] при размере {current_font_size} !!!")
                             max_line_width_calc = single_word_width; break
                # --- Конец цикла разбивки на строки ---
                if current_line: # Добавляем последнюю строку
                    log.debug(f"  Добавляем последнюю строку: '{current_line[:40]}...'")
                    current_lines.append(current_line)
                    current_line_width = font.getlength(current_line)
                    if current_line_width > max_line_width_calc: max_line_width_calc = current_line_width
                # --- Проверка ширины ---
                if max_line_width_calc > text_area_width:
                     log.debug(f"Размер {current_font_size} НЕ подходит по ШИРИНЕ (из-за слова). Уменьшаем...")
                     current_font_size -= 2; continue
                width_fits = max_line_width_calc <= text_area_width
                log.debug(f"Расчетная макс. ширина строки: {max_line_width_calc:.1f}. Ширина помещается? {width_fits}")
                if not width_fits:
                    log.debug(f"Размер {current_font_size} НЕ подходит по ШИРИНЕ. Уменьшаем...")
                    current_font_size -= 2; continue
                # --- Проверка высоты ---
                text_height_calc = 0
                if current_lines:
                    multiline_text_check = "\n".join(current_lines)
                    try: text_height_calc = font.getmask(multiline_text_check).size[1]
                    except AttributeError:
                        log.warning("font.getmask недоступен. Используем bbox для высоты.")
                        bbox_check = font.getbbox(multiline_text_check); text_height_calc = bbox_check[3] - bbox_check[1]
                    except Exception as mask_err:
                        log.warning(f"Ошибка font.getmask: {mask_err}. Используем bbox.")
                        bbox_check = font.getbbox(multiline_text_check); text_height_calc = bbox_check[3] - bbox_check[1]
                height_fits = text_height_calc <= text_area_height
                log.debug(f"Расчетная высота текста: {text_height_calc:.1f}, Доступная высота: {text_area_height}. Высота помещается? {height_fits}")
                # --- Проверка обоих условий ---
                if width_fits and height_fits:
                    log.info(f"Найден ПОДХОДЯЩИЙ размер шрифта: {current_font_size}")
                    best_font_size = current_font_size; best_wrapped_lines = current_lines; break
                else:
                    log.debug(f"Размер {current_font_size} НЕ подходит по ВЫСОТЕ. Уменьшаем...")
                    current_font_size -= 2
            except Exception as size_err:
                 log.error(f"Ошибка при расчете размера {current_font_size}: {size_err}", exc_info=True)
                 current_font_size -= 2
        # --- КОНЕЦ ЦИКЛА ПОДБОРА ---
        if best_font_size == -1:
            log.warning(f"Цикл завершен БЕЗ НАХОЖДЕНИЯ подходящего размера. Используем минимальный {min_font_size}.")
            best_font_size = min_font_size
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
                # Финальная разбивка с минимальным размером
                max_line_width_calc = 0; current_lines = []; words = text.split(); current_line = words[0] if words else ""
                for word in words[1:]:
                    test_line = current_line + " " + word; line_width = font.getlength(test_line)
                    if line_width <= text_area_width: current_line = test_line
                    else:
                        current_lines.append(current_line); current_line_width = font.getlength(current_line)
                        if current_line_width > max_line_width_calc: max_line_width_calc = current_line_width
                        current_line = word
                if current_line: current_lines.append(current_line)
                best_wrapped_lines = current_lines
                log.debug(f"Разбивка на строки с минимальным шрифтом ({min_font_size}):\n" + "\n".join(best_wrapped_lines))
            except Exception as min_font_err:
                 log.error(f"Критическая ошибка при работе с минимальным шрифтом: {min_font_err}"); return False
        if not best_wrapped_lines:
            log.error("Не удалось подготовить текст для нанесения."); return False

        final_text_string = "\n".join(best_wrapped_lines)
        font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
        log.info(f"Финальный размер шрифта: {best_font_size}")

        # 3. Расчет финальных координат X, Y
        try:
            bbox_final = font.getbbox(final_text_string)
            final_text_width = bbox_final[2] - bbox_final[0]
            final_text_height = font.getmask(final_text_string).size[1]
            top_offset = bbox_final[1]
            log.debug(f"Финальные размеры блока: Ширина={final_text_width:.1f}, Высота={final_text_height:.1f}, Смещение верха={top_offset}")
        except Exception as final_bbox_err:
             log.error(f"Ошибка финального расчета bbox/mask: {final_bbox_err}"); return False

        # Расчет X для выравнивания по правому краю (координата ЛЕВОГО края блока)
        x = text_area_x_start + text_area_width - final_text_width
        log.debug(f"Расчетная X (левый край блока): {x}")

        # Расчет Y с вертикальным смещением вверх
        base_y_center = text_area_y_start + (text_area_height - final_text_height) / 2
        vertical_offset = best_font_size // 2 # Смещение вверх на половину размера шрифта
        y = max(text_area_y_start, base_y_center - vertical_offset) # Y - верхняя граница блока
        log.debug(f"Расчетная Y (верхний край блока): {y}, Смещение: {vertical_offset}")

        text_position = (int(x), int(y))
        log.info(f"Финальная позиция текста (левый верх блока): {text_position}")

        # 4. Нанесение текста
        final_text_color = hex_to_rgba(text_color_hex)
        final_stroke_color = hex_to_rgba(stroke_color_hex)
        log_text_preview = final_text_string[:50].replace('\n', '\\n')
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {text_color_hex} с обводкой")

        try:
             # Используем align='right' для выравнивания строк внутри блока
             # Позиция text_position задает левый верхний угол блока
             draw.text(
                 text_position,
                 final_text_string,
                 font=font,
                 fill=final_text_color,
                 align='right', # Выравнивание строк по правому краю
                 stroke_width=stroke_width,
                 stroke_fill=final_stroke_color
             )
             log.debug("draw.text выполнен.")
        except Exception as draw_err:
             log.error(f"Ошибка вызова draw.text: {draw_err}", exc_info=True); return False

        # 5. Сохранение результата
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            img.save(output_path, format='PNG')
            log.info(f"✅ Изображение с текстом сарказма сохранено: {output_path.name}")
            return True
        except Exception as save_err:
             log.error(f"Ошибка сохранения изображения {output_path}: {save_err}"); return False

    except Exception as e:
        log.error(f"Критическая ошибка в add_text_to_image_sarcasm: {e}", exc_info=True)
        return False

