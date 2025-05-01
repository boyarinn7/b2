# -*- coding: utf-8 -*-
# modules/sarcasm_image_utils.py
"""
Вспомогательные функции для создания изображения с саркастическим комментарием.
"""
import logging
from pathlib import Path
import io
import textwrap # Импортируем textwrap

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
try:
    from modules.logger import get_logger
    logger = get_logger("sarcasm_image_utils")
except ImportError:
    logger = logging.getLogger("sarcasm_image_utils")
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        logger.warning("Кастомный логгер не найден, используется стандартный logging.")
    if logger.level > logging.DEBUG:
         logger.setLevel(logging.DEBUG)

# Вспомогательная функция
#def hex_to_rgba(hex_color, alpha=255):
#    """Конвертирует HEX цвет (#RRGGBB) в кортеж RGBA."""
#    hex_color = hex_color.lstrip('#')
#    default_color = (0, 0, 0, alpha) # Черный по умолчанию
#    if len(hex_color) != 6:
#        logger.warning(f"Некорректный HEX цвет '{hex_color}'. Используется черный.")
#        return default_color
#    try:
#        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
#        return rgb + (alpha,)
#    except ValueError:
#        logger.warning(f"Не удалось сконвертировать HEX '{hex_color}'. Используется черный.")
#        return default_color

# --- ИСПРАВЛЕННАЯ ВЕРСИЯ ФУНКЦИИ ---
def add_text_to_image_sarcasm(
    image_path_str: str,
    text: str,
    font_path_str: str,
    output_path_str: str,
    text_color_hex: str = "#FFFFFF",
    align: str = 'right', # Выравнивание текста внутри блока
    vertical_align: str = 'center', # Используется для расчета смещения
    padding_fraction: float = 0.05, # Отступ как доля от ширины/высоты
    stroke_width: int = 2,
    stroke_color_hex: str = "#404040", # Обводка для белого текста
    initial_font_size: int = 100, # Стартовый размер шрифта
    min_font_size: int = 24,      # Минимальный размер шрифта
    logger_instance=None # Возможность передать логгер извне
    ):
    """
    Наносит текст на ПРАВУЮ ПОЛОВИНУ изображения с автоподбором размера,
    выравниванием по правому краю и вертикальным центрированием.
    Использует textwrap для корректной разбивки на строки.
    """
    log = logger_instance if logger_instance else logger
    if log.level > logging.DEBUG: log.setLevel(logging.DEBUG)
    log.info(">>> Запуск add_text_to_image_sarcasm (v3 - textwrap, fix position)")

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

        # 1. Определить область для текста (правая половина с отступами)
        padding_x = int(img_width * padding_fraction)
        padding_y = int(img_height * padding_fraction)
        text_area_x_start = img_width // 2 + padding_x
        text_area_y_start = padding_y
        text_area_width = img_width // 2 - 2 * padding_x
        text_area_height = img_height - 2 * padding_y
        log.info(f"Область текста: X={text_area_x_start}, Y={text_area_y_start}, W={text_area_width}, H={text_area_height}")

        if text_area_width <= 0 or text_area_height <= 0:
            log.error("Некорректная область текста (слишком маленькая или отрицательная)."); return False

        # 2. Автоподбор размера шрифта с использованием textwrap
        font = None
        font_bytes = None
        try:
            with open(font_path, 'rb') as f_font: font_bytes = f_font.read()
            log.debug(f"Шрифт '{font_path.name}' прочитан ({len(font_bytes)} байт).")
        except Exception as read_font_err:
             log.error(f"Ошибка чтения шрифта '{font_path}': {read_font_err}"); return False

        current_font_size = initial_font_size
        best_font_size = -1
        best_wrapped_text = ""

        log.info(f"Подбор размера шрифта (старт: {initial_font_size}, мин: {min_font_size}) для текста: '{text[:50]}...'")
        # --- НАЧАЛО ЦИКЛА ПОДБОРА ---
        while current_font_size >= min_font_size:
            log.debug(f"--- Итерация цикла: Пробуем размер = {current_font_size} ---")
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_font_size)

                # Оценка максимальной ширины символа для textwrap
                # Можно взять среднюю ширину или ширину самого широкого символа (например, 'W' или 'Ш')
                avg_char_width = font.getlength("W") # Или другая широкая буква
                if avg_char_width <= 0: avg_char_width = current_font_size * 0.6 # Примерный fallback
                # Рассчитываем примерное количество символов в строке
                approx_chars_per_line = max(1, int(text_area_width / avg_char_width))
                log.debug(f"  Примерная ширина символа: {avg_char_width:.1f}, символов в строке: {approx_chars_per_line}")

                # Используем textwrap для разбивки
                wrapped_lines = textwrap.wrap(text, width=approx_chars_per_line, replace_whitespace=False)
                wrapped_text = "\n".join(wrapped_lines)
                log.debug(f"  Результат textwrap:\n{wrapped_text}")

                # Получаем реальные размеры блока текста с переносами
                try:
                    # Используем textbbox для получения габаритов
                    bbox = draw.textbbox((0, 0), wrapped_text, font=font, align=align)
                    text_width = bbox[2] - bbox[0]
                    text_height = bbox[3] - bbox[1]
                    log.debug(f"  Размер {current_font_size}: Ширина={text_width:.0f}, Высота={text_height:.0f}")
                except Exception as bbox_err:
                    log.warning(f"  Ошибка расчета bbox для размера {current_font_size}: {bbox_err}. Пропускаем размер.")
                    current_font_size -= 2
                    continue

                # Проверяем, помещается ли текст в область
                if text_width <= text_area_width and text_height <= text_area_height:
                    log.info(f"Найден ПОДХОДЯЩИЙ размер шрифта: {current_font_size}")
                    best_font_size = current_font_size
                    best_wrapped_text = wrapped_text
                    break # Найден оптимальный размер
                else:
                    log.debug(f"  Размер {current_font_size} НЕ подходит (Ширина: {text_width > text_area_width}, Высота: {text_height > text_area_height}). Уменьшаем...")
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
                avg_char_width = font.getlength("W")
                if avg_char_width <= 0: avg_char_width = best_font_size * 0.6
                approx_chars_per_line = max(1, int(text_area_width / avg_char_width))
                wrapped_lines = textwrap.wrap(text, width=approx_chars_per_line, replace_whitespace=False)
                best_wrapped_text = "\n".join(wrapped_lines)
                log.debug(f"Разбивка на строки с минимальным шрифтом ({min_font_size}):\n" + best_wrapped_text)
            except Exception as min_font_err:
                 log.error(f"Критическая ошибка при работе с минимальным шрифтом: {min_font_err}"); return False

        if not best_wrapped_text:
            log.error("Не удалось подготовить текст для нанесения (best_wrapped_text пуст)."); return False

        final_text_string = best_wrapped_text
        font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
        log.info(f"Финальный размер шрифта: {best_font_size}")

        # 3. Расчет финальных координат X, Y
        try:
            # Получаем точные размеры финального текстового блока
            bbox_final = draw.textbbox((0, 0), final_text_string, font=font, align=align)
            final_text_width = bbox_final[2] - bbox_final[0]
            final_text_height = bbox_final[3] - bbox_final[1]
            log.debug(f"Финальные размеры блока текста: Ширина={final_text_width:.1f}, Высота={final_text_height:.1f}")
        except Exception as final_bbox_err:
             log.error(f"Ошибка финального расчета bbox: {final_bbox_err}"); return False

        # Расчет X для выравнивания по правому краю
        # Координата X - это правый край области минус ширина текста
        x = text_area_x_start + text_area_width - final_text_width
        log.debug(f"Расчетная X (левый край блока): {x}")

        # Расчет Y для вертикального центрирования
        y = text_area_y_start + (text_area_height - final_text_height) / 2
        log.debug(f"Расчетная Y (верхний край блока): {y}")

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
                 align=align, # Используем переданное выравнивание ('right')
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
# --- КОНЕЦ ИСПРАВЛЕННОЙ ВЕРСИИ ---

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

# --- ОБНОВЛЕННАЯ ФУНКЦИЯ ---
def add_text_to_image_sarcasm_openai_ready(
    image_path_str: str,
    formatted_text: str, # Текст УЖЕ с переносами строк '\n' (от OpenAI)
    suggested_font_size: int, # Размер шрифта, предложенный OpenAI
    font_path_str: str,
    output_path_str: str,
    text_color_hex: str = "#FFFFFF",
    align: str = 'right', # Выравнивание текста внутри блока
    vertical_align: str = 'center', # Используется для расчета смещения Y
    padding_fraction: float = 0.05, # Отступ как доля от ширины/высоты
    stroke_width: int = 2,
    stroke_color_hex: str = "#404040", # Обводка для белого текста
    logger_instance=None # Возможность передать логгер извне
    ):
    """
    Наносит ПРЕДВАРИТЕЛЬНО ОТФОРМАТИРОВАННЫЙ текст (с переносами \n)
    на ПРАВУЮ ПОЛОВИНУ изображения с ЗАДАННЫМ размером шрифта.
    Выравнивание по правому краю и вертикальное центрирование.
    """
    log = logger_instance if logger_instance else logger
    if log.level > logging.DEBUG: log.setLevel(logging.DEBUG)
    log.info(">>> Запуск add_text_to_image_sarcasm_openai_ready (v4)")
    log.info(f"Получен текст:\n{formatted_text}")
    log.info(f"Предложенный размер шрифта: {suggested_font_size}")

    if not PIL_AVAILABLE:
        log.error("Pillow недоступна. Невозможно добавить текст.")
        return False
    if not formatted_text or not formatted_text.strip():
        log.error("Получен пустой текст для нанесения.")
        return False
    if suggested_font_size <= 0:
        log.error(f"Получен некорректный размер шрифта: {suggested_font_size}")
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

        # 1. Определить область для текста (правая половина с отступами) - БЕЗ ИЗМЕНЕНИЙ
        padding_x = int(img_width * padding_fraction)
        padding_y = int(img_height * padding_fraction)
        text_area_x_start = img_width // 2 + padding_x
        text_area_y_start = padding_y
        text_area_width = img_width // 2 - 2 * padding_x
        text_area_height = img_height - 2 * padding_y
        log.info(f"Область текста: X={text_area_x_start}, Y={text_area_y_start}, W={text_area_width}, H={text_area_height}")

        if text_area_width <= 0 or text_area_height <= 0:
            log.error("Некорректная область текста (слишком маленькая или отрицательная)."); return False

        # 2. Загрузка шрифта с предложенным размером
        font = None
        font_bytes = None
        try:
            with open(font_path, 'rb') as f_font: font_bytes = f_font.read()
            font = ImageFont.truetype(io.BytesIO(font_bytes), suggested_font_size)
            log.info(f"Шрифт '{font_path.name}' загружен с размером {suggested_font_size}.")
        except Exception as font_load_err:
             log.error(f"Ошибка загрузки шрифта '{font_path}' с размером {suggested_font_size}: {font_load_err}"); return False

        # 3. Расчет финальных координат X, Y на основе размеров блока с ЗАДАННЫМ шрифтом
        try:
            # Получаем точные размеры текстового блока с переносами
            bbox_final = draw.textbbox((0, 0), formatted_text, font=font, align=align)
            final_text_width = bbox_final[2] - bbox_final[0]
            final_text_height = bbox_final[3] - bbox_final[1]
            log.debug(f"Финальные размеры блока текста (шрифт {suggested_font_size}): Ширина={final_text_width:.1f}, Высота={final_text_height:.1f}")

            # --- Проверка на выход за границы (дополнительно) ---
            if final_text_width > text_area_width or final_text_height > text_area_height:
                log.warning(f"Текст с предложенным размером {suggested_font_size} выходит за границы области! "
                            f"W: {final_text_width:.0f}/{text_area_width}, H: {final_text_height:.0f}/{text_area_height}. "
                            f"Текст может быть обрезан.")
            # ----------------------------------------------------

        except Exception as final_bbox_err:
             log.error(f"Ошибка расчета bbox для шрифта {suggested_font_size}: {final_bbox_err}"); return False

        # Расчет X для выравнивания по правому краю
        x = text_area_x_start + text_area_width - final_text_width
        log.debug(f"Расчетная X (левый край блока): {x}")

        # Расчет Y для вертикального центрирования
        y = text_area_y_start + (text_area_height - final_text_height) / 2
        log.debug(f"Расчетная Y (верхний край блока): {y}")

        # Корректируем координаты, чтобы текст не уходил за пределы картинки (на всякий случай)
        final_x = max(0, int(x))
        final_y = max(0, int(y))
        text_position = (final_x, final_y)
        log.info(f"Финальная позиция текста (левый верх блока): {text_position}")

        # 4. Нанесение текста
        final_text_color = hex_to_rgba(text_color_hex)
        final_stroke_color = hex_to_rgba(stroke_color_hex)
        log_text_preview = formatted_text[:50].replace('\n', '\\n')
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {text_color_hex} с обводкой (размер: {suggested_font_size})")

        try:
             # Используем align='right' для выравнивания строк внутри блока
             # Позиция text_position задает левый верхний угол блока
             draw.text(
                 text_position,
                 formatted_text, # Используем текст с переносами от AI
                 font=font,      # Используем шрифт с размером от AI
                 fill=final_text_color,
                 align=align, # Используем переданное выравнивание ('right')
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
        log.error(f"Критическая ошибка в add_text_to_image_sarcasm_openai_ready: {e}", exc_info=True)
        return False
# --- КОНЕЦ ОБНОВЛЕННОЙ ФУНКЦИИ ---

# --- Старая функция (можно удалить или оставить для сравнения) ---
# def add_text_to_image_sarcasm(...): ...


