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

# --- Логгер ---
# Используем стандартный logging, если get_logger не доступен при прямом импорте
# или для обеспечения работы отладки независимо от конфигурации основного логгера.
logger = logging.getLogger("sarcasm_image_utils_debug") # Используем уникальное имя для отладки
# Устанавливаем уровень DEBUG и форматтер, если обработчики еще не добавлены
if not logger.hasHandlers():
    # Настраиваем базовую конфигурацию для вывода DEBUG сообщений в консоль
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    # Добавляем обработчик в консоль, если его еще нет
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG) # Устанавливаем уровень DEBUG
    logger.propagate = False # Предотвращаем дублирование в root логгере
    logger.debug("Стандартный логгер настроен на уровень DEBUG для sarcasm_image_utils.")


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

# Основная функция с добавленным логированием
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
    выравниванием по правому краю, смещением вверх и детальным логированием.
    """
    # Используем переданный логгер или логгер этого модуля
    log = logger_instance if logger_instance else logger
    # Убедимся, что у используемого логгера уровень DEBUG
    if log.level > logging.DEBUG:
        log.setLevel(logging.DEBUG)
        log.debug("Уровень логгера временно установлен на DEBUG для add_text_to_image_sarcasm.")

    log.info(">>> Запуск add_text_to_image_sarcasm (с верт. смещением и DEBUG логом)")

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

        # 1. Определить область для текста
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
            # Читаем байты шрифта один раз
            with open(font_path, 'rb') as f_font:
                font_bytes = f_font.read()
            log.debug(f"Файл шрифта '{font_path.name}' прочитан в память ({len(font_bytes)} байт).")
        except Exception as read_font_err:
             log.error(f"Ошибка чтения файла шрифта '{font_path}': {read_font_err}"); return False

        current_font_size = initial_font_size
        best_font_size = -1
        best_wrapped_lines = []

        log.info(f"Подбор размера шрифта (старт: {initial_font_size}, мин: {min_font_size}) для текста: '{text[:50]}...'")

        # --- НАЧАЛО ЦИКЛА ПОДБОРА ---
        while current_font_size >= min_font_size:
            log.debug(f"--- Итерация цикла: Пробуем размер = {current_font_size} ---") # DEBUG
            try:
                # Создаем объект шрифта из байтов
                font = ImageFont.truetype(io.BytesIO(font_bytes), current_font_size)

                # --- Логика разбиения на строки ---
                max_line_width_calc = 0 # Рассчитанная максимальная ширина строки
                current_lines = []
                words = text.split()
                if not words:
                    log.warning("Текст для нанесения пуст.")
                    current_lines = []
                    max_line_width_calc = 0
                    break # Выходим из while, если текст пуст

                current_line = words[0]
                for word in words[1:]:
                    test_line = current_line + " " + word
                    # Используем getlength для расчета ширины
                    line_width = font.getlength(test_line)
                    if line_width <= text_area_width:
                        # Слово помещается, добавляем к текущей строке
                        current_line = test_line
                    else:
                        # Слово не помещается, завершаем текущую строку
                        current_lines.append(current_line)
                        # Обновляем максимальную ширину, если текущая строка длиннее
                        current_line_width = font.getlength(current_line)
                        if current_line_width > max_line_width_calc:
                            max_line_width_calc = current_line_width
                        # Начинаем новую строку с текущего слова
                        current_line = word
                        # Проверяем, не шире ли само слово доступной области
                        single_word_width = font.getlength(current_line)
                        if single_word_width > text_area_width:
                             log.debug(f"!!! Слово '{current_line}' [{single_word_width:.0f}px] шире области [{text_area_width}px] при размере {current_font_size} !!!")
                             max_line_width_calc = single_word_width # Ширина текста равна ширине самого длинного слова
                             break # Выходим из цикла for word, т.к. даже одно слово не влезает

                # Добавляем последнюю собранную строку (если она есть)
                if current_line:
                    current_lines.append(current_line)
                    current_line_width = font.getlength(current_line)
                    if current_line_width > max_line_width_calc:
                        max_line_width_calc = current_line_width

                # Если break был из-за слишком широкого слова, max_line_width_calc будет > text_area_width
                if max_line_width_calc > text_area_width:
                     log.debug(f"Размер {current_font_size} НЕ подходит по ШИРИНЕ (из-за слова). Уменьшаем...")
                     current_font_size -= 2
                     continue # Переходим к следующей итерации while

                # --- Проверка ширины САМОЙ ДЛИННОЙ СТРОКИ ---
                width_fits = max_line_width_calc <= text_area_width
                log.debug(f"Расчетная макс. ширина строки: {max_line_width_calc:.1f}, Доступная ширина: {text_area_width}. Ширина помещается? {width_fits}")

                if not width_fits:
                    # Это условие не должно срабатывать, если сработал break из-за слова, но на всякий случай
                    log.debug(f"Размер {current_font_size} НЕ подходит по ШИРИНЕ. Уменьшаем...")
                    current_font_size -= 2
                    continue # Переходим к следующей итерации while

                # --- Если ширина подходит, проверяем высоту ---
                text_height_calc = 0 # Рассчитанная высота текста
                if current_lines:
                    multiline_text_check = "\n".join(current_lines)
                    try:
                        # Используем getmask для более точной высоты
                        text_height_calc = font.getmask(multiline_text_check).size[1]
                    except AttributeError:
                        # Fallback для старых версий Pillow или проблем с getmask
                        log.warning("font.getmask недоступен или вызвал ошибку. Используем bbox для высоты.")
                        bbox_check = font.getbbox(multiline_text_check)
                        text_height_calc = bbox_check[3] - bbox_check[1]
                    except Exception as mask_err:
                        log.warning(f"Ошибка font.getmask для расчета высоты: {mask_err}. Используем bbox.")
                        bbox_check = font.getbbox(multiline_text_check)
                        text_height_calc = bbox_check[3] - bbox_check[1]

                height_fits = text_height_calc <= text_area_height
                log.debug(f"Расчетная высота текста: {text_height_calc:.1f}, Доступная высота: {text_area_height}. Высота помещается? {height_fits}")

                # --- Проверяем оба условия ---
                if width_fits and height_fits:
                    log.info(f"Найден ПОДХОДЯЩИЙ размер шрифта: {current_font_size}")
                    best_font_size = current_font_size
                    best_wrapped_lines = current_lines
                    break # <--- ВЫХОДИМ ИЗ ЦИКЛА WHILE, РАЗМЕР НАЙДЕН
                else:
                    # Если не подошло по высоте (ширина уже проверена)
                    log.debug(f"Размер {current_font_size} НЕ подходит по ВЫСОТЕ. Уменьшаем...")
                    current_font_size -= 2

            except Exception as size_err:
                 log.error(f"Ошибка при расчете размера {current_font_size}: {size_err}", exc_info=True)
                 current_font_size -= 2 # Уменьшаем в случае ошибки

        # --- КОНЕЦ ЦИКЛА ПОДБОРА ---

        if best_font_size == -1:
            # Если цикл завершился без break (т.е. дошли до min_font_size и он не подошел)
            log.warning(f"Цикл завершен БЕЗ НАХОЖДЕНИЯ подходящего размера. Используем минимальный {min_font_size}.")
            best_font_size = min_font_size
            try:
                font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
                # Финальная разбивка с минимальным размером (повтор логики для получения best_wrapped_lines)
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
            except Exception as min_font_err:
                 log.error(f"Критическая ошибка при работе с минимальным шрифтом: {min_font_err}"); return False

        if not best_wrapped_lines:
            # Это может случиться, если исходный текст был пуст
            log.error("Не удалось подготовить текст для нанесения (возможно, текст пуст).")
            return False

        # Собираем финальный текст и создаем объект шрифта с лучшим размером
        final_text_string = "\n".join(best_wrapped_lines)
        font = ImageFont.truetype(io.BytesIO(font_bytes), best_font_size)
        log.info(f"Финальный размер шрифта: {best_font_size}")

        # 3. Расчет финальных координат X, Y
        try:
            # Используем getbbox для финального расчета ширины (он быстрее getmask)
            bbox_final = font.getbbox(final_text_string)
            # bbox возвращает (left, top, right, bottom)
            final_text_width = bbox_final[2] - bbox_final[0]
            # Используем getmask для точной высоты
            final_text_height = font.getmask(final_text_string).size[1]
            log.debug(f"Финальные размеры блока текста: Ширина={final_text_width:.1f}, Высота={final_text_height:.1f}")
        except Exception as final_bbox_err:
             log.error(f"Ошибка финального расчета bbox/mask: {final_bbox_err}"); return False

        # Координата X для выравнивания по правому краю области
        # Позиция X будет правым краем области текста
        x_anchor = text_area_x_start + text_area_width

        # Расчет координаты Y с вертикальным смещением
        base_y_center = text_area_y_start + (text_area_height - final_text_height) / 2
        vertical_offset = best_font_size // 2 # Смещение вверх на половину размера шрифта
        y_anchor = base_y_center - vertical_offset # Y-координата для привязки (верхняя)
        # Убедимся, что текст не выходит за верхнюю границу области
        y_anchor = max(text_area_y_start, y_anchor)

        # text_position теперь не используется напрямую в draw.text с anchor
        log.info(f"Координаты привязки (anchor='ra'): X={x_anchor}, Y={int(y_anchor)} (смещено вверх на {vertical_offset}px)")

        # 4. Нанесение текста
        final_text_color = hex_to_rgba(text_color_hex)
        final_stroke_color = hex_to_rgba(stroke_color_hex)
        log_text_preview = final_text_string[:50].replace('\n', '\\n')
        log.info(f"Нанесение текста '{log_text_preview}...' цветом {text_color_hex} с обводкой")

        try:
             # Используем anchor='ra' (Right Ascender)
             # Позиция (x_anchor, y_anchor) указывает точку привязки шрифта
             draw.text(
                 (x_anchor, int(y_anchor)), # Точка привязки
                 final_text_string,
                 font=font,
                 fill=final_text_color,
                 anchor="ra", # Привязка по правому верхнему краю (Ascender)
                 align='right', # Выравнивание строк между собой
                 stroke_width=stroke_width,
                 stroke_fill=final_stroke_color
             )
             log.debug("draw.text с anchor='ra' выполнен.")
        except Exception as draw_err:
             log.error(f"Ошибка вызова draw.text: {draw_err}", exc_info=True); return False

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

