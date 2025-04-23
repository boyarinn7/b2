# --- Начало scripts/b2_storage_manager.py ---
print("--- SCRIPT START ---", flush=True) # Оставим для отладки самого старта
import os
import json
import logging
import subprocess
import re
import sys
import time
import argparse
import io
from datetime import datetime, timezone, timedelta

# Импорты из ваших модулей
try:
    # Используем относительные пути для импорта из той же папки/пакета
    # Убедитесь, что структура папок позволяет такие импорты
    # Если запускаете как скрипт из корневой папки, абсолютные импорты могут работать лучше
    # Пример абсолютного импорта (если BASE_DIR настроен правильно):
    # from modules.utils import ...
    # Пример относительного (если запускается как часть пакета):
    # from ..modules.utils import ...
    # Оставляем как было, предполагая запуск из папки, где modules доступны
    from modules.utils import (
        is_folder_empty, ensure_directory_exists, generate_file_id,
        load_b2_json, save_b2_json, list_files_in_folder, # Добавлен list_files_in_folder
        move_b2_object, delete_b2_object # Добавлены move/delete
    )
    from modules.api_clients import get_b2_client
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.config_manager import ConfigManager
except ModuleNotFoundError as import_err:
    # Логируем ошибку перед выходом
    logging.basicConfig(level=logging.ERROR) # Базовый логгер на случай ошибки
    logging.error(f"Критическая ошибка: Не найдены модули проекта: {import_err}", exc_info=True)
    print(f"Критическая ошибка: Не найдены модули проекта: {import_err}")
    sys.exit(1)
except ImportError as import_err:
    logging.basicConfig(level=logging.ERROR)
    logging.error(f"Критическая ошибка импорта: {import_err}", exc_info=True)
    print(f"Критическая ошибка импорта: {import_err}")
    sys.exit(1)


# Импорт boto3 и его исключений
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    print("Ошибка: Необходима библиотека boto3.")
    sys.exit(1)

print("--- IMPORTS DONE ---", flush=True)

# === Инициализация конфигурации и логирования ===
try:
    config = ConfigManager()
    print("--- CONFIG MANAGER INIT DONE ---", flush=True)
    logger = get_logger("b2_storage_manager")
    print("--- LOGGER INIT DONE ---", flush=True)
    logger.info("Logger is now active.")
except Exception as init_err:
    # Используем print, так как логгер мог не инициализироваться
    print(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}", file=sys.stderr)
    sys.exit(1)


# === Константы ===
try:
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))
    if not B2_BUCKET_NAME: raise ValueError("B2_BUCKET_NAME не определен")

    CONFIG_PUBLIC_REMOTE_PATH = config.get('FILE_PATHS.config_public', "config/config_public.json")
    CONFIG_GEN_REMOTE_PATH = config.get('FILE_PATHS.config_gen', "config/config_gen.json")
    CONFIG_MJ_REMOTE_PATH = config.get('FILE_PATHS.config_midjourney', "config/config_midjourney.json")

    # Локальные пути для временных файлов (используем PID для уникальности)
    pid_suffix = os.getpid()
    CONFIG_PUBLIC_LOCAL_PATH = f"config_public_local_main_{pid_suffix}.json"
    CONFIG_GEN_LOCAL_PATH = f"config_gen_local_main_{pid_suffix}.json"
    CONFIG_MJ_LOCAL_PATH = f"config_mj_local_main_{pid_suffix}.json"
    # Дополнительные уникальные пути для перезагрузок внутри цикла
    CONFIG_MJ_LOCAL_RELOAD_PATH = f"config_mj_local_reload_{pid_suffix}.json"
    CONFIG_PUBLIC_LOCAL_RELOAD_PATH = f"config_public_local_reload_{pid_suffix}.json"
    CONFIG_MJ_LOCAL_CHECK_PATH = f"config_mj_local_check_{pid_suffix}.json"
    CONFIG_MJ_LOCAL_TIMEOUT_PATH = f"config_mj_local_timeout_{pid_suffix}.json"
    CONFIG_MJ_LOCAL_RESET_PATH = f"config_mj_local_reset_{pid_suffix}.json"

    FILE_EXTENSIONS = ['.json', '.png', '.mp4']
    FOLDERS = [
        config.get('FILE_PATHS.folder_444', '444/'),
        config.get('FILE_PATHS.folder_555', '555/'),
        config.get('FILE_PATHS.folder_666', '666/')
    ]
    ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder', 'archive/')
    FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}$")

    # Пути к скриптам
    SCRIPTS_FOLDER = config.get('FILE_PATHS.scripts_folder', 'scripts')
    GENERATE_CONTENT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_content.py")
    WORKSPACE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "Workspace_media.py")
    GENERATE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_media.py")

    # Таймаут MJ из конфига
    MJ_TIMEOUT_SECONDS = int(config.get('WORKFLOW.mj_timeout_seconds', 5 * 60 * 60)) # 5 часов по умолчанию

except Exception as const_err:
    logger.error(f"Критическая ошибка при чтении констант: {const_err}", exc_info=True)
    sys.exit(1)


# === Вспомогательные функции ===

def get_ready_groups(files):
    """Определяет готовые группы файлов по ID."""
    groups = {}
    required_extensions = set(FILE_EXTENSIONS)
    for file_key in files:
        base_name = os.path.basename(file_key)
        # Извлекаем ID, игнорируя возможное двойное расширение
        group_id_match = FILE_NAME_PATTERN.match(base_name)
        if group_id_match:
            group_id = group_id_match.group(0)
            # Получаем реальное расширение
            _, ext = os.path.splitext(base_name)
            # Обработка случая .png.png -> .png
            if base_name.endswith(ext + ext):
                 ext = ext # Используем одинарное расширение
            elif ext not in FILE_EXTENSIONS:
                 # Если расширение не стандартное, пробуем найти стандартное перед ним
                 potential_base = base_name[:-len(ext)]
                 potential_base_no_ext, standard_ext = os.path.splitext(potential_base)
                 if standard_ext in FILE_EXTENSIONS and FILE_NAME_PATTERN.match(potential_base_no_ext):
                     ext = standard_ext
                     group_id = potential_base_no_ext
                 else:
                     logger.warning(f"Неизвестное расширение '{ext}' в файле {file_key}. Пропуск.")
                     continue # Пропускаем файл с неизвестным расширением

            if ext in FILE_EXTENSIONS:
                groups.setdefault(group_id, set()).add(ext)
            else:
                 logger.debug(f"Файл {file_key} пропущен (не соответствует ID или расширению).")

        else:
            logger.debug(f"Имя файла {base_name} не соответствует паттерну ID. Пропуск.")

    ready_group_ids = [gid for gid, exts in groups.items() if exts == required_extensions]
    if ready_group_ids:
        logger.debug(f"Найдены готовые группы: {ready_group_ids}")
    else:
        logger.debug(f"Готовые группы не найдены среди {len(groups)} частичных групп.")
    return ready_group_ids


def move_group(s3, src_folder, dst_folder, group_id):
    """Перемещает все файлы группы (json, png, mp4) в другую папку."""
    logger.info(f"Перемещение группы '{group_id}' из {src_folder} в {dst_folder}...")
    all_moved = True
    files_moved_count = 0
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder.rstrip('/')}/{group_id}{ext}"
        dst_key = f"{dst_folder.rstrip('/')}/{group_id}{ext}"
        if move_b2_object(s3, B2_BUCKET_NAME, src_key, dst_key):
            files_moved_count += 1
        else:
            # move_b2_object уже логирует ошибку
            all_moved = False
            # Можно добавить break, если перемещение одного файла критично для всей группы
            # break
    if files_moved_count > 0:
         logger.info(f"Перемещено {files_moved_count}/{len(FILE_EXTENSIONS)} файлов для группы {group_id}.")
    if not all_moved:
         logger.warning(f"Не все файлы для группы {group_id} были успешно перемещены.")
    return all_moved

def process_folders(s3, folders_to_process):
    """Сортирует готовые группы файлов между папками 666 -> 555 -> 444."""
    logger.info("Начало сортировки папок...")
    # Проходим папки справа налево (666, 555)
    for i in range(len(folders_to_process) - 1, 0, -1):
        src_folder = folders_to_process[i]
        dst_folder = folders_to_process[i - 1]
        logger.info(f"Проверка папки {src_folder} для перемещения в {dst_folder}...")

        src_files = list_files_in_folder(s3, B2_BUCKET_NAME, src_folder) # Используем list_files_in_folder из utils
        ready_groups = get_ready_groups(src_files) # Используем обновленный get_ready_groups

        if not ready_groups:
            logger.info(f"В папке {src_folder} нет готовых групп.")
            continue

        logger.info(f"Найдены готовые группы в {src_folder}: {ready_groups}")

        # Проверяем, пуста ли целевая папка (dst_folder)
        # Используем is_folder_empty из utils
        if not is_folder_empty(s3, B2_BUCKET_NAME, dst_folder):
             logger.info(f"Целевая папка {dst_folder} не пуста. Перемещение из {src_folder} отложено.")
             continue # Переходим к следующей паре папок (например, от 555 к 444)

        logger.info(f"Целевая папка {dst_folder} пуста. Начинаем перемещение из {src_folder}.")
        moved_count = 0
        for group_id in ready_groups:
            if move_group(s3, src_folder, dst_folder, group_id):
                moved_count += 1
                # После успешного перемещения ОДНОЙ группы, целевая папка уже НЕ пуста.
                # Прерываем перемещение из текущей src_folder, чтобы не заполнять dst_folder
                # несколькими группами за один проход.
                logger.info(f"Группа {group_id} перемещена. Прерываем перемещение из {src_folder} на этой итерации.")
                break
            else:
                logger.error(f"Не удалось переместить группу {group_id}. Сортировка {src_folder} прервана.")
                # Прерываем перемещение из текущей папки при ошибке
                break

        logger.info(f"Из папки {src_folder} перемещено групп: {moved_count}")

    logger.info("Сортировка папок завершена.")


def handle_publish(s3, config_public):
    """Архивирует группы файлов по generation_id из config_public."""
    generation_ids_to_archive = config_public.get("generation_id", [])
    if not generation_ids_to_archive:
        logger.info("📂 Нет ID для архивации в config_public['generation_id'].")
        return False # Нет изменений

    if not isinstance(generation_ids_to_archive, list):
        logger.warning(f"Ключ 'generation_id' не список: {generation_ids_to_archive}. Преобразование.")
        # Преобразуем в список, если это не список (например, строка или null)
        generation_ids_to_archive = [str(gid) for gid in generation_ids_to_archive if gid] if generation_ids_to_archive else []


    logger.info(f"ID для архивации (из config_public): {generation_ids_to_archive}")
    archived_ids = []
    failed_ids = []

    # Создаем копию списка для безопасной итерации и удаления
    ids_to_process = list(generation_ids_to_archive)

    for generation_id in ids_to_process:
        clean_id_match = FILE_NAME_PATTERN.match(str(generation_id)) # Проверяем паттерн
        if not clean_id_match:
            logger.warning(f"ID '{generation_id}' не соответствует паттерну, пропуск архивации.")
            failed_ids.append(generation_id) # Считаем ошибкой формата
            continue

        clean_id = clean_id_match.group(0)
        logger.info(f"🔄 Архивируем группу: {clean_id}")
        success = True
        found_any_file = False

        # Ищем файлы во всех папках (444, 555, 666) для архивации
        for folder in FOLDERS:
            for ext in FILE_EXTENSIONS:
                src_key = f"{folder.rstrip('/')}/{clean_id}{ext}"
                dst_key = f"{ARCHIVE_FOLDER.rstrip('/')}/{clean_id}{ext}"
                try:
                    # Проверяем наличие объекта перед перемещением
                    s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    found_any_file = True # Нашли хотя бы один файл
                    logger.debug(f"Перемещение для архивации: {src_key} -> {dst_key}")
                    if not move_b2_object(s3, B2_BUCKET_NAME, src_key, dst_key):
                         success = False # Ошибка при перемещении
                         logger.error(f"Ошибка перемещения при архивации {src_key}")
                         # Можно добавить break, если ошибка одного файла критична
                         # break
                    else:
                         logger.info(f"✅ Заархивировано: {src_key} -> {dst_key}")

                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey' or '404' in str(e):
                        # Файл не найден в этой папке, это нормально, ищем в других
                        logger.debug(f"Файл {src_key} не найден в {folder}.")
                        continue
                    else:
                        # Другая ошибка Boto3
                        logger.error(f"Ошибка Boto3 при проверке/архивации {src_key}: {e}")
                        success = False
                        # break # Возможно, стоит прервать обработку группы при ошибке Boto3
                except Exception as e:
                    logger.error(f"Неизвестная ошибка при архивации {src_key}: {e}", exc_info=True)
                    success = False
                    # break

            # Если была ошибка при обработке расширений в текущей папке,
            # можно прервать поиск в следующих папках для этой группы ID
            # if not success:
            #     break

        # Оцениваем результат архивации для текущего ID
        if not found_any_file:
            logger.warning(f"Не найдено файлов для архивации ID {clean_id} ни в одной из папок {FOLDERS}. Считаем обработанным (удаляем из списка).")
            # Добавляем в archived_ids, чтобы удалить из списка ожидания
            archived_ids.append(generation_id)
        elif success:
            logger.info(f"Группа {clean_id} успешно заархивирована.")
            archived_ids.append(generation_id)
        else:
            logger.error(f"Не удалось полностью или корректно заархивировать группу {clean_id}.")
            failed_ids.append(generation_id) # Оставляем ID в списке, если были ошибки

    # Обновляем список generation_id в config_public, удаляя только успешно заархивированные
    if archived_ids:
        current_list = config_public.get("generation_id", [])
        # Создаем новый список, исключая только УСПЕШНО заархивированные ID
        new_archive_list = [gid for gid in current_list if gid not in archived_ids]

        if not new_archive_list:
            # Если список стал пустым, удаляем ключ или оставляем пустым списком
            # config_public.pop("generation_id", None) # Удалить ключ
            config_public["generation_id"] = [] # Оставить пустым списком
            logger.info("Список generation_id в config_public очищен (все ID обработаны).")
        else:
            config_public["generation_id"] = new_archive_list
            logger.info(f"Обновлен список generation_id в config_public: {new_archive_list}")

        # Возвращаем True, так как были изменения (успешно заархивированные ID удалены)
        return True
    else:
        logger.info("Не было успешно заархивировано ни одного ID из списка.")
        # Возвращаем False, так как список в config_public не изменился
        return False

# === Основная функция ===
def main():
    parser = argparse.ArgumentParser(description='Manage B2 storage and content generation workflow.')
    parser.add_argument('--zero-delay', action='store_true', default=False, help='Skip the 10-minute delay (less relevant now).')
    args = parser.parse_args()
    zero_delay_flag = args.zero_delay # Флаг сейчас не используется активно
    logger.info(f"Флаг --zero-delay установлен: {zero_delay_flag} (менее актуален)")

    tasks_processed_this_run = 0 # Счетчик задач, обработанных в ЭТОМ запуске
    try:
        max_tasks_per_run = int(config.get('WORKFLOW.max_tasks_per_run', 1))
    except ValueError:
        logger.warning("Некорректное значение WORKFLOW.max_tasks_per_run. Используется 1.")
        max_tasks_per_run = 1
    logger.info(f"Максимальное количество ПОЛНЫХ задач за запуск: {max_tasks_per_run}")

    b2_client = None
    config_public = {}
    config_gen = {}
    config_mj = {}
    lock_acquired = False
    # --- ИЗМЕНЕНО: Флаг перенесен внутрь цикла ---
    # task_completed_successfully = False # Флаг для финальной очистки config_gen

    try:
        b2_client = get_b2_client()
        if not b2_client:
            # Логгер уже должен был записать ошибку в get_b2_client
            raise ConnectionError("Не удалось инициализировать B2 клиент.")

        # --- Блокировка ---
        logger.info(f"Проверка блокировки в {CONFIG_PUBLIC_REMOTE_PATH}...")
        # Используем уникальный локальный путь для начальной загрузки
        config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, default_value=None)
        if config_public is None:
            # Пытаемся создать файл блокировки с lock: true, если он не существует
            logger.warning(f"{CONFIG_PUBLIC_REMOTE_PATH} не найден. Попытка создания с блокировкой.")
            config_public = {"processing_lock": True, "generation_id": []} # Начальное состояние
            if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public):
                logger.info("🔒 Файл блокировки создан и блокировка установлена.")
                lock_acquired = True
            else:
                logger.error("❌ Не удалось создать файл блокировки. Завершение работы.")
                return # Выход, если не удалось создать файл
        elif config_public.get("processing_lock", False):
            logger.warning("🔒 Обнаружена активная блокировка (processing_lock: true). Завершение работы.")
            return # Выход, если блокировка уже установлена
        else:
            # Блокировка не активна, устанавливаем ее
            config_public["processing_lock"] = True
            if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public):
                logger.info("🔒 Блокировка установлена.")
                lock_acquired = True
            else:
                logger.error("❌ Не удалось установить блокировку. Завершение работы.")
                return # Выход, если не удалось сохранить

        # --- Загрузка остальных конфигов ---
        logger.info("Загрузка остальных конфигурационных файлов...")
        config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, {"generation_id": None})
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None})

        # Проверяем успешность загрузки
        if config_gen is None: logger.warning("Не удалось загрузить config_gen.json. Используется дефолт."); config_gen = {"generation_id": None}
        if config_mj is None: logger.warning("Не удалось загрузить config_midjourney.json. Используется дефолт."); config_mj = {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None}
        # Гарантируем наличие ключей в config_mj
        config_mj.setdefault("midjourney_task", None); config_mj.setdefault("midjourney_results", {}); config_mj.setdefault("generation", False); config_mj.setdefault("status", None)


        logger.info("--- Начало основного цикла обработки ---")
        # Цикл продолжается, пока не обработано максимальное количество задач *за этот запуск*
        while tasks_processed_this_run < max_tasks_per_run:
            logger.info(f"--- Итерация цикла обработки #{tasks_processed_this_run + 1} / {max_tasks_per_run} ---")
            # --- ИЗМЕНЕНО: Флаг успешного завершения ПОЛНОЙ задачи сбрасывается в начале КАЖДОЙ итерации ---
            task_completed_successfully = False

            # Перезагружаем конфиги в начале КАЖДОЙ итерации для актуальности
            logger.debug("Перезагрузка конфигурационных файлов из B2...")
            # Используем уникальные локальные пути для перезагрузки
            config_public_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_RELOAD_PATH, default_value=config_public)
            config_mj_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_RELOAD_PATH, default_value=config_mj)

            # Проверяем, что перезагрузка удалась
            if config_public_reloaded is None or config_mj_reloaded is None:
                logger.error("Не удалось перезагрузить конфиги B2 внутри цикла. Прерывание.")
                break # Выход из цикла while при ошибке перезагрузки

            config_public = config_public_reloaded
            config_mj = config_mj_reloaded
            # Гарантируем наличие ключей после перезагрузки
            config_mj.setdefault("midjourney_task", None); config_mj.setdefault("midjourney_results", {}); config_mj.setdefault("generation", False); config_mj.setdefault("status", None)

            logger.debug(f"Текущие состояния: config_gen={json.dumps(config_gen)}, config_mj={json.dumps(config_mj)}")

            action_taken_in_iteration = False # Флаг, что в этой итерации было выполнено действие

            # --- Проверка состояний (Порядок важен!) ---

            # Сценарий 0: Таймаут MJ -> Генерация Mock
            if config_mj.get('status') == 'timed_out_mock_needed':
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id:
                    logger.error("❌ Статус 'timed_out_mock_needed', но нет generation_id! Прерывание.")
                    break # Выход из цикла
                logger.warning(f"Статус таймаута MJ для ID {current_generation_id}. Запуск генерации имитации.")
                try:
                    logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} --use-mock для ID: {current_generation_id}...")
                    # Увеличим таймаут для mock, на всякий случай
                    subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id, '--use-mock'], check=True, timeout=300)
                    logger.info(f"✅ {GENERATE_MEDIA_SCRIPT} --use-mock успешно завершен.")
                    # --- ИЗМЕНЕНО: Устанавливаем флаг УСПЕШНОГО ЗАВЕРШЕНИЯ ЗАДАЧИ ---
                    task_completed_successfully = True
                    tasks_processed_this_run += 1 # Увеличиваем счетчик обработанных задач
                    logger.info(f"Полная задача завершена (Mock). Обработано задач в этом запуске: {tasks_processed_this_run}")
                    break # Выход из цикла, так как задача завершена
                except subprocess.CalledProcessError as e: logger.error(f"Ошибка {GENERATE_MEDIA_SCRIPT} --use-mock: {e}. Прерывание."); break
                except subprocess.TimeoutExpired: logger.error(f"Таймаут {GENERATE_MEDIA_SCRIPT} --use-mock. Прерывание."); break
                except Exception as mock_gen_err: logger.error(f"Ошибка генерации имитации: {mock_gen_err}. Прерывание.", exc_info=True); break

            # Сценарий 3: Результаты MJ/Upscale Готовы -> Генерация Runway/Mock
            # Проверяем наличие 'task_result' и URL внутри него
            elif config_mj.get('midjourney_results') and isinstance(config_mj['midjourney_results'].get('task_result'), dict):
                task_result_data = config_mj['midjourney_results']['task_result']
                # Проверяем наличие ЛЮБОГО из ожидаемых ключей с URL
                has_urls = task_result_data.get("image_url") or \
                           task_result_data.get("temporary_image_urls") or \
                           task_result_data.get("image_urls") # Добавили проверку image_urls на всякий случай

                if has_urls:
                    action_taken_in_iteration = True
                    current_generation_id = config_gen.get("generation_id")
                    if not current_generation_id:
                        logger.error("❌ Результаты MJ/Upscale есть, но нет generation_id! Прерывание.")
                        break # Выход из цикла
                    logger.info(f"Готовые результаты MJ/Upscale для ID {current_generation_id}. Запуск генерации медиа (Runway/Mock).")
                    try:
                        logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} для ID: {current_generation_id}...")
                        # Увеличиваем таймаут для Runway
                        subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id], check=True, timeout=900) # 15 минут
                        logger.info(f"✅ {GENERATE_MEDIA_SCRIPT} успешно завершен (генерация видео Runway/Mock).")
                        # --- ИЗМЕНЕНО: Устанавливаем флаг УСПЕШНОГО ЗАВЕРШЕНИЯ ЗАДАЧИ ---
                        task_completed_successfully = True
                        tasks_processed_this_run += 1 # Увеличиваем счетчик обработанных задач
                        logger.info(f"Полная задача завершена (Runway/Mock). Обработано задач в этом запуске: {tasks_processed_this_run}")
                        break # Выход из цикла, так как задача завершена
                    except subprocess.CalledProcessError as e: logger.error(f"Ошибка {GENERATE_MEDIA_SCRIPT} (ген. видео): {e}. Прерывание."); break
                    except subprocess.TimeoutExpired: logger.error(f"Таймаут {GENERATE_MEDIA_SCRIPT} (ген. видео). Прерывание."); break
                    except Exception as media_gen_err: logger.error(f"Ошибка генерации медиа: {media_gen_err}. Прерывание.", exc_info=True); break
                else:
                    # Если есть task_result, но нет URL - это странно, логируем
                    logger.warning(f"Найдены midjourney_results с task_result, но нет URL изображений: {task_result_data}. Пропуск этого сценария.")
            elif config_mj.get('midjourney_results'):
                 # Если midjourney_results есть, но task_result не словарь
                 logger.warning(f"Поле 'task_result' в midjourney_results не является словарем: {config_mj['midjourney_results']}. Пропуск этого сценария.")


            # Сценарий 2: Ожидание/Проверка MJ (Imagine или Upscale)
            elif config_mj.get('midjourney_task'):
                action_taken_in_iteration = True
                task_info = config_mj['midjourney_task']
                task_id = None
                requested_at_str = None
                config_mj_needs_update = False # Флаг для сохранения конфига (если установили таймаут)

                # Извлекаем ID и время
                if isinstance(task_info, dict):
                    task_id = task_info.get("task_id")
                    requested_at_str = task_info.get("requested_at_utc")
                    logger.debug(f"Извлечено из задачи: task_id={task_id}, requested_at_utc={requested_at_str}")
                elif isinstance(task_info, str): # Поддержка старого формата
                    task_id = task_info
                    logger.warning("Обнаружен старый формат midjourney_task (строка). Метка времени недоступна.")
                else:
                    logger.error(f"Неожиданный формат midjourney_task: {task_info}. Прерывание.")
                    break # Выход из цикла

                if not task_id:
                    logger.error("Задача MJ есть, но task_id не найден. Прерывание.")
                    break # Выход из цикла

                logger.info(f"Активная задача MJ: {task_id}. Запуск проверки статуса.")
                try:
                    logger.info(f"Запуск {WORKSPACE_MEDIA_SCRIPT}...")
                    # Увеличим таймаут проверки на всякий случай
                    subprocess.run([sys.executable, WORKSPACE_MEDIA_SCRIPT], check=True, timeout=180)
                    logger.info(f"✅ {WORKSPACE_MEDIA_SCRIPT} успешно выполнен.")

                    # Перезагружаем config_mj СРАЗУ после проверки, чтобы увидеть результат
                    logger.info("Перезагрузка config_midjourney.json для проверки результата...")
                    # Используем уникальный локальный путь
                    config_mj_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_CHECK_PATH, default_value=None)
                    if config_mj_reloaded is None:
                        logger.error("Критическая ошибка: не удалось перезагрузить config_mj после проверки. Прерывание.")
                        break # Выход из цикла
                    config_mj = config_mj_reloaded # Обновляем локальную переменную
                    # Гарантируем наличие ключей
                    config_mj.setdefault("midjourney_task", None); config_mj.setdefault("midjourney_results", {}); config_mj.setdefault("generation", False); config_mj.setdefault("status", None)
                    logger.debug(f"Состояние config_mj после перезагрузки: {json.dumps(config_mj, indent=2, ensure_ascii=False)}")

                    # Проверяем, появились ли результаты ПОСЛЕ проверки
                    if config_mj.get('midjourney_results') and isinstance(config_mj['midjourney_results'].get('task_result'), dict):
                        logger.info("✅ Результаты Midjourney обнаружены после проверки! Переход к следующей итерации.")
                        continue # Переходим к следующей итерации цикла while, чтобы обработать Сценарий 3

                    # Если результатов нет, проверяем таймаут
                    logger.info("Результаты Midjourney еще не готовы.")
                    if requested_at_str:
                        try:
                            # Преобразуем строку времени в datetime объект с UTC
                            # Обработка Z и отсутствия смещения
                            if requested_at_str.endswith('Z'):
                                requested_at_str = requested_at_str[:-1] + '+00:00'
                            requested_at_dt = datetime.fromisoformat(requested_at_str)
                            # Если нет таймзоны, считаем UTC
                            if requested_at_dt.tzinfo is None:
                                requested_at_dt = requested_at_dt.replace(tzinfo=timezone.utc)
                            else: # Если есть, приводим к UTC
                                requested_at_dt = requested_at_dt.astimezone(timezone.utc)

                            now_utc = datetime.now(timezone.utc)
                            elapsed_time = now_utc - requested_at_dt
                            logger.info(f"Время с момента запроса MJ ({requested_at_str}): {elapsed_time}")

                            if elapsed_time > timedelta(seconds=MJ_TIMEOUT_SECONDS):
                                logger.warning(f"⏰ Превышен таймаут ожидания Midjourney ({MJ_TIMEOUT_SECONDS / 3600:.1f} ч)!")
                                # Устанавливаем статус таймаута и очищаем задачу
                                config_mj['midjourney_task'] = None
                                config_mj['status'] = 'timed_out_mock_needed'
                                config_mj['midjourney_results'] = {} # Очищаем на всякий случай
                                config_mj['generation'] = False # Сбрасываем флаг
                                config_mj_needs_update = True # Помечаем для сохранения
                                logger.info("Установлен статус 'timed_out_mock_needed', задача MJ очищена.")
                            else:
                                logger.info("Таймаут ожидания MJ не достигнут. Проверка завершена для этого запуска.")
                        except ValueError as date_err:
                            logger.error(f"Ошибка парсинга метки времени '{requested_at_str}': {date_err}. Проверка таймаута невозможна.")
                        except Exception as time_err:
                            logger.error(f"Ошибка при проверке времени: {time_err}. Проверка таймаута не удалась.", exc_info=True)
                    else:
                        logger.warning("Метка времени запроса MJ (requested_at_utc) отсутствует в задаче. Проверка таймаута невозможна.")

                    # Сохраняем конфиг, если установили таймаут
                    if config_mj_needs_update:
                        logger.info("Сохранение config_midjourney.json (статус таймаута) в B2...")
                        # Используем уникальный локальный путь
                        if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_TIMEOUT_PATH, config_mj):
                            logger.error("!!! Не удалось сохранить config_mj после установки таймаута!")
                        else:
                            logger.info("✅ Config_mj со статусом таймаута сохранен.")

                    # Прерываем цикл ПОСЛЕ проверки статуса (и возможной установки таймаута),
                    # чтобы ждать следующего запуска по расписанию.
                    logger.info("Завершение текущего запуска менеджера для ожидания следующего по расписанию (после проверки MJ).")
                    break # Выход из цикла while

                except subprocess.CalledProcessError as e: logger.error(f"Ошибка выполнения {WORKSPACE_MEDIA_SCRIPT}: {e}. Прерывание."); break
                except subprocess.TimeoutExpired: logger.error(f"Таймаут выполнения {WORKSPACE_MEDIA_SCRIPT}. Прерывание."); break
                except Exception as check_err: logger.error(f"Ошибка на этапе проверки MJ: {check_err}. Прерывание.", exc_info=True); break

            # Сценарий 4: Инициация Задачи MJ (Imagine или Upscale)
            # Этот сценарий теперь обрабатывается внутри generate_media.py
            # Здесь мы проверяем флаг generation: true, который устанавливает generate_content
            elif config_mj.get('generation') is True:
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")

                # Обработка неконсистентности: generation=true, но нет ID
                if not current_generation_id:
                    logger.warning("⚠️ Обнаружен флаг generation:true, но нет generation_id! Сброс флага.")
                    config_mj['generation'] = False
                    # Сохраняем исправленный config_mj
                    # Используем уникальный локальный путь
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_RESET_PATH, config_mj):
                         logger.info("Флаг 'generation' сброшен в B2 из-за отсутствия ID.")
                    else:
                         logger.error("Не удалось сохранить сброшенный флаг 'generation' в B2!")
                    continue # Переходим к следующей итерации, чтобы переоценить состояние

                # Если ID есть, запускаем generate_media для инициации /imagine
                logger.info(f"Флаг generation:true для ID {current_generation_id}. Запуск инициации задачи MJ (/imagine).")
                try:
                    logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} для ID: {current_generation_id}...")
                    # Таймаут для инициации может быть короче
                    subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id], check=True, timeout=120)
                    logger.info(f"✅ {GENERATE_MEDIA_SCRIPT} успешно завершен (инициация /imagine).")
                    # После успешной инициации переходим к следующей итерации,
                    # чтобы менеджер увидел новую задачу в config_mj и запустил проверку (Сценарий 2)
                    continue # Переход к следующей итерации цикла while
                except subprocess.CalledProcessError as e: logger.error(f"Ошибка {GENERATE_MEDIA_SCRIPT} (иниц. /imagine): {e}. Прерывание."); break
                except subprocess.TimeoutExpired: logger.error(f"Таймаут {GENERATE_MEDIA_SCRIPT} (иниц. /imagine). Прерывание."); break
                except Exception as media_init_err: logger.error(f"Ошибка при инициации /imagine: {media_init_err}. Прерывание.", exc_info=True); break

            # Сценарий 1: Уборка / Генерация Контента
            else:
                action_taken_in_iteration = True
                logger.info("Нет активных задач MJ или флага 'generation'. Выполнение Уборки и проверка папки 666/...")

                # --- "Уборка" ---
                logger.info("Запуск handle_publish (архивация)...")
                config_public_copy = config_public.copy() # Работаем с копией
                # Перезагружаем config_public перед handle_publish на всякий случай
                config_public_copy = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_RELOAD_PATH, default_value=config_public_copy)
                if config_public_copy is None:
                     logger.error("Не удалось перезагрузить config_public перед архивацией. Пропуск архивации.")
                elif handle_publish(b2_client, config_public_copy):
                    logger.info("handle_publish внес изменения, сохраняем config_public...")
                    if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_RELOAD_PATH, config_public_copy):
                        logger.error("Не удалось сохранить config_public после handle_publish!")
                    else:
                        # Обновляем основную переменную config_public после успешного сохранения
                        config_public = config_public_copy
                        logger.info("✅ config_public успешно обновлен после архивации.")
                else:
                     logger.info("handle_publish не внес изменений в config_public.")


                logger.info("Запуск process_folders (сортировка)...")
                process_folders(b2_client, FOLDERS) # Используем обновленную process_folders

                # --- Проверка 666/ и Генерация Контента ---
                logger.info("Проверка наличия ГОТОВЫХ ГРУПП в папке 666/...")
                # Используем is_folder_empty для проверки, пуста ли папка 666
                # (get_ready_groups вызывается внутри process_folders, здесь можно проще)
                if is_folder_empty(b2_client, B2_BUCKET_NAME, FOLDERS[-1]): # FOLDERS[-1] это '666/'
                    logger.info(f"✅ Папка {FOLDERS[-1]} пуста. Запуск генерации нового контента...")
                    try:
                        new_id_base = generate_file_id() # generate_file_id уже не добавляет .json
                        if not new_id_base or not FILE_NAME_PATTERN.match(new_id_base):
                            raise ValueError(f"Функция generate_file_id вернула некорректный ID: {new_id_base}")

                        logger.info(f"Сгенерирован новый ID: {new_id_base}")
                        # Обновляем config_gen с новым ID
                        config_gen["generation_id"] = new_id_base
                        # Используем уникальный локальный путь
                        if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen):
                            # Логгер уже записал ошибку
                            raise Exception(f"Не удалось сохранить новый ID {new_id_base} в {CONFIG_GEN_REMOTE_PATH}")
                        logger.info(f"Новый ID {new_id_base} сохранен в {CONFIG_GEN_REMOTE_PATH}")

                        # Запускаем generate_content.py
                        logger.info(f"Запуск {GENERATE_CONTENT_SCRIPT} для ID: {new_id_base}...")
                        # Увеличим таймаут для генерации контента
                        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, '--generation_id', new_id_base], check=True, timeout=600)
                        logger.info(f"✅ {GENERATE_CONTENT_SCRIPT} успешно завершен для ID: {new_id_base}.")

                        # После успешной генерации контента, переходим к следующей итерации,
                        # чтобы менеджер увидел флаг generation:true и запустил generate_media (Сценарий 4)
                        continue # Переход к следующей итерации цикла while

                    except subprocess.CalledProcessError as e: logger.error(f"Ошибка выполнения {GENERATE_CONTENT_SCRIPT}: {e}. Прерывание."); break
                    except subprocess.TimeoutExpired: logger.error(f"Таймаут выполнения {GENERATE_CONTENT_SCRIPT}. Прерывание."); break
                    except Exception as gen_err: logger.error(f"Ошибка при генерации/запуске контента: {gen_err}. Прерывание.", exc_info=True); break
                else:
                    # Если папка 666 НЕ пуста, значит там есть файлы (возможно, готовая группа или частичная)
                    # В любом случае, новую генерацию запускать не нужно.
                    logger.info(f"Папка {FOLDERS[-1]} не пуста. Генерация нового контента не требуется. Завершение цикла.")
                    break # Выходим из цикла while

            # Если ни один сценарий не сработал (маловероятно, но на всякий случай)
            if not action_taken_in_iteration:
                logger.warning("Не найдено активных состояний для обработки в этой итерации. Завершение цикла.")
                break

        # --- Конец основного цикла while ---
        logger.info(f"--- Основной цикл обработки завершен. Обработано ПОЛНЫХ задач в этом запуске: {tasks_processed_this_run} ---")

        # --- ИЗМЕНЕНО: Логика завершения задачи (очистка config_gen) ---
        # Очищаем config_gen ТОЛЬКО если флаг task_completed_successfully был установлен в True
        # во время последней итерации цикла (т.е. после Сценария 0 или Сценария 3)
        if task_completed_successfully:
            logger.info("Полная задача успешно завершена в последней итерации, обновление финальных статусов...")
            try:
                # Перезагружаем config_gen для актуальности перед очисткой
                config_gen_final = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, default_value=None)
                if config_gen_final is None:
                    raise Exception("Не удалось загрузить config_gen перед финальной очисткой")

                completed_id = config_gen_final.get("generation_id")
                if completed_id:
                    # Очищаем generation_id в config_gen
                    config_gen_final["generation_id"] = None
                    logger.info(f"Очистка generation_id ('{completed_id}') в config_gen.")
                    # Сохраняем ТОЛЬКО config_gen
                    if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen_final):
                         # Логгер уже записал ошибку
                         logger.error("!!! Не удалось сохранить очищенный config_gen!")
                    else:
                         logger.info("✅ Обновленный (очищенный) config_gen сохранен.")
                else:
                    # Это странно, если task_completed_successfully=True, но ID уже null
                    logger.warning("Флаг task_completed_successfully=True, но generation_id в config_gen уже был null.")
            except Exception as final_save_err:
                logger.error(f"Ошибка при финальной очистке config_gen: {final_save_err}", exc_info=True)
        else:
             logger.info("Флаг task_completed_successfully не установлен. Очистка config_gen не требуется.")
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    except ConnectionError as conn_err:
        # Логгер уже должен был записать ошибку
        logger.error(f"❌ Ошибка соединения B2: {conn_err}")
        # Убедимся, что блокировка будет снята, если она была установлена
        # lock_acquired может быть False, если ошибка произошла до установки блокировки
    except Exception as main_exec_err:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в главном блоке: {main_exec_err}", exc_info=True)
        # Блокировка будет снята в finally
    finally:
        # --- Снятие блокировки ---
        if lock_acquired:
            logger.info("Снятие блокировки (processing_lock=False)...")
            # Пытаемся получить клиент B2 еще раз, если он не был создан или потерян
            if not b2_client:
                 try:
                     b2_client = get_b2_client()
                 except Exception as final_b2_err:
                     logger.error(f"Не удалось получить B2 клиент в finally для снятия блокировки: {final_b2_err}")
                     b2_client = None # Убедимся, что он None

            if b2_client:
                # Пытаемся загрузить config_public еще раз перед снятием блокировки
                config_public_final = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, default_value=None)
                if config_public_final is not None:
                    config_public_final["processing_lock"] = False
                    # Используем тот же локальный путь для сохранения
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_final):
                        logger.info("🔓 Блокировка успешно снята.")
                    else:
                        # Логгер уже записал ошибку
                        logger.error("❌ НЕ УДАЛОСЬ СНЯТЬ БЛОКИРОВКУ!")
                else:
                    # Если не удалось загрузить, все равно пытаемся создать файл с lock: false
                    logger.error("❌ Не удалось загрузить config_public в finally! Попытка принудительного снятия блокировки.")
                    config_public_force_unlock = {"processing_lock": False, "generation_id": []} # Безопасное состояние
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_force_unlock):
                         logger.info("🔓 Блокировка принудительно снята (файл перезаписан).")
                    else:
                         logger.error("❌ НЕ УДАЛОСЬ ПРИНУДИТЕЛЬНО СНЯТЬ БЛОКИРОВКУ!")
            else:
                logger.error("❌ Не удалось получить B2 клиент в finally! Блокировка НЕ снята.")
        else:
            logger.info("Блокировка не была установлена или была снята ранее, снятие не требуется.")

        # Очистка временных файлов (необязательно, но полезно)
        temp_files = [
            CONFIG_PUBLIC_LOCAL_PATH, CONFIG_GEN_LOCAL_PATH, CONFIG_MJ_LOCAL_PATH,
            CONFIG_MJ_LOCAL_RELOAD_PATH, CONFIG_PUBLIC_LOCAL_RELOAD_PATH,
            CONFIG_MJ_LOCAL_CHECK_PATH, CONFIG_MJ_LOCAL_TIMEOUT_PATH,
            CONFIG_MJ_LOCAL_RESET_PATH
        ]
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    logger.debug(f"Удален временный файл: {temp_file}")
                except OSError as e:
                    logger.warning(f"Не удалось удалить временный файл {temp_file}: {e}")

        logger.info("--- Завершение работы b2_storage_manager.py ---")

# === Точка входа ===
if __name__ == "__main__":
    exit_code = 1 # По умолчанию - ошибка
    try:
        main()
        exit_code = 0 # Успех, если main завершился без исключений
    except SystemExit as e:
        exit_code = e.code # Пробрасываем код выхода из sys.exit()
    except Exception as top_level_err:
         # Логируем неперехваченные ошибки
         try:
             logger.error(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ: {top_level_err}", exc_info=True)
         except NameError: # Если логгер недоступен
             print(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ (логгер недоступен): {top_level_err}")
         exit_code = 1 # Общий код ошибки
    finally:
         # Выходим с финальным кодом
         sys.exit(exit_code)
