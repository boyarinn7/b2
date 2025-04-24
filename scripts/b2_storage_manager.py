# -*- coding: utf-8 -*-
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
    # Абсолютный импорт, если структура позволяет
    from modules.utils import (
        is_folder_empty, ensure_directory_exists, generate_file_id,
        load_b2_json, save_b2_json, list_b2_folder_contents, # Убедимся, что list_b2_folder_contents импортирован
        move_b2_object, delete_b2_object
    )
    from modules.api_clients import get_b2_client
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.config_manager import ConfigManager
except ModuleNotFoundError as import_err:
    # Попытка относительного импорта, если запускается из папки scripts
    # или если абсолютный не сработал
    try:
        # Добавляем родительскую директорию в sys.path
        BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if BASE_DIR not in sys.path:
            sys.path.insert(0, BASE_DIR) # Добавляем в начало для приоритета

        from modules.utils import (
            is_folder_empty, ensure_directory_exists, generate_file_id,
            load_b2_json, save_b2_json, list_b2_folder_contents, # Убедимся, что list_b2_folder_contents импортирован
            move_b2_object, delete_b2_object
        )
        from modules.api_clients import get_b2_client
        from modules.logger import get_logger
        from modules.error_handler import handle_error
        from modules.config_manager import ConfigManager
    except ModuleNotFoundError:
        print(f"Критическая Ошибка: Не найдены модули проекта: {import_err}", file=sys.stderr)
        sys.exit(1)
    except ImportError as import_err_rel:
        # Проверяем, не ошибка ли это импорта list_b2_folder_contents
        if 'list_b2_folder_contents' in str(import_err_rel):
             print(f"Критическая Ошибка: Функция 'list_b2_folder_contents' не найдена в 'modules.utils'. Убедитесь, что она добавлена.", file=sys.stderr)
        else:
             print(f"Критическая Ошибка импорта (относительный): {import_err_rel}", file=sys.stderr)
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
    sys.exit(1) # Выход с ошибкой


# === Константы ===
try:
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))
    if not B2_BUCKET_NAME: raise ValueError("B2_BUCKET_NAME не определен")

    CONFIG_PUBLIC_REMOTE_PATH = config.get('FILE_PATHS.config_public', "config/config_public.json")
    CONFIG_GEN_REMOTE_PATH = config.get('FILE_PATHS.config_gen', "config/config_gen.json")
    CONFIG_MJ_REMOTE_PATH = config.get('FILE_PATHS.config_midjourney', "config/config_midjourney.json")

    # Локальные пути для временных файлов (сделаем их уникальными для параллелизма)
    timestamp_suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    CONFIG_PUBLIC_LOCAL_PATH = f"config_public_local_main_{timestamp_suffix}.json"
    CONFIG_GEN_LOCAL_PATH = f"config_gen_local_main_{timestamp_suffix}.json"
    CONFIG_MJ_LOCAL_PATH = f"config_mj_local_main_{timestamp_suffix}.json"
    CONFIG_MJ_LOCAL_CHECK_PATH = f"config_mj_local_check_{timestamp_suffix}.json"
    CONFIG_MJ_LOCAL_TIMEOUT_PATH = f"config_mj_local_timeout_{timestamp_suffix}.json"
    CONFIG_MJ_LOCAL_RESET_PATH = f"config_mj_local_reset_{timestamp_suffix}.json"
    CONFIG_MJ_LOCAL_MEDIA_CHECK_PATH = f"config_mj_local_media_check_{timestamp_suffix}.json" # Новый для проверки после media

    FILE_EXTENSIONS = ['.json', '.png', '.mp4']
    FOLDERS = [
        config.get('FILE_PATHS.folder_444', '444/'),
        config.get('FILE_PATHS.folder_555', '555/'),
        config.get('FILE_PATHS.folder_666', '666/')
    ]
    ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder', 'archive/')
    FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}$") # Паттерн для ID

    # Пути к скриптам
    SCRIPTS_FOLDER = config.get('FILE_PATHS.scripts_folder', 'scripts')
    GENERATE_CONTENT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_content.py")
    WORKSPACE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "Workspace_media.py")
    GENERATE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_media.py")

    # Таймаут MJ из конфига
    MJ_TIMEOUT_SECONDS = int(config.get('WORKFLOW.mj_timeout_seconds', 5 * 60 * 60)) # 5 часов по умолчанию
    if MJ_TIMEOUT_SECONDS <= 0:
        logger.warning("MJ_TIMEOUT_SECONDS <= 0, используется 18000.")
        MJ_TIMEOUT_SECONDS = 18000

except Exception as cfg_err:
     logger.error(f"Критическая ошибка чтения констант: {cfg_err}", exc_info=True)
     sys.exit(1)


# === Вспомогательные функции ===

def list_files_in_folder(s3, folder_prefix):
    """Возвращает список КЛЮЧЕЙ файлов в папке, соответствующих паттерну ID."""
    files = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        prefix = folder_prefix if folder_prefix.endswith('/') else folder_prefix + '/'
        for page in paginator.paginate(Bucket=B2_BUCKET_NAME, Prefix=prefix, Delimiter='/'):
            if 'Contents' in page:
                for obj in page.get('Contents', []):
                    key = obj.get('Key')
                    # Пропускаем саму папку и placeholder'ы
                    if key == prefix or key.endswith('/') or key.endswith('.bzEmpty'):
                        continue
                    # Проверяем, соответствует ли *имя файла* (без расширения) паттерну
                    base_name = os.path.splitext(os.path.basename(key))[0]
                    if FILE_NAME_PATTERN.match(base_name):
                        files.append(key)
                    # else:
                    #     logger.debug(f"Файл {key} не соответствует паттерну ID, пропуск.")
    except ClientError as e:
        logger.error(f"Ошибка Boto3 при листинге папки '{folder_prefix}': {e}")
    except Exception as e:
        logger.error(f"Неизвестная ошибка при листинге папки '{folder_prefix}': {e}", exc_info=True)
    # logger.debug(f"Файлы в {folder_prefix}: {files}")
    return files

def get_ready_groups(files):
    """Определяет ID групп, для которых есть все 3 файла (.json, .png, .mp4)."""
    groups = {}
    required_extensions = set(FILE_EXTENSIONS)
    for file_key in files:
        base_name = os.path.splitext(os.path.basename(file_key))
        group_id = base_name[0]
        ext = base_name[1].lower()
        if FILE_NAME_PATTERN.match(group_id) and ext in required_extensions:
            groups.setdefault(group_id, set()).add(ext)

    ready_group_ids = [gid for gid, exts in groups.items() if exts == required_extensions]
    if ready_group_ids:
        logger.debug(f"Найдены готовые группы: {ready_group_ids}")
    # else:
    #     logger.debug(f"Готовые группы не найдены среди {len(groups)} частичных групп.")
    return ready_group_ids

def move_group(s3, src_folder, dst_folder, group_id):
    """Перемещает все файлы группы (json, png, mp4) из одной папки в другую."""
    logger.info(f"Перемещение группы '{group_id}' из {src_folder} в {dst_folder}...")
    all_moved = True
    src_folder_norm = src_folder.rstrip('/') + '/'
    dst_folder_norm = dst_folder.rstrip('/') + '/'

    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder_norm}{group_id}{ext}"
        dst_key = f"{dst_folder_norm}{group_id}{ext}"
        try:
            # Проверяем наличие исходного файла перед копированием
            try:
                s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                logger.debug(f"Копирование: {src_key} -> {dst_key}")
                s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
                logger.debug(f"Удаление: {src_key}")
                s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                logger.info(f"✅ Успешно перемещен: {src_key} -> {dst_key}")
            except ClientError as head_err:
                if head_err.response['Error']['Code'] == '404':
                    logger.warning(f"Исходный файл {src_key} не найден для перемещения.")
                    # Не считаем это ошибкой перемещения, если файла нет
                else:
                    raise # Пробрасываем другие ошибки head_object

        except ClientError as e:
            logger.error(f"Ошибка B2 при перемещении {src_key}: {e}")
            all_moved = False
        except Exception as e:
            logger.error(f"Неизвестная ошибка при перемещении {src_key}: {e}", exc_info=True)
            all_moved = False
    return all_moved

def process_folders(s3, folders):
    """Сортирует готовые группы файлов по папкам (666 -> 555 -> 444)."""
    logger.info("Начало сортировки папок...")
    # Проходим папки от конца к началу (666, 555)
    for i in range(len(folders) - 1, 0, -1):
        src_folder = folders[i] # e.g., 666/
        dst_folder = folders[i - 1] # e.g., 555/
        logger.info(f"Проверка папки {src_folder} для перемещения в {dst_folder}...")

        src_files = list_files_in_folder(s3, src_folder)
        ready_groups_src = get_ready_groups(src_files)

        if not ready_groups_src:
            logger.info(f"В папке {src_folder} нет готовых групп для перемещения.")
            continue

        logger.info(f"Найдены готовые группы в {src_folder}: {ready_groups_src}")

        # Проверяем, есть ли место в целевой папке (наличие хотя бы одной ГОТОВОЙ группы)
        dst_files = list_files_in_folder(s3, dst_folder)
        ready_groups_dst = get_ready_groups(dst_files)

        moved_count = 0
        can_move = len(ready_groups_dst) == 0 # Можно перемещать, если в целевой папке НЕТ готовых групп

        if not can_move:
            logger.info(f"Целевая папка {dst_folder} уже содержит готовые группы. Перемещение из {src_folder} отложено.")
            continue # Переходим к следующей паре папок (если есть)

        # Если можно перемещать
        for group_id in ready_groups_src:
            logger.info(f"Попытка перемещения группы {group_id} из {src_folder} в {dst_folder}...")
            if move_group(s3, src_folder, dst_folder, group_id):
                moved_count += 1
                # После успешного перемещения целевая папка становится "занятой"
                # и мы не можем перемещать другие группы в НЕЕ в ЭТОМ цикле
                logger.info(f"Группа {group_id} перемещена. Папка {dst_folder} теперь занята для этого цикла.")
                break # Прерываем перемещение из src_folder в этом цикле
            else:
                logger.error(f"Не удалось переместить группу {group_id}. Сортировка из {src_folder} прервана.")
                break # Прерываем перемещение из src_folder при ошибке

        logger.info(f"Из папки {src_folder} перемещено групп: {moved_count}")

    logger.info("Сортировка папок завершена.")


def handle_publish(s3, config_public):
    """
    Архивирует группы файлов по generation_id из config_public["generation_id"].
    Возвращает True, если были внесены изменения в переданный config_public, иначе False.
    """
    generation_ids_to_archive = config_public.get("generation_id", [])
    if not generation_ids_to_archive:
        logger.info("📂 Нет ID для архивации в config_public['generation_id'].")
        return False # Изменений нет

    if not isinstance(generation_ids_to_archive, list):
        logger.warning(f"Ключ 'generation_id' не список: {generation_ids_to_archive}. Преобразование.")
        # Преобразуем в список строк для единообразия
        generation_ids_to_archive = [str(gid) for gid in generation_ids_to_archive] \
                                       if isinstance(generation_ids_to_archive, (list, tuple)) \
                                       else [str(generation_ids_to_archive)]

    logger.info(f"ID для архивации (из config_public): {generation_ids_to_archive}")

    archived_ids = [] # Список ID, которые были успешно заархивированы
    failed_ids = []   # Список ID, которые не удалось заархивировать

    # Создаем копию списка для итерации, чтобы безопасно удалять из оригинала
    ids_to_process = list(generation_ids_to_archive)

    for generation_id in ids_to_process:
        clean_id = generation_id.replace(".json", "") # На всякий случай
        if not FILE_NAME_PATTERN.match(clean_id):
            logger.warning(f"ID '{generation_id}' не соответствует паттерну, пропуск архивации.")
            failed_ids.append(generation_id) # Считаем ошибкой, не удаляем из списка
            continue

        logger.info(f"🔄 Архивируем группу: {clean_id}")
        success = True
        found_any_file = False

        # Ищем файлы во всех рабочих папках (444, 555, 666)
        for folder in FOLDERS:
            folder_norm = folder.rstrip('/') + '/'
            for ext in FILE_EXTENSIONS:
                src_key = f"{folder_norm}{clean_id}{ext}"
                dst_key = f"{ARCHIVE_FOLDER.rstrip('/')}/{clean_id}{ext}"
                try:
                    # Проверяем наличие файла перед перемещением
                    s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    found_any_file = True # Нашли хотя бы один файл группы

                    logger.debug(f"Копирование для архивации: {src_key} -> {dst_key}")
                    s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)

                    logger.debug(f"Удаление оригинала: {src_key}")
                    s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)

                    logger.info(f"✅ Заархивировано и удалено: {src_key}")

                except ClientError as e:
                    error_code = e.response.get('Error', {}).get('Code')
                    if error_code == 'NoSuchKey' or '404' in str(e):
                        # Файл не найден в этой папке, это нормально, ищем в других
                        logger.debug(f"Файл {src_key} не найден в {folder_norm}.")
                        continue
                    else:
                        # Другая ошибка Boto3 при работе с файлом
                        logger.error(f"Ошибка B2 при архивации {src_key}: {e}")
                        success = False
                except Exception as e:
                    logger.error(f"Неизвестная ошибка при архивации {src_key}: {e}", exc_info=True)
                    success = False

        # Оцениваем результат архивации для данного ID
        if not found_any_file:
            logger.warning(f"Не найдено файлов для архивации ID {clean_id} ни в одной из папок. Считаем обработанным.")
            # Если файлов не было, считаем, что ID обработан и его можно убрать из списка
            archived_ids.append(generation_id)
        elif success:
            logger.info(f"Группа {clean_id} успешно заархивирована.")
            archived_ids.append(generation_id)
        else:
            logger.error(f"Не удалось полностью заархивировать {clean_id}.")
            failed_ids.append(generation_id) # Оставляем в списке для повторной попытки

    # Обновляем список generation_id в переданном словаре config_public
    if archived_ids:
        current_list = config_public.get("generation_id", [])
        # Создаем новый список, исключая успешно заархивированные ID
        new_archive_list = [gid for gid in current_list if gid not in archived_ids]

        if not new_archive_list:
            # Если список стал пустым, удаляем ключ (или ставим null/пустой список)
            if "generation_id" in config_public:
                # del config_public["generation_id"] # Вариант с удалением ключа
                config_public["generation_id"] = [] # Вариант с пустым списком
                logger.info("Список generation_id в config_public очищен.")
        else:
            config_public["generation_id"] = new_archive_list
            logger.info(f"Обновлен список generation_id в config_public: {new_archive_list}")

        return True # Были изменения в config_public
    else:
        logger.info("Не было успешно заархивировано ни одного ID из списка.")
        return False # Изменений не было

def run_script(script_path, args_list=[], timeout=600):
    """Запускает дочерний Python скрипт и возвращает True при успехе."""
    command = [sys.executable, script_path] + args_list
    logger.info(f"Запуск команды: {' '.join(command)}")
    try:
        # Используем PIPE для перехвата stdout/stderr
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8')
        stdout, stderr = process.communicate(timeout=timeout)

        # Логируем stdout и stderr
        if stdout:
            logger.info(f"Stdout от {os.path.basename(script_path)}:\n--- START STDOUT ---\n{stdout.strip()}\n--- END STDOUT ---")
        if stderr:
            # Логируем stderr как ошибку, если процесс завершился неуспешно, иначе как warning
            if process.returncode != 0:
                logger.error(f"Stderr от {os.path.basename(script_path)} (код {process.returncode}):\n--- START STDERR ---\n{stderr.strip()}\n--- END STDERR ---")
            else:
                logger.warning(f"Stderr от {os.path.basename(script_path)} (код 0):\n--- START STDERR ---\n{stderr.strip()}\n--- END STDERR ---")

        if process.returncode == 0:
            logger.info(f"✅ Скрипт {os.path.basename(script_path)} успешно завершен.")
            return True
        else:
            logger.error(f"❌ Скрипт {os.path.basename(script_path)} завершился с кодом {process.returncode}.")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"⏰ Таймаут ({timeout} сек) при выполнении {os.path.basename(script_path)}.")
        # Попытка завершить процесс
        try:
            process.terminate()
            time.sleep(1) # Даем время завершиться
            if process.poll() is None: # Если все еще работает
                process.kill()
                logger.warning(f"Процесс {os.path.basename(script_path)} был принудительно завершен (kill).")
        except Exception as kill_err:
            logger.error(f"Ошибка при попытке завершить процесс {os.path.basename(script_path)}: {kill_err}")
        return False
    except FileNotFoundError:
        logger.error(f"❌ Скрипт не найден: {script_path}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске {os.path.basename(script_path)}: {e}", exc_info=True)
        return False


# === Основная функция ===
def main():
    parser = argparse.ArgumentParser(description='Manage B2 storage and content generation workflow.')
    parser.add_argument('--zero-delay', action='store_true', default=False, help='Skip initial delay (less relevant now).')
    args = parser.parse_args()
    zero_delay_flag = args.zero_delay # Флаг сейчас мало влияет
    logger.info(f"Флаг --zero-delay установлен: {zero_delay_flag} (менее актуален)")

    tasks_processed = 0
    try:
        max_tasks_per_run = int(config.get('WORKFLOW.max_tasks_per_run', 1))
    except (ValueError, TypeError):
        logger.warning("Некорректное значение WORKFLOW.max_tasks_per_run. Используется 1.")
        max_tasks_per_run = 1
    logger.info(f"Максимальное количество задач за запуск: {max_tasks_per_run}")

    b2_client = None
    config_public = {}
    config_gen = {}
    config_mj = {}
    lock_acquired = False
    task_completed_successfully = False # Флаг для финальной очистки config_gen

    # --- Блок try/finally для гарантированного снятия блокировки ---
    try:
        b2_client = get_b2_client()
        if not b2_client:
            # Логируем ошибку и выходим, если клиент B2 не создан
            logger.critical("Не удалось инициализировать B2 клиент. Завершение работы.")
            sys.exit(1) # Выход с кодом ошибки

        # --- Проверка и установка блокировки ---
        logger.info(f"Проверка блокировки в {CONFIG_PUBLIC_REMOTE_PATH}...")
        # Используем дефолтное значение, если файл не найден или пуст
        config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, default_value={"processing_lock": False})
        if config_public is None:
             # Если load_b2_json вернул None (ошибка загрузки/парсинга)
             logger.error("Критическая ошибка: Не удалось загрузить или распарсить config_public.json. Завершение работы.")
             sys.exit(1)

        # Проверяем значение ключа, обрабатывая случай его отсутствия
        if config_public.get("processing_lock", False): # False - безопасное значение по умолчанию
            logger.warning("🔒 Обнаружена активная блокировка (processing_lock: true). Завершение работы.")
            return # Выходим без ошибки, т.к. это ожидаемое поведение

        # Устанавливаем блокировку
        config_public["processing_lock"] = True
        if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public):
            logger.info("🔒 Блокировка установлена.")
            lock_acquired = True # Флаг, что блокировка была успешно установлена нами
        else:
            logger.error("❌ Не удалось установить блокировку (ошибка сохранения config_public.json). Завершение работы.")
            # Не устанавливаем lock_acquired = True
            sys.exit(1) # Выходим с ошибкой

        # --- Загрузка остальных конфигов ---
        logger.info("Загрузка остальных конфигурационных файлов...")
        config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, default_value={"generation_id": None})
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, default_value={"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None})

        if config_gen is None or config_mj is None:
             logger.error("Критическая ошибка: Не удалось загрузить config_gen.json или config_midjourney.json. Завершение работы.")
             sys.exit(1) # Выходим с ошибкой

        # --- Основной цикл обработки ---
        logger.info("--- Начало основного цикла обработки ---")
        while tasks_processed < max_tasks_per_run:
            logger.info(f"--- Итерация цикла обработки #{tasks_processed + 1} / {max_tasks_per_run} ---")

            # Перезагрузка конфигов для актуальности состояния
            logger.debug("Перезагрузка конфигурационных файлов из B2...")
            # Передаем текущие словари как default_value для сохранения состояния при ошибке загрузки
            config_public_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, default_value=config_public)
            config_mj_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, default_value=config_mj)

            # Проверяем, что перезагрузка прошла успешно
            if config_public_reloaded is None or config_mj_reloaded is None:
                logger.error("Не удалось перезагрузить конфиги B2 внутри цикла. Прерывание.")
                break # Выходим из цикла while

            # Обновляем рабочие переменные
            config_public = config_public_reloaded
            config_mj = config_mj_reloaded

            # Убедимся, что ключи существуют (для безопасного .get)
            config_mj.setdefault("midjourney_task", None)
            config_mj.setdefault("midjourney_results", {})
            config_mj.setdefault("generation", False)
            config_mj.setdefault("status", None)

            # *** ДОБАВЛЕНО ЛОГИРОВАНИЕ СОСТОЯНИЯ ***
            logger.info(f"Текущее состояние config_gen: {json.dumps(config_gen)}")
            logger.info(f"Текущее состояние config_mj: {json.dumps(config_mj)}")
            # *** КОНЕЦ ЛОГИРОВАНИЯ ***

            action_taken_in_iteration = False # Флаг, что в этой итерации было выполнено действие

            # --- Проверка состояний (Порядок важен!) ---

            # Сценарий 0: Таймаут MJ -> Генерация Mock
            if config_mj.get('status') == 'timed_out_mock_needed':
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id:
                    logger.error("❌ Статус 'timed_out_mock_needed', но нет generation_id в config_gen! Прерывание.")
                    break
                logger.warning(f"Статус таймаута MJ для ID {current_generation_id}. Запуск генерации имитации.")
                script_args = ['--generation_id', current_generation_id, '--use-mock']
                if run_script(GENERATE_MEDIA_SCRIPT, script_args, timeout=300):
                    logger.info(f"Имитация успешно создана для ID {current_generation_id}.")
                    tasks_processed += 1
                    task_completed_successfully = True # Задача завершена (имитацией)
                    break # Выходим из цикла while, т.к. задача выполнена
                else:
                    logger.error(f"Ошибка генерации имитации для ID {current_generation_id}. Прерывание.")
                    break # Выходим из цикла while при ошибке

            # Сценарий 3: Результаты MJ Готовы -> Генерация Видео / Запуск Upscale
            elif config_mj.get('midjourney_results') and isinstance(config_mj['midjourney_results'].get('task_result'), dict):
                task_res = config_mj['midjourney_results']['task_result']
                # Проверяем наличие URL или actions (для /imagine)
                has_urls = (isinstance(task_res.get("temporary_image_urls"), list) and task_res["temporary_image_urls"]) or \
                           (isinstance(task_res.get("image_urls"), list) and task_res["image_urls"]) or \
                           (isinstance(task_res.get("image_url"), str) and task_res["image_url"].startswith("http"))
                has_actions = isinstance(task_res.get("actions"), list)

                if has_urls or has_actions: # Достаточно наличия URL или Actions
                    action_taken_in_iteration = True
                    current_generation_id = config_gen.get("generation_id")
                    if not current_generation_id:
                        logger.error("❌ Результаты MJ есть, но нет generation_id в config_gen! Прерывание.")
                        break
                    logger.info(f"Обнаружены результаты MJ для ID {current_generation_id}. Запуск обработки медиа.")
                    script_args = ['--generation_id', current_generation_id]
                    if run_script(GENERATE_MEDIA_SCRIPT, script_args, timeout=600): # Увеличенный таймаут для Runway
                        logger.info(f"Обработка медиа (generate_media.py) успешно запущена/выполнена для ID {current_generation_id}.")

                        # --- ИСПРАВЛЕНИЕ ЛОГИКИ ЗАВЕРШЕНИЯ ---
                        logger.info("Перезагрузка config_midjourney.json ПОСЛЕ generate_media...")
                        config_mj_after_media = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_MEDIA_CHECK_PATH, default_value=None)
                        if config_mj_after_media is None:
                            logger.error("Критическая ошибка: не удалось перезагрузить config_mj после generate_media. Прерывание.")
                            break
                        config_mj = config_mj_after_media # Обновляем состояние

                        # *** ДОБАВЛЕНО ЛОГИРОВАНИЕ СОСТОЯНИЯ ПОСЛЕ MEDIA ***
                        logger.info(f"Состояние config_mj ПОСЛЕ generate_media: {json.dumps(config_mj)}")
                        # *** КОНЕЦ ЛОГИРОВАНИЯ ***

                        # Проверяем, была ли запущена НОВАЯ задача (upscale/variation)
                        if config_mj.get('midjourney_task') and isinstance(config_mj['midjourney_task'], dict):
                            logger.info("Обнаружена НОВАЯ задача MJ (вероятно, upscale/variation). Задача НЕ завершена. Продолжаем цикл.")
                            continue # Переходим к следующей итерации для обработки новой задачи
                        else:
                            logger.info("Новая задача MJ не обнаружена. Считаем задачу ЗАВЕРШЕННОЙ.")
                            tasks_processed += 1
                            task_completed_successfully = True # Задача действительно завершена
                            break # Выходим из цикла while
                        # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                    else:
                        logger.error(f"Ошибка обработки медиа (generate_media.py) для ID {current_generation_id}. Прерывание.")
                        break # Выходим из цикла while при ошибке
                else:
                    logger.warning(f"Найдены midjourney_results, но нет URL изображений или actions: {json.dumps(task_res, indent=2)[:500]}... Пропуск.")

            # Сценарий 2: Ожидание/Проверка MJ
            elif config_mj.get('midjourney_task') and isinstance(config_mj['midjourney_task'], dict):
                action_taken_in_iteration = True
                task_info = config_mj['midjourney_task']
                task_id = task_info.get("task_id")
                requested_at_str = task_info.get("requested_at_utc")
                if not task_id:
                    logger.error("❌ Задача MJ есть, но task_id не найден в словаре. Прерывание."); break

                logger.info(f"Активная задача MJ: {task_id}. Запуск проверки статуса.")
                if run_script(WORKSPACE_MEDIA_SCRIPT, timeout=180):
                    logger.info(f"{os.path.basename(WORKSPACE_MEDIA_SCRIPT)} успешно выполнен.")
                    logger.info("Перезагрузка config_midjourney.json для проверки результата...")
                    config_mj_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_CHECK_PATH, default_value=None)
                    if config_mj_reloaded is None:
                        logger.error("Критическая ошибка: не удалось перезагрузить config_mj после проверки статуса. Прерывание."); break
                    config_mj = config_mj_reloaded # Обновляем состояние

                    # *** ДОБАВЛЕНО ЛОГИРОВАНИЕ СОСТОЯНИЯ ПОСЛЕ ПРОВЕРКИ ***
                    logger.info(f"Состояние config_mj ПОСЛЕ Workspace_media: {json.dumps(config_mj)}")
                    # *** КОНЕЦ ЛОГИРОВАНИЯ ***

                    # Проверяем, появились ли результаты ПОСЛЕ проверки
                    if config_mj.get('midjourney_results') and isinstance(config_mj['midjourney_results'].get('task_result'), dict):
                        logger.info("✅ Результаты Midjourney обнаружены после проверки! Продолжаем цикл.")
                        continue # Переходим к следующей итерации для обработки результатов

                    logger.info("Результаты Midjourney еще не готовы.")
                    # Проверка таймаута
                    if requested_at_str:
                        try:
                            # Убираем 'Z' если есть, добавляем часовой пояс UTC
                            if requested_at_str.endswith('Z'):
                                requested_at_str = requested_at_str[:-1] + '+00:00'
                            # Парсим ISO строку
                            requested_at_dt = datetime.fromisoformat(requested_at_str)
                            # Убедимся, что время в UTC
                            if requested_at_dt.tzinfo is None:
                                requested_at_dt = requested_at_dt.replace(tzinfo=timezone.utc)
                            else:
                                requested_at_dt = requested_at_dt.astimezone(timezone.utc)

                            now_utc = datetime.now(timezone.utc)
                            elapsed_time = now_utc - requested_at_dt
                            logger.info(f"Время с момента запроса MJ ({task_id}): {elapsed_time}")

                            if elapsed_time > timedelta(seconds=MJ_TIMEOUT_SECONDS):
                                logger.warning(f"⏰ Превышен таймаут ожидания Midjourney ({MJ_TIMEOUT_SECONDS / 3600:.1f} ч) для задачи {task_id}!")
                                # Устанавливаем статус таймаута и очищаем задачу
                                config_mj['midjourney_task'] = None
                                config_mj['status'] = 'timed_out_mock_needed'
                                config_mj['midjourney_results'] = {} # Очищаем старые результаты на всякий случай
                                config_mj['generation'] = False
                                logger.info("Установлен статус 'timed_out_mock_needed', задача MJ очищена.")
                                # Сохраняем измененный конфиг
                                logger.info("Сохранение config_midjourney.json (статус таймаута) в B2...")
                                if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_TIMEOUT_PATH, config_mj):
                                    logger.error("!!! Не удалось сохранить config_mj после установки таймаута!")
                                else:
                                    logger.info("✅ Config_mj со статусом таймаута сохранен.")
                                # Не прерываем цикл, следующая итерация обработает статус таймаута
                                continue
                            else:
                                logger.info("Таймаут ожидания MJ не достигнут.")
                        except ValueError as date_err:
                            logger.error(f"Ошибка парсинга метки времени '{requested_at_str}': {date_err}. Проверка таймаута невозможна.")
                        except Exception as time_err:
                            logger.error(f"Ошибка при проверке времени: {time_err}. Проверка таймаута не удалась.", exc_info=True)
                    else:
                        logger.warning("Метка времени 'requested_at_utc' отсутствует в задаче MJ. Проверка таймаута невозможна.")

                    # Если таймаут не истек и результаты не готовы, прерываем ЦИКЛ МЕНЕДЖЕРА
                    logger.info("Проверка статуса MJ завершена для этого запуска. Ожидание следующего запуска по расписанию.")
                    break # Выходим из цикла while

                else:
                    logger.error(f"Ошибка выполнения {os.path.basename(WORKSPACE_MEDIA_SCRIPT)}. Прерывание.")
                    break # Выходим из цикла while при ошибке

            # Сценарий 4: Инициация Задачи MJ (если generation: true)
            elif config_mj.get('generation') is True:
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                # Обработка неконсистентности: generation=true, но нет ID
                if not current_generation_id:
                    logger.warning("⚠️ Обнаружен флаг generation:true, но нет generation_id в config_gen! Сброс флага.")
                    config_mj['generation'] = False
                    # Сохраняем исправленный config_mj
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_RESET_PATH, config_mj):
                         logger.info("Флаг 'generation' сброшен в B2 из-за отсутствия ID.")
                    else:
                         logger.error("Не удалось сохранить сброшенный флаг 'generation' в B2!")
                    continue # Переходим к следующей итерации для переоценки состояния

                # Если ID есть, запускаем инициацию
                logger.info(f"Флаг generation:true для ID {current_generation_id}. Запуск инициации задачи MJ.")
                script_args = ['--generation_id', current_generation_id]
                if run_script(GENERATE_MEDIA_SCRIPT, script_args, timeout=120):
                    logger.info(f"Инициация задачи MJ (generate_media.py) успешно запущена для ID {current_generation_id}.")
                    continue # Переходим к следующей итерации для обработки новой задачи
                else:
                    logger.error(f"Ошибка инициации задачи MJ для ID {current_generation_id}. Прерывание.")
                    break # Выходим из цикла while при ошибке

            # Сценарий 1: Уборка / Генерация Контента (если ничего другого не активно)
            else:
                action_taken_in_iteration = True
                logger.info("Нет активных задач MJ или флага генерации. Выполнение Уборки и проверка папки 666/...")

                # "Уборка"
                logger.info("Запуск handle_publish (архивация)...")
                config_public_copy = config_public.copy() # Работаем с копией
                if handle_publish(b2_client, config_public_copy):
                    logger.info("handle_publish внес изменения, сохраняем config_public...")
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_copy):
                        config_public = config_public_copy # Обновляем основную переменную
                    else:
                        logger.error("Не удалось сохранить config_public после handle_publish!")
                        # Решаем, прерывать ли работу или продолжить со старым config_public
                        # Пока продолжим, но это может привести к повторной архивации
                else:
                    logger.info("handle_publish не внес изменений в config_public.")

                logger.info("Запуск process_folders (сортировка)...")
                process_folders(b2_client, FOLDERS)

                # Проверка папки 666/ на ГОТОВЫЕ группы
                logger.info("Проверка наличия ГОТОВЫХ ГРУПП в папке 666/...")
                files_in_666 = list_files_in_folder(b2_client, FOLDERS[-1]) # FOLDERS[-1] это '666/'
                ready_groups_in_666 = get_ready_groups(files_in_666)

                if not ready_groups_in_666:
                    # Если ГОТОВЫХ групп нет, запускаем генерацию нового контента
                    logger.info(f"В папке 666/ нет готовых групп. Запуск генерации нового контента...")
                    try:
                        new_id_base = generate_file_id() # Генерируем ID
                        if not new_id_base:
                            raise ValueError("Функция generate_file_id не вернула ID")
                        logger.info(f"Сгенерирован новый ID: {new_id_base}")

                        # Сохраняем новый ID в config_gen.json
                        config_gen["generation_id"] = new_id_base
                        if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen):
                             raise Exception(f"Не удалось сохранить новый ID {new_id_base} в {CONFIG_GEN_REMOTE_PATH}")
                        logger.info(f"Новый ID {new_id_base} сохранен в {CONFIG_GEN_REMOTE_PATH}")

                        # Запускаем generate_content.py
                        script_args = ['--generation_id', new_id_base]
                        if run_script(GENERATE_CONTENT_SCRIPT, script_args, timeout=600):
                            logger.info(f"Генерация контента (generate_content.py) успешно запущена для ID {new_id_base}.")
                            continue # Переходим к след итерации (обработать generation:true)
                        else:
                            logger.error(f"Ошибка генерации контента для ID {new_id_base}. Прерывание.")
                            # Очищаем ID, если генерация не удалась? (Пока нет)
                            break # Выходим из цикла while при ошибке

                    except Exception as gen_err:
                        logger.error(f"Ошибка при генерации/запуске контента: {gen_err}. Прерывание.", exc_info=True)
                        break # Выходим из цикла while при ошибке
                else:
                    # Если ГОТОВЫЕ группы есть, ничего не генерируем
                    logger.info(f"В папке 666/ есть готовые группы ({len(ready_groups_in_666)} шт.). Генерация нового контента не требуется. Завершение цикла.")
                    break # Выходим из цикла while

            # Если ни одно из условий не сработало (маловероятно, но на всякий случай)
            if not action_taken_in_iteration:
                logger.warning("Не найдено активных состояний для обработки в этой итерации. Завершение цикла.")
                break

        # --- Конец основного цикла while ---
        logger.info(f"--- Основной цикл обработки завершен. Обработано задач: {tasks_processed} ---")

        # --- Логика завершения задачи (очистка config_gen) ---
        if task_completed_successfully:
            logger.info("Задача успешно обработана, обновление финальных статусов...")
            try:
                # Перезагружаем config_gen для актуальности перед очисткой
                config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, default_value=config_gen)
                if config_gen is None:
                    raise Exception("Не удалось загрузить config_gen перед финальной очисткой")

                completed_id = config_gen.get("generation_id")
                if completed_id:
                    # Очищаем generation_id в config_gen
                    config_gen["generation_id"] = None
                    logger.info(f"Очистка generation_id ('{completed_id}') в config_gen.")
                    # Сохраняем обновленный config_gen
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen):
                        logger.info("Обновленный config_gen (с null ID) сохранен.")
                    else:
                        logger.error("!!! Не удалось сохранить очищенный config_gen!")
                else:
                    logger.warning("Не найден generation_id в config_gen для очистки (возможно, уже был очищен).")
            except Exception as final_save_err:
                logger.error(f"Ошибка при финальной очистке config_gen: {final_save_err}", exc_info=True)
        else:
             logger.info("Флаг task_completed_successfully не установлен, очистка config_gen не требуется.")

    # --- Обработка исключений основного блока ---
    except ConnectionError as conn_err:
        logger.error(f"❌ Ошибка соединения B2: {conn_err}")
        # Не пытаемся снять блокировку, если ошибка соединения
        lock_acquired = False # Считаем, что блокировку не удерживаем
    except Exception as main_exec_err:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в главном блоке: {main_exec_err}", exc_info=True)
        # Блокировка может остаться, попытаемся снять в finally

    # --- Блок finally для снятия блокировки ---
    finally:
        if lock_acquired:
            logger.info("Снятие блокировки (processing_lock=false)...")
            # Пытаемся получить клиент B2 еще раз, если он был потерян
            if not b2_client:
                 try: b2_client = get_b2_client()
                 except Exception: b2_client = None

            if b2_client:
                # Загружаем последнюю версию config_public перед снятием блокировки
                config_public_final = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, default_value=None)
                if config_public_final is not None:
                    config_public_final["processing_lock"] = False
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_final):
                        logger.info("🔓 Блокировка успешно снята.")
                    else:
                        # Это критично, блокировка останется!
                        logger.critical("!!! НЕ УДАЛОСЬ СНЯТЬ БЛОКИРОВКУ !!!")
                else:
                    logger.error("❌ Не удалось загрузить config_public в finally для снятия блокировки!")
            else:
                logger.error("❌ Не удалось получить B2 клиент в finally для снятия блокировки!")
        else:
            logger.info("Блокировка не была установлена или была потеряна, снятие не требуется.")

        # Очистка временных локальных файлов
        temp_files = [
            CONFIG_PUBLIC_LOCAL_PATH, CONFIG_GEN_LOCAL_PATH, CONFIG_MJ_LOCAL_PATH,
            CONFIG_MJ_LOCAL_CHECK_PATH, CONFIG_MJ_LOCAL_TIMEOUT_PATH,
            CONFIG_MJ_LOCAL_RESET_PATH, CONFIG_MJ_LOCAL_MEDIA_CHECK_PATH
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
    exit_code = 1 # По умолчанию - код ошибки
    try:
        main()
        exit_code = 0 # Если main() завершился без исключений
    except SystemExit as e:
        exit_code = e.code if isinstance(e.code, int) else 1 # Используем код из SystemExit
    except Exception as top_level_err:
         # Логируем неперехваченные ошибки
         try:
             logger.critical(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ: {top_level_err}", exc_info=True)
         except NameError: # Если логгер недоступен
             print(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ (логгер недоступен): {top_level_err}")
         exit_code = 1 # Общий код ошибки
    finally:
        logging.info(f"Скрипт завершается с кодом выхода: {exit_code}")
        sys.exit(exit_code) # Выход с финальным кодом
