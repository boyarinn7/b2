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
    from modules.utils import is_folder_empty, ensure_directory_exists, generate_file_id, load_b2_json, \
        save_b2_json
    from modules.api_clients import get_b2_client
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.config_manager import ConfigManager
except ModuleNotFoundError as import_err:
    print(f"Ошибка импорта модулей проекта: {import_err}")
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
    print(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}", file=sys.stderr)
    sys.exit(1)


# === Константы ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME', 'default-bucket-name'))
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"
CONFIG_GEN_REMOTE_PATH = "config/config_gen.json"
CONFIG_MJ_REMOTE_PATH = "config/config_midjourney.json"

# Локальные пути для временных файлов
CONFIG_PUBLIC_LOCAL_PATH = "config_public_local_main.json"
CONFIG_GEN_LOCAL_PATH = "config_gen_local_main.json"
CONFIG_MJ_LOCAL_PATH = "config_mj_local_main.json"
CONFIG_MJ_LOCAL_CHECK_PATH = "config_mj_local_check.json"
CONFIG_MJ_LOCAL_TIMEOUT_PATH = "config_mj_local_timeout.json"
CONFIG_MJ_LOCAL_RESET_PATH = "config_mj_local_reset.json"

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

# === Вспомогательные функции === (без изменений)
def list_files_in_folder(s3, folder_prefix):
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if not obj['Key'].endswith('/') and not obj['Key'].endswith('.bzEmpty') and \
               FILE_NAME_PATTERN.match(os.path.splitext(os.path.basename(obj['Key']))[0])
        ]
    except Exception as e: logger.error(f"Ошибка получения списка файлов для папки '{folder_prefix}': {e}"); return []

def get_ready_groups(files):
    groups = {}; required_extensions = set(FILE_EXTENSIONS)
    for file_key in files:
        base_name = os.path.basename(file_key); group_id, ext = os.path.splitext(base_name)
        if FILE_NAME_PATTERN.match(group_id) and ext in FILE_EXTENSIONS: groups.setdefault(group_id, set()).add(ext)
    ready_group_ids = [gid for gid, exts in groups.items() if exts == required_extensions]
    if ready_group_ids: logger.debug(f"Найдены готовые группы: {ready_group_ids}")
    else: logger.debug(f"Готовые группы не найдены среди {len(groups)} частичных групп.")
    return ready_group_ids

def move_group(s3, src_folder, dst_folder, group_id):
    logger.info(f"Перемещение группы '{group_id}' из {src_folder} в {dst_folder}...")
    all_moved = True
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"; dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            logger.debug(f"Копирование: {src_key} -> {dst_key}")
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
            logger.debug(f"Удаление: {src_key}")
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            logger.info(f"✅ Успешно перемещен: {src_key} -> {dst_key}")
        except ClientError as e:
             if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404': logger.warning(f"Файл {src_key} не найден. Пропускаем.")
             else: logger.error(f"Ошибка B2 при перемещении {src_key}: {e}"); all_moved = False
        except Exception as e: logger.error(f"Неизвестная ошибка при перемещении {src_key}: {e}"); all_moved = False
    return all_moved

def process_folders(s3, folders):
    logger.info("Начало сортировки папок...")
    for i in range(len(folders) - 1, 0, -1):
        src_folder = folders[i]; dst_folder = folders[i - 1]
        logger.info(f"Проверка папки {src_folder} для перемещения в {dst_folder}...")
        src_files = list_files_in_folder(s3, src_folder); ready_groups = get_ready_groups(src_files)
        if not ready_groups: logger.info(f"В папке {src_folder} нет готовых групп."); continue
        logger.info(f"Найдены готовые группы в {src_folder}: {ready_groups}")
        dst_files = list_files_in_folder(s3, dst_folder); dst_ready_groups = get_ready_groups(dst_files)
        moved_count = 0
        for group_id in ready_groups:
            if len(dst_ready_groups) < 1:
                if move_group(s3, src_folder, dst_folder, group_id):
                    dst_files = list_files_in_folder(s3, dst_folder); dst_ready_groups = get_ready_groups(dst_files); moved_count += 1
                else: logger.error(f"Не удалось переместить группу {group_id}. Сортировка {src_folder} прервана."); break
            else: logger.info(f"Целевая папка {dst_folder} занята. Перемещение {group_id} отложено."); break
        logger.info(f"Из папки {src_folder} перемещено групп: {moved_count}")
    logger.info("Сортировка папок завершена.")

def handle_publish(s3, config_public):
    """Архивирует группы файлов по generation_id из config_public."""
    # Эта функция теперь просто читает список из config_public и архивирует.
    # Добавление ID в этот список теперь происходит во внешнем скрипте публикации.
    generation_ids_to_archive = config_public.get("generation_id", [])
    if not generation_ids_to_archive: logger.info("📂 Нет ID для архивации в config_public['generation_id']."); return False
    if not isinstance(generation_ids_to_archive, list):
        logger.warning(f"Ключ 'generation_id' не список: {generation_ids_to_archive}. Преобразование.")
        generation_ids_to_archive = [str(generation_ids_to_archive)]
    logger.info(f"ID для архивации (из config_public): {generation_ids_to_archive}")
    archived_ids = []; failed_ids = []
    for generation_id in list(generation_ids_to_archive):
        clean_id = generation_id.replace(".json", "")
        if not FILE_NAME_PATTERN.match(clean_id): logger.warning(f"ID '{generation_id}' не соответствует паттерну, пропуск."); failed_ids.append(generation_id); continue
        logger.info(f"🔄 Архивируем группу: {clean_id}")
        success = True; found_any_file = False
        # Ищем файлы во всех папках (444, 555, 666) для архивации
        for folder in FOLDERS:
            for ext in FILE_EXTENSIONS:
                src_key = f"{folder}{clean_id}{ext}"; dst_key = f"{ARCHIVE_FOLDER.rstrip('/')}/{clean_id}{ext}"
                try:
                    s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key); found_any_file = True
                    logger.debug(f"Копирование для архивации: {src_key} -> {dst_key}")
                    s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
                    logger.debug(f"Удаление оригинала: {src_key}")
                    s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    logger.info(f"✅ Заархивировано и удалено: {src_key}")
                except ClientError as e:
                    if e.response['Error']['Code'] == 'NoSuchKey' or '404' in str(e): logger.debug(f"Файл {src_key} не найден."); continue
                    else: logger.error(f"Ошибка B2 при архивации {src_key}: {e}"); success = False
                except Exception as e: logger.error(f"Ошибка при архивации {src_key}: {e}"); success = False
        if not found_any_file: logger.warning(f"Не найдено файлов для архивации {clean_id}. Считаем обработанным."); archived_ids.append(generation_id)
        elif success: logger.info(f"Группа {clean_id} успешно заархивирована."); archived_ids.append(generation_id)
        else: logger.error(f"Не удалось полностью заархивировать {clean_id}."); failed_ids.append(generation_id)
    if archived_ids:
        current_list = config_public.get("generation_id", []); new_archive_list = [gid for gid in current_list if gid not in archived_ids]
        if not new_archive_list:
            if "generation_id" in config_public: del config_public["generation_id"]; logger.info("Список generation_id в config_public очищен.")
        else: config_public["generation_id"] = new_archive_list; logger.info(f"Обновлен список generation_id: {new_archive_list}")
        return True # Были изменения
    else: logger.info("Не было успешно заархивировано ни одного ID."); return False


# === Основная функция ===
def main():
    parser = argparse.ArgumentParser(description='Manage B2 storage and content generation workflow.')
    parser.add_argument('--zero-delay', action='store_true', default=False, help='Skip the 10-minute delay (less relevant now).')
    args = parser.parse_args()
    zero_delay_flag = args.zero_delay
    logger.info(f"Флаг --zero-delay установлен: {zero_delay_flag} (менее актуален в гибридной модели)")

    tasks_processed = 0
    try:
        max_tasks_per_run = int(config.get('WORKFLOW.max_tasks_per_run', 1))
    except ValueError:
        logger.warning("Некорректное значение WORKFLOW.max_tasks_per_run. Используется 1.")
        max_tasks_per_run = 1
    logger.info(f"Максимальное количество задач за запуск: {max_tasks_per_run}")

    b2_client = None
    config_public = {}
    config_gen = {}
    config_mj = {}
    lock_acquired = False
    task_completed_successfully = False # Флаг для финальной очистки config_gen

    try:
        b2_client = get_b2_client()
        if not b2_client: raise ConnectionError("Не удалось инициализировать B2 клиент.")

        logger.info(f"Проверка блокировки в {CONFIG_PUBLIC_REMOTE_PATH}...")
        config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, {"processing_lock": False})
        if config_public is None: raise Exception("Не удалось загрузить config_public.json")

        if config_public.get("processing_lock", False):
            logger.warning("🔒 Обнаружена активная блокировка. Завершение работы.")
            return

        config_public["processing_lock"] = True
        if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public):
            logger.info("🔒 Блокировка установлена.")
            lock_acquired = True
        else:
            logger.error("❌ Не удалось установить блокировку. Завершение работы.")
            return

        logger.info("Загрузка остальных конфигурационных файлов...")
        config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, {"generation_id": None})
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, {"midjourney_task": None, "midjourney_results": {}, "generation": False, "status": None})

        if config_gen is None or config_mj is None:
             raise Exception("Не удалось загрузить config_gen.json или config_midjourney.json")

        logger.info("--- Начало основного цикла обработки ---")
        while tasks_processed < max_tasks_per_run:
            logger.info(f"--- Итерация цикла обработки #{tasks_processed + 1} / {max_tasks_per_run} ---")

            logger.debug("Перезагрузка конфигурационных файлов из B2...")
            config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public)
            config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, config_mj)
            if config_public is None or config_mj is None:
                logger.error("Не удалось перезагрузить конфиги B2 внутри цикла. Прерывание.")
                break

            config_mj.setdefault("status", None)
            logger.debug(f"Текущие состояния: config_gen={json.dumps(config_gen)}, config_mj={json.dumps(config_mj)}")

            action_taken_in_iteration = False

            # --- Проверка состояний (Порядок важен!) ---

            # Проверка статуса таймаута MJ
            if config_mj.get('status') == 'timed_out_mock_needed':
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id: logger.error("❌ Статус 'timed_out_mock_needed', но нет generation_id! Прерывание."); break
                logger.warning(f"Статус таймаута MJ для ID {current_generation_id}. Запуск генерации имитации.")
                try:
                    logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} --use-mock для ID: {current_generation_id}...")
                    subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id, '--use-mock'], check=True, timeout=300)
                    logger.info(f"{GENERATE_MEDIA_SCRIPT} --use-mock успешно завершен.")
                    tasks_processed += 1; task_completed_successfully = True; break # Задача выполнена (с имитацией)
                except subprocess.CalledProcessError as e: logger.error(f"Ошибка {GENERATE_MEDIA_SCRIPT} --use-mock: {e}. Прерывание."); break
                except subprocess.TimeoutExpired: logger.error(f"Таймаут {GENERATE_MEDIA_SCRIPT} --use-mock. Прерывание."); break
                except Exception as mock_gen_err: logger.error(f"Ошибка генерации имитации: {mock_gen_err}. Прерывание.", exc_info=True); break

            # Сценарий 3: MidJourney Готово
            elif config_mj.get('midjourney_results') and config_mj['midjourney_results'].get('task_result'):
                task_res = config_mj['midjourney_results']['task_result']
                has_urls = (isinstance(task_res.get("temporary_image_urls"), list) and task_res["temporary_image_urls"]) or \
                           (isinstance(task_res.get("image_urls"), list) and task_res["image_urls"]) or \
                           task_res.get("image_url")
                if has_urls:
                    action_taken_in_iteration = True
                    current_generation_id = config_gen.get("generation_id")
                    if not current_generation_id: logger.error("❌ Результаты MJ есть, но нет generation_id! Прерывание."); break
                    logger.info(f"Готовые результаты MJ для ID {current_generation_id}. Запуск генерации медиа.")
                    try:
                        logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} для ID: {current_generation_id}...")
                        subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id], check=True, timeout=600) # Таймаут для Runway
                        logger.info(f"{GENERATE_MEDIA_SCRIPT} успешно завершен (генерация видео).")
                        tasks_processed += 1; task_completed_successfully = True; break # Задача выполнена
                    except subprocess.CalledProcessError as e: logger.error(f"Ошибка {GENERATE_MEDIA_SCRIPT} (ген. видео): {e}. Прерывание."); break
                    except subprocess.TimeoutExpired: logger.error(f"Таймаут {GENERATE_MEDIA_SCRIPT} (ген. видео). Прерывание."); break
                    except Exception as media_gen_err: logger.error(f"Ошибка генерации медиа: {media_gen_err}. Прерывание.", exc_info=True); break
                else: logger.warning(f"Найдены midjourney_results, но нет URL изображений: {task_res}. Пропуск.")

            # Сценарий 2: Проверка MidJourney (Гибридный подход)
            elif config_mj.get('midjourney_task'):
                action_taken_in_iteration = True
                task_info = config_mj['midjourney_task']; task_id = None; requested_at_str = None; config_mj_needs_update = False
                if isinstance(task_info, dict): task_id = task_info.get("task_id"); requested_at_str = task_info.get("requested_at_utc"); logger.debug(f"Извлечено: task_id={task_id}, requested_at_utc={requested_at_str}")
                elif isinstance(task_info, str): task_id = task_info; logger.warning("Старый формат midjourney_task (строка).")
                else: logger.error(f"Неожиданный формат midjourney_task: {task_info}. Прерывание."); break
                if not task_id: logger.error("Задача MJ есть, но task_id не найден. Прерывание."); break
                logger.info(f"Активная задача MJ: {task_id}. Запуск проверки статуса.")
                try:
                    logger.info(f"Запуск {WORKSPACE_MEDIA_SCRIPT}...")
                    subprocess.run([sys.executable, WORKSPACE_MEDIA_SCRIPT], check=True, timeout=180)
                    logger.info(f"{WORKSPACE_MEDIA_SCRIPT} успешно выполнен.")
                    logger.info("Перезагрузка config_midjourney.json для проверки результата...")
                    config_mj_reloaded = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_CHECK_PATH, default_value=None)
                    if config_mj_reloaded is None: logger.error("Критическая ошибка: не удалось перезагрузить config_mj. Прерывание."); break
                    config_mj = config_mj_reloaded
                    logger.debug(f"Состояние config_mj после перезагрузки: {json.dumps(config_mj, indent=2, ensure_ascii=False)}")
                    if config_mj.get('midjourney_results') and config_mj['midjourney_results'].get('task_result'): logger.info("✅ Результаты Midjourney обнаружены после проверки!"); continue
                    logger.info("Результаты Midjourney еще не готовы.")
                    if requested_at_str:
                        try:
                            if requested_at_str.endswith('Z'): requested_at_str = requested_at_str[:-1] + '+00:00'
                            requested_at_dt = datetime.fromisoformat(requested_at_str)
                            if requested_at_dt.tzinfo is None: requested_at_dt = requested_at_dt.replace(tzinfo=timezone.utc)
                            else: requested_at_dt = requested_at_dt.astimezone(timezone.utc)
                            now_utc = datetime.now(timezone.utc); elapsed_time = now_utc - requested_at_dt
                            logger.info(f"Время с момента запроса MJ: {elapsed_time}")
                            if elapsed_time > timedelta(seconds=MJ_TIMEOUT_SECONDS):
                                logger.warning(f"⏰ Превышен таймаут ожидания Midjourney ({MJ_TIMEOUT_SECONDS / 3600:.1f} ч)!")
                                config_mj['midjourney_task'] = None; config_mj['status'] = 'timed_out_mock_needed'; config_mj['midjourney_results'] = {}; config_mj['generation'] = False; config_mj_needs_update = True
                                logger.info("Установлен статус 'timed_out_mock_needed', задача MJ очищена.")
                            else: logger.info("Таймаут ожидания MJ не достигнут. Проверка завершена для этого запуска.")
                        except ValueError as date_err: logger.error(f"Ошибка парсинга метки времени '{requested_at_str}': {date_err}. Проверка таймаута невозможна.");
                        except Exception as time_err: logger.error(f"Ошибка при проверке времени: {time_err}. Проверка таймаута не удалась.", exc_info=True);
                    else: logger.info("Метка времени запроса MJ отсутствует. Проверка таймаута невозможна.")
                    if config_mj_needs_update:
                        logger.info("Сохранение config_midjourney.json (статус таймаута) в B2...")
                        if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_TIMEOUT_PATH, config_mj): logger.error("!!! Не удалось сохранить config_mj после установки таймаута!")
                        else: logger.info("✅ Config_mj со статусом таймаута сохранен.")
                    # Прерываем цикл после проверки, чтобы ждать след. запуска по расписанию
                    logger.info("Завершение текущего запуска менеджера для ожидания следующего по расписанию.")
                    break
                except subprocess.CalledProcessError as e: logger.error(f"Ошибка выполнения {WORKSPACE_MEDIA_SCRIPT}: {e}. Прерывание."); break
                except subprocess.TimeoutExpired: logger.error(f"Таймаут выполнения {WORKSPACE_MEDIA_SCRIPT}. Прерывание."); break
                except Exception as check_err: logger.error(f"Ошибка на этапе проверки MJ: {check_err}. Прерывание.", exc_info=True); break

            # Сценарий 4: Генерация Медиа (Инициация)
            elif config_mj.get('generation') is True:
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                # --- ИЗМЕНЕНО: Обработка неконсистентности ---
                if not current_generation_id:
                    logger.warning("⚠️ Обнаружен флаг generation:true, но нет generation_id! Сброс флага.")
                    config_mj['generation'] = False
                    # Сохраняем исправленный config_mj
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_RESET_PATH, config_mj):
                         logger.info("Флаг 'generation' сброшен в B2 из-за отсутствия ID.")
                    else:
                         logger.error("Не удалось сохранить сброшенный флаг 'generation' в B2!")
                    continue # Переходим к следующей итерации, чтобы переоценить состояние
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                logger.info(f"Флаг generation:true для ID {current_generation_id}. Запуск инициации задачи MJ.")
                try:
                    logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} для ID: {current_generation_id}...")
                    subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id], check=True, timeout=120)
                    logger.info(f"{GENERATE_MEDIA_SCRIPT} успешно завершен (инициация MJ).")
                    continue # Переходим к следующей итерации (проверить task_id)
                except subprocess.CalledProcessError as e: logger.error(f"Ошибка {GENERATE_MEDIA_SCRIPT} (иниц. MJ): {e}. Прерывание."); break
                except subprocess.TimeoutExpired: logger.error(f"Таймаут {GENERATE_MEDIA_SCRIPT} (иниц. MJ). Прерывание."); break
                except Exception as media_init_err: logger.error(f"Ошибка при инициации MJ: {media_init_err}. Прерывание.", exc_info=True); break

            # Сценарий 1: Уборка / Генерация Контента
            else:
                action_taken_in_iteration = True
                logger.info("Нет активных задач MJ. Выполнение Уборки и проверка папки 666/...")
                logger.info("Запуск handle_publish (архивация)...")
                config_public_copy = config_public.copy()
                if handle_publish(b2_client, config_public_copy):
                    logger.info("handle_publish внес изменения, сохраняем config_public...")
                    if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_copy): logger.error("Не удалось сохранить config_public после handle_publish!")
                    else: config_public = config_public_copy
                logger.info("Запуск process_folders (сортировка)...")
                process_folders(b2_client, FOLDERS)
                logger.info("Проверка наличия ГОТОВЫХ ГРУПП в папке 666/...")
                files_in_666 = list_files_in_folder(b2_client, FOLDERS[-1])
                ready_groups_in_666 = get_ready_groups(files_in_666)
                if not ready_groups_in_666:
                    logger.info(f"⚠️ В папке 666/ нет готовых групп. Запуск генерации нового контента...")
                    try:
                        new_id_base = generate_file_id().replace(".json", "")
                        if not new_id_base: raise ValueError("Функция generate_file_id не вернула ID")
                        logger.info(f"Сгенерирован новый ID: {new_id_base}")
                        config_gen["generation_id"] = new_id_base
                        if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen): raise Exception(f"Не удалось сохранить новый ID {new_id_base}")
                        logger.info(f"Новый ID {new_id_base} сохранен в {CONFIG_GEN_REMOTE_PATH}")
                        logger.info(f"Запуск {GENERATE_CONTENT_SCRIPT} для ID: {new_id_base}...")
                        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, '--generation_id', new_id_base], check=True, timeout=600)
                        logger.info(f"{GENERATE_CONTENT_SCRIPT} успешно завершен для ID: {new_id_base}.")
                        continue # Переходим к след итерации (обработать generation:true)
                    except subprocess.CalledProcessError as e: logger.error(f"Ошибка выполнения {GENERATE_CONTENT_SCRIPT}: {e}. Прерывание."); break
                    except subprocess.TimeoutExpired: logger.error(f"Таймаут выполнения {GENERATE_CONTENT_SCRIPT}. Прерывание."); break
                    except Exception as gen_err: logger.error(f"Ошибка при генерации/запуске контента: {gen_err}. Прерывание.", exc_info=True); break
                else:
                    logger.info(f"В папке 666/ есть готовые группы ({len(ready_groups_in_666)} шт.). Генерация нового контента не требуется. Завершение цикла.")
                    break # Выходим из цикла while

            if not action_taken_in_iteration:
                logger.info("Не найдено активных состояний для обработки в этой итерации. Завершение цикла.")
                break

        # --- Конец основного цикла while ---
        logger.info(f"--- Основной цикл обработки завершен. Обработано задач: {tasks_processed} ---")

        # --- Шаг 4.4: Логика завершения задачи ---
        # --- ИЗМЕНЕНО: Убрано добавление ID в config_public ---
        if task_completed_successfully:
            logger.info("Задача успешно обработана, обновление финальных статусов...")
            try:
                # Загружаем только config_gen для очистки
                config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen)
                if config_gen is None: raise Exception("Не удалось загрузить config_gen перед финальной очисткой")

                completed_id = config_gen.get("generation_id")
                if completed_id:
                    # НЕ добавляем в config_public["generation_id"]
                    # logger.info(f"Перенос ID '{completed_id}' в config_public для архивации.")
                    # archive_list = config_public.get("generation_id", [])
                    # ... (логика добавления удалена) ...
                    # config_public["generation_id"] = archive_list

                    # Просто очищаем config_gen
                    config_gen["generation_id"] = None
                    logger.info("Очистка generation_id в config_gen.")
                    # Сохраняем только config_gen
                    # save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public) # Не сохраняем public здесь
                    save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen)
                    logger.info("Обновленный config_gen сохранен.")
                else:
                    logger.warning("Не найден generation_id в config_gen для очистки.")
            except Exception as final_save_err:
                logger.error(f"Ошибка при финальной очистке config_gen: {final_save_err}", exc_info=True)
        # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    except ConnectionError as conn_err: logger.error(f"❌ Ошибка соединения B2: {conn_err}"); lock_acquired = False
    except Exception as main_exec_err: logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в главном блоке: {main_exec_err}", exc_info=True)
    finally:
        if lock_acquired:
            logger.info("Снятие блокировки (processing_lock=False)...")
            if not b2_client:
                 try: b2_client = get_b2_client()
                 except Exception: b2_client = None
            if b2_client:
                config_public_final = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, default_value=None)
                if config_public_final is not None:
                    config_public_final["processing_lock"] = False
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_final): logger.info("🔓 Блокировка успешно снята.")
                    else: logger.error("❌ НЕ УДАЛОСЬ СНЯТЬ БЛОКИРОВКУ!")
                else: logger.error("❌ Не удалось загрузить config_public в finally!")
            else: logger.error("❌ Не удалось получить B2 клиент в finally!")
        else: logger.info("Блокировка не была установлена, снятие не требуется.")
        logger.info("--- Завершение работы b2_storage_manager.py ---")

# === Точка входа ===
if __name__ == "__main__":
    try: main()
    except Exception as top_level_err:
         try: logger.error(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ: {top_level_err}", exc_info=True)
         except NameError: print(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ (логгер недоступен): {top_level_err}")
         sys.exit(1)