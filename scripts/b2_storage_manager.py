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

# Импорты из ваших модулей
try:
    # Попытка импортировать зависимости проекта
    from modules.utils import is_folder_empty, ensure_directory_exists, generate_file_id
    from modules.api_clients import get_b2_client
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.config_manager import ConfigManager
except ModuleNotFoundError as import_err:
    print(f"Ошибка импорта модулей проекта: {import_err}")
    print("Убедитесь, что PYTHONPATH настроен правильно или скрипт запускается из корневой папки.")
    sys.exit(1)

# Импорт boto3 и его исключений
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    print("Ошибка: Необходима библиотека boto3. Установите ее: pip install boto3")
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
    print(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}")
    # Записываем в stderr, так как логгер мог не инициализироваться
    print(f"Критическая ошибка инициализации ConfigManager или Logger: {init_err}", file=sys.stderr)
    sys.exit(1)


# === Константы ===
# Стараемся брать все из конфига или окружения
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME', 'default-bucket-name')) # Добавлен getenv как fallback
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"
CONFIG_GEN_REMOTE_PATH = "config/config_gen.json"
CONFIG_MJ_REMOTE_PATH = "config/config_midjourney.json"

# Локальные пути для временных файлов (лучше использовать папку tmp или уникальные имена)
CONFIG_PUBLIC_LOCAL_PATH = "config_public_local.json"
CONFIG_GEN_LOCAL_PATH = "config_gen_local.json"
CONFIG_MJ_LOCAL_PATH = "config_mj_local.json"

FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get('FILE_PATHS.folder_444', '444/'),
    config.get('FILE_PATHS.folder_555', '555/'),
    config.get('FILE_PATHS.folder_666', '666/')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder', 'archive/') # Использовал другое значение по умолчанию из вашего конфига
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}$") # Убрал расширение из паттерна ID

# Пути к скриптам
SCRIPTS_FOLDER = config.get('FILE_PATHS.scripts_folder', 'scripts')
GENERATE_CONTENT_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_content.py")
WORKSPACE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "Workspace_media.py") # Используем переименованный файл
GENERATE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_media.py")

# === Вспомогательные функции ===

def load_b2_json(client, bucket, remote_path, local_path, default_value=None):
    """Загружает JSON из B2, возвращает default_value при ошибке или отсутствии."""
    # default_value=None лучше, чем {}, чтобы различать пустой файл и его отсутствие
    try:
        logger.debug(f"Загрузка {remote_path} из B2 в {local_path}")
        client.download_file(bucket, remote_path, local_path)
        if os.path.getsize(local_path) > 0:
            with open(local_path, 'r', encoding='utf-8') as f:
                content = json.load(f)
        else:
            logger.warning(f"Загруженный файл {local_path} ({remote_path}) пуст, используем значение по умолчанию.")
            content = default_value
        logger.info(f"Успешно загружен и распарсен {remote_path} из B2.")
        return content
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f"{remote_path} не найден в B2. Используем значение по умолчанию.")
        else:
            logger.error(f"Ошибка B2 при загрузке {remote_path}: {e}")
        return default_value
    except json.JSONDecodeError as json_err:
        logger.error(f"Ошибка парсинга JSON из {local_path} ({remote_path}): {json_err}. Используем значение по умолчанию.")
        return default_value
    except Exception as e:
        logger.error(f"Критическая ошибка загрузки {remote_path}: {e}")
        return default_value # Возвращаем дефолт, чтобы не упасть сразу
    finally:
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except OSError: logger.warning(f"Не удалось удалить временный файл {local_path}")

def save_b2_json(client, bucket, remote_path, local_path, data):
    """Сохраняет словарь data как JSON в B2."""
    try:
        logger.debug(f"Сохранение данных в {remote_path} в B2 через {local_path}")
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        client.upload_file(local_path, bucket, remote_path)
        logger.info(f"Данные успешно сохранены в {remote_path} в B2: {json.dumps(data, ensure_ascii=False)}") # Логируем сохраненные данные
        return True
    except Exception as e:
        logger.error(f"Критическая ошибка сохранения {remote_path}: {e}")
        return False
    finally:
        if os.path.exists(local_path):
            try: os.remove(local_path)
            except OSError: logger.warning(f"Не удалось удалить временный файл {local_path}")

# --- Функции из старого кода (слегка адаптированы) ---
# Убедитесь, что B2_BUCKET_NAME доступен глобально или передается как аргумент

def list_files_in_folder(s3, folder_prefix):
    """Возвращает список файлов в указанной папке (кроме placeholder)."""
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        # Фильтруем по паттерну ИМЕНИ файла (без расширения)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if not obj['Key'].endswith('/') and not obj['Key'].endswith('.bzEmpty') and \
               FILE_NAME_PATTERN.match(os.path.splitext(os.path.basename(obj['Key']))[0])
        ]
    except Exception as e:
        logger.error(f"Ошибка получения списка файлов для папки '{folder_prefix}': {e}")
        return []

def get_ready_groups(files):
    """Возвращает список идентификаторов групп с файлами всех расширений."""
    groups = {}
    for file_key in files:
        base_name = os.path.basename(file_key)
        group_id, ext = os.path.splitext(base_name)
        if FILE_NAME_PATTERN.match(group_id) and ext in FILE_EXTENSIONS:
            groups.setdefault(group_id, set()).add(ext) # Используем set для расширений

    required_extensions = set(FILE_EXTENSIONS)
    ready_group_ids = [
        group_id for group_id, found_extensions in groups.items()
        if found_extensions == required_extensions # Проверяем, что найдены ВСЕ нужные расширения
    ]
    if ready_group_ids:
        logger.debug(f"Найдены готовые группы: {ready_group_ids}")
    else:
        logger.debug(f"Готовые группы не найдены среди {len(groups)} частичных групп.")
    return ready_group_ids

def move_group(s3, src_folder, dst_folder, group_id):
    """Перемещает файлы группы из src_folder в dst_folder."""
    logger.info(f"Перемещение группы '{group_id}' из {src_folder} в {dst_folder}...")
    all_moved = True
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"
        dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            # Проверяем существование перед копированием (опционально, copy_object может выдать ошибку сам)
            # s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            logger.debug(f"Копирование: {src_key} -> {dst_key}")
            s3.copy_object(
                Bucket=B2_BUCKET_NAME,
                CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key},
                Key=dst_key
            )
            logger.debug(f"Удаление: {src_key}")
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            logger.info(f"✅ Успешно перемещен: {src_key} -> {dst_key}")
        except ClientError as e:
             # Если файла нет - это странно, но может случиться при параллельных запусках?
             if e.response['Error']['Code'] == 'NoSuchKey' or e.response['Error']['Code'] == '404':
                 logger.warning(f"Файл {src_key} не найден при попытке перемещения группы {group_id}. Пропускаем.")
             else:
                 logger.error(f"Ошибка B2 при перемещении {src_key}: {e}")
                 all_moved = False # Помечаем, что группа перемещена не полностью
        except Exception as e:
            logger.error(f"Неизвестная ошибка при перемещении {src_key}: {e}")
            all_moved = False
    return all_moved

def process_folders(s3, folders):
    """Перемещает готовые группы файлов между папками."""
    # Убрана логика обновления 'empty' списка - она не нужна по новому ТЗ
    logger.info("Начало сортировки папок...")
    # Идем от предпоследней папки к первой (666 -> 555, 555 -> 444)
    for i in range(len(folders) - 1, 0, -1):
        src_folder = folders[i] # Например, 666/
        dst_folder = folders[i - 1] # Например, 555/
        logger.info(f"Проверка папки {src_folder} для перемещения в {dst_folder}...")

        src_files = list_files_in_folder(s3, src_folder)
        ready_groups = get_ready_groups(src_files)

        if not ready_groups:
            logger.info(f"В папке {src_folder} нет готовых групп для перемещения.")
            continue

        logger.info(f"Найдены готовые группы в {src_folder}: {ready_groups}")
        # Проверяем, не переполнена ли целевая папка (по старому лимиту = 1 группа)
        dst_files = list_files_in_folder(s3, dst_folder)
        dst_ready_groups = get_ready_groups(dst_files)

        moved_count = 0
        for group_id in ready_groups:
            # Старый код проверял len(dst_ready_groups) < 1. Оставляем его пока.
            if len(dst_ready_groups) < 1: # Разрешаем перемещать только одну группу за раз?
                if move_group(s3, src_folder, dst_folder, group_id):
                    # Обновляем состояние целевой папки после успешного перемещения
                    dst_files = list_files_in_folder(s3, dst_folder)
                    dst_ready_groups = get_ready_groups(dst_files)
                    moved_count += 1
                else:
                    logger.error(f"Не удалось полностью переместить группу {group_id} из {src_folder}. Сортировка этой папки прервана.")
                    break # Прерываем перемещение из этой папки при ошибке
            else:
                logger.info(f"Целевая папка {dst_folder} уже содержит готовую группу. Перемещение {group_id} отложено.")
                break # Прерываем перемещение из этой папки, т.к. целевая "занята"
        logger.info(f"Из папки {src_folder} перемещено групп: {moved_count}")
    logger.info("Сортировка папок завершена.")

def handle_publish(s3, config_public):
    """Архивирует группы файлов по generation_id из config_public."""
    # Получаем ID для архивации из ПЕРЕДАННОГО config_public
    generation_ids_to_archive = config_public.get("generation_id", []) # Ожидаем список

    if not generation_ids_to_archive:
        logger.info("📂 Нет generation_id в config_public для архивации.")
        return False # Возвращаем признак, что ничего не изменилось

    # Убедимся, что это список, на всякий случай
    if not isinstance(generation_ids_to_archive, list):
        logger.warning(f"Ключ 'generation_id' в config_public не является списком: {generation_ids_to_archive}. Преобразование в список.")
        generation_ids_to_archive = [str(generation_ids_to_archive)] # Преобразуем в список из одного элемента

    logger.info(f"ID для архивации из config_public: {generation_ids_to_archive}")
    archived_ids = []
    failed_ids = []

    # Используем копию списка для итерации, чтобы безопасно удалять из оригинала
    for generation_id in list(generation_ids_to_archive):
        # Убираем возможное расширение из ID, если оно там есть
        clean_id = generation_id.replace(".json", "")
        if not FILE_NAME_PATTERN.match(clean_id):
            logger.warning(f"ID '{generation_id}' не соответствует паттерну {FILE_NAME_PATTERN.pattern}, пропуск архивации.")
            failed_ids.append(generation_id) # Считаем его "ошибочным"
            continue

        logger.info(f"🔄 Архивируем группу: {clean_id}")
        success = True
        found_any_file = False
        # Архивируем из всех папок FOLDERS (444, 555, 666)
        for folder in FOLDERS:
            for ext in FILE_EXTENSIONS:
                src_key = f"{folder}{clean_id}{ext}"
                # Архивируем в корень ARCHIVE_FOLDER/<id>.<ext>
                dst_key = f"{ARCHIVE_FOLDER.rstrip('/')}/{clean_id}{ext}"
                try:
                    s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    # Файл найден, начинаем перемещение
                    found_any_file = True
                    logger.debug(f"Копирование для архивации: {src_key} -> {dst_key}")
                    s3.copy_object(
                        Bucket=B2_BUCKET_NAME,
                        CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key},
                        Key=dst_key
                    )
                    logger.debug(f"Удаление оригинала: {src_key}")
                    s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    logger.info(f"✅ Успешно заархивировано и удалено: {src_key}")
                except ClientError as e:
                    # Если файла нет - это нормально, ищем в других папках/с другими расширениями
                    if e.response['Error']['Code'] == 'NoSuchKey' or '404' in str(e):
                        logger.debug(f"Файл {src_key} не найден, пропуск.")
                        continue
                    else:
                        logger.error(f"Ошибка B2 при архивации {src_key}: {e}")
                        success = False
                except Exception as e:
                    logger.error(f"Неизвестная ошибка при архивации {src_key}: {e}")
                    success = False

        if not found_any_file:
             logger.warning(f"Не найдено ни одного файла для архивации группы {clean_id} в папках {FOLDERS}. Возможно, ID ошибочный или файлы уже удалены.")
             # Считаем такой ID обработанным, чтобы не пытаться архивировать его снова
             archived_ids.append(generation_id)
        elif success:
            logger.info(f"Группа {clean_id} успешно заархивирована.")
            archived_ids.append(generation_id) # Добавляем оригинальный ID (с .json?) в список успешно заархивированных
        else:
            logger.error(f"Не удалось полностью заархивировать группу {clean_id}. ID останется в списке для следующей попытки.")
            failed_ids.append(generation_id) # Сохраняем ID, которые не удалось обработать

    # Обновляем список ID в переданном словаре config_public
    if archived_ids:
        # Создаем новый список, исключая успешно заархивированные
        current_list = config_public.get("generation_id", [])
        if not isinstance(current_list, list): # Защита
            current_list = []
        new_archive_list = [gid for gid in current_list if gid not in archived_ids]
        if not new_archive_list:
            # Если список стал пуст, удаляем ключ
            if "generation_id" in config_public:
                 del config_public["generation_id"]
                 logger.info("Список generation_id в config_public очищен.")
        else:
             config_public["generation_id"] = new_archive_list
             logger.info(f"Обновлен список generation_id в config_public: {new_archive_list}")
        return True # Возвращаем True, т.к. были изменения
    else:
        logger.info("Не было успешно заархивировано ни одного ID.")
        return False # Нет изменений в списке

# === Основная функция ===
def main():
    # --- Шаг 4.1.2: Обработка аргумента ---
    parser = argparse.ArgumentParser(description='Manage B2 storage and content generation workflow.')
    parser.add_argument('--zero-delay', action='store_true', default=False,
                        help='Skip the 10-minute delay when checking Midjourney task.')
    args = parser.parse_args()
    zero_delay_flag = args.zero_delay
    logger.info(f"Флаг --zero-delay установлен: {zero_delay_flag}")

    # --- Шаг 4.1.3: Инициализация счетчика/лимита ---
    tasks_processed = 0
    max_tasks_per_run = int(config.get('WORKFLOW.max_tasks_per_run', 1)) # Явно преобразуем в int
    logger.info(f"Максимальное количество задач за запуск: {max_tasks_per_run}")

    # --- Шаги 4.1.4 и 4.1.5: Блокировка и Загрузка Конфигов ---
    b2_client = None
    config_public = {}
    config_gen = {}
    config_mj = {}
    lock_acquired = False
    task_completed_successfully = False # Флаг для Шага 4.4

    try:
        # Получаем B2 клиент
        b2_client = get_b2_client()
        if not b2_client:
            raise ConnectionError("Не удалось инициализировать B2 клиент.")

        # 1. Проверяем и устанавливаем блокировку
        logger.info(f"Проверка блокировки в {CONFIG_PUBLIC_REMOTE_PATH}...")
        config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, {"processing_lock": False})

        if config_public is None: # Проверка, если load_b2_json вернул None из-за крит. ошибки
             raise Exception("Не удалось загрузить config_public.json")

        if config_public.get("processing_lock", False):
            logger.warning("🔒 Обнаружена активная блокировка (processing_lock=True). Завершение работы.")
            return

        # Устанавливаем блокировку
        config_public["processing_lock"] = True
        if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public):
            logger.info("🔒 Блокировка (processing_lock=True) успешно установлена в B2.")
            lock_acquired = True
        else:
            logger.error("❌ Не удалось установить блокировку в B2. Завершение работы.")
            return

        # 2. Загружаем остальные конфиги
        logger.info("Загрузка остальных конфигурационных файлов...")
        config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, {"generation_id": None})
        config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, {"midjourney_task": None, "midjourney_results": {}, "generation": False})

        if config_gen is None or config_mj is None:
             raise Exception("Не удалось загрузить config_gen.json или config_midjourney.json")

        # --- Шаг 4.2: Основной цикл и логика состояний ---
        logger.info("--- Начало основного цикла обработки ---")
        while tasks_processed < max_tasks_per_run:
            logger.info(f"--- Итерация цикла обработки #{tasks_processed + 1} / {max_tasks_per_run} ---")

            # Перезагружаем конфиги в начале каждой итерации (кроме gen)
            logger.debug("Перезагрузка конфигурационных файлов из B2...")
            config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public)
            config_mj = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, config_mj)
            # config_gen читаем только один раз в начале, т.к. он хранит ID текущей задачи
            if config_public is None or config_mj is None:
                logger.error("Не удалось перезагрузить конфиги B2 внутри цикла. Прерывание.")
                break # Прерываем цикл, если не можем получить актуальное состояние

            logger.debug(f"Текущие состояния: config_gen={config_gen}, config_mj={config_mj}")

            action_taken_in_iteration = False

            # --- Проверка состояний ---

            # Сценарий 2: Проверка MidJourney
            if config_mj.get('midjourney_task'):
                action_taken_in_iteration = True
                task_id = config_mj['midjourney_task']
                logger.info(f"Обнаружена активная задача Midjourney: {task_id}. Запуск проверки статуса.")
                if not zero_delay_flag:
                    logger.info("Ожидание 10 минут перед проверкой статуса Midjourney...")
                    time.sleep(600)
                    logger.info("Ожидание завершено.")
                try:
                    logger.info(f"Запуск {WORKSPACE_MEDIA_SCRIPT} (ранее fetch_media.py)...")
                    subprocess.run([sys.executable, WORKSPACE_MEDIA_SCRIPT], check=True, timeout=120)
                    logger.info(f"{WORKSPACE_MEDIA_SCRIPT} успешно выполнен.")
                    # Не перезагружаем config_mj здесь, перезагрузим в начале следующей итерации
                    continue # Переходим к следующей итерации, чтобы проверить обновленное состояние
                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка выполнения {WORKSPACE_MEDIA_SCRIPT}: {e}")
                    break # Выходим из цикла при ошибке проверки
                except subprocess.TimeoutExpired:
                    logger.error(f"Таймаут выполнения {WORKSPACE_MEDIA_SCRIPT}.")
                    break # Выходим из цикла при ошибке проверки

            # Сценарий 3: MidJourney Готово
            elif config_mj.get('midjourney_results') and config_mj['midjourney_results'].get('image_urls'):
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id:
                    logger.error("❌ Обнаружены результаты Midjourney, но нет активного generation_id в config_gen.json!")
                    break
                logger.info(f"Обнаружены готовые результаты Midjourney для ID {current_generation_id}. Запуск генерации медиа.")
                try:
                    logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} для ID: {current_generation_id}...")
                    subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id], check=True, timeout=600)
                    logger.info(f"{GENERATE_MEDIA_SCRIPT} успешно завершен для ID: {current_generation_id} (генерация видео).")
                    tasks_processed += 1
                    task_completed_successfully = True # Ставим флаг для Шага 4.4
                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка выполнения {GENERATE_MEDIA_SCRIPT} (генерация видео): {e}")
                    break
                except subprocess.TimeoutExpired:
                    logger.error(f"Таймаут выполнения {GENERATE_MEDIA_SCRIPT} (генерация видео).")
                    break

            # Сценарий 4: Генерация Медиа (Инициация)
            elif config_mj.get('generation') is True:
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id:
                    logger.error("❌ Обнаружен флаг generation:true, но нет активного generation_id в config_gen.json!")
                    break
                logger.info(f"Обнаружен флаг generation:true для ID {current_generation_id}. Запуск инициации задачи Midjourney.")
                try:
                    logger.info(f"Запуск {GENERATE_MEDIA_SCRIPT} для ID: {current_generation_id}...")
                    subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, '--generation_id', current_generation_id], check=True, timeout=120)
                    logger.info(f"{GENERATE_MEDIA_SCRIPT} успешно завершен для ID: {current_generation_id} (инициация Midjourney).")
                    continue # Продолжаем цикл для проверки task_id
                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка выполнения {GENERATE_MEDIA_SCRIPT} (инициация Midjourney): {e}")
                    break
                except subprocess.TimeoutExpired:
                    logger.error(f"Таймаут выполнения {GENERATE_MEDIA_SCRIPT} (инициация Midjourney).")
                    break

            # Сценарий 1: Уборка / Генерация Контента
            else:
                action_taken_in_iteration = True
                logger.info("Нет активных задач Midjourney. Выполнение Уборки и проверка папки 666/...")
                # 1. Уборка - Архивируем ID из config_public
                logger.info("Запуск handle_publish (архивация)...")
                config_public_copy = config_public.copy() # Работаем с копией
                if handle_publish(b2_client, config_public_copy): # handle_publish теперь меняет словарь и возвращает True/False
                    # Если handle_publish что-то изменил, сохраняем config_public
                    logger.info("handle_publish внес изменения в список архивации, сохраняем config_public...")
                    if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_LOCAL_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public_copy):
                         logger.error("Не удалось сохранить config_public после handle_publish!")
                         # Не прерываем цикл, но логируем ошибку
                    else:
                         config_public = config_public_copy # Обновляем нашу переменную config_public
                # 2. Уборка - Сортируем папки
                logger.info("Запуск process_folders (сортировка)...")
                process_folders(b2_client, FOLDERS)
                # 3. Проверяем папку 666/ на наличие ГОТОВЫХ групп
                logger.info("Проверка наличия ГОТОВЫХ ГРУПП в папке 666/...")
                files_in_666 = list_files_in_folder(b2_client, FOLDERS[-1])
                ready_groups_in_666 = get_ready_groups(files_in_666)

                if not ready_groups_in_666:
                    logger.info(f"⚠️ В папке 666/ нет готовых групп ({len(files_in_666)} файлов всего). Запуск генерации нового контента...")
                    try:
                        # Генерируем НОВЫЙ ID
                        new_id = generate_file_id()
                        if not new_id:
                            raise ValueError("Функция generate_file_id не вернула ID")
                        logger.info(f"Сгенерирован новый ID: {new_id}")
                        # Сохраняем НОВЫЙ ID в config_gen.json
                        config_gen["generation_id"] = new_id
                        if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_LOCAL_PATH, CONFIG_GEN_LOCAL_PATH, config_gen):
                            raise Exception(f"Не удалось сохранить новый ID {new_id} в {CONFIG_GEN_REMOTE_PATH}")
                        logger.info(f"Новый ID {new_id} сохранен в {CONFIG_GEN_REMOTE_PATH}")
                        # Запускаем generate_content.py с НОВЫМ ID
                        logger.info(f"Запуск {GENERATE_CONTENT_SCRIPT} для ID: {new_id}...")
                        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, '--generation_id', new_id], check=True, timeout=600)
                        logger.info(f"{GENERATE_CONTENT_SCRIPT} успешно завершен для ID: {new_id}.")
                        # Не увеличиваем tasks_processed здесь, т.к. задача только началась (флаг generation установлен)
                        # Но нужно продолжить цикл, чтобы обработать новый флаг generation: true
                        continue # Переходим к след итерации
                    except subprocess.CalledProcessError as e:
                        logger.error(f"Ошибка выполнения {GENERATE_CONTENT_SCRIPT}: {e}")
                        break
                    except subprocess.TimeoutExpired:
                        logger.error(f"Таймаут выполнения {GENERATE_CONTENT_SCRIPT}.")
                        break
                    except Exception as gen_err:
                        logger.error(f"Ошибка при генерации нового ID, сохранении config_gen или вызове {GENERATE_CONTENT_SCRIPT}: {gen_err}")
                        break
                else:
                    logger.info(f"В папке 666/ есть готовые группы ({len(ready_groups_in_666)} шт.). Генерация нового контента не требуется. Завершение цикла.")
                    break

            # Конец проверки if/elif/else для состояний

            if not action_taken_in_iteration:
                logger.info("Не найдено активных состояний для обработки в этой итерации. Завершение цикла.")
                break

            # Проверка, если лимит задач достигнут после выполнения действия
            if tasks_processed >= max_tasks_per_run:
                logger.info(f"Достигнут лимит задач ({max_tasks_per_run}) за этот запуск.")
                break

        # --- Конец основного цикла while ---
        logger.info(f"--- Основной цикл обработки завершен. Обработано задач: {tasks_processed} ---")

        # --- Шаг 4.4: Логика завершения задачи (если она была) ---
        if task_completed_successfully: # Используем флаг, установленный при tasks_processed += 1
            logger.info("Задача успешно обработана, обновление финальных статусов...")
            try:
                # Перезагружаем конфиги перед финальным обновлением
                config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public)
                config_gen = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen)
                if config_public is None or config_gen is None:
                     raise Exception("Не удалось загрузить config_public или config_gen перед финальным обновлением")

                completed_id = config_gen.get("generation_id")
                if completed_id:
                    logger.info(f"Перенос ID завершенной задачи '{completed_id}' в config_public для архивации.")
                    archive_list = config_public.get("generation_id", [])
                    if not isinstance(archive_list, list): archive_list = []
                    # Убираем .json перед добавлением в список архивации, если он там есть
                    clean_completed_id = completed_id.replace(".json", "")
                    if clean_completed_id not in archive_list:
                        archive_list.append(clean_completed_id)
                    config_public["generation_id"] = archive_list

                    config_gen["generation_id"] = None
                    logger.info("Очистка generation_id в config_gen.")

                    save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public)
                    save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_GEN_REMOTE_PATH, CONFIG_GEN_LOCAL_PATH, config_gen)
                    logger.info("Обновленные config_public и config_gen сохранены в B2.")
                else:
                    logger.warning("Не найден generation_id в config_gen для переноса в config_public.")
            except Exception as final_save_err:
                logger.error(f"Ошибка при финальном обновлении конфигов после завершения задачи: {final_save_err}")

        # --- Конец Шага 4.4 ---

    except Exception as main_exec_err:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в главном блоке: {main_exec_err}")
        # handle_error(...) можно использовать здесь

    finally:
        # --- Гарантированное снятие блокировки ---
        if lock_acquired:
            logger.info("Снятие блокировки (processing_lock=False)...")
            # Пересоздаем клиент на всякий случай, если предыдущий вызов упал
            if not b2_client: b2_client = get_b2_client()

            if b2_client:
                # Загружаем последнюю версию перед снятием лока
                config_public = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, {"processing_lock": True})
                if config_public is not None: # Только если загрузка успешна
                    config_public["processing_lock"] = False
                    if save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH, config_public):
                        logger.info("🔓 Блокировка успешно снята в B2.")
                    else:
                        logger.error("❌ НЕ УДАЛОСЬ СНЯТЬ БЛОКИРОВКУ в B2!")
                else:
                     logger.error("❌ Не удалось загрузить config_public в finally для снятия блокировки!")
            else:
                 logger.error("❌ Не удалось получить B2 клиент в блоке finally для снятия блокировки!")
        else:
             logger.info("Блокировка не была установлена этим запуском, снятие не требуется.")

        logger.info("--- Завершение работы b2_storage_manager.py ---")
    # --- Конец Шагов 4.1.4 и 4.1.5 ---

# === Точка входа ===
if __name__ == "__main__":
    # Помещаем вызов main внутрь try/except для ловли самых ранних ошибок
    try:
         main()
    except Exception as top_level_err:
         # Используем logging, если он успел инициализироваться, иначе print
         try:
              logger.error(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ: {top_level_err}", exc_info=True)
         except NameError:
              print(f"!!! НЕПЕРЕХВАЧЕННАЯ ОШИБКА ВЫСШЕГО УРОВНЯ (логгер недоступен): {top_level_err}")
         sys.exit(1) # Завершаемся с ошибкой