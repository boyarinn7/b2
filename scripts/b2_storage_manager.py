import os
import json
import logging
import subprocess
import re
import sys
import time # Понадобится для задержки в Сценарии 2
import argparse # Для обработки --zero-delay
import io # Может понадобиться для save_to_b2 в generate_content

from modules.utils import is_folder_empty, ensure_directory_exists, generate_file_id # Добавляем generate_file_id
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager
# Импорт boto3 и его исключений для надежной работы с B2
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    print("Ошибка: Необходима библиотека boto3. Установите ее: pip install boto3")
    sys.exit(1)

# === Инициализация конфигурации и логирования ===
config = ConfigManager()
logger = get_logger("b2_storage_manager")

# === Константы ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', 'boyarinnbotbucket')
CONFIG_PUBLIC_LOCAL_PATH = "config/config_public.json"  # Фиксированный локальный путь
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"  # Путь к файлу в B2
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get('FILE_PATHS.folder_444', '444/'),
    config.get('FILE_PATHS.folder_555', '555/'),
    config.get('FILE_PATHS.folder_666', '666/')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder', 'data/archive/')
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")

# Путь к скрипту генерации контента
GENERATE_CONTENT_SCRIPT = os.path.join(config.get('FILE_PATHS.scripts_folder', 'scripts'), "generate_content.py")
SCRIPTS_FOLDER = config.get('FILE_PATHS.scripts_folder', 'scripts')

def download_file_from_b2(client, remote_path, local_path):
    """Загружает файл из B2 в локальное хранилище."""
    try:
        logger.info(f"🔄 Загрузка файла из B2: {remote_path} -> {local_path}")
        ensure_directory_exists(os.path.dirname(local_path))
        client.download_file(B2_BUCKET_NAME, remote_path, local_path)
        logger.info(f"✅ Файл '{remote_path}' успешно загружен в {local_path}")
    except Exception as e:
        handle_error("B2 Download Error", str(e), e)

def load_config_public(s3):
    """Загружает config_public.json из B2."""
    try:
        local_path = CONFIG_PUBLIC_LOCAL_PATH
        download_file_from_b2(s3, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info("✅ Конфигурация успешно загружена.")
        return data
    except Exception as e:
        logger.warning("⚠️ Конфиг не найден, создаём новый.")
        return {"processing_lock": False, "empty": [], "generation_id": []}

def save_config_public(s3, data):
    """Сохраняет config_public.json в B2."""
    try:
        os.makedirs(os.path.dirname(CONFIG_PUBLIC_LOCAL_PATH), exist_ok=True)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info("✅ Конфигурация успешно сохранена.")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
        logger.info(f"🗑️ Локальный файл {CONFIG_PUBLIC_LOCAL_PATH} удалён.")
    except Exception as e:
        handle_error("Save Config Public Error", str(e), e)

def list_files_in_folder(s3, folder_prefix):
    """Возвращает список файлов в указанной папке (кроме placeholder)."""
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'] != folder_prefix and not obj['Key'].endswith('.bzEmpty')
               and FILE_NAME_PATTERN.match(os.path.basename(obj['Key']))
        ]
    except Exception as e:
        logger.error(f"Ошибка получения списка файлов: {e}")
        return []

def get_ready_groups(files):
    """Возвращает список идентификаторов групп с файлами всех расширений."""
    groups = {}
    for file_key in files:
        base_name = os.path.basename(file_key)
        if FILE_NAME_PATTERN.match(base_name):
            group_id = base_name.rsplit('.', 1)[0]
            groups.setdefault(group_id, []).append(base_name)
    return [
        group_id for group_id, file_list in groups.items()
        if all(f"{group_id}{ext}" in file_list for ext in FILE_EXTENSIONS)
    ]

def move_group(s3, src_folder, dst_folder, group_id):
    """Перемещает файлы группы из src_folder в dst_folder."""
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"
        dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            s3.copy_object(
                Bucket=B2_BUCKET_NAME,
                CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key},
                Key=dst_key
            )
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            logger.info(f"✅ Перемещено: {src_key} -> {dst_key}")
        except Exception as e:
            logger.error(f"Ошибка перемещения {src_key}: {e}")

def process_folders(s3, folders):
    """Перемещает группы файлов между папками и обновляет пустые папки."""
    empty_folders = set()
    changes_made = True

    while changes_made:
        changes_made = False
        for i in range(len(folders) - 1, 0, -1):
            src_folder = folders[i]
            dst_folder = folders[i - 1]

            if src_folder in empty_folders:
                continue

            src_files = list_files_in_folder(s3, src_folder)
            dst_files = list_files_in_folder(s3, dst_folder)

            src_ready = get_ready_groups(src_files)
            dst_ready = get_ready_groups(dst_files)

            for group_id in src_ready:
                if len(dst_ready) < 1:
                    move_group(s3, src_folder, dst_folder, group_id)
                    changes_made = True

            if not src_ready:
                empty_folders.add(src_folder)

    if is_folder_empty(s3, B2_BUCKET_NAME, folders[-1]):
        logger.info("⚠️ Папка 666/ пуста. Запуск генерации контента...")
        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT], check=True)

    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)
    save_config_public(s3, config_data)
    logger.info(f"📂 Обновлены пустые папки: {config_data.get('empty')}")

def handle_publish(s3, config_data):
    """Архивирует группы файлов по generation_id."""
    generation_ids = config_data.get("generation_id", [])

    if not generation_ids:
        logger.info("📂 Нет generation_id для архивации.")
        return

    if isinstance(generation_ids, str):
        generation_ids = [generation_ids]

    archived_ids = []

    for generation_id in generation_ids:
        logger.info(f"🔄 Архивируем группу: {generation_id}")

        files_exist = any(list_files_in_folder(s3, folder) for folder in FOLDERS)
        if not files_exist:
            logger.error(f"❌ Файлы группы {generation_id} не найдены!")
            continue

        success = True
        for folder in FOLDERS:
            for ext in FILE_EXTENSIONS:
                src_key = f"{folder}{generation_id}{ext}"
                dst_key = f"{ARCHIVE_FOLDER}{generation_id}{ext}"
                try:
                    s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    s3.copy_object(
                        Bucket=B2_BUCKET_NAME,
                        CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key},
                        Key=dst_key
                    )
                    s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                    logger.info(f"✅ Успешно перемещено: {src_key} -> {dst_key}")
                except Exception as e:
                    if '404' not in str(e):
                        logger.error(f"❌ Ошибка архивации {src_key}: {e}")
                        success = False
        if success:
            archived_ids.append(generation_id)

    if archived_ids:
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]
        save_config_public(s3, config_data)
        logger.info(f"✅ Успешно заархивированы: {archived_ids}")
    else:
        logger.warning("⚠️ Не удалось заархивировать ни одну группу.")

def check_midjourney_results(b2_client):
    """Проверяет наличие midjourney_results в config_public.json."""
    try:
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
        return config_data.get("midjourney_results", None)
    except Exception as e:
        logger.error(f"Ошибка при проверке midjourney_results: {e}")
        return None


def main():
    # --- Начало Шага 4.1.2: Обработка аргумента ---
    parser = argparse.ArgumentParser(description='Manage B2 storage and content generation workflow.')
    parser.add_argument('--zero-delay', action='store_true', default=False,
                        help='Skip the 10-minute delay when checking Midjourney task.')
    args = parser.parse_args()
    zero_delay_flag = args.zero_delay
    logger.info(f"Флаг --zero-delay установлен: {zero_delay_flag}")
    # --- Конец Шага 4.1.2 ---
    # --- Начало Шага 4.1.3: Инициализация счетчика/лимита ---
    tasks_processed = 0
    # Получаем лимит из конфига, по умолчанию 1
    # Убедитесь, что 'config' (экземпляр ConfigManager) доступен здесь
    max_tasks_per_run = config.get('WORKFLOW.max_tasks_per_run', 1)  # Используем новый ключ конфига WORKFLOW
    logger.info(f"Максимальное количество задач за запуск: {max_tasks_per_run}")
    # --- Конец Шага 4.1.3 ---
    # --- Начало Шагов 4.1.4 и 4.1.5: Блокировка и Загрузка Конфигов ---
    b2_client = None
    config_public = {}
    config_gen = {}
    config_mj = {}
    lock_acquired = False  # Флаг, что мы успешно установили блокировку

    # --- Определим пути к конфигам ---
    # Убедитесь, что B2_BUCKET_NAME определен (из os.getenv или config)
    bucket_name = config.get('API_KEYS.b2.bucket_name', 'boyarinnbotbucket')  # Пример
    config_public_remote_path = "config/config_public.json"
    config_gen_remote_path = "config/config_gen.json"
    config_mj_remote_path = "config/config_midjourney.json"

    # --- Локальные пути для временного хранения ---
    config_public_local_path = "config_public_local.json"  # В текущей папке
    config_gen_local_path = "config_gen_local.json"
    config_mj_local_path = "config_mj_local.json"

    # --- Вспомогательная функция для загрузки JSON из B2 (поместите ее выше main() или импортируйте) ---
    def load_b2_json(client, bucket, remote_path, local_path, default_value={}):
        """Загружает JSON из B2, возвращает default_value при ошибке или отсутствии."""
        try:
            logger.debug(f"Загрузка {remote_path} из B2 в {local_path}")
            # Убедимся, что папка для локального файла есть (если путь сложный)
            # os.makedirs(os.path.dirname(local_path), exist_ok=True) # Если сохраняем не в текущую папку
            client.download_file(bucket, remote_path, local_path)
            with open(local_path, 'r', encoding='utf-8') as f:
                # Проверка на пустой файл перед загрузкой JSON
                if os.path.getsize(local_path) > 0:
                    content = json.load(f)
                else:
                    logger.warning(f"Загруженный файл {local_path} пуст, используем значение по умолчанию.")
                    content = default_value
            logger.info(f"Успешно загружен и распарсен {remote_path} из B2.")
            return content
        except client.exceptions.NoSuchKey:
            logger.warning(f"{remote_path} не найден в B2. Используем значение по умолчанию.")
            return default_value
        except json.JSONDecodeError as json_err:
            logger.error(
                f"Ошибка парсинга JSON из {local_path} ({remote_path}): {json_err}. Используем значение по умолчанию.")
            return default_value
        except Exception as e:
            logger.error(f"Критическая ошибка загрузки {remote_path}: {e}")
            # Возможно, стоит прервать выполнение, если конфиг критичен
            # raise Exception(f"Не удалось загрузить {remote_path}") from e
            return default_value  # Возвращаем дефолт, чтобы не упасть сразу
        finally:
            # Удаляем временный локальный файл
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    logger.warning(f"Не удалось удалить временный файл {local_path}")

    # --- Вспомогательная функция для сохранения JSON в B2 (поместите ее выше main() или импортируйте) ---
    def save_b2_json(client, bucket, remote_path, local_path, data):
        """Сохраняет словарь data как JSON в B2."""
        try:
            logger.debug(f"Сохранение данных в {remote_path} в B2 через {local_path}")
            # os.makedirs(os.path.dirname(local_path), exist_ok=True) # Если нужно
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            client.upload_file(local_path, bucket, remote_path)
            logger.info(f"Данные успешно сохранены в {remote_path} в B2.")
            return True
        except Exception as e:
            logger.error(f"Критическая ошибка сохранения {remote_path}: {e}")
            # raise Exception(f"Не удалось сохранить {remote_path}") from e
            return False
        finally:
            # Удаляем временный локальный файл
            if os.path.exists(local_path):
                try:
                    os.remove(local_path)
                except OSError:
                    logger.warning(f"Не удалось удалить временный файл {local_path}")

    # --- Основной блок try...finally для блокировки ---
    try:
        # Получаем B2 клиент
        b2_client = get_b2_client()
        if not b2_client:
            raise ConnectionError("Не удалось инициализировать B2 клиент.")

        # 1. Проверяем и устанавливаем блокировку
        logger.info(f"Проверка блокировки в {config_public_remote_path}...")
        config_public = load_b2_json(b2_client, bucket_name, config_public_remote_path, config_public_local_path,
                                     {"processing_lock": False})

        if config_public.get("processing_lock", False):
            logger.warning("🔒 Обнаружена активная блокировка (processing_lock=True). Завершение работы.")
            return  # Или sys.exit(0)

        # Устанавливаем блокировку
        config_public["processing_lock"] = True
        if save_b2_json(b2_client, bucket_name, config_public_remote_path, config_public_local_path, config_public):
            logger.info("🔒 Блокировка (processing_lock=True) успешно установлена в B2.")
            lock_acquired = True  # Установка прошла успешно
        else:
            logger.error("❌ Не удалось установить блокировку в B2. Завершение работы.")
            return  # Или sys.exit(1)

        # 2. Загружаем остальные конфиги (уже после установки лока)
        logger.info("Загрузка остальных конфигурационных файлов...")
        config_gen = load_b2_json(b2_client, bucket_name, config_gen_remote_path, config_gen_local_path,
                                  {"generation_id": None})
        config_mj = load_b2_json(b2_client, bucket_name, config_mj_remote_path, config_mj_local_path,
                                 {"midjourney_task": None, "midjourney_results": {}, "generation": False})

        # --- Здесь начнется основной цикл while tasks_processed < max_tasks_per_run: ---
        logger.info("--- Начало основного цикла обработки ---")
        # ... (Код для Шага 4.2 будет вставлен сюда на следующем этапе) ...
        # --- Начало Шага 4.2: Основной цикл и логика состояний ---
        while tasks_processed < max_tasks_per_run:
            logger.info(f"--- Итерация цикла обработки #{tasks_processed + 1} / {max_tasks_per_run} ---")

            # Перезагружаем конфиги, которые могли измениться
            logger.debug("Перезагрузка конфигурационных файлов из B2...")
            config_public = load_b2_json(b2_client, bucket_name, config_public_remote_path, config_public_local_path,
                                         {"processing_lock": True})  # Перезагружаем public
            config_gen = load_b2_json(b2_client, bucket_name, config_gen_remote_path, config_gen_local_path,
                                      {"generation_id": None})  # Перезагружаем gen
            config_mj = load_b2_json(b2_client, bucket_name, config_mj_remote_path, config_mj_local_path,
                                     {"midjourney_task": None, "midjourney_results": {},
                                      "generation": False})  # Перезагружаем mj
            logger.debug(f"Загруженные состояния: config_gen={config_gen}, config_mj={config_mj}")

            action_taken_in_iteration = False  # Флаг, что какое-то действие было предпринято

            # --- Проверка состояний (порядок важен!) ---

            # Сценарий 2: Проверка MidJourney (есть активная задача)
            if config_mj.get('midjourney_task'):
                action_taken_in_iteration = True
                task_id = config_mj['midjourney_task']
                logger.info(f"Обнаружена активная задача Midjourney: {task_id}. Запуск проверки статуса.")

                if not zero_delay_flag:
                    logger.info("Ожидание 10 минут перед проверкой статуса Midjourney...")
                    time.sleep(600)  # 10 минут = 600 секунд
                    logger.info("Ожидание завершено.")

                try:
                    logger.info("Запуск fetch_media.py...")
                    # Убедитесь, что путь к fetch_media.py правильный
                    fetch_script_path = os.path.join(SCRIPTS_FOLDER, "Workspace_media.py") # <--- ИЗМЕНЕНИЕ ЗДЕСЬ
                    subprocess.run([sys.executable, fetch_script_path], check=True, timeout=120)  # Таймаут 2 мин
                    logger.info("fetch_media.py успешно выполнен.")

                    # Сразу ПЕРЕЗАГРУЖАЕМ config_mj, чтобы увидеть результат fetch_media
                    logger.info("Повторная загрузка config_midjourney.json после fetch_media...")
                    config_mj = load_b2_json(b2_client, bucket_name, config_mj_remote_path, config_mj_local_path,
                                             config_mj)  # Передаем старое значение как дефолт

                    if config_mj.get('midjourney_results') and config_mj['midjourney_results'].get('image_urls'):
                        logger.info("Обнаружены результаты Midjourney после проверки! Переход к генерации медиа.")
                        # Логика ниже (elif для scenario 3) обработает это на СЛЕДУЮЩЕЙ итерации или сразу, если убрать break
                        # Чтобы обработать сразу, можно было бы дублировать код вызова generate_media здесь,
                        # но лучше оставить для следующей итерации для чистоты логики.
                        # Просто продолжаем цикл, чтобы состояние обработалось следующим elif.
                        continue  # Переходим к следующей итерации цикла while
                    else:
                        logger.info("Результаты Midjourney еще не готовы после проверки.")
                        break  # Выходим из цикла while, т.к. нужно ждать дальше (в следующем запуске)

                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка выполнения fetch_media.py: {e}")
                    break  # Выходим из цикла при ошибке проверки
                except subprocess.TimeoutExpired:
                    logger.error("Таймаут выполнения fetch_media.py.")
                    break  # Выходим из цикла при ошибке проверки

            # Сценарий 3: MidJourney Готово (есть результаты)
            elif config_mj.get('midjourney_results') and config_mj['midjourney_results'].get('image_urls'):
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id:
                    logger.error(
                        "❌ Обнаружены результаты Midjourney, но нет активного generation_id в config_gen.json!")
                    break  # Ошибка состояния, выходим
                logger.info(
                    f"Обнаружены готовые результаты Midjourney для ID {current_generation_id}. Запуск генерации медиа.")
                try:
                    logger.info(f"Запуск generate_media.py для ID: {current_generation_id}...")
                    media_script_path = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
                    subprocess.run([sys.executable, media_script_path, '--generation_id', current_generation_id],
                                   check=True, timeout=600)  # Таймаут 10 мин
                    logger.info(
                        f"generate_media.py успешно завершен для ID: {current_generation_id} (генерация видео).")
                    tasks_processed += 1  # Считаем задачу выполненной
                    # continue не нужен, просто перейдем к проверке лимита в конце цикла
                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка выполнения generate_media.py (генерация видео): {e}")
                    break  # Выходим при ошибке
                except subprocess.TimeoutExpired:
                    logger.error("Таймаут выполнения generate_media.py (генерация видео).")
                    break  # Выходим при ошибке

            # Сценарий 4: Генерация Медиа (Инициация) (есть флаг generation: true)
            elif config_mj.get('generation') is True:
                action_taken_in_iteration = True
                current_generation_id = config_gen.get("generation_id")
                if not current_generation_id:
                    logger.error("❌ Обнаружен флаг generation:true, но нет активного generation_id в config_gen.json!")
                    break  # Ошибка состояния, выходим
                logger.info(
                    f"Обнаружен флаг generation:true для ID {current_generation_id}. Запуск инициации задачи Midjourney.")
                try:
                    logger.info(f"Запуск generate_media.py для ID: {current_generation_id}...")
                    media_script_path = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
                    subprocess.run([sys.executable, media_script_path, '--generation_id', current_generation_id],
                                   check=True, timeout=120)  # Таймаут 2 мин
                    logger.info(
                        f"generate_media.py успешно завершен для ID: {current_generation_id} (инициация Midjourney).")
                    # Не увеличиваем tasks_processed здесь, т.к. это только начало медиа-генерации
                    # Но работа на этой итерации сделана, продолжаем цикл, чтобы проверить task_id
                    continue
                except subprocess.CalledProcessError as e:
                    logger.error(f"Ошибка выполнения generate_media.py (инициация Midjourney): {e}")
                    break  # Выходим при ошибке
                except subprocess.TimeoutExpired:
                    logger.error("Таймаут выполнения generate_media.py (инициация Midjourney).")
                    break  # Выходим при ошибке

            # Сценарий 1: Уборка / Генерация Контента (нет активных задач Midjourney)
            else:
                action_taken_in_iteration = True
                logger.info("Нет активных задач Midjourney. Выполнение Уборки и проверка папки 666/...")

                # 1. Уборка - Архивируем ID из config_public
                logger.info("Запуск handle_publish (архивация)...")
                # Передаем КОПИЮ словаря, чтобы handle_publish не изменил наш текущий config_public
                handle_publish(b2_client, config_public.copy())
                # Перезагружаем config_public на случай, если handle_publish его изменил (хотя не должен по ТЗ)
                config_public = load_b2_json(b2_client, bucket_name, config_public_remote_path,
                                             config_public_local_path, config_public)

                # 2. Уборка - Сортируем папки
                logger.info("Запуск process_folders (сортировка)...")
                process_folders(b2_client, FOLDERS)  # FOLDERS определены как константа

                # 3. Проверяем папку 666/
                logger.info("Проверка состояния папки 666/...")
                # Убедитесь, что is_folder_empty импортирована и работает
                # ... после process_folders ...
                logger.info("Проверка наличия ГОТОВЫХ ГРУПП в папке 666/...")
                # Получаем список файлов в 666/
                files_in_666 = list_files_in_folder(b2_client, FOLDERS[-1])  # FOLDERS[-1] это '666/'
                # Получаем список ID готовых групп
                ready_groups_in_666 = get_ready_groups(files_in_666)

                # НОВОЕ УСЛОВИЕ: Генерируем новый контент, ЕСЛИ НЕТ готовых групп
                if not ready_groups_in_666:  # <<<--- ЗАМЕНА УСЛОВИЯ
                    logger.info(
                        f"⚠️ В папке 666/ нет готовых групп ({len(files_in_666)} файлов всего). Запуск генерации нового контента...")
                    # ... код генерации ID и вызова generate_content ... (остается как есть)
                else:
                    logger.info(
                        f"В папке 666/ есть готовые группы ({len(ready_groups_in_666)} шт.). Генерация нового контента не требуется. Завершение цикла.")
                    break  # Есть готовые группы, которые должны быть перемещены или обработаны
                    # ВНУТРИ if is_folder_empty(b2_client, bucket_name, FOLDERS[-1]):
                    logger.info("⚠️ Папка 666/ пуста. Запуск генерации нового контента...")
                    try:
                        # --- Начало: Генерация ID, сохранение, вызов ---
                        # 1. Генерируем НОВЫЙ ID
                        # Убедитесь, что generate_file_id импортирована из utils
                        new_id = generate_file_id()
                        if not new_id:
                            raise ValueError("Функция generate_file_id не вернула ID")
                        logger.info(f"Сгенерирован новый ID: {new_id}")

                        # 2. Сохраняем НОВЫЙ ID в config_gen.json
                        config_gen["generation_id"] = new_id
                        # Используем вспомогательную функцию save_b2_json
                        if not save_b2_json(b2_client, bucket_name, config_gen_remote_path, config_gen_local_path,
                                            config_gen):
                            # Если не удалось сохранить, это критично - задача не сможет продолжиться
                            raise Exception(f"Не удалось сохранить новый ID {new_id} в {config_gen_remote_path}")
                        logger.info(f"Новый ID {new_id} сохранен в {config_gen_remote_path}")

                        # 3. Запускаем generate_content.py с НОВЫМ ID
                        logger.info(f"Запуск generate_content.py для ID: {new_id}...")
                        # Убедитесь, что путь GENERATE_CONTENT_SCRIPT правильный
                        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, '--generation_id', new_id], check=True,
                                       timeout=600)  # Таймаут 10 мин
                        logger.info(f"generate_content.py успешно завершен для ID: {new_id}.")
                        tasks_processed += 1  # Считаем эту задачу (генерацию контента) выполненной
                        # --- Конец: Генерация ID, сохранение, вызов ---

                    except subprocess.CalledProcessError as e:
                        logger.error(f"Ошибка выполнения generate_content.py: {e}")
                        break  # Выходим из цикла while при ошибке
                    except subprocess.TimeoutExpired:
                        logger.error("Таймаут выполнения generate_content.py.")
                        break  # Выходим из цикла while при ошибке
                    except Exception as gen_err:
                        logger.error(
                            f"Ошибка при генерации нового ID, сохранении config_gen или вызове generate_content: {gen_err}")
                        break  # Выходим из цикла while при ошибке
                    try:
                        # Генерируем НОВЫЙ ID
                        new_id = generate_file_id()  # Убедитесь, что функция импортирована/определена и возвращает ID без .json
                        if not new_id:
                            raise ValueError("Функция generate_file_id не вернула ID")
                        logger.info(f"Сгенерирован новый ID: {new_id}")

                        # Сохраняем НОВЫЙ ID в config_gen.json
                        config_gen["generation_id"] = new_id
                        if not save_b2_json(b2_client, bucket_name, config_gen_remote_path, config_gen_local_path,
                                            config_gen):
                            raise Exception(f"Не удалось сохранить новый ID {new_id} в {config_gen_remote_path}")
                        logger.info(f"Новый ID {new_id} сохранен в {config_gen_remote_path}")

                        # Запускаем generate_content.py с НОВЫМ ID
                        logger.info(f"Запуск generate_content.py для ID: {new_id}...")
                        # Убедитесь, что путь GENERATE_CONTENT_SCRIPT правильный
                        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, '--generation_id', new_id], check=True,
                                       timeout=600)  # Таймаут 10 мин
                        logger.info(f"generate_content.py успешно завершен для ID: {new_id}.")
                        tasks_processed += 1  # Считаем эту задачу (генерацию контента) выполненной

                    except subprocess.CalledProcessError as e:
                        logger.error(f"Ошибка выполнения generate_content.py: {e}")
                        break  # Выходим при ошибке
                    except subprocess.TimeoutExpired:
                        logger.error("Таймаут выполнения generate_content.py.")
                        break  # Выходим при ошибке
                    except Exception as gen_err:
                        logger.error(f"Ошибка при генерации нового ID или вызове generate_content: {gen_err}")
                        break  # Выходим при ошибке
                    else:
                        logger.info("Папка 666/ не пуста и нет активных задач Midjourney. Завершение цикла.")
                    break  # Папка не пуста, новых задач нет - выходим из while

            # Конец проверки if/elif/else для состояний

            # Проверка, если лимит задач достигнут после выполнения действия
            if tasks_processed >= max_tasks_per_run:
                logger.info(f"Достигнут лимит задач ({max_tasks_per_run}) за этот запуск.")
                break  # Выходим из цикла while

            # Небольшая пауза между итерациями, чтобы не перегружать B2/API (опционально)
            # time.sleep(5)

        # --- Конец основного цикла while ---
        logger.info(f"--- Основной цикл обработки завершен. Обработано задач: {tasks_processed} ---")

        # --- Начало Шага 4.4: Логика завершения задачи (если она была) ---
        if tasks_processed > 0:  # Обновляем только если была успешно обработана хотя бы одна полная задача
            logger.info("Задача успешно обработана, обновление финальных статусов...")
            try:
                # Перезагружаем конфиги на всякий случай
                config_public = load_b2_json(b2_client, bucket_name, config_public_remote_path,
                                             config_public_local_path, config_public)
                config_gen = load_b2_json(b2_client, bucket_name, config_gen_remote_path, config_gen_local_path,
                                          config_gen)

                completed_id = config_gen.get("generation_id")
                if completed_id:
                    logger.info(f"Перенос ID завершенной задачи '{completed_id}' в config_public для архивации.")
                    # Добавляем ID в список в config_public (создаем список, если его нет)
                    archive_list = config_public.get("generation_id", [])
                    if not isinstance(archive_list, list):  # Исправление, если там была строка
                        archive_list = []
                    if completed_id not in archive_list:
                        archive_list.append(completed_id)
                    config_public["generation_id"] = archive_list

                    # Очищаем config_gen
                    config_gen["generation_id"] = None
                    logger.info("Очистка generation_id в config_gen.")

                    # Сохраняем оба конфига
                    save_b2_json(b2_client, bucket_name, config_public_remote_path, config_public_local_path,
                                 config_public)
                    save_b2_json(b2_client, bucket_name, config_gen_remote_path, config_gen_local_path, config_gen)
                    logger.info("Обновленные config_public и config_gen сохранены в B2.")
                else:
                    logger.warning("Не найден generation_id в config_gen для переноса в config_public.")

            except Exception as final_save_err:
                logger.error(f"Ошибка при финальном обновлении конфигов после завершения задачи: {final_save_err}")
                # Блокировка все равно снимется в finally

        # --- Конец Шага 4.4 ---

        # Остальной код (например, завершающие логи) будет после этого блока, перед finally

        # --- Конец Шага 4.2 ---

    except Exception as main_exec_err:
        # Ловим ошибки на этапе инициализации или внутри основного цикла (если он будет здесь)
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в главном блоке: {main_exec_err}")
        # handle_error(...) можно использовать здесь
        # Важно все равно попытаться снять лок

    finally:
        # --- Гарантированное снятие блокировки ---
        if lock_acquired:  # Снимаем лок только если мы его успешно установили
            logger.info("Снятие блокировки (processing_lock=False)...")
            if not b2_client:
                b2_client = get_b2_client()  # Попытка получить клиент еще раз

            if b2_client:
                # Загружаем последнюю версию (на случай изменений другими процессами, хотя их быть не должно)
                config_public = load_b2_json(b2_client, bucket_name, config_public_remote_path,
                                             config_public_local_path,
                                             {"processing_lock": True})  # По умолчанию считаем, что лок был
                config_public["processing_lock"] = False
                if save_b2_json(b2_client, bucket_name, config_public_remote_path, config_public_local_path,
                                config_public):
                    logger.info("🔓 Блокировка успешно снята в B2.")
                else:
                    logger.error(
                        "❌ НЕ УДАЛОСЬ СНЯТЬ БЛОКИРОВКУ в B2! Это может вызвать проблемы при следующем запуске.")
            else:
                logger.error("❌ Не удалось получить B2 клиент в блоке finally для снятия блокировки!")
        else:
            logger.info("Блокировка не была установлена этим запуском, снятие не требуется.")

        logger.info("--- Завершение работы b2_storage_manager.py ---")
    # --- Конец Шагов 4.1.4 и 4.1.5 ---




if __name__ == "__main__":
    main()