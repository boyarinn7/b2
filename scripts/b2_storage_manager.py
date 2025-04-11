import os
import json
import logging
import subprocess
import re
import sys

from datetime import datetime
from b2sdk.v2 import B2Api, InMemoryAccountInfo

script_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(parent_dir)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("b2_storage_manager")

from modules.utils import is_folder_empty, ensure_directory_exists
from scripts.generate_media import download_file_from_b2
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

logger = logging.getLogger(__name__)
config = ConfigManager()
logger = get_logger("b2_storage_manager")
logger.info("Начало выполнения b2_storage_manager")

CONFIG_PUBLIC_PATH = "config/config_public.json"
CONFIG_PUBLIC_LOCAL_PATH = "config_public.json"
CONFIG_GEN_PATH = "config/config_gen.json"
CONFIG_GEN_LOCAL_PATH = "config/config_gen.json"
CONFIG_MIDJOURNEY_PATH = "config/config_midjourney.json"
CONFIG_MIDJOURNEY_LOCAL_PATH = "config_midjourney.json"
SCRIPTS_FOLDER = "scripts/"
CONTENT_OUTPUT_PATH = "generated_content.json"
B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")
GENERATE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
FETCH_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "fetch_media.py")
TARGET_FOLDER = "666/"
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get("FILE_PATHS.folder_444", "444/"),
    config.get("FILE_PATHS.folder_555", "555/"),
    config.get("FILE_PATHS.folder_666", "666/")
]
ARCHIVE_FOLDER = config.get("FILE_PATHS.archive_folder", "archive/")
GENERATE_CONTENT_SCRIPT = os.path.join(config.get("FILE_PATHS.scripts_folder", "scripts"), "generate_content.py")
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")

def load_config_public(b2_client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = "config/config_public.json"  # Явно задаем путь
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    if not local_path:
        raise ValueError("❌ Локальный путь для config_public.json не задан")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)  # Создаем папку config
        bucket = b2_client.get_bucket_by_name(bucket_name)
        file_info = bucket.get_file_info_by_name("config/config_public.json")
        file_id = file_info.id_
        download_dest = b2_client.download_file_by_id(file_id)
        download_dest.save_to(local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"✅ Загружен config_public.json: {json.dumps(data, ensure_ascii=False)}")
        return data
    except Exception as e:
        if "not found" in str(e).lower():
            logger.warning("⚠️ Конфиг не найден, создаем новый.")
            return {"processing_lock": False, "empty": [], "generation_id": []}
        logger.error(f"❌ Ошибка при загрузке config_public.json: {str(e)}")
        return {}

def save_config_public(b2_client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = "config/config_public.json"  # Явно задаем путь
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    if not local_path:
        raise ValueError("❌ Локальный путь для config_public.json не задан")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)  # Создаем папку config
        with open(local_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=local_path, file_name="config/config_public.json")
        logger.info(f"✅ Сохранен config_public.json: {json.dumps(data, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_public.json: {str(e)}")

def load_config_gen(b2_client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = CONFIG_GEN_LOCAL_PATH  # "config/config_gen.json"
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    if not local_path:
        raise ValueError("❌ Локальный путь для config_gen.json не задан")
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(CONFIG_GEN_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {CONFIG_GEN_PATH}: {json.dumps(data, ensure_ascii=False)}")
        return data
    except Exception as e:
        logger.warning(f"Config {CONFIG_GEN_PATH} not found or invalid: {e}")
        return {}

def save_config_gen(b2_client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = CONFIG_GEN_LOCAL_PATH  # "config/config_gen.json"
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    if not local_path:
        raise ValueError("❌ Локальный путь для config_gen.json не задан")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=local_path, file_name=CONFIG_GEN_PATH)
        logger.info(f"Saved {CONFIG_GEN_PATH}: {json.dumps(data, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига: {e}")

def load_config_midjourney(b2_client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = CONFIG_MIDJOURNEY_LOCAL_PATH  # "config_midjourney.json"
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    if not local_path:
        raise ValueError("❌ Локальный путь для config_midjourney.json не задан")
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(CONFIG_MIDJOURNEY_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(f"Loaded {CONFIG_MIDJOURNEY_PATH}: {json.dumps(data, ensure_ascii=False)}")
        return data
    except Exception as e:
        logger.warning(f"Config {CONFIG_MIDJOURNEY_PATH} not found or invalid: {e}")
        return {}

def save_config_midjourney(b2_client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = CONFIG_MIDJOURNEY_LOCAL_PATH  # "config_midjourney.json"
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    if not local_path:
        raise ValueError("❌ Локальный путь для config_midjourney.json не задан")
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=local_path, file_name=CONFIG_MIDJOURNEY_PATH)
        logger.info(f"Saved {CONFIG_MIDJOURNEY_PATH}: {json.dumps(data, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига: {e}")

def generate_file_id():
    now = datetime.utcnow()
    return f"{now.strftime('%Y%m%d-%H%M')}"

def update_content_json(b2_client, target_folder, generation_id):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    json_path = f"{target_folder}{generation_id}.json"
    local_json = "temp_content.json"
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(json_path, local_json)
        with open(local_json, 'r', encoding='utf-8') as f:
            content_dict = json.load(f)
        with open(CONTENT_OUTPUT_PATH, 'r', encoding='utf-8') as f:
            media_data = json.load(f)
        content_dict["script"] = media_data.get("script", "")
        content_dict["image_url"] = f"{target_folder}{generation_id}.png"
        content_dict["video_url"] = f"{target_folder}{generation_id}.mp4"
        with open(local_json, 'w', encoding='utf-8') as f:
            json.dump(content_dict, f, indent=4, ensure_ascii=False)
        bucket.upload_local_file(local_file=local_json, file_name=json_path)
        logger.info(f"Updated {json_path}: {json.dumps(content_dict, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"Failed to update {json_path}: {e}")

def list_files_in_folder(b2_client, folder_prefix):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        # Используем ls для перебора файлов
        files = [file_info.file_name for file_info, _ in bucket.ls(folder_prefix, recursive=False)
                 if file_info.file_name != folder_prefix and not file_info.file_name.endswith('.bzEmpty')
                 and FILE_NAME_PATTERN.match(os.path.basename(file_info.file_name))]
        return files
    except Exception as e:
        logger.error(f"Ошибка получения списка файлов: {e}")
        return []

def get_ready_groups(files):
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

def move_group(b2_client, src_folder, dst_folder, group_id):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    bucket = b2_client.get_bucket_by_name(bucket_name)
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"
        dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            response = bucket.list_file_names(start_file_name=src_key, max_file_count=1)
            file_info = next((f for f in response['files'] if f['fileName'] == src_key), None)
            if not file_info:
                continue
            temp_path = f"temp_{group_id}{ext}"
            bucket.download_file_by_name(src_key, temp_path)
            bucket.upload_local_file(local_file=temp_path, file_name=dst_key)
            bucket.delete_file(file_info['fileId'], src_key)
            os.remove(temp_path)
            logger.info(f"✅ Перемещено: {src_key} -> {dst_key}")
        except Exception as e:
            logger.error(f"Ошибка перемещения {src_key}: {e}")

def process_folders(b2_client, folders):
    empty_folders = set()
    changes_made = True
    while changes_made:
        changes_made = False
        for i in range(len(folders) - 1, 0, -1):
            src_folder = folders[i]
            dst_folder = folders[i - 1]
            if src_folder in empty_folders:
                continue
            src_files = list_files_in_folder(b2_client, src_folder)
            dst_files = list_files_in_folder(b2_client, dst_folder)
            src_ready = get_ready_groups(src_files)
            dst_ready = get_ready_groups(dst_files)
            for group_id in src_ready:
                if len(dst_ready) < 1:
                    move_group(b2_client, src_folder, dst_folder, group_id)
                    changes_made = True
            if not src_ready:
                empty_folders.add(src_folder)
    config_data = load_config_public(b2_client)
    config_data["empty"] = list(empty_folders)
    save_config_public(b2_client, config_data)
    logger.info(f"📂 Обновлены пустые папки: {config_data.get('empty')}")

def handle_publish(b2_client, config_data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    bucket = b2_client.get_bucket_by_name(bucket_name)
    generation_ids = config_data.get("generation_id", [])
    if not generation_ids:
        logger.info("📂 Нет generation_id для архивации.")
        return
    if isinstance(generation_ids, str):
        generation_ids = [generation_ids]
    archived_ids = []
    for generation_id in generation_ids:
        logger.info(f"🔄 Архивируем группу: {generation_id}")
        files_exist = any(list_files_in_folder(b2_client, folder) for folder in FOLDERS)
        if not files_exist:
            logger.error(f"❌ Файлы группы {generation_id} не найдены!")
            continue
        success = True
        for folder in FOLDERS:
            for ext in FILE_EXTENSIONS:
                src_key = f"{folder}{generation_id}{ext}"
                dst_key = f"{ARCHIVE_FOLDER}{generation_id}{ext}"
                try:
                    response = bucket.list_file_names(start_file_name=src_key, max_file_count=1)
                    file_info = next((f for f in response['files'] if f['fileName'] == src_key), None)
                    if file_info:
                        temp_path = f"temp_{generation_id}{ext}"
                        bucket.download_file_by_name(src_key, temp_path)
                        bucket.upload_local_file(local_file=temp_path, file_name=dst_key)
                        bucket.delete_file(file_info['fileId'], src_key)
                        os.remove(temp_path)
                        logger.info(f"✅ Успешно перемещено: {src_key} -> {dst_key}")
                except Exception as e:
                    logger.error(f"❌ Ошибка архивации {src_key}: {e}")
                    success = False
        if success:
            archived_ids.append(generation_id)
    if archived_ids:
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]
        save_config_public(b2_client, config_data)
        logger.info(f"✅ Успешно заархивированы: {archived_ids}")

def any_folder_empty(b2_client, folders):
    for folder in folders:
        files = list_files_in_folder(b2_client, folder)
        ready_groups = get_ready_groups(files)
        if not ready_groups:
            logger.info(f"Папка {folder} считается пустой (нет полных групп).")
            return True
    return False

def check_content_exists(b2_client, generation_id):
    bucket_name = "boyarinnbotbucket"
    remote_path = f"666/{generation_id}.json"
    try:
        logger.info(f"🔍 Проверка файла {remote_path} в B2")
        bucket = b2_client.get_bucket_by_name(bucket_name)
        response = bucket.list_file_names(start_file_name=remote_path, max_file_count=1)
        exists = any(file_info['fileName'] == remote_path for file_info in response.get('files', []))
        logger.info(f"ℹ️ Файл {remote_path} {'существует' if exists else 'не найден'}")
        return exists
    except Exception as e:
        logger.error(f"❌ Ошибка проверки файла {remote_path}: {e}")
        return False

def main():
    b2_client = None
    SCRIPTS_FOLDER = os.path.abspath(config.get("FILE_PATHS.scripts_folder", "scripts"))
    import argparse
    import time

    parser = argparse.ArgumentParser(description="B2 Storage Manager")
    parser.add_argument("--zero-delay", action="store_true", help="Run without delay for scheduled checks")
    args = parser.parse_args()

    try:
        b2_client = get_b2_client()
        logger.info("Клиент B2 успешно создан.")
        logger.info(f"Путь к config_public.json: {'config/config_public.json'}")
        config_public = load_config_public(b2_client)
        config_gen = load_config_gen(b2_client)
        config_midjourney = load_config_midjourney(b2_client)

        if config_public.get("processing_lock"):
            logger.info("🔒 Процесс уже выполняется. Завершаем работу.")
            return

        max_tasks_per_run = config.get("max_tasks_per_run", 1)
        tasks_processed = 0

        config_public["processing_lock"] = True
        save_config_public(b2_client, config_public)
        logger.info("🔒 Блокировка установлена.")

        while tasks_processed < max_tasks_per_run:
            midjourney_results = config_midjourney.get("midjourney_results")
            generation_id = config_gen.get("generation_id")

            if midjourney_results and "image_urls" in midjourney_results:
                image_urls = midjourney_results.get("image_urls", [])
                if image_urls and all(isinstance(url, str) and url.startswith("http") for url in image_urls):
                    logger.info("Найден валидный midjourney_results, проверяем контент")
                    if generation_id and check_content_exists(b2_client, generation_id):
                        logger.info(f"✅ Файл 666/{generation_id}.json существует, запускаем generate_media.py")
                        generate_media_path = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
                        if not os.path.isfile(generate_media_path):
                            raise FileNotFoundError(f"❌ Файл {generate_media_path} не найден")
                        result = subprocess.run([sys.executable, generate_media_path, "--generation_id", generation_id],
                                                check=True)
                        if result.returncode == 0:
                            update_content_json(b2_client, "666/", generation_id)
                            config_midjourney["midjourney_results"] = {}
                            save_config_midjourney(b2_client, config_midjourney)
                            tasks_processed += 1
                            config_public["processing_lock"] = False
                            save_config_public(b2_client, config_public)
                            logger.info("🔓 Блокировка снята перед запуском b2_storage_manager.py")
                            subprocess.run([sys.executable, __file__])
                            return
                    else:
                        logger.warning(
                            f"⚠️ Файл 666/{generation_id}.json не найден или generation_id отсутствует, генерируем новый")
                        generation_id = generate_file_id()
                        config_gen["generation_id"] = generation_id
                        save_config_gen(b2_client, config_gen)
                        logger.info(f"ℹ️ Новый generation_id: {generation_id}, запускаем generate_content.py")
                        try:
                            result = subprocess.run(
                                [sys.executable, GENERATE_CONTENT_SCRIPT, "--generation_id", generation_id], check=True)
                            if result.returncode == 0:
                                result = subprocess.run(
                                    [sys.executable, GENERATE_MEDIA_SCRIPT, "--generation_id", generation_id],
                                    check=True)
                                if result.returncode == 0:
                                    tasks_processed += 1
                                    config_public["processing_lock"] = False
                                    save_config_public(b2_client, config_public)
                                    logger.info("🔓 Блокировка снята перед запуском b2_storage_manager.py")
                                    subprocess.run([sys.executable, __file__])
                                    return
                        except subprocess.CalledProcessError as e:
                            logger.error(f"❌ Ошибка запуска скрипта: {e}")
                            raise

            midjourney_task = config_midjourney.get("midjourney_task")
            if midjourney_task:
                fetch_media_path = os.path.join(SCRIPTS_FOLDER, "fetch_media.py")
                if not os.path.isfile(fetch_media_path):
                    raise FileNotFoundError(f"❌ Файл {fetch_media_path} не найден")
                if args.zero_delay:
                    logger.info("Повторный запуск с --zero-delay, проверяем fetch_media.py")
                    result = subprocess.run([sys.executable, fetch_media_path], check=True)
                    config_midjourney = load_config_midjourney(b2_client)
                    if config_midjourney.get("midjourney_results"):
                        generation_id = config_gen.get("generation_id")
                        if not generation_id or not check_content_exists(b2_client, generation_id):
                            generation_id = generate_file_id()
                            config_gen["generation_id"] = generation_id
                            save_config_gen(b2_client, config_gen)
                            logger.info(f"ℹ️ Новый generation_id: {generation_id}, запускаем generate_content.py")
                            result = subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, "--generation_id", generation_id], check=True)
                        result = subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, "--generation_id", generation_id], check=True)
                        if result.returncode == 0:
                            tasks_processed += 1
                            config_public["processing_lock"] = False
                            save_config_public(b2_client, config_public)
                            logger.info("🔓 Блокировка снята перед запуском b2_storage_manager.py")
                            subprocess.run([sys.executable, __file__])
                            return
                    else:
                        logger.info("ℹ️ Результат не получен, проверка продолжится по расписанию")
                        return
                else:
                    logger.info("Первый запуск, ждем 10 минут перед fetch_media.py")
                    time.sleep(600)
                    result = subprocess.run([sys.executable, fetch_media_path], check=True)
                    config_midjourney = load_config_midjourney(b2_client)
                    if config_midjourney.get("midjourney_results"):
                        generation_id = config_gen.get("generation_id")
                        if not generation_id or not check_content_exists(b2_client, generation_id):
                            generation_id = generate_file_id()
                            config_gen["generation_id"] = generation_id
                            save_config_gen(b2_client, config_gen)
                            logger.info(f"ℹ️ Новый generation_id: {generation_id}, запускаем generate_content.py")
                            result = subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, "--generation_id", generation_id], check=True)
                        result = subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, "--generation_id", generation_id], check=True)
                        if result.returncode == 0:
                            tasks_processed += 1
                            config_public["processing_lock"] = False
                            save_config_public(b2_client, config_public)
                            logger.info("🔓 Блокировка снята перед запуском b2_storage_manager.py")
                            subprocess.run([sys.executable, __file__])
                            return
                    else:
                        logger.info("ℹ️ MidJourney не ответил за 10 минут, проверка продолжится по расписанию")
                        return

            handle_publish(b2_client, config_public)
            process_folders(b2_client, FOLDERS)
            config_public = load_config_public(b2_client)

            if any_folder_empty(b2_client, FOLDERS) and tasks_processed < max_tasks_per_run:
                generation_id = config_gen.get("generation_id")
                if not generation_id:
                    generation_id = generate_file_id()
                    config_gen["generation_id"] = generation_id
                    save_config_gen(b2_client, config_gen)
                logger.info(f"⚠️ Обнаружены пустые папки, запускаем генерацию для {generation_id}")
                result = subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT, "--generation_id", generation_id], check=True)
                if result.returncode == 0:
                    result = subprocess.run([sys.executable, GENERATE_MEDIA_SCRIPT, "--generation_id", generation_id], check=True)
                    if result.returncode == 0:
                        tasks_processed += 1
                        config_public["processing_lock"] = False
                        save_config_public(b2_client, config_public)
                        logger.info("🔓 Блокировка снята перед запуском b2_storage_manager.py")
                        subprocess.run([sys.executable, __file__])
                        return
            else:
                logger.info("✅ Все папки заполнены, работа завершена")
                break

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        if b2_client:
            try:
                config_public = load_config_public(b2_client)
                if config_public.get("processing_lock"):
                    config_public["processing_lock"] = False
                    save_config_public(b2_client, config_public)
                    logger.info("🔓 Блокировка снята в finally")
            except Exception as e:
                logger.error(f"❌ Ошибка при завершении работы: {e}")
        sys.exit(0)

if __name__ == "__main__":
    main()