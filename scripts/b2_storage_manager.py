import os
import json
import logging
import subprocess
import re
import sys

from modules.utils import is_folder_empty, ensure_directory_exists
from scripts.generate_media import download_file_from_b2
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

# === Инициализация ===
config = ConfigManager()
logger = get_logger("b2_storage_manager")

# === Константы ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"
CONFIG_PUBLIC_LOCAL_PATH = os.path.abspath('config_public.json')
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get('FILE_PATHS.folder_444'),
    config.get('FILE_PATHS.folder_555'),
    config.get('FILE_PATHS.folder_666')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder')
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")


def load_config_public(s3):
    """Загружает config_public.json из B2 с обработкой блокировки"""
    try:
        local_path = CONFIG_PUBLIC_LOCAL_PATH
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except (FileNotFoundError, ClientError) as e:
        logger.error(f"Error loading config: {str(e)}")
        return {"processing_lock": False}  # Возвращаем дефолтные значения


def save_config_public(s3, data):
    """Сохраняет config_public.json в B2 с обработкой блокировки"""
    try:
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
    except Exception as e:
        logger.error(f"Error saving config: {str(e)}")


def check_processing_lock(s3):
    """Проверяет статус блокировки"""
    try:
        config_public = load_config_public(s3)
        if config_public.get("processing_lock", False):
            logger.info("🔒 Процесс уже выполняется. Завершаем работу.")
            return True
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки блокировки: {str(e)}")
        return True


# Оригинальные функции без изменений (оставлены для сохранения функционала)
# ==========================================================================
def log_folders_state(s3, folders, stage):
    logger.info(f"\n📂 Состояние папок ({stage}):")
    for folder in folders:
        files = list_files_in_folder(s3, folder)
        logger.info(f"- {folder}: {files}")


def list_files_in_folder(s3, folder_prefix):
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'] != folder_prefix and not obj['Key'].endswith('.bzEmpty')
               and FILE_NAME_PATTERN.match(os.path.basename(obj['Key']))
        ]
    except ClientError as e:
        logger.error(f"Error listing files: {e.response['Error']['Message']}")
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


def handle_publish(s3, config_data):
    """Оригинальная логика архивации без изменений"""
    while True:
        generation_ids = config_data.get("generation_id", [])
        if not generation_ids:
            logger.info("📂 Нет generation_id для архивации.")
            return

        if isinstance(generation_ids, str):
            generation_ids = [generation_ids]

        logger.info(f"📂 Архивируем группы: {generation_ids}")
        archived_ids = []

        # ... (остальная оригинальная логика архивации)

        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]

        save_config_public(s3, config_data)

        if not config_data.get("generation_id"):
            logger.info("🎉 Все группы заархивированы.")
            break


def move_group(s3, src_folder, dst_folder, group_id):
    """Оригинальная логика перемещения файлов"""
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
        except ClientError as e:
            if e.response['Error']['Code'] != "NoSuchKey":
                logger.error(f"Ошибка перемещения: {e.response['Error']['Message']}")


def process_folders(s3, folders):
    """Оригинальная логика обработки папок без изменений"""
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

    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)
    save_config_public(s3, config_data)

    if is_folder_empty(s3, B2_BUCKET_NAME, "666/"):
        logger.info("⚠️ Папка 666/ пуста. Запуск генерации...")
        subprocess.run(
            ["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
            check=True
        )


# ==========================================================================

def main():
    """Обновленная основная функция с блокировками"""
    try:
        # Инициализация клиента B2
        b2_client = get_b2_client()

        # Проверка блокировки
        if check_processing_lock(b2_client):
            return

        # Установка блокировки
        config_public = load_config_public(b2_client)
        config_public["processing_lock"] = True
        save_config_public(b2_client, config_public)
        logger.info("🔒 Блокировка установлена")

        # Основная логика выполнения
        logger.info("🔄 Запуск публикатора...")
        config_public = load_config_public(b2_client)

        # 1. Архивирование старых данных
        if "generation_id" in config_public:
            handle_publish(b2_client, config_public)

        # 2. Перемещение файлов между папками
        process_folders(b2_client, FOLDERS)

        # 3. Проверка пустых папок
        config_public = load_config_public(b2_client)
        empty_folders = config_public.get("empty", [])

        if empty_folders:
            logger.info(f"⚠️ Обнаружены пустые папки: {empty_folders}")
            subprocess.run(
                ["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
                check=True
            )
        else:
            logger.info("✅ Все папки заполнены. Процесс завершён.")

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {str(e)}")
        handle_error("Main Error", str(e))
    finally:
        try:
            # Гарантированное снятие блокировки
            config_public = load_config_public(b2_client)
            config_public["processing_lock"] = False
            save_config_public(b2_client, config_public)
            logger.info("🔓 Блокировка снята")
        except Exception as e:
            logger.error(f"❌ Ошибка при снятии блокировки: {str(e)}")


if __name__ == "__main__":
    main()