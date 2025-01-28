import os
import json
import logging
import re
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists, list_files_in_folder
from modules.config_manager import ConfigManager
import subprocess

# === Инициализация конфигурации и логирования ===
config = ConfigManager()
logger = get_logger("b2_storage_manager")

# === Константы ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
PUBLISH_EXTENSION = '.pbl'  # Расширение системного файла публикации
FOLDERS = [
    config.get('FILE_PATHS.folder_444'),
    config.get('FILE_PATHS.folder_555'),
    config.get('FILE_PATHS.folder_666')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder')

FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")


def create_publish_marker(folder, group_id):
    """Создает .pbl-файл для опубликованной группы."""
    marker_path = os.path.join(folder, f"{group_id}{PUBLISH_EXTENSION}")
    with open(marker_path, 'w', encoding='utf-8') as marker:
        marker.write("published")
    logger.info(f"✅ Создан файл {marker_path}")


def is_group_ready_for_archive(folder, group_id):
    """Проверяет, полностью ли укомплектована группа и есть ли .pbl."""
    expected_files = [f"{group_id}{ext}" for ext in FILE_EXTENSIONS] + [f"{group_id}{PUBLISH_EXTENSION}"]
    folder_files = os.listdir(folder)
    return all(file in folder_files for file in expected_files)


def handle_publish(s3, config_data):
    """Перемещает опубликованные файлы в архив, если они полностью готовы."""
    publish_folders = config_data.get("publish", "").split(", ")

    for publish_folder in publish_folders:
        logger.info(f"🔄 Проверяем публикацию в папке: {publish_folder}")
        files = list_files_in_folder(s3, publish_folder)

        groups = set(f.split('.')[0] for f in files if FILE_NAME_PATTERN.match(os.path.basename(f)))

        for group_id in groups:
            if is_group_ready_for_archive(publish_folder, group_id):
                archive_group(s3, publish_folder, group_id)

    config_data.pop("publish", None)
    save_config_public(s3, config_data)


def archive_group(s3, src_folder, group_id):
    """Перемещает готовую группу в архив B2 (data/archive/)."""
    for ext in FILE_EXTENSIONS + [PUBLISH_EXTENSION]:
        src_key = os.path.join(src_folder, f"{group_id}{ext}")
        archive_key = f"data/archive/{group_id}{ext}"

        try:
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key},
                           Key=archive_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            logger.info(f"✅ {src_key} перемещен в архив B2: {archive_key}")
        except ClientError as e:
            logger.error(f"❌ Ошибка при архивировании {src_key}: {e.response['Error']['Message']}")


def process_folders(s3, folders):
    """Обрабатывает папки и запускает генератор контента после архивирования."""
    handle_publish(s3, load_config_public(s3))
    empty_folders = []

    for folder in folders:
        if not os.listdir(folder):
            empty_folders.append(folder)

    if empty_folders:
        run_content_generator()
    return empty_folders


def run_content_generator():
    """Запускает генератор контента при наличии пустых папок."""
    try:
        subprocess.run(["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
                       check=True)
        logger.info("✅ Контент успешно сгенерирован")
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Ошибка генерации контента: {e}")


def main():
    try:
        s3 = get_b2_client()
        log_folders_state(s3, FOLDERS, "Начало процесса")
        process_folders(s3, FOLDERS)
        log_folders_state(s3, FOLDERS, "Конец процесса")
    except Exception as e:
        handle_error(logger, e, "Error in main process")


if __name__ == "__main__":
    main()
