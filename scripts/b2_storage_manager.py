# core/scripts/b2_storage_manager.py

import os
import logging
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists

# === Инициализация конфигурации и логирования ===
from modules.config_manager import ConfigManager
config = ConfigManager()

logger = get_logger("b2_storage_manager")

# === Константы из конфигурации ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
META_FOLDER = config.get('FILE_PATHS.meta_folder')
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder')
FILE_EXTENSIONS = config.get('OTHER.file_extensions', ['-metadata.json', '-image.png', '-video.mp4'])


# === Методы для работы с B2 ===
def check_marker_file(s3):
    """
    Проверяет существование файла маркера публикации.
    """
    marker_key = f"{META_FOLDER}published_marker.json"
    try:
        s3.head_object(Bucket=B2_BUCKET_NAME, Key=marker_key)
        logger.info(f"✅ Файл маркера публикации существует: {marker_key}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.warning(f"⚠️ Файл маркера публикации отсутствует: {marker_key}")
        else:
            handle_error("B2 Marker Check Error", e)
        return False


def create_marker_file(s3):
    """
    Создаёт пустой файл маркера публикации.
    """
    marker_key = f"{META_FOLDER}published_marker.json"
    local_file = "published_marker.json"
    try:
        with open(local_file, "w") as f:
            f.write("")
        s3.upload_file(local_file, B2_BUCKET_NAME, marker_key)
        logger.info(f"✅ Создан новый файл маркера публикации: {marker_key}")
    except ClientError as e:
        handle_error("B2 Marker Creation Error", e)


def list_files_in_folder(s3, folder_prefix):
    """
    Возвращает список файлов в указанной папке.
    """
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"] != folder_prefix]
    except ClientError as e:
        handle_error("B2 List Files Error", e)
        return []


def get_ready_groups(s3, folder_prefix):
    """
    Возвращает список групп файлов, готовых для обработки.
    """
    files = list_files_in_folder(s3, folder_prefix)
    groups = {}

    for file_key in files:
        parts = file_key.split("/")[-1].split("-")
        if len(parts) >= 3:
            calendar_date, group_id = parts[0], parts[1]
            combined_id = f"{calendar_date}-{group_id}"
            groups.setdefault(combined_id, []).append(file_key)

    ready_groups = [
        group_id for group_id, group_files in groups.items()
        if len(group_files) == len(FILE_EXTENSIONS)
    ]
    return ready_groups


def move_group(s3, src_folder, dst_folder, group_id):
    """
    Перемещает группу файлов в целевую папку.
    """
    calendar_date, group_id_part = group_id.split("-")
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{calendar_date}-{group_id_part}{ext}"
        dst_key = f"{dst_folder}{calendar_date}-{group_id_part}{ext}"
        try:
            s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            logger.info(f"✅ Файл {src_key} перемещён в {dst_key}")
        except ClientError as e:
            handle_error("B2 File Move Error", e)


# === Основной процесс ===
def main():
    """
    Основной процесс управления контентом в B2.
    """
    try:
        s3 = get_b2_client()

        # Проверка и создание маркера публикации
        if not check_marker_file(s3):
            create_marker_file(s3)

        # Обработка файловых групп
        ready_444 = get_ready_groups(s3, config.get('FILE_PATHS.folder_444'))
        if ready_444:
            move_group(s3, config.get('FILE_PATHS.folder_444'), ARCHIVE_FOLDER, ready_444[0])

    except Exception as e:
        handle_error("B2 Storage Manager Main Error", e)


if __name__ == "__main__":
    main()
