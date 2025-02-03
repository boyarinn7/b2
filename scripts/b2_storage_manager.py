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

# === Инициализация конфигурации и логирования ===
config = ConfigManager()
logger = get_logger("b2_storage_manager")

# === Константы из конфигурации ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get('FILE_PATHS.folder_444'),
    config.get('FILE_PATHS.folder_555'),
    config.get('FILE_PATHS.folder_666')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder')
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")


def check_files_exist(s3, generation_id):
    """Проверяет наличие всех файлов группы в рабочих папках."""
    for folder in FOLDERS:
        for ext in FILE_EXTENSIONS:
            key = f"{folder}{generation_id}{ext}"
            try:
                s3.head_object(Bucket=B2_BUCKET_NAME, Key=key)
                return True
            except ClientError:
                continue
    return False


def move_to_archive(s3, generation_id):
    """Перемещает все файлы группы в архив."""
    success = True
    for folder in FOLDERS:
        for ext in FILE_EXTENSIONS:
            src_key = f"{folder}{generation_id}{ext}"
            dst_key = f"{ARCHIVE_FOLDER}{generation_id}{ext}"

            try:
                # Проверяем существование файла перед перемещением
                s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)

                # Копируем в архив
                s3.copy_object(
                    Bucket=B2_BUCKET_NAME,
                    CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key},
                    Key=dst_key
                )
                # Удаляем оригинал
                s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
                logger.info(f"✅ Успешно перемещено: {src_key} -> {dst_key}")

            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    logger.error(f"❌ Ошибка архивации {src_key}: {e}")
                    success = False
    return success


def handle_publish(s3, config_data):
    """Улучшенная логика архивации с проверками"""
    generation_ids = config_data.get("generation_id", [])

    if not generation_ids:
        logger.info("📂 Нет generation_id для архивации.")
        return

    if isinstance(generation_ids, str):
        generation_ids = [generation_ids]

    archived_ids = []

    for generation_id in generation_ids:
        logger.info(f"🔄 Архивируем группу: {generation_id}")

        if not check_files_exist(s3, generation_id):
            logger.error(f"❌ Файлы группы {generation_id} не найдены!")
            continue

        if move_to_archive(s3, generation_id):
            archived_ids.append(generation_id)
        else:
            logger.warning(f"⚠️ Частичная ошибка архивации {generation_id}")

    # Обновляем конфиг только при успешной архивации
    if archived_ids:
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]

        try:
            with open(CONFIG_PUBLIC_PATH, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=4, ensure_ascii=False)
            s3.upload_file(CONFIG_PUBLIC_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
            logger.info(f"✅ Успешно заархивированы: {archived_ids}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения конфига: {e}")
    else:
        logger.warning("⚠️ Не удалось заархивировать ни одну группу")


# Остальные функции остаются без изменений
# ================================================
def load_config_public(s3):
    """Загружает config_public.json из B2"""
    try:
        local_path = CONFIG_PUBLIC_PATH
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Ошибка загрузки конфига: {e}")
        return {}


def process_folders(s3, folders):
    """Оригинальная логика перемещения файлов между папками"""
    # ... (без изменений)


def main():
    """Обновленная основная функция"""
    try:
        b2_client = get_b2_client()

        # Проверка блокировки
        config_public = load_config_public(b2_client)
        if config_public.get("processing_lock"):
            logger.info("🔒 Процесс уже выполняется")
            return

        # Установка блокировки
        config_public["processing_lock"] = True
        with open(CONFIG_PUBLIC_PATH, 'w', encoding='utf-8') as f:
            json.dump(config_public, f, indent=4)
        b2_client.upload_file(CONFIG_PUBLIC_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)

        # Основная логика
        config_public = load_config_public(b2_client)
        handle_publish(b2_client, config_public)
        process_folders(b2_client, FOLDERS)

        # Проверка пустых папок
        config_public = load_config_public(b2_client)
        if config_public.get("empty"):
            logger.info("⚠️ Обнаружены пустые папки, запускаем генерацию...")
            subprocess.run(["python", "scripts/generate_content.py"], check=True)

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        # Гарантированное снятие блокировки
        try:
            config_public = load_config_public(b2_client)
            config_public["processing_lock"] = False
            with open(CONFIG_PUBLIC_PATH, 'w', encoding='utf-8') as f:
                json.dump(config_public, f, indent=4)
            b2_client.upload_file(CONFIG_PUBLIC_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
            logger.info("🔓 Блокировка снята")
        except Exception as e:
            logger.error(f"❌ Ошибка снятия блокировки: {e}")


if __name__ == "__main__":
    main()