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


def load_config_public(s3):
    """Загружает config_public.json из B2"""
    try:
        local_path = CONFIG_PUBLIC_PATH
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning("⚠️ Конфиг не найден, создаем новый")
            return {"processing_lock": False, "empty": [], "generation_id": []}
        logger.error(f"❌ Ошибка загрузки конфига: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка: {e}")
        return {}


def save_config_public(s3, data):
    """Сохраняет config_public.json в B2"""
    try:
        with open(CONFIG_PUBLIC_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(CONFIG_PUBLIC_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига: {e}")


def list_files_in_folder(s3, folder_prefix):
    """Возвращает список файлов в папке"""
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'] != folder_prefix and not obj['Key'].endswith('.bzEmpty')
               and FILE_NAME_PATTERN.match(os.path.basename(obj['Key']))
        ]
    except ClientError as e:
        logger.error(f"Ошибка получения списка файлов: {e}")
        return []


def get_ready_groups(files):
    """Возвращает список готовых групп файлов"""
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
    """Перемещает группу файлов между папками"""
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
        except ClientError as e:
            if e.response['Error']['Code'] != "NoSuchKey":
                logger.error(f"Ошибка перемещения {src_key}: {e}")


def process_folders(s3, folders):
    """Перемещает готовые группы файлов между папками и обновляет статус пустых папок"""
    empty_folders = set()
    changes_made = True

    while changes_made:
        changes_made = False
        for i in range(len(folders) - 1, 0, -1):
            src_folder = folders[i]
            dst_folder = folders[i - 1]

            if src_folder in empty_folders:
                continue

            # Получаем списки файлов
            src_files = list_files_in_folder(s3, src_folder)
            dst_files = list_files_in_folder(s3, dst_folder)

            # Определяем готовые группы
            src_ready = get_ready_groups(src_files)
            dst_ready = get_ready_groups(dst_files)

            # Перемещаем группы из src в dst
            for group_id in src_ready:
                if len(dst_ready) < 1:  # Проверка емкости целевой папки
                    move_group(s3, src_folder, dst_folder, group_id)
                    changes_made = True

            # Обновляем список пустых папок
            if not src_ready:
                empty_folders.add(src_folder)

    # Проверяем папку 666/ на пустоту
    if is_folder_empty(s3, B2_BUCKET_NAME, "666/"):
        logger.info("⚠️ Папка 666/ пуста. Запускаем генерацию...")
        subprocess.run(
            ["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
            check=True
        )

    # Обновляем конфиг
    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)
    save_config_public(s3, config_data)
    logger.info(f"📂 Обновлены пустые папки: {config_data['empty']}")


def handle_publish(s3, config_data):
    """Архивирует старые группы файлов"""
    generation_ids = config_data.get("generation_id", [])

    if not generation_ids:
        logger.info("📂 Нет generation_id для архивации.")
        return

    if isinstance(generation_ids, str):
        generation_ids = [generation_ids]

    archived_ids = []

    for generation_id in generation_ids:
        logger.info(f"🔄 Архивируем группу: {generation_id}")

        # Проверяем наличие файлов
        files_exist = any(
            list_files_in_folder(s3, folder) for folder in FOLDERS
        )
        if not files_exist:
            logger.error(f"❌ Файлы группы {generation_id} не найдены!")
            continue

        # Перемещаем файлы в архив
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
                except ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        logger.error(f"❌ Ошибка архивации {src_key}: {e}")
                        success = False

        if success:
            archived_ids.append(generation_id)

    # Обновляем конфиг
    if archived_ids:
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]
        save_config_public(s3, config_data)
        logger.info(f"✅ Успешно заархивированы: {archived_ids}")
    else:
        logger.warning("⚠️ Не удалось заархивировать ни одну группу")


def main():
    """Основной процесс управления B2-хранилищем"""
    b2_client = None
    try:
        b2_client = get_b2_client()

        # Проверка блокировки
        config_public = load_config_public(b2_client)
        if config_public.get("processing_lock"):
            logger.info("🔒 Процесс уже выполняется. Завершаем.")
            return

        # Установка блокировки
        config_public["processing_lock"] = True
        save_config_public(b2_client, config_public)
        logger.info("🔒 Блокировка установлена")

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
        if b2_client:
            try:
                config_public = load_config_public(b2_client)
                if config_public.get("processing_lock"):
                    config_public["processing_lock"] = False
                    save_config_public(b2_client, config_public)
                    logger.info("🔓 Блокировка снята")
            except Exception as e:
                logger.error(f"❌ Ошибка при завершении: {e}")


if __name__ == "__main__":
    main()