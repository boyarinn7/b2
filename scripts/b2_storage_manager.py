import os
import json
import logging
import re

from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists
from modules.config_manager import ConfigManager
import subprocess  # Для запуска внешнего скрипта

# === Инициализация конфигурации и логирования ===
config = ConfigManager()
logger = get_logger("b2_storage_manager")


# === Константы из конфигурации ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get('FILE_PATHS.folder_444'),
    config.get('FILE_PATHS.folder_555'),
    config.get('FILE_PATHS.folder_666')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder')

# Регулярное выражение для проверки формата имени файла
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")

def log_folders_state(s3, folders, stage):
    logger.info(f"\n📂 Состояние папок ({stage}):")
    for folder in folders:
        files = list_files_in_folder(s3, folder)
        logger.info(f"- {folder}: {files}")


def load_config_public(s3):
    """Загружает config_public.json из B2."""
    logger.info("🔄 Начало загрузки config_public.json...")
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        logger.info(f"📥 Попытка загрузить {CONFIG_PUBLIC_PATH} в {local_path}...")
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_PATH, local_path)

        with open(local_path, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
            logger.info(f"✅ config_public.json успешно загружен и прочитан: {config_data}")
            return config_data
    except FileNotFoundError:
        logger.warning("⚠️ Файл config_public.json не найден в локальном хранилище.")
        return {}
    except ClientError as e:
        logger.error(f"❌ Ошибка при загрузке config_public.json: {e.response['Error']['Message']}")
        return {}
    except json.JSONDecodeError:
        logger.error("❌ Ошибка парсинга JSON: config_public.json повреждён.")
        return {}


def save_config_public(s3, config_data):
    """Сохраняет config_public.json в B2."""
    logger.info("💾 Начало сохранения config_public.json...")
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)

        with open(local_path, 'w', encoding='utf-8') as file:
            json.dump(config_data, file, ensure_ascii=False, indent=4)
        logger.info(f"✅ config_public.json успешно обновлён: {config_data}")

        logger.info(f"📤 Попытка загрузить {local_path} обратно в B2...")
        s3.upload_file(local_path, B2_BUCKET_NAME, CONFIG_PUBLIC_PATH)
        logger.info("✅ config_public.json успешно загружен в B2.")
    except FileNotFoundError:
        logger.error("❌ Ошибка: config_public.json не найден для сохранения.")
    except ClientError as e:
        logger.error(f"❌ Ошибка загрузки config_public.json в B2: {e.response['Error']['Message']}")
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при сохранении config_public.json: {e}")

def list_files_in_folder(s3, folder_prefix):
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'] != folder_prefix and not obj['Key'].endswith('.bzEmpty') and FILE_NAME_PATTERN.match(os.path.basename(obj['Key']))
        ]
    except ClientError as e:
        logger.error(f"Error listing files in {folder_prefix}: {e.response['Error']['Message']}")
        return []

def get_ready_groups(files):
    groups = {}
    for file_key in files:
        base_name = os.path.basename(file_key)
        if FILE_NAME_PATTERN.match(base_name):
            group_id = base_name.rsplit('.', 1)[0]
            groups.setdefault(group_id, []).append(base_name)

    ready_groups = []
    for group_id, file_list in groups.items():
        expected_files = [group_id + ext for ext in FILE_EXTENSIONS]
        if all(file in file_list for file in expected_files):
            ready_groups.append(group_id)

    return ready_groups

def handle_publish(s3, config_data):
    """Перемещает опубликованные файлы в архив и обновляет config_public.json."""
    publish_folder = config_data.get("publish")
    if not publish_folder:
        logger.info("✅ Нет файлов для публикации. Пропускаем handle_publish.")
        return

    logger.info(f"🔄 Обнаружены файлы в папке публикации: {publish_folder}")
    files = list_files_in_folder(s3, publish_folder)
    if not files:
        logger.info("✅ Нет файлов для перемещения. Завершаем handle_publish.")
        return

    for file_key in files:
        archive_key = file_key.replace(publish_folder, ARCHIVE_FOLDER)
        try:
            logger.info(f"📤 Перемещение {file_key} в архив {archive_key}...")
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": file_key}, Key=archive_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file_key)
            logger.info(f"✅ Файл {file_key} успешно архивирован.")
        except ClientError as e:
            logger.error(f"❌ Ошибка при архивации {file_key}: {e.response['Error']['Message']}")

    config_data.pop("publish", None)
    save_config_public(s3, config_data)

def move_group(s3, src_folder, dst_folder, group_id):
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"
        dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
        except ClientError as e:
            if e.response['Error']['Code'] != "NoSuchKey":
                logger.error(f"Error moving {src_key}: {e.response['Error']['Message']}")

def process_folders(s3, folders):
    """Обрабатывает папки и возвращает список пустых."""
    empty_folders = set()
    changes_made = True
    logger.info("🔄 Начинаем обработку папок...")

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

            logger.info(f"📂 Анализ папки {src_folder}: {len(src_ready)} готовых групп")
            logger.info(f"📂 Анализ папки {dst_folder}: {len(dst_ready)} готовых групп")

            for group_id in src_ready:
                if len(dst_ready) < 1:
                    logger.info(f"📤 Перемещение группы {group_id} из {src_folder} в {dst_folder}")
                    move_group(s3, src_folder, dst_folder, group_id)
                    changes_made = True

            if not src_ready:
                logger.info(f"📂 Папка {src_folder} теперь пустая.")
                empty_folders.add(src_folder)

    return list(empty_folders)

def main():
    try:
        s3 = get_b2_client()

        # Лог начального состояния папок
        log_folders_state(s3, FOLDERS, "Начало процесса")

        config_data = load_config_public(s3)

        handle_publish(s3, config_data)

        empty_folders = process_folders(s3, FOLDERS)

        if empty_folders:
            config_data['empty'] = empty_folders
        else:
            config_data.pop('empty', None)

        save_config_public(s3, config_data)

        # Лог конечного состояния папок
        log_folders_state(s3, FOLDERS, "Конец процесса")

        # Лог содержимого config_public.json в конце процесса
        logger.info(f"✅ Финальное содержимое config_public.json: {config_data}")

        # Запуск generate_content.py при наличии пустых папок
        if empty_folders:
            logger.info("⚠️ Найдены пустые папки. Запуск generate_content.py...")
            try:
                subprocess.run(["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")], check=True)
                logger.info("✅ Скрипт generate_content.py выполнен успешно.")
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Ошибка при выполнении generate_content.py: {e}")

    except Exception as e:
        handle_error(logger, e, "Error in main process")

if __name__ == "__main__":
    main()
