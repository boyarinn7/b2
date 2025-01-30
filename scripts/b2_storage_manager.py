import os
import json
import logging
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists
from modules.config_manager import ConfigManager
import subprocess  # Для запуска внешнего скрипта
from scripts.generate_content import generate_file_id


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
import re
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")

def log_folders_state(s3, folders, stage):
    logger.info(f"\n📂 Состояние папок ({stage}):")
    for folder in folders:
        files = list_files_in_folder(s3, folder)
        logger.info(f"- {folder}: {files}")

def load_config_public(s3):
    """Загружает config_public.json из B2 и извлекает опубликованные generation_id."""
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_PATH, local_path)

        with open(local_path, 'r', encoding='utf-8') as file:
            config_data = json.load(file)

        # Извлекаем список опубликованных generation_id
        published_generations = config_data.get("published", [])

        return config_data, published_generations

    except FileNotFoundError:
        logger.error("❌ config_public.json не найден в B2.")
        return {}, []

    except ClientError as e:
        logger.error(f"❌ Ошибка загрузки config_public.json: {e.response['Error']['Message']}")
        return {}, []

def list_files_by_generation_id(s3, gen_id):
    """Возвращает список файлов в B2, содержащих generation_id в названии."""
    try:
        all_files = list_files_in_bucket(s3)  # Получаем список всех файлов в B2
        matched_files = [f for f in all_files if gen_id in f]  # Фильтруем по generation_id

        if not matched_files:
            logger.info(f"🔍 Найденные файлы для {gen_id}: {matched_files}")
        return matched_files

    except ClientError as e:
        logger.error(f"❌ Ошибка при поиске файлов с generation_id {gen_id}: {e.response['Error']['Message']}")
        return []


def list_files_in_bucket(s3):
    """Возвращает список всех файлов в B2."""
    try:
        response = s3.list_objects_v2(Bucket="boyarinnbotbucket")
        return [obj["Key"] for obj in response.get("Contents", [])]
    except Exception as e:
        logger.error(f"❌ Ошибка получения списка файлов в B2: {e}")
        return []


def archive_files(s3, files):
    """Перемещает файлы, содержащие generation_id, в data/archive/ в B2."""
    try:
        logger.info(f"📦 Архивируем файлы: {files}")

        for file in files:
            new_path = f"data/archive/{file.split('/')[-1]}"

            s3.copy_object(Bucket="boyarinnbotbucket",
                           CopySource={"Bucket": "boyarinnbotbucket", "Key": file},
                           Key=new_path)

            s3.delete_object(Bucket="boyarinnbotbucket", Key=file)

            logger.info(f"✅ Файл {file} перемещён в {new_path}.")

    except Exception as e:
        logger.error(f"❌ Ошибка при архивировании файлов: {e}")


def save_config_public(s3, data):
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        with open(local_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(local_path, B2_BUCKET_NAME, CONFIG_PUBLIC_PATH)
    except Exception as e:
        logger.error(f"Error saving config_public.json: {e}")

def cleanup_archive(s3, max_files=200):
    """Удаляет самые старые файлы в data/archive/, если их больше max_files.
       Файл с расширением .bzEmpty не удаляется."""
    try:
        # Получаем список всех файлов в папке data/archive/
        all_files = list_files_in_folder(s3, "data/archive/")

        # Игнорируем .bzEmpty файлы
        filtered_files = [f for f in all_files if not f.endswith(".bzEmpty")]

        # Если файлов <= max_files, ничего не делаем
        if len(filtered_files) <= max_files:
            logger.info(f"✅ В архиве {len(filtered_files)} файлов, очистка не требуется.")
            return

        # Получаем информацию о файлах (время создания)
        file_info = []
        for file in filtered_files:
            response = s3.head_object(Bucket="boyarinnbotbucket", Key=file)
            last_modified = response["LastModified"]
            file_info.append((file, last_modified))

        # Сортируем файлы по дате (старые → новые)
        file_info.sort(key=lambda x: x[1])

        # Удаляем старейшие файлы (оставляем max_files)
        files_to_delete = file_info[:len(filtered_files) - max_files]
        for file, _ in files_to_delete:
            s3.delete_object(Bucket="boyarinnbotbucket", Key=file)
            logger.info(f"🗑 Удалён старый архивный файл: {file}")

        logger.info(f"✅ Очистка архива завершена. Оставлено {max_files} файлов.")

    except Exception as e:
        logger.error(f"❌ Ошибка при очистке архива: {e}")


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
    publish_folder = config_data.get("publish")
    if not publish_folder:
        return

    files = list_files_in_folder(s3, publish_folder)
    if not files:
        return

    for file_key in files:
        archive_key = file_key.replace(publish_folder, ARCHIVE_FOLDER)
        try:
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": file_key}, Key=archive_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file_key)
        except ClientError as e:
            logger.error(f"Error archiving {file_key}: {e.response['Error']['Message']}")

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
    """Перемещает файлы между 666/ → 555/ → 444/ (от большей папки к меньшей)."""
    empty_folders = set()
    changes_made = True

    while changes_made:
        changes_made = False
        for i in range(len(folders) - 1):  # Изменено: теперь идём от 666 к 444
            src_folder = folders[i]
            dst_folder = folders[i + 1]

            if src_folder in empty_folders:
                continue

            src_files = list_files_in_folder(s3, src_folder)
            dst_files = list_files_in_folder(s3, dst_folder)

            logger.info(f"📂 Проверяем {src_folder} → {dst_folder}")
            logger.info(f"Файлы в {src_folder}: {src_files}")
            logger.info(f"Файлы в {dst_folder}: {dst_files}")

            src_ready = get_ready_groups(src_files)
            dst_ready = get_ready_groups(dst_files)

            for group_id in src_ready:
                logger.info(f"📦 Перемещаем группу {group_id} из {src_folder} в {dst_folder}")
                move_group(s3, src_folder, dst_folder, group_id)
                changes_made = True

            if not src_ready:
                empty_folders.add(src_folder)

    return list(empty_folders)

def main():
    """Основной процесс B2 Storage Manager."""
    logger.info("🔄 Запуск B2 Storage Manager...")

    try:
        s3 = get_b2_client()  # Подключение к B2

        # 1️⃣ Загружаем config_public.json и получаем опубликованные generation_id
        config_public, published_generations = load_config_public(s3)

        # 2️⃣ Перемещение файлов между папками (666 → 555 → 444)
        logger.info("📂 Перемещение файлов между папками...")
        process_folders(s3, ["666/", "555/", "444/"])

        # 3️⃣ Проверяем, есть ли файлы в 444/ (готовые к публикации)
        files_to_publish = list_files_in_folder(s3, "444/")

        if files_to_publish:
            # 4️⃣ Генерируем новый generation_id
            generation_id = generate_file_id().replace(".json", "")  # Убираем .json из имени
            logger.info(f"📄 Публикация группы с generation_id: {generation_id}")

            # 5️⃣ Записываем generation_id в config_public.json
            handle_publish(s3, config_public, generation_id)

        # 6️⃣ Архивация опубликованных файлов по generation_id
        for gen_id in published_generations:
            logger.info(f"🔍 Архивируем generation_id {gen_id}, найденные файлы: {files}")
            files = list_files_by_generation_id(s3, gen_id)
            if files:
                archive_files(s3, files)
            else:
                logger.info(f"⚠️ Нет файлов для архивации по generation_id {gen_id}")

        # 7️⃣ Очистка архива (удаление старых файлов)
        cleanup_archive(s3)

        # 8️⃣ Запускаем generate_content.py, если 666/ пустая
        files_in_666 = list_files_in_folder(s3, "666/")  # Получаем список файлов

        if not files_in_666:  # Если папка пуста
            logger.info("⚠️ Папка 666/ пустая. Запускаем generate_content.py...")
            try:
                subprocess.run(["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
                               check=True)
                logger.info("✅ Скрипт generate_content.py выполнен успешно.")
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Ошибка при выполнении generate_content.py: {e}")
        else:
            logger.info(f"📂 В 666/ остались файлы: {files_in_666}")

    except Exception as e:
        handle_error(logger, e, "❌ Ошибка в B2 Storage Manager")

if __name__ == "__main__":
    main()
