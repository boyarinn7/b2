import os
import json
import logging
import subprocess
import re
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'b2')))
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("b2_storage_manager")
logger.info(f"sys.path: {sys.path}")

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

CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')  # Локальный путь для временной записи
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"  # Путь к файлу в B2
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get('FILE_PATHS.folder_444'),
    config.get('FILE_PATHS.folder_555'),
    config.get('FILE_PATHS.folder_666')
]
ARCHIVE_FOLDER = config.get('FILE_PATHS.archive_folder')
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")

# Путь к скрипту генерации контента (generate_content.py)
GENERATE_CONTENT_SCRIPT = os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")


def load_config_public(s3):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        local_path = CONFIG_PUBLIC_PATH
        s3.download_file(bucket_name, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info("✅ Конфигурация успешно загружена.")
        return data
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning("⚠️ Конфиг не найден, создаём новый.")
            return {"processing_lock": False, "empty": [], "generation_id": []}
        logger.error(f"❌ Ошибка загрузки конфига: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при загрузке конфига: {e}")
        return {}

def save_config_public(s3, data):
    try:
        bucket_name = os.getenv("B2_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
        with open(CONFIG_PUBLIC_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(CONFIG_PUBLIC_PATH, bucket_name, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info("✅ Конфигурация успешно сохранена.")
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига: {e}")


def list_files_in_folder(s3, folder_prefix):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
        return [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'] != folder_prefix and not obj['Key'].endswith('.bzEmpty')
               and FILE_NAME_PATTERN.match(os.path.basename(obj['Key']))
        ]
    except ClientError as e:
        logger.error(f"Ошибка получения списка файлов: {e}")
        return []

def get_ready_groups(files):
    """
    Возвращает список идентификаторов групп, для которых присутствуют файлы со всеми требуемыми расширениями.
    Идентификатор группы получается как имя файла без расширения.
    """
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
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"
        dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            s3.head_object(Bucket=bucket_name, Key=src_key)
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": src_key},
                Key=dst_key
            )
            s3.delete_object(Bucket=bucket_name, Key=src_key)
            logger.info(f"✅ Перемещено: {src_key} -> {dst_key}")
        except ClientError as e:
            if e.response['Error']['Code'] != "NoSuchKey":
                logger.error(f"Ошибка перемещения {src_key}: {e}")

def process_folders(s3, folders):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
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

    if is_folder_empty(s3, bucket_name, folders[-1]):
        logger.info("⚠️ Папка 666/ пуста. Запуск генерации контента...")
        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT], check=True)
        sys.exit(0)  # Остановка после вызова generate_content.py

    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)
    save_config_public(s3, config_data)
    logger.info(f"📂 Обновлены пустые папки: {config_data.get('empty')}")

def handle_publish(s3, config_data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
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
                    s3.head_object(Bucket=bucket_name, Key=src_key)
                    s3.copy_object(
                        Bucket=bucket_name,
                        CopySource={"Bucket": bucket_name, "Key": src_key},
                        Key=dst_key
                    )
                    s3.delete_object(Bucket=bucket_name, Key=src_key)
                    logger.info(f"✅ Успешно перемещено: {src_key} -> {dst_key}")
                except ClientError as e:
                    if e.response['Error']['Code'] != '404':
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
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    remote_config = "config/config_public.json"
    try:
        config_obj = b2_client.get_object(Bucket=bucket_name, Key=remote_config)
        config_data = json.loads(config_obj['Body'].read().decode('utf-8'))
        return config_data.get("midjourney_results", None)
    except Exception as e:
        logger.error(f"Ошибка при проверке midjourney_results: {e}")
        return None

def main():
    b2_client = None
    generation_count = 0
    MAX_GENERATIONS = 3
    SCRIPTS_FOLDER = os.path.abspath(config.get("FILE_PATHS.scripts_folder", "scripts"))

    try:
        b2_client = get_b2_client()
        midjourney_results = check_midjourney_results(b2_client)
        if midjourney_results:
            image_urls = midjourney_results.get("image_urls", [])
            # Проверка валидности: все URL должны быть строками и начинаться с http/https
            if not image_urls or not all(isinstance(url, str) and url.startswith("http") for url in image_urls):
                logger.warning("⚠️ Некорректные данные в midjourney_results, очищаем ключ")
                config_public = load_config_public(b2_client)
                if "midjourney_results" in config_public:
                    del config_public["midjourney_results"]
                    save_config_public(b2_client, config_public)
            else:
                logger.info("Найден валидный midjourney_results, запускаем generate_media.py")
                generate_media_path = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
                if not os.path.isfile(generate_media_path):
                    raise FileNotFoundError(f"❌ Файл {generate_media_path} не найден")
                # Снимаем блокировку перед запуском
                config_public = load_config_public(b2_client)
                if config_public.get("processing_lock"):
                    config_public["processing_lock"] = False
                    save_config_public(b2_client, config_public)
                    logger.info("🔓 Блокировка снята перед запуском generate_media.py")
                subprocess.run([sys.executable, generate_media_path], check=True)
                sys.exit(0)  # Полное отключение после вызова generate_media.py

        config_public = load_config_public(b2_client)

        if not config_public.get("generation_id") and not config_public.get("empty"):
            logger.info("🚦 Нет записей о публикациях и пустых папок. Скрипт завершает работу.")
            return

        if config_public.get("processing_lock"):
            logger.info("🔒 Процесс уже выполняется. Завершаем работу.")
            return

        config_public["processing_lock"] = True
        save_config_public(b2_client, config_public)
        logger.info("🔒 Блокировка установлена.")

        config_public = load_config_public(b2_client)
        if config_public.get("generation_id"):
            handle_publish(b2_client, config_public)

        process_folders(b2_client, FOLDERS)

        config_public = load_config_public(b2_client)
        while config_public.get("empty") and generation_count < MAX_GENERATIONS:
            logger.info(f"⚠️ Обнаружены пустые папки ({config_public['empty']}), генерация #{generation_count + 1} из {MAX_GENERATIONS}...")
            subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT], check=True)
            sys.exit(0)  # Остановка после вызова generate_content.py
            generation_count += 1  # Эта строка не выполнится
            config_public = load_config_public(b2_client)  # Обновляем состояние после генерации
            logger.info(f"✅ Завершена генерация #{generation_count}. Пустые папки: {config_public.get('empty', [])}")

        if generation_count >= MAX_GENERATIONS:
            logger.info(f"🚫 Достигнут лимит генераций ({MAX_GENERATIONS}). Завершаем работу, даже если остались пустые папки: {config_public.get('empty', [])}")
        elif not config_public.get("empty"):
            logger.info("✅ Нет пустых папок – генерация контента завершена.")

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        # Снимаем блокировку независимо от результатов
        if b2_client:
            try:
                config_public = load_config_public(b2_client)
                if config_public.get("processing_lock"):
                    config_public["processing_lock"] = False
                    save_config_public(b2_client, config_public)
                    logger.info("🔓 Блокировка снята.")
            except Exception as e:
                logger.error(f"❌ Ошибка при завершении работы: {e}")

if __name__ == "__main__":
    main()