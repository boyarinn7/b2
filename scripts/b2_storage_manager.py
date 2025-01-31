import os
import json
import logging
import subprocess  # Для запуска внешнего скрипта
import re

from modules.utils import is_folder_empty, ensure_directory_exists, move_to_archive
from scripts.generate_media import download_file_from_b2
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager
from scripts.generate_media import download_file_from_b2, generate_mock_video
from scripts.generate_media import (
    download_file_from_b2, generate_mock_video,
    update_config_public, upload_to_b2
)

# === Инициализация конфигурации и логирования ===
config = ConfigManager()
logger = get_logger("b2_storage_manager")


# === Константы из конфигурации ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')
CONFIG_GEN_PATH = os.path.abspath('config/config_gen.json')
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"
CONFIG_PUBLIC_LOCAL_PATH = os.path.abspath('config_public.json')
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
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
            logger.info(f"✅ Содержимое config_public.json: {config_data}")
            return config_data
    except FileNotFoundError:
        return {}
    except ClientError as e:
        logger.error(f"Error loading config_public.json: {e.response['Error']['Message']}")
        return {}

def save_config_public(s3, data):
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        with open(local_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(local_path, B2_BUCKET_NAME, CONFIG_PUBLIC_PATH)
    except Exception as e:
        logger.error(f"Error saving config_public.json: {e}")

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
    """Перемещает все файлы с указанными generation_id в архив B2, пока список не станет пустым."""

    while True:
        generation_ids = config_data.get("generation_id", [])

        if not generation_ids:
            logger.info("📂 Нет generation_id в config_public.json, публикация завершена.")
            return  # ❌ Если generation_id пуст – процесс завершается

        if isinstance(generation_ids, str):
            generation_ids = [generation_ids]  # Приводим к списку, если это строка

        logger.info(f"📂 Найдены generation_id: {generation_ids}, перемещаем файлы в архив...")

        # Папки, где ищем файлы с этими generation_id
        source_folders = ["444/", "555/", "666/"]

        archived_ids = []  # 🔹 Список ID, которые отправлены в архив

        for generation_id in generation_ids:
            for folder in source_folders:
                files_to_move = list_files_in_folder(s3, folder)  # Получаем список файлов

                for file_key in files_to_move:
                    if generation_id in file_key:  # 🏷 Фильтруем файлы по generation_id
                        archive_path = f"data/archive/{os.path.basename(file_key)}"

                        try:
                            # 📤 Перемещаем файл в архив
                            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": file_key},
                                           Key=archive_path)
                            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file_key)
                            logger.info(f"✅ Файл {file_key} перемещён в архив: {archive_path}")

                            if generation_id not in archived_ids:
                                archived_ids.append(generation_id)  # Запоминаем, что этот ID заархивирован

                        except ClientError as e:
                            logger.error(f"❌ Ошибка при архивировании {file_key}: {e.response['Error']['Message']}")

        # 🏷 Удаляем только заархивированные generation_id из списка
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]

        # ✅ Если список generation_id пуст – удаляем ключ
        if not config_data["generation_id"]:
            del config_data["generation_id"]

        # 📤 Загружаем обновлённый config_public.json в B2
        save_config_public(s3, config_data)
        logger.info(f"✅ Архивация завершена для: {archived_ids}")

        # 🔄 Проверяем, остались ли generation_id, если нет – выходим из цикла
        if not config_data.get("generation_id"):
            logger.info("🎉 Все опубликованные группы заархивированы, завершаем процесс.")
            break


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

    # ✅ Финальная проверка: если 666/ пустая, запускаем генерацию контента
    if is_folder_empty(s3, "666/"):
        logger.info("⚠️ Папка 666/ пуста. Запускаем генерацию контента...")
        subprocess.run(["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")], check=True)

    else:
        logger.info("✅ Все папки заполнены. Завершаем процесс.")

    # Обновляем config_public.json пустыми папками
    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)  # Записываем пустые папки
    save_config_public(s3, config_data)
    logger.info(f"📂 Обновлены пустые папки в config_public.json: {config_data['empty']}")


def main():
    """Основной процесс генерации медиа."""
    logger.info("🔄 Начинаем процесс генерации медиа...")
    try:
        # Читаем config_gen.json
        logger.info(f"📄 Читаем config_gen.json: {CONFIG_GEN_PATH}")
        with open(CONFIG_GEN_PATH, 'r', encoding='utf-8') as file:
            config_gen = json.load(file)

        file_id = os.path.splitext(config_gen["generation_id"])[0]
        logger.info(f"📂 ID генерации: {file_id}")

        # Создаём клиент B2
        b2_client = get_b2_client()
        logger.info(f"ℹ️ Тип объекта b2_client: {type(b2_client)}")

        # Логируем вызов генератора
        logger.info(f"🚀 generate_media.py вызван из: {os.environ.get('GITHUB_WORKFLOW', 'локальный запуск')}")

        # Загружаем config_public.json
        logger.info(f"🔍 Перед вызовом download_file_from_b2(): {type(b2_client)}")
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        config_public = load_config_public(CONFIG_PUBLIC_LOCAL_PATH)
        logger.info(f"📄 Загруженный config_public.json: {config_public}")

        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]  # Берём первую пустую папку
            logger.info(f"🎯 Выбрана папка для загрузки: {target_folder}")
        else:
            logger.error("❌ Нет пустых папок для загрузки контента.")
            return  # Прерываем выполнение, если нет папок

        if "empty" in config_public and config_public["empty"]:
            logger.info(f"📂 Обнаружены пустые папки: {config_public['empty']}")
            for empty_folder in config_public["empty"]:
                if empty_folder == "666/":
                    logger.info("⚠️ Папка 666/ пуста. Запускаем генерацию контента...")
                    subprocess.run(
                        ["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
                        check=True)

        if "generation_id" in config_public:
            for gen_id in config_public["generation_id"]:
                logger.info(f"📂 Перемещаем файлы группы {gen_id} в архив...")
                move_to_archive(b2_client, B2_BUCKET_NAME, gen_id, logger)  # ✅ Исправленный вызов

                # Удаляем generation_id, которые уже заархивированы
            config_public["generation_id"] = []
            save_config_public(CONFIG_PUBLIC_LOCAL_PATH, config_public)  # ✅ Сохраняем изменения
            logger.info("✅ Все generation_id удалены из config_public.json")

        else:
            logger.info("⚠️ В config_public.json отсутствует generation_id. Пропускаем архивирование.")

        # Генерация видео и загрузка в B2
        video_path = generate_mock_video(file_id)
        upload_to_b2(b2_client, target_folder, video_path)

        # Обновление config_public.json
        update_config_public(b2_client, target_folder)

        # 🔄 После генерации запускаем b2_storage_manager.py
        logger.info("🔄 Завершена генерация медиа. Запускаем b2_storage_manager.py для проверки состояния папок...")
        subprocess.run(["python", os.path.join(os.path.dirname(__file__), "b2_storage_manager.py")], check=True)

    except Exception as e:
        logger.error(f"❌ Ошибка в основном процессе: {e}")
        handle_error(logger, "Ошибка основного процесса", e)


if __name__ == "__main__":
    main()
