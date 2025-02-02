import os
import json
import logging
import subprocess  # Для запуска внешних скриптов
import re

from modules.utils import is_folder_empty, ensure_directory_exists, move_to_archive
from scripts.generate_media import download_file_from_b2, generate_mock_video, update_config_public, upload_to_b2
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
CONFIG_PUBLIC_PATH = config.get('FILE_PATHS.config_public')  # например, "config/config_public.json"
CONFIG_GEN_PATH = os.path.abspath('config/config_gen.json')
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"  # ключ в B2
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
    """
    Загружает config_public.json из B2 и возвращает его содержимое как словарь.
    """
    try:
        local_path = CONFIG_PUBLIC_LOCAL_PATH
        logger.info(f"🔍 s3 перед .download_file(): {type(s3)}")
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
            logger.info(f"✅ Содержимое config_public.json: {config_data}")
            return config_data
    except FileNotFoundError:
        logger.error("❌ Файл config_public.json не найден локально.")
        return {}
    except ClientError as e:
        logger.error(f"Error loading config_public.json: {e.response['Error']['Message']}")
        return {}


def save_config_public(s3, data):
    """
    Сохраняет данные в config_public.json локально и загружает его в B2.
    """
    try:
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
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
            return
        if isinstance(generation_ids, str):
            generation_ids = [generation_ids]
        logger.info(f"📂 Найдены generation_id: {generation_ids}, перемещаем файлы в архив...")
        source_folders = ["444/", "555/", "666/"]
        archived_ids = []
        for generation_id in generation_ids:
            for folder in source_folders:
                files_to_move = list_files_in_folder(s3, folder)
                for file_key in files_to_move:
                    if generation_id in file_key:
                        archive_path = f"data/archive/{os.path.basename(file_key)}"
                        try:
                            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": file_key},
                                           Key=archive_path)
                            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file_key)
                            logger.info(f"✅ Файл {file_key} перемещён в архив: {archive_path}")
                            if generation_id not in archived_ids:
                                archived_ids.append(generation_id)
                        except ClientError as e:
                            logger.error(f"❌ Ошибка при архивировании {file_key}: {e.response['Error']['Message']}")
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]
        save_config_public(s3, config_data)
        logger.info(f"✅ Архивация завершена для: {archived_ids}")
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
    """
    Обходит список папок, пытается переместить "готовые" группы из папок с более низким приоритетом
    в папки с более высоким, и определяет пустые папки.
    После этого обновляет ключ "empty" в config_public.json.
    """
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
    # Вызов is_folder_empty с корректными параметрами: (s3, bucket_name, folder_prefix)
    if is_folder_empty(s3, B2_BUCKET_NAME, "666/"):
        logger.info("⚠️ Папка 666/ пуста. Запускаем генерацию контента...")
        subprocess.run(["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")], check=True)
    else:
        logger.info("✅ Все папки заполнены. Завершаем процесс.")
    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)
    save_config_public(s3, config_data)
    logger.info(f"📂 Обновлены пустые папки в config_public.json: {config_data['empty']}")


def run_generate_media():
    """Запускает скрипт generate_media.py по локальному пути."""
    try:
        scripts_folder = config.get("FILE_PATHS.scripts_folder", "scripts")
        script_path = os.path.join(scripts_folder, "generate_media.py")
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт generate_media.py не найден по пути: {script_path}")
        logger.info(f"🔄 Запуск скрипта: {script_path}")
        subprocess.run(["python", script_path], check=True)
        logger.info(f"✅ Скрипт {script_path} выполнен успешно.")
    except subprocess.CalledProcessError as e:
        handle_error("Script Execution Error", f"Ошибка при выполнении скрипта {script_path}: {e}")
    except FileNotFoundError as e:
        handle_error("File Not Found Error", str(e))
    except Exception as e:
        handle_error("Unknown Error", f"Ошибка при запуске скрипта {script_path}: {e}")


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
        logger.info(f"🚀 generate_media.py вызван из: {os.environ.get('GITHUB_WORKFLOW', 'локальный запуск')}")
        import inspect
        logger.info(f"🛠 Проверка b2_client в {__file__}, строка {inspect.currentframe().f_lineno}: {type(b2_client)}")
        logger.info(f"🔍 Перед вызовом download_file_from_b2(): {type(b2_client)}")
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        logger.info(f"🔍 После download_file_from_b2() b2_client: {type(b2_client)}")
        logger.info(f"🔍 Тип объекта b2_client перед вызовом download_file_from_b2: {type(b2_client)}")

        # Обновляем состояние папок через process_folders
        logger.info("🔄 Обновление состояния папок через process_folders()")
        process_folders(b2_client, FOLDERS)

        # Перезагружаем config_public.json после обновления пустых папок
        config_public = load_config_public(b2_client)
        logger.info(f"📄 Загруженный config_public.json: {config_public}")

        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]
            logger.info(f"🎯 Выбрана папка для загрузки: {target_folder}")
        else:
            if not config_public.get("empty", []):
                logger.info("✅ Нет пустых папок для загрузки. Завершаем процесс.")
                return  # Завершаем без ошибки

        if "empty" in config_public and config_public["empty"]:
            logger.info(f"📂 Обнаружены пустые папки: {config_public['empty']}")
            for empty_folder in config_public["empty"]:
                if empty_folder == "666/":
                    logger.info("⚠️ Папка 666/ пуста. Запускаем генерацию контента...")
                    subprocess.run(
                        ["python", os.path.join(config.get('FILE_PATHS.scripts_folder'), "generate_content.py")],
                        check=True)
                    import inspect
                    logger.info(f"🛠 Проверка b2_client в {__file__}, строка {inspect.currentframe().f_lineno}: {type(b2_client)}")

        # Исправленный вызов: вызываем move_to_archive без передачи b2_client
        if "generation_id" in config_public:
            for gen_id in config_public["generation_id"]:
                logger.info(f"📂 Перемещаем файлы группы {gen_id} в архив...")
                move_to_archive(b2_client, B2_BUCKET_NAME, gen_id, logger)
            config_public["generation_id"] = []
            save_config_public(b2_client, config_public)
            logger.info("✅ Все generation_id удалены из config_public.json")
        else:
            logger.info("⚠️ В config_public.json отсутствует generation_id. Пропускаем архивирование.")

        # Генерация видео и загрузка в B2
        video_path = generate_mock_video(file_id)
        upload_to_b2(b2_client, target_folder, video_path)

        # Обновление config_public.json
        update_config_public(b2_client, target_folder)

        # После генерации запускаем b2_storage_manager.py для проверки состояния папок
        logger.info("🔄 Завершена генерация медиа. Запускаем b2_storage_manager.py для проверки состояния папок...")
        subprocess.run(["python", os.path.join(os.path.dirname(__file__), "b2_storage_manager.py")], check=True)

    except Exception as e:
        logger.error(f"❌ Ошибка в основном процессе: {e}")
        handle_error(logger, "Ошибка основного процесса", e)


if __name__ == "__main__":
    main()
