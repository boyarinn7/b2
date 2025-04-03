import os
import json
import logging
import subprocess
import re
import sys
from botocore.exceptions import ClientError

script_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.append(parent_dir)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("b2_storage_manager")
logger.info(f"Script directory: {script_dir}")
logger.info(f"Parent directory added to sys.path: {parent_dir}")
logger.info(f"Full sys.path: {sys.path}")
logger.info(f"Does modules/ exist? {os.path.isdir(os.path.join(parent_dir, 'modules'))}")
logger.info(f"Does modules/utils.py exist? {os.path.isfile(os.path.join(parent_dir, 'modules', 'utils.py'))}")

from modules.utils import is_folder_empty, ensure_directory_exists
from scripts.generate_media import download_file_from_b2
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

# Инициализация (оставляем ConfigManager для локальной отладки, но используем os.getenv для Actions)
config = ConfigManager()
logger = get_logger("b2_storage_manager")
logger.info("Начало выполнения b2_storage_manager")
logger.info(f"Текущая рабочая директория: {os.getcwd()}")

# Константы с использованием переменных окружения
CONFIG_PUBLIC_PATH = os.getenv("CONFIG_PUBLIC_PATH", "config_public.json")
logger.info(f"Локальный путь CONFIG_PUBLIC_PATH: {CONFIG_PUBLIC_PATH}")
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"
FILE_EXTENSIONS = ['.json', '.png', '.mp4']
FOLDERS = [
    config.get("FILE_PATHS.folder_444", "444/"),
    config.get("FILE_PATHS.folder_555", "555/"),
    config.get("FILE_PATHS.folder_666", "666/")
]
ARCHIVE_FOLDER = config.get("FILE_PATHS.archive_folder", "archive/")
GENERATE_CONTENT_SCRIPT = os.path.join(config.get("FILE_PATHS.scripts_folder", "scripts"), "generate_content.py")
FILE_NAME_PATTERN = re.compile(r"^\d{8}-\d{4}\.\w+$")
logger.info(f"Путь к generate_content.py: {GENERATE_CONTENT_SCRIPT}")

# Добавим отладку для проверки значений
logger.info(f"FOLDERS: {FOLDERS}")
logger.info(f"ARCHIVE_FOLDER: {ARCHIVE_FOLDER}")

def load_config_public(s3):
    """Загружает config_public.json из B2."""
    try:
        local_path = CONFIG_PUBLIC_PATH
        bucket_name = os.getenv("B2_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        logger.info(f"Загрузка конфигурации из B2: {CONFIG_PUBLIC_REMOTE_PATH} в {local_path}")
        s3.download_file(bucket_name, CONFIG_PUBLIC_REMOTE_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        logger.info(f"Конфигурация загружена из {local_path}: {json.dumps(data, ensure_ascii=False)}")
        return data
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning("⚠️ Конфиг не найден, создаём новый.")
            new_data = {"processing_lock": False, "empty": [], "generation_id": []}
            logger.info(f"Создан новый конфиг: {json.dumps(new_data, ensure_ascii=False)}")
            return new_data
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
        logger.info(f"✅ Конфигурация успешно сохранена. Новое содержимое: {json.dumps(data, ensure_ascii=False)}")
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
    logger.info(f"Используемый bucket_name: {bucket_name}")
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

    # Используем только проверку через any_folder_empty (проверка наличия полных групп)
    # if is_folder_empty(s3, bucket_name, folders[-1]):
    #     logger.info("⚠️ Папка 666/ пуста. Запуск генерации контента...")
    #     if not os.path.exists(GENERATE_CONTENT_SCRIPT):
    #         logger.error(f"Ошибка: generate_content.py не найден по пути {GENERATE_CONTENT_SCRIPT}")
    #         return
    #     subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT], check=True)
    #     sys.exit(0)  # Остановка после вызова generate_content.py

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
        midjourney_results = config_data.get("midjourney_results", None)
        if midjourney_results and not isinstance(midjourney_results, dict):
            logger.warning("⚠️ midjourney_results не является словарем, очищаем ключ")
            config_data["midjourney_results"] = None
            save_config_public(b2_client, config_data)
        return midjourney_results
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка разбора JSON в config_public.json: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке midjourney_results: {e}")
        return None

def any_folder_empty(s3, folders):
    """
    Проверяет, есть ли среди заданных папок хотя бы одна, в которой нет полных групп файлов.
    Полная группа — это наличие файлов с одинаковым идентификатором и требуемыми расширениями (.json, .png, .mp4).
    """
    for folder in folders:
        logger.info(f"Проверяем папку: {folder}")
        files = list_files_in_folder(s3, folder)
        logger.info(f"Найденные файлы в папке {folder}: {files}")
        ready_groups = get_ready_groups(files)
        logger.info(f"Полные группы в папке {folder}: {ready_groups}")
        if not ready_groups:
            logger.info(f"Папка {folder} считается пустой (нет полных групп).")
            return True
        else:
            logger.info(f"Папка {folder} содержит полные группы.")
    return False


def main():
    b2_client = None
    generation_count = 0
    MAX_GENERATIONS = 3
    SCRIPTS_FOLDER = os.path.abspath(config.get("FILE_PATHS.scripts_folder", "scripts"))

    try:
        b2_client = get_b2_client()
        logger.info("Клиент B2 успешно создан.")
        config_public = load_config_public(b2_client)

        # Шаг 1: Проверка наличия ключа midjourney_results
        midjourney_results = config_public.get("midjourney_results")
        if midjourney_results:
            image_urls = midjourney_results.get("image_urls", [])
            # Проверка валидности: все URL должны быть строками, начинающимися с http/https
            if image_urls and all(isinstance(url, str) and url.startswith("http") for url in image_urls):
                logger.info("Найден валидный midjourney_results, запускаем generate_media.py")
                generate_media_path = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
                if not os.path.isfile(generate_media_path):
                    raise FileNotFoundError(f"❌ Файл {generate_media_path} не найден")
                # Снимаем блокировку перед запуском генератора медиа, если она установлена
                if config_public.get("processing_lock"):
                    config_public["processing_lock"] = False
                    save_config_public(b2_client, config_public)
                    logger.info("🔓 Блокировка снята перед запуском generate_media.py")
                subprocess.run([sys.executable, generate_media_path], check=True)
                sys.exit(0)
            else:
                logger.warning("⚠️ Некорректные данные в midjourney_results, очищаем ключ")
                if "midjourney_results" in config_public:
                    del config_public["midjourney_results"]
                    save_config_public(b2_client, config_public)

        # Обновляем конфигурацию после возможной очистки midjourney_results
        config_public = load_config_public(b2_client)

        # Убираем прежнюю проверку, чтобы не завершать работу раньше времени:
        # if not config_public.get("generation_id") and not config_public.get("empty"):
        #     logger.info("🚦 Нет записей о публикациях и пустых папок. Скрипт завершает работу.")
        #     return

        # Если процесс уже запущен (блокировка установлена), завершаем работу.
        if config_public.get("processing_lock"):
            logger.info("🔒 Процесс уже выполняется. Завершаем работу.")
            return

        # Устанавливаем блокировку
        config_public["processing_lock"] = True
        save_config_public(b2_client, config_public)
        logger.info("🔒 Блокировка установлена.")

        # Архивация опубликованных групп (если имеются)
        config_public = load_config_public(b2_client)
        if config_public.get("generation_id"):
            handle_publish(b2_client, config_public)

        # Перемещение групп между папками (обеспечивает очередность публикаций)
        process_folders(b2_client, FOLDERS)
        config_public = load_config_public(b2_client)
        logger.info(f"После process_folders() config_public: {config_public}")

        # Добавляем дополнительное логирование после перемещения папок
        config_public = load_config_public(b2_client)
        logger.info(f"После process_folders() config_public: {config_public}")

        # Цикл генерации контента: если в конфигурации остаются пустые папки (нет полных групп), запускаем генератор контента
        config_public = load_config_public(b2_client)
        while config_public.get("empty") and generation_count < MAX_GENERATIONS:
            logger.info(
                f"⚠️ Обнаружены пустые папки ({config_public['empty']}), генерация #{generation_count + 1} из {MAX_GENERATIONS}...")
            subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT], check=True)
            sys.exit(0)  # Завершаем работу сразу после вызова генератора контента
            generation_count += 1  # Эта строка, вероятно, не выполнится из-за sys.exit(0)
            config_public = load_config_public(b2_client)
            logger.info(f"✅ Завершена генерация #{generation_count}. Пустые папки: {config_public.get('empty', [])}")

        if generation_count >= MAX_GENERATIONS:
            logger.info(
                f"🚫 Достигнут лимит генераций ({MAX_GENERATIONS}). Завершаем работу, даже если остались пустые папки: {config_public.get('empty', [])}")
        elif not config_public.get("empty"):
            logger.info("✅ Нет пустых папок – генерация контента завершена.")

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        # Сбрасываем блокировку независимо от результатов
        if b2_client:
            try:
                config_public = load_config_public(b2_client)
                if config_public.get("processing_lock"):
                    config_public["processing_lock"] = False
                    save_config_public(b2_client, config_public)
                    logger.info("🔓 Блокировка снята.")
            except Exception as e:
                logger.error(f"❌ Ошибка при завершении работы: {e}")
        sys.exit(0)

if __name__ == "__main__":
    main()
