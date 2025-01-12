import os
import json
import logging
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists
from modules.config_manager import ConfigManager

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


def load_config_public(s3):
    """Загружает файл config_public.json из B2."""
    logger.info("🔄 Загрузка config_public.json из B2")
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        logger.debug(f"📂 Локальный путь для загрузки: {local_path}")
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            config_data = json.load(file)
            logger.info(f"✅ config_public.json успешно загружен: {config_data}")
            return config_data
    except FileNotFoundError:
        logger.warning("⚠️ Файл config_public.json отсутствует. Создаётся новый.")
        return {}
    except ClientError as e:
        logger.error(f"❌ Ошибка загрузки config_public.json: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
        return {}

def save_config_public(s3, data):
    """Сохраняет файл config_public.json в B2."""
    logger.info("🔄 Сохранение config_public.json в B2")
    try:
        local_path = os.path.basename(CONFIG_PUBLIC_PATH)
        with open(local_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
            logger.debug(f"📄 Содержимое для сохранения: {data}")
        s3.upload_file(local_path, B2_BUCKET_NAME, CONFIG_PUBLIC_PATH)
        logger.info("✅ Файл config_public.json успешно обновлён.")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_public.json: {e}")

def list_files_in_folder(s3, folder_prefix):
    """Возвращает список файлов в указанной папке."""
    logger.info(f"🔄 Получение списка файлов в папке {folder_prefix}")
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder_prefix)
        files = [
            obj['Key'] for obj in response.get('Contents', [])
            if obj['Key'] != folder_prefix and not obj['Key'].endswith('.bzEmpty') and FILE_NAME_PATTERN.match(os.path.basename(obj['Key']))
        ]
        logger.debug(f"🔍 Список файлов в {folder_prefix}: {files}")
        return files
    except ClientError as e:
        logger.error(f"❌ Ошибка при получении списка файлов из {folder_prefix}: {e.response['Error']['Code']} - {e.response['Error']['Message']}")
        return []

def get_ready_groups(files):
    """Возвращает список готовых групп на основе файлов."""
    logger.info("🔄 Определение готовых групп")
    groups = {}
    for file_key in files:
        base_name = os.path.basename(file_key)
        if FILE_NAME_PATTERN.match(base_name):
            group_id = base_name.rsplit('.', 1)[0]  # Убираем расширение
            groups.setdefault(group_id, []).append(base_name)
            logger.debug(f"🔧 Добавлено в группу {group_id}: {base_name}")
        else:
            logger.warning(f"⚠️ Файл {base_name} не соответствует шаблону, пропущен")

    ready_groups = []
    for group_id, file_list in groups.items():
        logger.debug(f"🔍 Проверяем группу {group_id}: {file_list}")
        expected_files = [group_id + ext for ext in FILE_EXTENSIONS]
        missing_files = [file for file in expected_files if file not in file_list]
        if not missing_files:
            ready_groups.append(group_id)
            logger.info(f"✅ Группа {group_id} готова: {file_list}")
        else:
            logger.warning(f"⚠️ Для группы {group_id} не хватает файлов: {missing_files}")

    logger.info(f"✅ Найдены готовые группы: {ready_groups}")
    return ready_groups

def handle_publish(s3, config_data):
    """Обрабатывает поле 'publish' в config_public.json."""
    publish_folder = config_data.get("publish")
    if not publish_folder:
        return

    logger.info(f"🔄 Обработка публикации из папки: {publish_folder}")
    files = list_files_in_folder(s3, publish_folder)
    if not files:
        logger.warning(f"⚠️ Папка {publish_folder} пуста. Пропуск обработки.")
        return

    archived_files = []
    for file_key in files:
        archive_key = file_key.replace(publish_folder, ARCHIVE_FOLDER)
        try:
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": file_key}, Key=archive_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file_key)
            archived_files.append(file_key)
        except ClientError as e:
            logger.error(f"❌ Ошибка перемещения {file_key} в архив: {e}")

    logger.info(f"✅ Перемещённые файлы из {publish_folder} в архив: {archived_files}")
    config_data.pop("publish", None)
    save_config_public(s3, config_data)
    logger.info("✅ Поле 'publish' обработано и удалено из config_public.json.")


def move_group(s3, src_folder, dst_folder, group_id):
    """Перемещает группу файлов в другую папку."""
    logger.info(f"🔄 Перемещение группы {group_id} из {src_folder} в {dst_folder}")
    moved_files = []
    for ext in FILE_EXTENSIONS:
        src_key = f"{src_folder}{group_id}{ext}"
        dst_key = f"{dst_folder}{group_id}{ext}"
        try:
            s3.head_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
            moved_files.append(src_key)
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                logger.warning(f"⚠️ Файл {src_key} не найден. Пропуск.")
            else:
                logger.error(f"❌ Ошибка при работе с ключом {src_key}: {e.response['Error']['Code']} - {e.response['Error']['Message']}")

    logger.info(f"✅ Группа {group_id} перемещена. Файлы: {moved_files}")

def process_folders(s3, folders):
    """Проверяет папки и перемещает группы по принципу 'от большего к меньшему'."""
    logger.info("🔄 Начало обработки папок")
    empty_folders = set()
    changes_made = True

    while changes_made:
        changes_made = False
        for i in range(len(folders) - 1, 0, -1):
            src_folder = folders[i]
            dst_folder = folders[i - 1]

            # Пропускаем пустые папки
            if src_folder in empty_folders:
                logger.debug(f"⏩ Пропуск пустой папки {src_folder}")
                continue

            logger.info(f"🔄 Обработка папки {src_folder}")
            src_files = list_files_in_folder(s3, src_folder)
            dst_files = list_files_in_folder(s3, dst_folder)

            # Логгирование групп единым сообщением
            logger.debug(f"📂 Содержимое папки {src_folder}: {src_files}")
            logger.debug(f"📂 Содержимое папки {dst_folder}: {dst_files}")

            src_ready = get_ready_groups(src_files)
            dst_ready = get_ready_groups(dst_files)

            logger.info(f"📦 Готовые группы в {src_folder}: {src_ready}")
            logger.info(f"📦 Готовые группы в {dst_folder}: {dst_ready}")

            for group_id in src_ready:
                if len(dst_ready) < 1:
                    move_group(s3, src_folder, dst_folder, group_id)
                    changes_made = True

            if not src_ready:
                empty_folders.add(src_folder)

    # Финальный список всех папок
    for folder in folders:
        files = list_files_in_folder(s3, folder)
        logger.info(f"📂 Итоговое содержимое {folder}: {files}")

    logger.info(f"🗂️ Пустые папки после обработки: {list(empty_folders)}")
    return list(empty_folders)

def main():
    """Основной процесс."""
    try:
        logger.info("🔄 Инициализация основного процесса")
        s3 = get_b2_client()
        config_data = load_config_public(s3)

        # Обработка публикации
        handle_publish(s3, config_data)

        # Обработка папок
        empty_folders = process_folders(s3, FOLDERS)

        # Обновление информации о пустых папках
        config_data['empty'] = empty_folders
        save_config_public(s3, config_data)

        logger.info("🏁 Работа завершена успешно.")
    except Exception as e:
        handle_error(logger, e, "Ошибка в основном процессе")

if __name__ == "__main__":
    main()
