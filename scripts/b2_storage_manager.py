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
    """Загружает config_public.json из B2."""
    try:
        local_path = CONFIG_PUBLIC_PATH
        s3.download_file(B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH, local_path)
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
    """Сохраняет config_public.json в B2."""
    try:
        with open(CONFIG_PUBLIC_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        s3.upload_file(CONFIG_PUBLIC_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info("✅ Конфигурация успешно сохранена.")
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига: {e}")


def list_files_in_folder(s3, folder_prefix):
    """Возвращает список файлов в указанной папке (кроме placeholder)."""
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
    """Перемещает файлы группы (по всем расширениям) из src_folder в dst_folder."""
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
    """
    Перемещает готовые группы файлов между папками сверху вниз:
    из 666/ в 555/ и из 555/ в 444/. При этом, если в исходной папке нет готовых групп,
    папка отмечается как пустая.
    Если папка 666/ оказывается пустой, запускается генерация контента.
    После перемещений обновляется список пустых папок в config_public.json.
    """
    empty_folders = set()
    changes_made = True

    while changes_made:
        changes_made = False
        # Проходим по папкам снизу вверх (индексы: [444, 555, 666])
        for i in range(len(folders) - 1, 0, -1):
            src_folder = folders[i]
            dst_folder = folders[i - 1]

            if src_folder in empty_folders:
                continue

            # Получаем списки файлов для исходной и целевой папок
            src_files = list_files_in_folder(s3, src_folder)
            dst_files = list_files_in_folder(s3, dst_folder)

            # Определяем готовые группы в исходной папке
            src_ready = get_ready_groups(src_files)
            # Если в целевой папке уже есть готовая группа, считаем, что она заполнена
            dst_ready = get_ready_groups(dst_files)

            # Перемещаем группы из src в dst, если в целевой папке их ещё нет (емкость проверяется как наличие готовых групп)
            for group_id in src_ready:
                if len(dst_ready) < 1:
                    move_group(s3, src_folder, dst_folder, group_id)
                    changes_made = True

            # Если в исходной папке нет готовых групп, отмечаем её как пустую
            if not src_ready:
                empty_folders.add(src_folder)

    # Если папка 666/ пуста, можно запускать генерацию контента (она будет заполнена медиа-файлом)
    if is_folder_empty(s3, B2_BUCKET_NAME, folders[-1]):
        logger.info("⚠️ Папка 666/ пуста. Запуск генерации контента...")
        subprocess.run([sys.executable, GENERATE_CONTENT_SCRIPT], check=True)

    # Обновляем список пустых папок в конфигурации
    config_data = load_config_public(s3)
    config_data["empty"] = list(empty_folders)
    save_config_public(s3, config_data)
    logger.info(f"📂 Обновлены пустые папки: {config_data.get('empty')}")


def handle_publish(s3, config_data):
    """
    Архивирует старые группы файлов по generation_id.
    Для каждого идентификатора группы файлы из всех рабочих папок копируются в архивную папку.
    После успешной архивации обновляется config_public.json.
    """
    generation_ids = config_data.get("generation_id", [])

    if not generation_ids:
        logger.info("📂 Нет generation_id для архивации.")
        return

    # Если передан один идентификатор в виде строки – преобразуем в список
    if isinstance(generation_ids, str):
        generation_ids = [generation_ids]

    archived_ids = []

    for generation_id in generation_ids:
        logger.info(f"🔄 Архивируем группу: {generation_id}")

        # Проверяем наличие файлов хотя бы в одной из рабочих папок
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

    # Обновляем конфигурацию: удаляем заархивированные generation_id
    if archived_ids:
        config_data["generation_id"] = [gid for gid in generation_ids if gid not in archived_ids]
        if not config_data["generation_id"]:
            del config_data["generation_id"]
        save_config_public(s3, config_data)
        logger.info(f"✅ Успешно заархивированы: {archived_ids}")
    else:
        logger.warning("⚠️ Не удалось заархивировать ни одну группу.")


def main():
    """
    Основной процесс управления B2-хранилищем с лимитом генераций.

    Логика:
      1. При запуске считывается config_public.json из B2.
      2. Если отсутствует и запись generation_id, и пустые папки – скрипт завершает работу.
      3. Если есть запись generation_id – архивируются соответствующие группы файлов.
      4. Затем выполняется перемещение готовых групп файлов:
         из 666/ в 555/, из 555/ в 444/.
      5. После перемещений обновляется конфигурация пустых папок.
      6. Если обнаружены пустые папки и лимит генераций (3) не достигнут,
         запускается генерация контента (generate_content.py).
      7. Скрипт использует блокировку (processing_lock) для предотвращения параллельного запуска.
    """
    b2_client = None
    generation_count = 0  # Счётчик генераций за один запуск
    MAX_GENERATIONS = 3   # Максимум генераций

    try:
        b2_client = get_b2_client()
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
            generation_count += 1
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
