# -*- coding: utf-8 -*-
import os
import boto3
from pathlib import Path
import logging
from collections import defaultdict
import sys # Добавлен импорт sys

# --- Настройка Логгирования ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)]) # Явное указание stdout
logger = logging.getLogger("b2_group_uploader")

# --- Настройки ---
# Локальная папка, где ищем готовые группы файлов
LOCAL_SOURCE_FOLDER = r"C:\Users\boyar\777\555\готовые\загруженные"
# Папка в B2, куда загружаем найденные группы
REMOTE_TARGET_FOLDER = "444/"
# Необходимые расширения для полной группы
REQUIRED_EXTENSIONS = {".json", ".png", ".mp4"}

# Настройки B2 (читаются из переменных окружения)
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

def initialize_b2_client():
    """Инициализирует и возвращает клиент B2 S3."""
    if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
        logger.error("❌ Не все переменные окружения B2 установлены (B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT).")
        return None
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_ACCESS_KEY,
            aws_secret_access_key=B2_SECRET_KEY,
        )
        logger.info("✅ Клиент B2 S3 успешно инициализирован.")
        return s3
    except Exception as e:
        logger.exception(f"❌ Ошибка инициализации клиента B2: {e}")
        return None

def find_complete_groups(local_folder: str) -> dict:
    """
    Сканирует локальную папку и находит полные группы файлов (.json, .png, .mp4).
    Возвращает словарь, где ключ - базовое имя группы, значение - список полных путей к файлам группы.
    """
    source_path = Path(local_folder)
    if not source_path.is_dir():
        logger.error(f"❌ Локальная папка не найдена: {local_folder}")
        return {}

    logger.info(f"🔍 Сканирование папки: {local_folder}")
    files_by_stem = defaultdict(set)
    file_paths_by_stem = defaultdict(list)

    # Собираем информацию о файлах
    for item in source_path.iterdir():
        if item.is_file():
            stem = item.stem  # Имя файла без расширения
            ext = item.suffix.lower() # Расширение в нижнем регистре
            if ext in REQUIRED_EXTENSIONS:
                files_by_stem[stem].add(ext)
                file_paths_by_stem[stem].append(item) # Сохраняем полный путь

    # Находим полные группы
    complete_groups = {}
    for stem, extensions in files_by_stem.items():
        if extensions == REQUIRED_EXTENSIONS:
            logger.info(f"  Найден кандидат на полную группу: {stem} (файлы: {[p.name for p in file_paths_by_stem[stem]]})")
            complete_groups[stem] = file_paths_by_stem[stem]
        else:
            missing = REQUIRED_EXTENSIONS - extensions
            logger.debug(f"  Неполная группа для '{stem}'. Отсутствуют расширения: {missing}")


    if not complete_groups:
        logger.warning(f"Полные группы ({', '.join(REQUIRED_EXTENSIONS)}) не найдены в {local_folder}.")
    else:
         logger.info(f"✅ Найдено полных групп: {len(complete_groups)}")

    return complete_groups

def upload_file(s3_client, bucket: str, local_file_path: Path, remote_key: str) -> bool:
    """Загружает один локальный файл в B2."""
    try:
        logger.info(f"  🔼 Загрузка {local_file_path.name} -> {remote_key}...")
        s3_client.upload_file(str(local_file_path), bucket, remote_key)
        logger.info(f"    ✅ Успешно загружено: {remote_key}")
        return True
    except Exception as e:
        logger.error(f"    ❌ Ошибка загрузки {local_file_path.name} в {remote_key}: {e}")
        return False

def process_and_upload_groups(s3_client, bucket: str, groups: dict, target_folder: str):
    """Обрабатывает найденные группы и загружает их в B2."""
    if not s3_client:
        logger.error("Клиент B2 не инициализирован, загрузка невозможна.")
        return

    if not groups:
        logger.info("Нет полных групп для загрузки.")
        return

    logger.info(f"🚀 Начало загрузки {len(groups)} групп в B2 папку '{target_folder}'...")
    upload_count = 0
    group_count = 0
    for stem, file_paths in groups.items():
        group_count += 1
        logger.info(f"--- Обработка группы '{stem}' ({group_count}/{len(groups)}) ---")
        success_in_group = True
        for local_path in file_paths:
            remote_key = f"{target_folder.rstrip('/')}/{local_path.name}"
            if not upload_file(s3_client, bucket, local_path, remote_key):
                success_in_group = False
                # Можно добавить логику повторных попыток или остановки при ошибке
        if success_in_group:
            upload_count += 1
            logger.info(f"--- Группа '{stem}' успешно загружена ---")
        else:
             logger.error(f"--- Ошибки при загрузке группы '{stem}' ---")

    logger.info(f"🏁 Загрузка завершена. Успешно загружено групп: {upload_count} из {len(groups)}.")

if __name__ == "__main__":
    logger.info("===== Запуск скрипта загрузки в B2 =====")
    s3 = initialize_b2_client()
    if s3:
        complete_groups_found = find_complete_groups(LOCAL_SOURCE_FOLDER)
        process_and_upload_groups(s3, B2_BUCKET_NAME, complete_groups_found, REMOTE_TARGET_FOLDER)
    else:
        logger.error("Завершение работы из-за ошибки инициализации клиента B2.")
    logger.info("===== Скрипт загрузки в B2 завершил работу =====")
