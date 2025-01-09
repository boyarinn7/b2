# core/scripts/itself.py

import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from modules.api_clients import get_b2_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists, validate_json_structure
from modules.config_manager import ConfigManager
from botocore.exceptions import ClientError

# === Инициализация ===
config = ConfigManager()
logger = get_logger("itself")
s3 = get_b2_client()

# === Константы ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
ARCHIVE_FOLDERS = config.get('FILE_PATHS.archive_folder')
SUCCESS_THRESHOLD = config.get('LEARNING.success_threshold', 8)
DELETE_THRESHOLD = config.get('LEARNING.delete_threshold', 3)
MAX_WORKERS = config.get('LEARNING.max_workers', 5)


def list_files(folder):
    """Возвращает список файлов из папки на B2."""
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder)
        return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith("-metadata.json")]
    except ClientError as e:
        handle_error("B2 List Files Error", e)


def load_meta_file(file_key):
    """Загружает и валидирует мета-файл."""
    required_fields = [
        "topic", "text", "likes", "shares", "views",
        "ocp", "seo_keywords", "date", "comments", "has_media"
    ]
    local_file = os.path.basename(file_key)
    try:
        s3.download_file(B2_BUCKET_NAME, file_key, local_file)
        with open(local_file, "r", encoding="utf-8") as f:
            content = json.load(f)

        validate_json_structure(content, required_fields)
        return content
    except (json.JSONDecodeError, ClientError) as e:
        handle_error("Meta File Load Error", e)
    finally:
        if os.path.exists(local_file):
            os.remove(local_file)


def calculate_rating(meta):
    """Вычисляет рейтинг на основе метрик."""
    try:
        likes = meta.get("likes", 0)
        comments = len(meta.get("comments", []))
        shares = meta.get("shares", 0)
        views = meta.get("views", 1)
        engagement_rate = (likes + comments + shares) / views if views > 0 else 0

        base_score = (likes + comments + shares) / (datetime.now() - datetime.strptime(meta["date"], "%Y-%m-%d")).days
        rating = round(
            base_score +
            (meta.get("topic_score", 0) * 0.2) +
            (meta.get("text_score", 0) * 0.3) +
            (engagement_rate * 0.2),
            2
        )
        logger.info(f"📊 Итоговый рейтинг: {rating}")
        return rating
    except Exception as e:
        handle_error("Rating Calculation Error", e)


def move_file(file_key, dest_folder):
    """Перемещает файл в указанную папку."""
    try:
        src_key = file_key
        dst_key = f"{dest_folder}/{os.path.basename(file_key)}"
        s3.copy_object(Bucket=B2_BUCKET_NAME, CopySource={"Bucket": B2_BUCKET_NAME, "Key": src_key}, Key=dst_key)
        s3.delete_object(Bucket=B2_BUCKET_NAME, Key=src_key)
        logger.info(f"✅ Файл {src_key} перемещён в {dest_folder}.")
    except ClientError as e:
        handle_error("File Move Error", e)


def process_file(file_key):
    """Обрабатывает отдельный файл."""
    meta = load_meta_file(file_key)
    if not meta:
        return

    rating = calculate_rating(meta)
    if rating > SUCCESS_THRESHOLD:
        move_file(file_key, f"{ARCHIVE_FOLDERS}/successful")
    elif rating < DELETE_THRESHOLD:
        move_file(file_key, f"{ARCHIVE_FOLDERS}/pending_delete")


def update_archive():
    """Обновляет архив на основе анализа файлов."""
    try:
        in_progress_files = list_files(f"{ARCHIVE_FOLDERS}/in_progress")
        if not in_progress_files:
            logger.info("✅ Нет файлов для обработки.")
            return

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(process_file, in_progress_files)
        logger.info("🏁 Архив успешно обновлён.")
    except Exception as e:
        handle_error("Archive Update Error", e)


def main():
    """Основной процесс управления архивом."""
    logger.info("🔄 Запуск процесса управления архивом...")
    update_archive()
    logger.info("🏁 Процесс завершён успешно.")


# === Точка входа ===
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
