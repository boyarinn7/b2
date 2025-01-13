# core/scripts/generate_media.py

import os
import json
import boto3
from modules.utils import ensure_directory_exists
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

# === Инициализация ===
logger = get_logger("generate_media")
config = ConfigManager()

# === Константы ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
B2_ENDPOINT = config.get('API_KEYS.b2.endpoint')
B2_ACCESS_KEY = config.get('API_KEYS.b2.access_key')
B2_SECRET_KEY = config.get('API_KEYS.b2.secret_key')
CONFIG_GEN_PATH = os.path.abspath('core/config/config_gen.json')  # Локальный путь config_gen.json
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"  # Путь в B2
CONFIG_PUBLIC_LOCAL_PATH = os.path.abspath('config_public.json')  # Временный локальный файл

# === Функции ===
def get_b2_client():
    try:
        return boto3.client(
            's3',
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_ACCESS_KEY,
            aws_secret_access_key=B2_SECRET_KEY
        )
    except Exception as e:
        handle_error(logger, f"B2 Client Initialization Error: {e}")

def download_file_from_b2(client, remote_path, local_path):
    try:
        ensure_directory_exists(os.path.dirname(local_path))
        client.download_file(B2_BUCKET_NAME, remote_path, local_path)
        logger.info(f"✅ Файл '{remote_path}' успешно загружен из B2 в {local_path}")
    except Exception as e:
        handle_error(logger, f"B2 Download Error: {e}")

def upload_to_b2(client, folder, file_path):
    try:
        file_name = os.path.basename(file_path)
        s3_key = os.path.join(folder, file_name)
        client.upload_file(file_path, B2_BUCKET_NAME, s3_key)
        logger.info(f"✅ Файл '{file_name}' успешно загружен в B2: {s3_key}")
        os.remove(file_path)
    except Exception as e:
        handle_error(logger, f"B2 Upload Error: {e}")

def generate_mock_video(file_id):
    video_path = f"{file_id}.mp4"
    try:
        with open(video_path, 'wb') as video_file:
            video_file.write(b'\0' * 1024 * 1024)  # 1 MB файл
        logger.info(f"✅ Видео '{video_path}' успешно сгенерировано.")
        return video_path
    except Exception as e:
        handle_error(logger, f"Video Generation Error: {e}")

def update_config_public(client, folder):
    try:
        download_file_from_b2(client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)

        if "empty" in config_public and folder in config_public["empty"]:
            config_public["empty"].remove(folder)

        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info(f"✅ Файл config_public.json обновлён: удалена папка {folder}")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)  # Удаление временного файла
    except Exception as e:
        handle_error(logger, f"Config Public Update Error: {e}")

def main():
    logger.info("🔄 Начинаем процесс генерации медиа...")
    try:
        # Читаем локальный файл config_gen.json
        with open(CONFIG_GEN_PATH, 'r', encoding='utf-8') as file:
            config_gen = json.load(file)
        file_id = os.path.splitext(config_gen["generation_id"])[0]

        # Создаём клиент B2
        b2_client = get_b2_client()

        # Загрузка config_public.json из B2
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)

        logger.info(f"Содержимое config_public: {config_public}")

        # Проверяем наличие пустых папок
        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]
        else:
            raise ValueError("Список 'empty' отсутствует или пуст в config_public.json")

        # Генерация видео и загрузка в B2
        video_path = generate_mock_video(file_id)
        upload_to_b2(b2_client, target_folder, video_path)

        # Обновление config_public.json
        update_config_public(b2_client, target_folder)

    except Exception as e:
        handle_error(logger, f"Main Process Error: {e}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
