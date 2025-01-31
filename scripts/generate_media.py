# core/scripts/generate_media.py

import os
import json
import boto3
import botocore

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
CONFIG_GEN_PATH = os.path.abspath('config/config_gen.json')  # Локальный путь config_gen.json
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
    """Загружает файл из B2 (S3)."""
    try:
        logger.info(f"🔄 Начинаем загрузку файла из B2: {remote_path} -> {local_path}")
        ensure_directory_exists(os.path.dirname(local_path))

        b2_client = get_b2_client()
        if not isinstance(b2_client, botocore.client.BaseClient):
            raise TypeError("❌ Ошибка: b2_client не является объектом S3-клиента!")

        logger.info(f"🛠 b2_client перед вызовом download_file_from_b2(): {b2_client}")
        logger.info(f"🔍 download_file(): Bucket={B2_BUCKET_NAME}, Key={remote_path}, Filename={local_path}")
        logger.info(f"🔍 Тип client: {type(client)}")
        logger.info(f"🔍 Тип remote_path: {type(remote_path)}")
        logger.info(f"🔍 Перед вызовом .download_file() в download_file_from_b2(): {type(client)}")
        client.download_file(Bucket=B2_BUCKET_NAME, Key=remote_path, Filename=local_path)  # ✅ Исправлено

        logger.info(f"✅ Файл '{remote_path}' успешно загружен из B2 в {local_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки {remote_path}: {e}")
        handle_error(logger, f"B2 Download Error: {e}")


def upload_to_b2(client, folder, file_path):
    """Загружает файл в B2."""
    try:
        file_name = os.path.basename(file_path)
        s3_key = os.path.join(folder, file_name)
        logger.info(f"🔄 Начинаем загрузку файла в B2: {file_path} -> {s3_key}")
        client.upload_file(file_path, B2_BUCKET_NAME, s3_key)
        logger.info(f"✅ Файл '{file_name}' успешно загружен в B2: {s3_key}")
        os.remove(file_path)
        logger.info(f"🗑️ Временный файл {file_path} удалён после загрузки.")
    except Exception as e:
        handle_error(logger, f"B2 Upload Error: {e}")

def generate_mock_video(file_id):
    """Создаёт заглушку видеофайла."""
    video_path = f"{file_id}.mp4"
    try:
        logger.info(f"🎥 Генерация видеофайла: {video_path}")
        with open(video_path, 'wb') as video_file:
            video_file.write(b'\0' * 1024 * 1024)  # 1 MB файл
        logger.info(f"✅ Видео '{video_path}' успешно сгенерировано.")
        return video_path
    except Exception as e:
        handle_error(logger, f"Video Generation Error: {e}")

def update_config_public(client, folder):
    """Обновляет config_public.json, удаляя папку из 'empty'."""
    try:
        logger.info(f"🔄 Обновление config_public.json: удаление {folder} из списка 'empty'")
        logger.info(f"🛠 Перед вызовом download_file_from_b2(): client={type(client)}")
        download_file_from_b2(client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)

        if "empty" in config_public and folder in config_public["empty"]:
            config_public["empty"].remove(folder)
            logger.info(f"✅ Папка {folder} удалена из 'empty'. Обновлённое содержимое: {config_public}")

        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)

        logger.info(f"🛠 Перед вызовом upload_file(): client={type(client)}")
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info(f"✅ config_public.json обновлён и загружен обратно в B2.")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, f"Config Public Update Error: {e}")

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

        # Логируем вызов генератора
        logger.info(f"🚀 generate_media.py вызван из: {os.environ.get('GITHUB_WORKFLOW', 'локальный запуск')}")

        # Загружаем config_public.json
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)

        logger.info(f"📄 Загруженный config_public.json: {config_public}")

        # Проверяем наличие пустых папок
        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]
            logger.info(f"🎯 Выбрана папка для загрузки: {target_folder}")
        else:
            raise ValueError("❌ Ошибка: Список 'empty' отсутствует или пуст в config_public.json")

        # Генерация видео и загрузка в B2
        video_path = generate_mock_video(file_id)
        upload_to_b2(b2_client, target_folder, video_path)

        # Обновление config_public.json
        update_config_public(b2_client, target_folder)

    except Exception as e:
        logger.error(f"❌ Ошибка в основном процессе: {e}")
        handle_error(logger, "Ошибка загрузки файла из B2", e)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")

