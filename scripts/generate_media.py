import os
import json
import boto3
import botocore
import sys
import subprocess
import openai
import requests

from modules.utils import ensure_directory_exists
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

# === Инициализация конфигурации и логгера ===
config = ConfigManager()
logger = get_logger("generate_media")

# === Загрузка всех настроек из конфига ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name')
B2_ENDPOINT = config.get('API_KEYS.b2.endpoint')
B2_ACCESS_KEY = config.get('API_KEYS.b2.access_key')
B2_SECRET_KEY = config.get('API_KEYS.b2.secret_key')

# Пути к файлам (вынимаются из конфигурации)
CONFIG_GEN_PATH = os.path.abspath(config.get("FILE_PATHS.config_gen", "config/config_gen.json"))
CONFIG_PUBLIC_REMOTE_PATH = config.get("FILE_PATHS.config_public", "config/config_public.json")
CONFIG_PUBLIC_LOCAL_PATH = os.path.abspath(config.get("FILE_PATHS.config_public_local", "config_public.json"))
CONTENT_OUTPUT_PATH = config.get("FILE_PATHS.content_output_path", "generated_content.json")
SCRIPTS_FOLDER = os.path.abspath(config.get("FILE_PATHS.scripts_folder", "scripts"))

# Настройки генерации видео-сценария и изображения из раздела MEDIA конфига
VIDEO_SCENARIO_PROMPT = config.get("MEDIA.video_scenario_prompt")
VIDEO_MAX_TOKENS = config.get("MEDIA.video_max_tokens", 300)
VIDEO_TEMPERATURE = config.get("MEDIA.video_temperature", 0.7)
IMAGE_SIZE = config.get("MEDIA.image_size", "1024x768")

# Путь к скрипту b2_storage_manager.py (вынимается из конфига)
B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")

# === Функции работы с Backblaze B2 ===

def get_b2_client():
    """Создаёт и возвращает клиент B2 (S3) на основе настроек из конфига."""
    try:
        client = boto3.client(
            's3',
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_ACCESS_KEY,
            aws_secret_access_key=B2_SECRET_KEY
        )
        return client
    except Exception as e:
        handle_error(logger, f"B2 Client Initialization Error: {e}")

def download_file_from_b2(client, remote_path, local_path):
    """Загружает файл из B2 (S3) в локальное хранилище."""
    try:
        logger.info(f"🔄 Загрузка файла из B2: {remote_path} -> {local_path}")
        ensure_directory_exists(os.path.dirname(local_path))
        if not hasattr(client, 'download_file'):
            raise TypeError("❌ Ошибка: client не является объектом S3-клиента!")
        client.download_file(B2_BUCKET_NAME, remote_path, local_path)
        logger.info(f"✅ Файл '{remote_path}' успешно загружен в {local_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки {remote_path}: {e}")
        handle_error(logger, f"B2 Download Error: {e}")

def upload_to_b2(client, folder, file_path):
    """Загружает локальный файл в указанную папку B2 и удаляет локальную копию."""
    try:
        file_name = os.path.basename(file_path)
        if not folder.endswith('/'):
            folder += '/'
        s3_key = f"{folder}{file_name}"
        logger.info(f"🔄 Загрузка файла в B2: {file_path} -> {s3_key}")
        client.upload_file(file_path, B2_BUCKET_NAME, s3_key)
        logger.info(f"✅ Файл '{file_name}' успешно загружен в B2: {s3_key}")
        os.remove(file_path)
        logger.info(f"🗑️ Локальный файл {file_path} удалён после загрузки.")
    except Exception as e:
        handle_error(logger, f"B2 Upload Error: {e}")

def update_config_public(client, folder):
    """
    Обновляет config_public.json: удаляет указанную папку из списка 'empty'.
    После загрузки медиафайла папка считается заполненной.
    """
    try:
        logger.info(f"🔄 Обновление config_public.json: удаление {folder} из списка 'empty'")
        download_file_from_b2(client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        if "empty" in config_public and folder in config_public["empty"]:
            config_public["empty"].remove(folder)
            logger.info(f"✅ Папка {folder} удалена из 'empty'. Текущее содержимое: {config_public}")
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info("✅ config_public.json обновлён и загружен обратно в B2.")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, f"Config Public Update Error: {e}")

def reset_processing_lock(client):
    """
    Сбрасывает флаг блокировки processing_lock в config_public.json, устанавливая его в false.
    """
    try:
        logger.info("🔄 Сброс processing_lock в config_public.json")
        download_file_from_b2(client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        if config_public.get("processing_lock", False):
            config_public["processing_lock"] = False
            logger.info("✅ Флаг processing_lock сброшен.")
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, f"Processing Lock Reset Error: {e}")

# === Новые функции генерации сценария видео и изображения первого кадра ===

def generate_video_scenario(post_text):
    """
    Генерирует сценарий 10-секундного видео на основе текста поста.
    Использует промпт из конфига, подставляя {text} вместо исходного текста.
    Ожидается ответ в формате JSON с ключами:
      - 'video_scenario': сценарий видео,
      - 'first_frame_prompt': промпт для генерации изображения первого кадра.
    """
    try:
        prompt = VIDEO_SCENARIO_PROMPT.format(text=post_text)
        logger.info(f"🔎 Отправка запроса для видео-сценария с prompt: {prompt}")
        response = openai.ChatCompletion.create(
            model=config.get("API_KEYS.openai.model", "gpt-4"),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=VIDEO_MAX_TOKENS,
            temperature=VIDEO_TEMPERATURE,
        )
        response_text = response['choices'][0]['message']['content'].strip()
        logger.info(f"Сырой ответ OpenAI для видео-сценария: {response_text}")
        try:
            scenario_dict = json.loads(response_text)
            return scenario_dict
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка генерации видео-сценария: {e}. Сырой ответ: {response_text}")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка генерации видео-сценария: {e}")
        return None

def generate_image_with_dalle(prompt, generation_id):
    """
    Генерирует изображение первого кадра с помощью DALL‑E 3 по заданному промпту.
    Использует размер изображения из конфига. Скачивает изображение и сохраняет его локально с именем на основе generation_id.
    """
    try:
        logger.info(f"🔎 Генерация изображения через DALL‑E 3 с промптом: {prompt}")
        response = openai.Image.create(
            prompt=prompt,
            n=1,
            size=IMAGE_SIZE
        )
        image_url = response['data'][0]['url']
        logger.info(f"📤 Получен URL изображения: {image_url}")
        image_response = requests.get(image_url)
        if image_response.status_code == 200:
            image_path = f"{generation_id}.png"
            with open(image_path, "wb") as f:
                f.write(image_response.content)
            logger.info(f"✅ Изображение сохранено локально: {image_path}")
            return image_path
        else:
            logger.error("❌ Ошибка загрузки изображения с DALL‑E 3")
            return None
    except Exception as e:
        logger.error(f"❌ Ошибка генерации изображения DALL‑E 3: {e}")
        return None

def main():
    logger.info("🔄 Начало процесса генерации медиа...")
    try:
        # Чтение config_gen.json для получения уникального идентификатора генерации
        logger.info(f"📄 Чтение файла: {CONFIG_GEN_PATH}")
        with open(CONFIG_GEN_PATH, 'r', encoding='utf-8') as file:
            config_gen = json.load(file)
        file_id = os.path.splitext(config_gen["generation_id"])[0]
        logger.info(f"📂 ID генерации: {file_id}")

        # Создание клиента B2
        b2_client = get_b2_client()

        # Загрузка config_public.json из B2
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        logger.info(f"📄 Загруженный config_public.json: {config_public}")

        # Выбор целевой папки из списка 'empty'
        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]
            logger.info(f"🎯 Выбрана папка для загрузки: {target_folder}")
        else:
            raise ValueError("❌ Ошибка: Список 'empty' пуст или отсутствует в config_public.json")

        # Загрузка сгенерированного контента (текста поста)
        logger.info(f"📄 Чтение сгенерированного контента из: {CONTENT_OUTPUT_PATH}")
        with open(CONTENT_OUTPUT_PATH, 'r', encoding='utf-8') as f:
            generated_content = json.load(f)
        post_text = generated_content.get("content", "")
        if not post_text:
            raise ValueError("❌ Ошибка: Текст поста пуст!")
        logger.info(f"📝 Текст поста: {post_text[:100]}...")

        # Генерация видео-сценария и извлечение промпта для первого кадра
        scenario = generate_video_scenario(post_text)
        if scenario is None or "first_frame_prompt" not in scenario:
            raise ValueError("❌ Ошибка: Не удалось получить промпт для первого кадра!")
        first_frame_prompt = scenario["first_frame_prompt"]
        logger.info(f"🎬 Промпт для первого кадра: {first_frame_prompt}")

        # Генерация изображения первого кадра с помощью DALL‑E 3
        image_path = generate_image_with_dalle(first_frame_prompt, file_id)
        if image_path is None:
            raise ValueError("❌ Ошибка: Генерация изображения DALL‑E 3 не удалась!")

        # Загрузка сгенерированного изображения в B2
        upload_to_b2(b2_client, target_folder, image_path)

        # Обновление config_public.json: удаление заполненной папки и сброс блокировки
        update_config_public(b2_client, target_folder)
        reset_processing_lock(b2_client)

        # Запуск скрипта b2_storage_manager.py для дальнейшей обработки
        logger.info(f"🔄 Запуск скрипта: {B2_STORAGE_MANAGER_SCRIPT}")
        subprocess.run([sys.executable, B2_STORAGE_MANAGER_SCRIPT], check=True)

    except Exception as e:
        logger.error(f"❌ Ошибка в процессе генерации медиа: {e}")
        handle_error(logger, "Ошибка в процессе генерации медиа", e)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
