import os
import json
import boto3
import sys
import subprocess
import openai
import requests
import base64
import time
from PIL import Image
from runwayml import RunwayML

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

# Пути к файлам
CONFIG_GEN_PATH = os.path.abspath(config.get("FILE_PATHS.config_gen", "config/config_gen.json"))
CONFIG_PUBLIC_REMOTE_PATH = config.get("FILE_PATHS.config_public", "config/config_public.json")
CONFIG_PUBLIC_LOCAL_PATH = os.path.abspath(config.get("FILE_PATHS.config_public_local", "config_public.json"))
CONTENT_OUTPUT_PATH = config.get("FILE_PATHS.content_output_path", "generated_content.json")
SCRIPTS_FOLDER = os.path.abspath(config.get("FILE_PATHS.scripts_folder", "scripts"))

# Настройки генерации из конфига (с значениями по умолчанию)
USER_PROMPT_COMBINED = config.get("PROMPTS.user_prompt_combined", "Write a detailed script for a video on '{topic}'...")
OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4-turbo")
OPENAI_MAX_TOKENS = config.get("OPENAI_SETTINGS.max_tokens", 1000)
OPENAI_TEMPERATURE = config.get("OPENAI_SETTINGS.temperature", 0.7)
MIN_SCRIPT_LENGTH = config.get("VISUAL_ANALYSIS.min_script_length", 200)
IMAGE_SIZE_DALLE = config.get("IMAGE_GENERATION.image_size", "1024x1024")
NUM_IMAGES = config.get("IMAGE_GENERATION.num_images", 1)
OUTPUT_IMAGE_FORMAT = config.get("PATHS.output_image_format", "png")

B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")

# Установка API-ключа OpenAI из переменной окружения (секреты GitHub)
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    raise ValueError("API-ключ OpenAI не найден в переменной окружения OPENAI_API_KEY")

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
        handle_error(logger, "B2 Client Initialization Error", e)
        return None

def download_file_from_b2(client, remote_path, local_path):
    """Загружает файл из B2 (S3) в локальное хранилище."""
    try:
        logger.info(f"🔄 Загрузка файла из B2: {remote_path} -> {local_path}")
        ensure_directory_exists(os.path.dirname(local_path))
        client.download_file(B2_BUCKET_NAME, remote_path, local_path)
        logger.info(f"✅ Файл '{remote_path}' успешно загружен в {local_path}")
    except Exception as e:
        handle_error(logger, "B2 Download Error", e)

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
        handle_error(logger, "B2 Upload Error", e)

def update_config_public(client, folder):
    """Обновляет config_public.json: удаляет папку из списка 'empty'."""
    try:
        logger.info(f"🔄 Обновление config_public.json: удаление {folder} из списка 'empty'")
        download_file_from_b2(client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        if "empty" in config_public and folder in config_public["empty"]:
            config_public["empty"].remove(folder)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        logger.info("✅ config_public.json обновлён и загружен обратно в B2.")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, "Config Public Update Error", e)

def reset_processing_lock(client):
    """Сбрасывает флаг processing_lock в config_public.json."""
    try:
        logger.info("🔄 Сброс processing_lock в config_public.json")
        download_file_from_b2(client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        if config_public.get("processing_lock", False):
            config_public["processing_lock"] = False
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, B2_BUCKET_NAME, CONFIG_PUBLIC_REMOTE_PATH)
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, "Processing Lock Reset Error", e)

# === Функции генерации сценария и видео ===
def generate_script_and_frame(topic):
    """Генерирует сценарий и описание первого кадра для видео."""
    try:
        combined_prompt = USER_PROMPT_COMBINED.replace("{topic}", topic)
        logger.info(f"🔎 Отправка запроса для генерации сценария и описания: {combined_prompt[:100]}...")
        response = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": combined_prompt}],
            max_tokens=OPENAI_MAX_TOKENS,
            temperature=OPENAI_TEMPERATURE,
        )
        combined_response = response['choices'][0]['message']['content'].strip()
        if len(combined_response) < MIN_SCRIPT_LENGTH:
            logger.error(f"❌ Ответ слишком короткий: {len(combined_response)} символов")
            return None, None
        if "First Frame Description:" not in combined_response or "End of Description" not in combined_response:
            logger.error("❌ Маркеры кадра не найдены в ответе!")
            return None, None
        script_text = combined_response.split("First Frame Description:")[0].strip()
        first_frame_description = combined_response.split("First Frame Description:")[1].split("End of Description")[0].strip()
        logger.info(f"🎬 Сценарий: {script_text[:100]}...")
        logger.info(f"🖼️ Описание первого кадра: {first_frame_description[:100]}...")
        return script_text, first_frame_description
    except Exception as e:
        handle_error(logger, "Script and Frame Generation Error", e)
        return None, None

def generate_image_with_dalle(prompt, generation_id):
    response = openai.Image.create(
        prompt=prompt,
        n=NUM_IMAGES,
        size=IMAGE_SIZE_DALLE,
        model="dall-e-3",
        response_format="b64_json"
    )
    image_data = response["data"][0]["b64_json"]
    image_path = f"{generation_id}.png"
    with open(image_path, "wb") as f:
        f.write(base64.b64decode(image_data))
    with Image.open(image_path) as img:
        logger.info(f"✅ Сгенерировано изображение размером: {img.size}")
    return image_path

def resize_existing_image(image_path):
    """Изменяет размер изображения до 1280x768."""
    try:
        with Image.open(image_path) as img:
            resized = img.resize((1280, 768))
            resized.save(image_path)
        logger.info(f"✅ Размер изображения изменен: {image_path}")
        return True
    except Exception as e:
        handle_error(logger, "Image Resize Error", e)
        return False

def clean_script_text(text):
    """Очищает текст сценария для Runway."""
    cleaned = text.replace('\n', ' ').replace('\r', ' ')
    cleaned = ' '.join(cleaned.split())
    return cleaned[:980]

def generate_runway_video(image_path, script_text):
    """Генерирует видео через Runway ML."""
    api_key = os.getenv("RUNWAY_API_KEY")
    if not api_key:
        logger.error("❌ RUNWAY_API_KEY не найден в переменных окружения")
        return None
    try:
        with open(image_path, "rb") as img_file:
            base64_image = base64.b64encode(img_file.read()).decode("utf-8")
        client = RunwayML(api_key=api_key)
        task = client.image_to_video.create(
            model="gen3a_turbo",
            prompt_image=f"data:image/png;base64,{base64_image}",
            prompt_text=script_text,
            duration=10,
            ratio="1280:768"
        )
        logger.info(f"✅ Задача Runway создана. ID: {task.id}")
        while True:
            status = client.tasks.retrieve(task.id)
            if status.status == "SUCCEEDED":
                logger.info("✅ Видео успешно сгенерировано")
                return status.output[0]
            elif status.status == "FAILED":
                logger.error("❌ Ошибка генерации видео")
                return None
            time.sleep(5)
    except Exception as e:
        handle_error(logger, "Runway Video Generation Error", e)
        return None

def download_video(url, output_path):
    """Скачивает видео по URL."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"✅ Видео сохранено: {output_path}")
        return True
    except Exception as e:
        handle_error(logger, "Video Download Error", e)
        return False

# === Основная функция ===
def main():
    logger.info("🔄 Начало процесса генерации медиа...")
    try:
        # Чтение config_gen.json для получения ID генерации
        with open(CONFIG_GEN_PATH, 'r', encoding='utf-8') as file:
            config_gen = json.load(file)
        generation_id = config_gen["generation_id"].split('.')[0]  # Убираем расширение .json
        logger.info(f"📂 ID генерации: {generation_id}")

        # Создание клиента B2
        b2_client = get_b2_client()
        if not b2_client:
            raise Exception("Не удалось создать клиент B2")

        # Загрузка config_public.json
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)

        # Выбор целевой папки
        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]
            logger.info(f"🎯 Выбрана папка: {target_folder}")
        else:
            raise ValueError("Список 'empty' пуст или отсутствует")

        # Загрузка темы из generated_content.json
        with open(CONTENT_OUTPUT_PATH, 'r', encoding='utf-8') as f:
            generated_content = json.load(f)
        topic_data = generated_content.get("topic", "")
        if isinstance(topic_data, dict):
            topic = topic_data.get("topic", "")  # Извлекаем строку из {"topic": "..."}
        else:
            topic = topic_data or generated_content.get("content", "")
        if not topic:
            raise ValueError("Тема или текст поста пусты!")
        logger.info(f"📝 Тема: {topic[:100]}...")  # Срез применён к строке

        # Генерация сценария и описания первого кадра
        script_text, first_frame_description = generate_script_and_frame(topic)
        if not script_text or not first_frame_description:
            raise ValueError("Не удалось сгенерировать сценарий или описание")

        # Сохранение в JSON
        generated_content["script"] = script_text
        generated_content["first_frame_description"] = first_frame_description
        with open(CONTENT_OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(generated_content, f, ensure_ascii=False, indent=4)
        logger.info(f"✅ JSON сохранён: {CONTENT_OUTPUT_PATH}")

        # Генерация изображения
        image_path = generate_image_with_dalle(first_frame_description, generation_id)
        if not image_path:
            raise ValueError("Не удалось сгенерировать изображение")

        # Изменение размера изображения
        if not resize_existing_image(image_path):
            raise ValueError("Не удалось изменить размер изображения")

        # Генерация видео
        cleaned_script = clean_script_text(script_text)
        video_url = generate_runway_video(image_path, cleaned_script)
        video_path = None
        if video_url:
            video_path = f"{generation_id}.mp4"
            if not download_video(video_url, video_path):
                logger.warning("❌ Не удалось скачать видео")
        else:
            logger.warning("❌ Не удалось сгенерировать видео")

        # Загрузка файлов в B2 (только .png и .mp4, как в старом коде)
        upload_to_b2(b2_client, target_folder, image_path)
        if video_path and os.path.exists(video_path):
            upload_to_b2(b2_client, target_folder, video_path)

        # Обновление конфигурации
        update_config_public(b2_client, target_folder)
        reset_processing_lock(b2_client)

        # Запуск скрипта b2_storage_manager.py
        logger.info(f"🔄 Запуск скрипта: {B2_STORAGE_MANAGER_SCRIPT}")
        subprocess.run([sys.executable, B2_STORAGE_MANAGER_SCRIPT], check=True)

    except Exception as e:
        handle_error(logger, "Ошибка в процессе генерации", e)
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        sys.exit(1)