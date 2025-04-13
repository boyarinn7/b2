import os
import json
import boto3
import sys
import subprocess
import openai
import requests
import base64
import time
import re

from PIL import Image
from runwayml import RunwayML
from moviepy.editor import ImageClip, concatenate_videoclips
from modules.utils import ensure_directory_exists
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

# === Инициализация конфигурации и логгера ===
config = ConfigManager()
logger = get_logger("generate_media")

# === Загрузка всех настроек из конфига ===
B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', "boyarinnbotbucket")
CONFIG_PUBLIC_REMOTE_PATH = "config/config_public.json"  # Путь в B2
CONFIG_PUBLIC_LOCAL_PATH = "config/config_public.json"  # Фиксированный локальный путь
CONTENT_OUTPUT_PATH = config.get("FILE_PATHS.content_output_path", "generated_content.json")
SCRIPTS_FOLDER = os.path.abspath(config.get("FILE_PATHS.scripts_folder", "scripts"))

# Настройки генерации из конфига
USER_PROMPT_COMBINED = config.get("PROMPTS.user_prompt_combined")
OPENAI_MODEL = config.get("OPENAI_SETTINGS.model", "gpt-4o")
OPENAI_MAX_TOKENS = config.get("OPENAI_SETTINGS.max_tokens", 1000)
OPENAI_TEMPERATURE = config.get("OPENAI_SETTINGS.temperature", 0.7)
MIN_SCRIPT_LENGTH = config.get("VISUAL_ANALYSIS.min_script_length", 200)
IMAGE_SIZE = config.get("IMAGE_GENERATION.image_size", "1792x1024")
NUM_IMAGES = config.get("IMAGE_GENERATION.num_images", 1)
MIDJOURNEY_ENABLED = config.get("IMAGE_GENERATION.midjourney_enabled", True)
DALLE_ENABLED = config.get("IMAGE_GENERATION.dalle_enabled", True)
OUTPUT_IMAGE_FORMAT = config.get("PATHS.output_image_format", "png")
MIDJOURNEY_ENDPOINT = config.get("API_KEYS.midjourney.endpoint")
MIDJOURNEY_TASK_ENDPOINT = config.get("API_KEYS.midjourney.task_endpoint")
IMAGE_SELECTION_CRITERIA = config.get("VISUAL_ANALYSIS.image_selection_criteria", [])
MAX_ATTEMPTS = config.get("GENERATE.max_attempts", 3)

B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")

# Установка ключей API из переменных окружения
openai.api_key = os.getenv("OPENAI_API_KEY")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
if not openai.api_key:
    raise ValueError("API-ключ OpenAI не найден в переменной окружения OPENAI_API_KEY")
if MIDJOURNEY_ENABLED and not MIDJOURNEY_API_KEY:
    raise ValueError("API-ключ Midjourney не найден в переменной окружения MIDJOURNEY_API_KEY")


def check_midjourney_results(b2_client):
    try:
        config_obj = b2_client.get_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_REMOTE_PATH)
        config_data = json.loads(config_obj['Body'].read().decode('utf-8'))
        return config_data.get("midjourney_results", None)
    except Exception as e:
        logger.error(f"Ошибка при проверке midjourney_results: {e}")
        return None


def select_best_image(b2_client, image_urls, prompt):
    try:
        criteria = config.get("VISUAL_ANALYSIS.image_selection_criteria")
        selection_prompt = config.get("VISUAL_ANALYSIS.image_selection_prompt")
        max_tokens = config.get("VISUAL_ANALYSIS.image_selection_max_tokens", 500)
        criteria_text = ", ".join([f"{c['name']} (weight: {c['weight']})" for c in criteria])
        full_prompt = selection_prompt.format(prompt=prompt, criteria=criteria_text)

        for attempt in range(MAX_ATTEMPTS):
            try:
                gpt_response = openai.ChatCompletion.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": full_prompt},
                                *[{"type": "image_url", "image_url": {"url": url}} for url in image_urls]
                            ]
                        }
                    ],
                    max_tokens=max_tokens
                )
                answer = gpt_response.choices[0].message.content
                logger.info(f"OpenAI выбор: {answer[:100]}...")
                best_index_match = re.search(r"Image (\d+)", answer)
                if best_index_match:
                    best_index = int(best_index_match.group(1)) - 1
                    if 0 <= best_index < len(image_urls):
                        return image_urls[best_index]
                logger.error(f"Неверный индекс в ответе OpenAI: {answer}, выбираем первое изображение")
                return image_urls[0]
            except openai.error.OpenAIError as e:
                logger.error(f"Ошибка OpenAI API (попытка {attempt + 1}/{MAX_ATTEMPTS}): {e}")
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(5)
                else:
                    logger.error("Превышено количество попыток OpenAI, выбираем первое изображение")
                    return image_urls[0]
    except Exception as e:
        logger.error(f"Ошибка в select_best_image: {e}")
        return image_urls[0]


def get_b2_client():
    """Создаёт клиент boto3 для B2."""
    try:
        return boto3.client(
            's3',
            endpoint_url=os.getenv("B2_ENDPOINT"),
            aws_access_key_id=os.getenv("B2_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("B2_SECRET_KEY")
        )
    except Exception as e:
        logger.error(f"❌ Ошибка создания клиента B2: {e}")
        raise


def find_file(generation_id):
    """Ищет файл в папках 666/ или 555/."""
    s3 = get_b2_client()
    bucket_name = config.get("API_KEYS.b2.bucket_name", B2_BUCKET_NAME)
    folders = ["666/", "555/"]
    for folder in folders:
        try:
            s3.head_object(Bucket=bucket_name, Key=f"{folder}{generation_id}.json")
            return folder
        except:
            continue
    logger.error(f"❌ Файл {generation_id}.json не найден в 666/ или 555/")
    return None


def download_file_from_b2(client, remote_path, local_path):
    """Загружает файл из B2 в локальное хранилище."""
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


def generate_image_with_midjourney(prompt, generation_id):
    for attempt in range(MAX_ATTEMPTS):
        try:
            headers = {"X-API-KEY": MIDJOURNEY_API_KEY}
            payload = {
                "prompt": prompt,
                "aspect_ratio": "16:9",
                "process_mode": "fast",
                "skip_prompt_check": False
            }
            logger.info(f"Попытка {attempt + 1}/{MAX_ATTEMPTS}: Запрос к Midjourney: {prompt[:100]}...")
            response = requests.post(MIDJOURNEY_ENDPOINT, json=payload, headers=headers)
            response.raise_for_status()
            task_id = response.json()["task_id"]
            logger.info(f"Задача {task_id} отправлена в Midjourney, завершение работы")
            sys.exit(0)
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка Midjourney (попытка {attempt + 1}/{MAX_ATTEMPTS}): {e}")
            if attempt < MAX_ATTEMPTS - 1:
                time.sleep(5)
            else:
                logger.error("Превышено количество попыток Midjourney")
                return None
    return None


def remove_midjourney_results(b2_client):
    try:
        config_obj = b2_client.get_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_REMOTE_PATH)
        config_data = json.loads(config_obj['Body'].read().decode('utf-8'))
        if "midjourney_results" in config_data:
            del config_data["midjourney_results"]
            updated_config = json.dumps(config_data, ensure_ascii=False).encode('utf-8')
            b2_client.put_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_REMOTE_PATH, Body=updated_config)
            logger.info("Ключ midjourney_results удалён из config_public.json")
    except Exception as e:
        logger.error(f"Ошибка при удалении midjourney_results: {e}")


def generate_image_with_dalle(prompt, generation_id):
    """Генерирует изображение через DALL·E 3 с повторными попытками."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            logger.info(f"🔄 Попытка {attempt + 1}/{MAX_ATTEMPTS}: Генерация через DALL·E 3: {prompt[:100]}...")
            response = openai.Image.create(
                prompt=prompt,
                n=NUM_IMAGES,
                size=IMAGE_SIZE,
                model="dall-e-3",
                response_format="b64_json"
            )
            image_data = response["data"][0]["b64_json"]
            image_path = f"{generation_id}.{OUTPUT_IMAGE_FORMAT}"
            with open(image_path, "wb") as f:
                f.write(base64.b64decode(image_data))
            with Image.open(image_path) as img:
                logger.info(f"✅ Сгенерировано изображение размером: {img.size}")
            return image_path
        except Exception as e:
            handle_error(logger, f"DALL·E Image Generation Error (попытка {attempt + 1}/{MAX_ATTEMPTS})", e)
            if attempt == MAX_ATTEMPTS - 1:
                logger.error("❌ Превышено максимальное количество попыток генерации DALL·E.")
                return None
    return None


def generate_image(prompt, generation_id):
    if MIDJOURNEY_ENABLED:
        logger.info("🎨 Используем Midjourney для генерации изображения")
        return generate_image_with_midjourney(prompt, generation_id)
    elif DALLE_ENABLED:
        logger.info("🎨 Используем DALL·E 3 для генерации изображения")
        return generate_image_with_dalle(prompt, generation_id)
    else:
        raise ValueError("Ни Midjourney, ни DALL·E 3 не включены в конфиге")


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


def create_mock_video(image_path, output_path, duration=10):
    """Создает имитацию видео из изображения."""
    try:
        logger.info(f"🎥 Создание имитации видео из {image_path} длительностью {duration} сек")
        clip = ImageClip(image_path, duration=duration)
        clip.write_videofile(
            output_path,
            codec="libx264",
            fps=24,
            audio=False,
            logger=None
        )
        logger.info(f"✅ Имитация видео создана: {output_path}")
        return output_path
    except Exception as e:
        handle_error(logger, "Mock Video Creation Error", e)
        return None


def generate_runway_video(image_path, script_text):
    """Генерирует видео через Runway ML или создаёт имитацию при недостатке кредитов."""
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
                return status.output[0]  # URL настоящего видео
            elif status.status == "FAILED":
                logger.error("❌ Ошибка генерации видео в Runway")
                return None
            time.sleep(5)
    except Exception as e:
        error_msg = str(e)
        if "credits" in error_msg.lower():
            logger.warning(f"⚠️ Недостаток кредитов в Runway: {error_msg}")
            video_path = image_path.replace(".png", ".mp4")
            mock_video = create_mock_video(image_path, video_path)
            if mock_video:
                logger.info(f"🔄 Замена: сгенерирована имитация видео: {mock_video}")
                return mock_video
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


def main():
    logger.info("🔄 Начало процесса генерации медиа...")
    try:
        if len(sys.argv) < 2:
            logger.error("❌ Не указан generation_id")
            raise ValueError("Требуется generation_id как аргумент командной строки")
        generation_id = sys.argv[1].replace(".json", "")
        logger.info(f"📂 ID генерации: {generation_id}")

        b2_client = get_b2_client()
        download_file_from_b2(b2_client, CONFIG_PUBLIC_REMOTE_PATH, CONFIG_PUBLIC_LOCAL_PATH)

        with open(CONTENT_OUTPUT_PATH, 'r', encoding='utf-8') as f:
            content_dict = json.load(f)
        topic = content_dict.get("topic", "")
        script_text = content_dict.get("script", "")
        first_frame_description = content_dict.get("first_frame_description", "")
        if not script_text or not first_frame_description:
            logger.error("Сценарий или описание отсутствуют в generated_content.json")
            sys.exit(1)

        logger.info(f"📝 Тема: {topic[:100]}...")
        logger.info(f"📜 Сценарий: {script_text[:100]}...")
        logger.info(f"🖼️ Описание первого кадра: {first_frame_description[:100]}...")

        midjourney_results = check_midjourney_results(b2_client)
        if midjourney_results:
            image_urls = midjourney_results["image_urls"]
            best_image_url = select_best_image(b2_client, image_urls, first_frame_description)
            image_path = f"{generation_id}.{OUTPUT_IMAGE_FORMAT}"
            response = requests.get(best_image_url, stream=True)
            response.raise_for_status()
            with open(image_path, "wb") as f:
                f.write(response.content)
            logger.info(f"✅ Лучшее изображение сохранено: {image_path}")
            remove_midjourney_results(b2_client)
        else:
            image_path = generate_image(first_frame_description, generation_id)

        if not image_path or not resize_existing_image(image_path):
            raise ValueError("Не удалось сгенерировать или изменить размер изображения")

        cleaned_script = clean_script_text(script_text)
        video_result = generate_runway_video(image_path, cleaned_script)
        video_path = None
        if video_result:
            if video_result.startswith("http"):
                video_path = f"{generation_id}.mp4"
                if not download_video(video_result, video_path):
                    logger.warning("❌ Не удалось скачать видео")
            else:
                video_path = video_result
                logger.info(f"🔄 Используем имитацию видео: {video_path}")

        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        if "empty" in config_public and config_public["empty"]:
            target_folder = config_public["empty"][0]
            logger.info(f"🎯 Выбрана папка: {target_folder}")
        else:
            raise ValueError("Список 'empty' пуст или отсутствует")

        upload_to_b2(b2_client, target_folder, image_path)
        if video_path and os.path.exists(video_path):
            upload_to_b2(b2_client, target_folder, video_path)

        update_config_public(b2_client, target_folder)
        reset_processing_lock(b2_client)

        logger.info(f"🔄 Запуск скрипта: {B2_STORAGE_MANAGER_SCRIPT}")
        subprocess.run([sys.executable, B2_STORAGE_MANAGER_SCRIPT], check=True)

        logger.info("✅ Генерация медиа завершена")
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