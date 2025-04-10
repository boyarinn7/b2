import os
import json
import sys
import subprocess
import openai
import requests
import base64
import time
import re
import random
import logging
from PIL import Image
from runwayml import RunwayML
from moviepy.editor import ImageClip, concatenate_videoclips
from modules.utils import ensure_directory_exists
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager
from modules.api_clients import get_b2_client
from io import BytesIO

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# === Инициализация конфигурации и логгера ===
config = ConfigManager()
logger = get_logger("generate_media")
logger.info("sys.path = " + str(sys.path))
os.makedirs("config", exist_ok=True)

# Определяем каталог скрипта и добавляем родительский каталог в sys.path
script_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, parent_dir)

# Пути к файлам
CONFIG_GEN_PATH = "config/config_gen.json"
CONFIG_GEN_LOCAL_PATH = "config/config_gen.json"
CONFIG_MIDJOURNEY_PATH = "config/config_midjourney.json"  # Было CONFIG_MIDJOURNEY_REMOTE_PATH
CONFIG_MIDJOURNEY_LOCAL_PATH = "config/config_midjourney.json"
CONTENT_OUTPUT_PATH = "generated_content.json"
SCRIPTS_FOLDER = "scripts/"
B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")
TARGET_FOLDER = "666/"
CONFIG_PUBLIC_PATH = "config/config_public.json"
CONFIG_PUBLIC_LOCAL_PATH = "config/config_public.json"

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

# Установка ключей API
openai.api_key = os.getenv("OPENAI_API_KEY")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")

if not openai.api_key:
    raise ValueError("API-ключ OpenAI не найден в переменной окружения OPENAI_API_KEY")
if MIDJOURNEY_ENABLED and not MIDJOURNEY_API_KEY:
    raise ValueError("API-ключ Midjourney не найден в переменной окружения MIDJOURNEY_API_KEY")

def split_midjourney_grid(url):
    try:
        # Скачиваем изображение
        response = requests.get(url, stream=True)
        response.raise_for_status()
        img = Image.open(BytesIO(response.content))

        # Предполагаем, что сетка 2x2 равных частей
        width, height = img.size
        w, h = width // 2, height // 2

        # Делим на 4 части
        images = [
            img.crop((0, 0, w, h)),  # Верхний левый
            img.crop((w, 0, width, h)),  # Верхний правый
            img.crop((0, h, w, height)),  # Нижний левый
            img.crop((w, h, width, height))  # Нижний правый
        ]

        # Сохраняем временные файлы и возвращаем пути
        temp_paths = []
        for i, sub_img in enumerate(images):
            temp_path = f"temp_midjourney_{i}.png"
            sub_img.save(temp_path)
            temp_paths.append(temp_path)

        logger.info("✅ Сетка MidJourney разделена на 4 части")
        return temp_paths
    except Exception as e:
        handle_error(logger, "Ошибка при разделении сетки MidJourney", e)
        return None

# === Вспомогательные функции ===
def check_midjourney_results(b2_client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    remote_config = "config/config_public.json"
    temp_path = "temp_config.json"
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(remote_config, temp_path)
        with open(temp_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        os.remove(temp_path)
        return config_data.get("midjourney_results", None)
    except Exception as e:
        logger.error(f"Ошибка при проверке midjourney_results: {e}")
        return None

def select_best_image(b2_client, image_urls, prompt):
    try:
        criteria = config.get("VISUAL_ANALYSIS.image_selection_criteria")
        selection_prompt = config.get("VISUAL_ANALYSIS.image_selection_prompt",
                                   "Select the best image based on the prompt '{prompt}' and these criteria: {criteria}")
        max_tokens = config.get("VISUAL_ANALYSIS.image_selection_max_tokens", 500)
        criteria_text = ", ".join([f"{c['name']} (weight: {c['weight']})" for c in criteria])
        full_prompt = selection_prompt.format(prompt=prompt, criteria=criteria_text)

        # Если один URL, предполагаем, что это сетка
        if len(image_urls) == 1:
            logger.info("Обнаружен один URL, разделяем сетку MidJourney")
            image_paths = split_midjourney_grid(image_urls[0])
            if not image_paths:
                logger.error("Не удалось разделить сетку, выбираем первый URL")
                return image_urls[0]
        else:
            image_paths = image_urls  # Если уже отдельные URL, используем их

        for attempt in range(MAX_ATTEMPTS):
            try:
                # Отправляем локальные файлы в OpenAI
                message_content = [{"type": "text", "text": full_prompt}]
                for path in image_paths:
                    with open(path, "rb") as img_file:
                        base64_image = base64.b64encode(img_file.read()).decode("utf-8")
                        message_content.append({
                            "type": "image_url",
                            "image_url": {"url": f"data:image/png;base64,{base64_image}"}
                        })

                gpt_response = openai.ChatCompletion.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": message_content}],
                    max_tokens=max_tokens
                )
                answer = gpt_response.choices[0].message.content
                logger.info(f"OpenAI выбор: {answer[:100]}...")
                best_index_match = re.search(r"Image (\d+)", answer)
                if best_index_match:
                    best_index = int(best_index_match.group(1)) - 1
                    if best_index in range(len(image_paths)):
                        best_url = image_urls[0] if len(image_urls) == 1 else image_paths[best_index]
                        # Очистка временных файлов
                        if len(image_urls) == 1:
                            for path in image_paths:
                                if path != image_paths[best_index]:
                                    os.remove(path)
                        return image_paths[best_index] if len(image_urls) == 1 else best_url
                logger.error(f"Неверный индекс в ответе OpenAI: {answer}, выбираем первое изображение")
                return image_paths[0] if len(image_urls) == 1 else image_urls[0]
            except openai.error.OpenAIError as e:
                logger.error(f"Ошибка OpenAI API (попытка {attempt + 1}/{MAX_ATTEMPTS}): {e}")
                if attempt < MAX_ATTEMPTS - 1:
                    time.sleep(5)
                else:
                    logger.error("Превышено количество попыток OpenAI, выбираем первое изображение")
                    return image_paths[0] if len(image_urls) == 1 else image_urls[0]
    except Exception as e:
        logger.error(f"Ошибка в select_best_image: {e}")
        return image_urls[0]

def download_file_from_b2(b2_client, remote_path, local_path):
    bucket_name = "boyarinnbotbucket"
    max_attempts = 3
    if not bucket_name:
        raise ValueError("❌ Имя бакета не задано")
    for attempt in range(max_attempts):
        try:
            logger.info(f"🔄 Попытка {attempt + 1}/{max_attempts} загрузки файла из B2: {remote_path} -> {local_path}")
            ensure_directory_exists(os.path.dirname(local_path))
            bucket = b2_client.get_bucket_by_name(bucket_name)
            response = bucket.list_file_names(start_file_name=remote_path, max_file_count=1)
            exists = any(file_info['fileName'] == remote_path for file_info in response.get('files', []))
            if not exists:
                logger.error(f"❌ Файл {remote_path} не найден в B2")
                raise FileNotFoundError(f"Файл {remote_path} отсутствует в бакете {bucket_name}")
            bucket.download_file_by_name(remote_path, local_path)
            logger.info(f"✅ Файл '{remote_path}' успешно загружен в {local_path}")
            return
        except FileNotFoundError as e:
            logger.error(f"❌ Ошибка на попытке {attempt + 1}: {str(e)}")
            handle_error(logger, "B2 Download Error", e)
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка на попытке {attempt + 1}: {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(2 ** attempt)
            else:
                handle_error(logger, "B2 Download Error", e)
                raise

def upload_to_b2(b2_client, folder, file_path):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        file_name = os.path.basename(file_path)
        if not folder.endswith('/'):
            folder += '/'
        s3_key = f"{folder}{file_name}"
        logger.info(f"🔄 Загрузка файла в B2: {file_path} -> {s3_key}")
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=file_path, file_name=s3_key)
        logger.info(f"✅ Файл '{file_name}' успешно загружен в B2: {s3_key}")
        os.remove(file_path)
        logger.info(f"🗑️ Локальный файл {file_path} удалён после загрузки.")
    except Exception as e:
        handle_error(logger, "B2 Upload Error", e)

def update_config_public(client, folder):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        logger.info(f"🔄 Обновление config_public.json: удаление {folder} из списка 'empty'")
        download_file_from_b2(client, CONFIG_PUBLIC_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        if "empty" in config_public and folder in config_public["empty"]:
            config_public["empty"].remove(folder)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        bucket = client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=CONFIG_PUBLIC_LOCAL_PATH, file_name=CONFIG_PUBLIC_PATH)
        logger.info("✅ config_public.json обновлён и загружен обратно в B2.")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, "Config Public Update Error", e)

def reset_processing_lock(client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        logger.info("🔄 Сброс processing_lock в config_public.json")
        download_file_from_b2(client, CONFIG_PUBLIC_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            config_public = json.load(file)
        logger.info(f"Перед сбросом, processing_lock: {config_public.get('processing_lock')}")
        if config_public.get("processing_lock", False):
            config_public["processing_lock"] = False
        else:
            logger.info("processing_lock уже сброшен.")
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(config_public, file, ensure_ascii=False, indent=4)
        bucket = client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=CONFIG_PUBLIC_LOCAL_PATH, file_name=CONFIG_PUBLIC_PATH)
        logger.info("✅ processing_lock успешно сброшен в config_public.json")
        download_file_from_b2(client, CONFIG_PUBLIC_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            new_config = json.load(file)
        logger.info(f"После сброса, config_public: {json.dumps(new_config, ensure_ascii=False)}")
        os.remove(CONFIG_PUBLIC_LOCAL_PATH)
    except Exception as e:
        handle_error(logger, "Processing Lock Reset Error", e)

def generate_script_and_frame(topic):
    creative_prompts = config.get("creative_prompts")
    if not creative_prompts or not isinstance(creative_prompts, list):
        logger.error(f"❌ Ошибка: 'creative_prompts' не найден или не является списком")
        raise ValueError("Список 'creative_prompts' не найден")
    for attempt in range(MAX_ATTEMPTS):
        try:
            selected_prompt = random.choice(creative_prompts)
            logger.info(f"✨ Выбран творческий подход: '{selected_prompt}'")
            combined_prompt = USER_PROMPT_COMBINED.replace("{topic}", topic).replace(
                "Затем выберите один творческий подход из 'creative_prompts' в конфиге",
                f"Затем используйте творческий подход: '{selected_prompt}'"
            )
            logger.info(f"🔎 Попытка {attempt + 1}/{MAX_ATTEMPTS}: Генерация сценария для '{topic[:100]}'...")
            response = openai.ChatCompletion.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": combined_prompt}],
                max_tokens=OPENAI_MAX_TOKENS,
                temperature=OPENAI_TEMPERATURE,
            )
            combined_response = response['choices'][0]['message']['content'].strip()
            if len(combined_response) < MIN_SCRIPT_LENGTH:
                logger.error(f"❌ Ответ слишком короткий: {len(combined_response)} символов")
                continue
            if "First Frame Description:" not in combined_response or "End of Description" not in combined_response:
                logger.error("❌ Маркеры кадра не найдены в ответе!")
                continue
            script_text = combined_response.split("First Frame Description:")[0].strip()
            first_frame_description = combined_response.split("First Frame Description:")[1].split("End of Description")[0].strip()
            logger.info(f"🎬 Сценарий: {script_text[:100]}...")
            logger.info(f"🖼️ Описание первого кадра: {first_frame_description[:100]}...")
            return script_text, first_frame_description
        except Exception as e:
            handle_error(logger, f"Script Generation Error (попытка {attempt + 1}/{MAX_ATTEMPTS})", e)
            if attempt == MAX_ATTEMPTS - 1:
                logger.error("❌ Превышено максимальное количество попыток генерации сценария.")
                return None, None
    return None, None

def load_config_public(b2_client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    local_path = CONFIG_PUBLIC_LOCAL_PATH
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        bucket.download_file_by_name(CONFIG_PUBLIC_PATH, local_path)
        with open(local_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки config_public.json: {e}")
        return {}

def save_config_public(b2_client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=CONFIG_PUBLIC_LOCAL_PATH, file_name=CONFIG_PUBLIC_PATH)
        logger.info("✅ config_public.json сохранён в B2.")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_public.json: {e}")

def load_config_midjourney(client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        bucket = client.get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(CONFIG_MIDJOURNEY_PATH, CONFIG_MIDJOURNEY_LOCAL_PATH)
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.warning(f"⚠️ Конфиг {CONFIG_MIDJOURNEY_PATH} не найден, создаём новый: {e}")
        return {"midjourney_task": None}

def save_config_midjourney(b2_client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=CONFIG_MIDJOURNEY_LOCAL_PATH, file_name=CONFIG_MIDJOURNEY_PATH)
        logger.info(f"✅ config_midjourney.json сохранён в B2: {json.dumps(data, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_midjourney.json: {e}")
        raise

# Обновленная функция generate_image_with_midjourney с расписанием
def generate_image_with_midjourney(prompt, generation_id, target_folder):
    b2_client = get_b2_client()
    try:
        headers = {
            "X-API-Key": MIDJOURNEY_API_KEY,
            "Content-Type": "application/json"
        }
        payload = {
            "model": "midjourney",
            "task_type": "imagine",
            "input": {
                "prompt": prompt,
                "aspect_ratio": "16:9",
                "process_mode": "v5"
            }
        }
        logger.info(f"Запрос к MidJourney: {prompt[:100]}...")
        response = requests.post(MIDJOURNEY_ENDPOINT, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        response_json = response.json()
        task_id = response_json.get("data", {}).get("task_id")
        if not task_id:
            raise ValueError(f"Ключ 'task_id' отсутствует в 'data' ответа: {response.text}")

        config_midjourney = load_config_midjourney(b2_client)
        config_midjourney["midjourney_task"] = {
            "task_id": task_id,
            "sent_at": int(time.time())
        }
        save_config_midjourney(b2_client, config_midjourney)
        logger.info(f"✅ Задача {task_id} отправлена в MidJourney и сохранена в config_midjourney.json")

        # Запуск b2_storage_manager.py
        if not os.path.isfile(B2_STORAGE_MANAGER_SCRIPT):
            raise FileNotFoundError(f"❌ Скрипт {B2_STORAGE_MANAGER_SCRIPT} не найден")
        logger.info(f"🔄 Запуск скрипта: {B2_STORAGE_MANAGER_SCRIPT}")
        subprocess.run([sys.executable, B2_STORAGE_MANAGER_SCRIPT], check=True)

    except (requests.exceptions.RequestException, ValueError) as e:
        logger.error(f"Ошибка MidJourney: {e}")
        if 'response' in locals():
            logger.error(f"Текст ответа: {response.text}")
        if DALLE_ENABLED:
            logger.warning("⚠️ MidJourney не ответил, переключаемся на DALL·E 3")
            image_path = generate_image_with_dalle(prompt, generation_id)
            if image_path:
                logger.info(f"✅ DALL·E 3 сгенерировал изображение: {image_path}")
                upload_to_b2(b2_client, target_folder, image_path)
                subprocess.run([sys.executable, B2_STORAGE_MANAGER_SCRIPT], check=True)
            else:
                raise Exception("Не удалось сгенерировать изображение через DALL·E 3")
        else:
            raise Exception("MidJourney не ответил, и DALL·E 3 отключен")

def remove_midjourney_results(b2_client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    remote_config = "config/config_public.json"
    temp_path = "temp_config.json"
    try:
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.download_file_by_name(remote_config, temp_path)
        with open(temp_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        if "midjourney_results" in config_data:
            logger.info("Удаляем ключ midjourney_results из config_public.")
            del config_data["midjourney_results"]
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=4)
            bucket.upload_local_file(local_file=temp_path, file_name=remote_config)
            logger.info("✅ Ключ midjourney_results удалён из config_public.")
        else:
            logger.info("Ключ midjourney_results отсутствует в config_public, ничего не удаляем.")
        os.remove(temp_path)
    except Exception as e:
        logger.error(f"Ошибка при удалении midjourney_results: {e}")

def generate_image_with_dalle(prompt, generation_id):
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

# Обновленная функция generate_image для передачи target_folder
def generate_image(prompt, generation_id, target_folder):
    if MIDJOURNEY_ENABLED:
        logger.info("🎨 Используем Midjourney для генерации изображения")
        generate_image_with_midjourney(prompt, generation_id, target_folder)
    elif DALLE_ENABLED:
        logger.info("🎨 Используем DALL·E 3 для генерации изображения")
        image_path = generate_image_with_dalle(prompt, generation_id)
        if image_path:
            upload_to_b2(get_b2_client(), target_folder, image_path)
    else:
        raise ValueError("Ни Midjourney, ни DALL·E 3 не включены в конфиге")

def resize_existing_image(image_path):
    try:
        with Image.open(image_path) as img:
            resized = img.resize((1280, 768))
            resized.save(image_path)
        logger.info(f"✅ Размер изображения изменен: {image_path}")
        return True
    except Exception as e:
        handle_error(logger, "Image Resize Error", e)
        return False

def load_content_from_b2(b2_client, generation_id):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    remote_path = f"666/{generation_id}.json"
    local_path = f"temp_{generation_id}.json"
    try:
        download_file_from_b2(b2_client, remote_path, local_path)
        with open(local_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
        os.remove(local_path)
        return content
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки 666/{generation_id}.json: {e}")
        sys.exit(1)

def clean_script_text(text):
    cleaned = text.replace('\n', ' ').replace('\r', ' ')
    cleaned = ' '.join(cleaned.split())
    return cleaned[:980]

def create_mock_video(image_path, output_path, duration=10):
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
    api_key = os.getenv("RUNWAY_API_KEY")
    if not api_key:
        logger.error("❌ RUNWAY_API_KEY не найден в переменных окружения")
        return None
    try:
        with open(image_path, "rb") as img_file:
            base64_image = base64.b64encode(img_file.read()).decode("utf-8")
        client = RunwayML(api_key=api_key)
        task = client.image_to_video.create(
            model="gen4",
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
    import argparse
    parser = argparse.ArgumentParser(description="Generate Media")
    parser.add_argument("--generation_id", type=str, required=True, help="ID for content generation")
    args = parser.parse_args()

    try:
        b2_client = get_b2_client()
        if not b2_client:
            raise Exception("Не удалось создать клиент B2")

        generation_id = args.generation_id
        logger.info(f"📂 ID генерации: {generation_id}")

        # Загрузка контента из B2 (666/<generation_id>.json)
        generated_content = load_content_from_b2(b2_client, generation_id)
        topic_data = generated_content.get("topic", "")
        if isinstance(topic_data, dict):
            topic = topic_data.get("full_topic", "")
        else:
            topic = topic_data or generated_content.get("content", "")
        if not topic:
            raise ValueError("Тема или текст поста пусты!")
        script_text = generated_content.get("script", "")
        first_frame_description = generated_content.get("first_frame_description", "")
        if not script_text or not first_frame_description:
            raise ValueError(f"Сценарий или описание первого кадра отсутствуют в 666/{generation_id}.json")

        # Загрузка config_midjourney.json из B2
        download_file_from_b2(b2_client, CONFIG_MIDJOURNEY_PATH, CONFIG_MIDJOURNEY_LOCAL_PATH)
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'r', encoding='utf-8') as f:
            config_midjourney = json.load(f)
        midjourney_results = config_midjourney.get("midjourney_results", {})

        # Используем фиксированный TARGET_FOLDER
        target_folder = TARGET_FOLDER

        # Сценарий 1: Есть midjourney_results
        if midjourney_results and "image_urls" in midjourney_results:
            image_urls = midjourney_results.get("image_urls", [])
            if not image_urls or not all(isinstance(url, str) and url.startswith("http") for url in image_urls):
                logger.warning("⚠️ Некорректные URL в midjourney_results, очищаем ключ")
                config_midjourney["midjourney_results"] = {}
                with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'w', encoding='utf-8') as f:
                    json.dump(config_midjourney, f, ensure_ascii=False, indent=4)
                upload_to_b2(b2_client, os.path.dirname(CONFIG_MIDJOURNEY_PATH), CONFIG_MIDJOURNEY_LOCAL_PATH)
            else:
                import shutil

                # Выбор лучшего изображения
                best_image_path = select_best_image(b2_client, image_urls, first_frame_description)
                image_path = f"{generation_id}.{OUTPUT_IMAGE_FORMAT}"

                if best_image_path.startswith("http"):
                    response = requests.get(best_image_path, stream=True)
                    response.raise_for_status()
                    with open(image_path, "wb") as f:
                        f.write(response.content)
                else:
                    shutil.move(best_image_path, image_path)

                logger.info(f"✅ Лучшее изображение сохранено: {image_path}")

                # Удаление временных файлов
                for i in range(4):
                    temp_path = f"temp_midjourney_{i}.png"
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        logger.info(f"🗑️ Удалён временный файл: {temp_path}")

                # Изменение размера изображения
                if not resize_existing_image(image_path):
                    raise ValueError("Не удалось изменить размер изображения")

                # Генерация видео
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

                # Загрузка в B2
                upload_to_b2(b2_client, target_folder, image_path)
                if video_path and os.path.exists(video_path):
                    upload_to_b2(b2_client, target_folder, video_path)

                # Очистка midjourney_results
                config_midjourney["midjourney_results"] = {}
                with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'w', encoding='utf-8') as f:
                    json.dump(config_midjourney, f, ensure_ascii=False, indent=4)
                upload_to_b2(b2_client, os.path.dirname(CONFIG_MIDJOURNEY_PATH), CONFIG_MIDJOURNEY_LOCAL_PATH)

                # Запуск b2_storage_manager.py
                if not os.path.isfile(B2_STORAGE_MANAGER_SCRIPT):
                    raise FileNotFoundError(f"❌ Скрипт {B2_STORAGE_MANAGER_SCRIPT} не найден")
                logger.info(f"🔄 Запуск скрипта: {B2_STORAGE_MANAGER_SCRIPT}")
                subprocess.run([sys.executable, B2_STORAGE_MANAGER_SCRIPT], check=True)

        # Сценарий 2: Нет midjourney_results
        else:
            # Отправка задачи MidJourney
            generate_image(first_frame_description, generation_id, target_folder)
            # Запуск b2_storage_manager.py уже внутри generate_image

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