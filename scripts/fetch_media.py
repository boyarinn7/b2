import os
import time
import requests
import json
import subprocess
import logging
import sys

# Добавляем корень проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.config_manager import ConfigManager
from modules.api_clients import get_b2_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_media")

# Константы
CONFIG_MIDJOURNEY_LOCAL_PATH = "config_midjourney.json"
CONFIG_MIDJOURNEY_REMOTE_PATH = "config/config_midjourney.json"
CONFIG_PUBLIC_LOCAL_PATH = "config_public.json"  # Добавлено для локального пути
CONFIG_PUBLIC_PATH = "config/config_public.json"
CONFIG_FETCH_PATH = "config/config_fetch.json"
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")

# Инициализация
config = ConfigManager()
b2_client = get_b2_client()

# Функции для работы с конфигами
def load_config(file_path):
    try:
        config_obj = b2_client.get_object(Bucket=config.get("API_KEYS.b2.bucket_name"), Key=file_path)
        return json.loads(config_obj['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфига {file_path}: {e}")
        if "NoSuchKey" in str(e):
            logger.warning(f"⚠️ Конфиг {file_path} не найден, создаём новый.")
            return {"done": False, "fetch_attempts": 0} if file_path == CONFIG_FETCH_PATH else {}
        return {}

def save_config(file_path, config_data):
    json_str = json.dumps(config_data, ensure_ascii=False, indent=4)
    b2_client.put_object(
        Bucket=config.get("API_KEYS.b2.bucket_name"),
        Key=file_path,
        Body=json_str.encode('utf-8')
    )
    logger.info(f"✅ Конфигурация сохранена в {file_path}")

def load_config_midjourney(client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        client.download_file(bucket_name, CONFIG_MIDJOURNEY_REMOTE_PATH, CONFIG_MIDJOURNEY_LOCAL_PATH)
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.warning(f"⚠️ Конфиг {CONFIG_MIDJOURNEY_REMOTE_PATH} не найден, создаём новый: {e}")
        return {"midjourney_task": None}

def save_config_midjourney(client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_MIDJOURNEY_LOCAL_PATH, bucket_name, CONFIG_MIDJOURNEY_REMOTE_PATH)
        logger.info(f"✅ config_midjourney.json сохранён в B2: {json.dumps(data, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_midjourney.json: {e}")
        raise

def load_config_public(client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        client.download_file(bucket_name, CONFIG_PUBLIC_PATH, CONFIG_PUBLIC_LOCAL_PATH)
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.warning(f"⚠️ Конфиг {CONFIG_PUBLIC_PATH} не найден, создаём новый: {e}")
        return {}

def save_config_public(client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        with open(CONFIG_PUBLIC_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_PUBLIC_LOCAL_PATH, bucket_name, CONFIG_PUBLIC_PATH)
        logger.info(f"✅ config_public.json сохранён в B2")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_public.json: {e}")
        raise

# Функции для получения результатов
def fetch_midjourney_result(task_id):
    headers = {"X-API-Key": MIDJOURNEY_API_KEY}
    response = requests.get(f"{config.get('API_KEYS.midjourney.task_endpoint')}/{task_id}", headers=headers, timeout=30)
    data = response.json()
    if data["code"] == 200 and data["data"]["status"] == "completed":
        image_url = data["data"]["output"]["image_url"]
        logger.info(f"✅ Результат получен: {image_url}")
        return image_url
    elif data["data"]["status"] == "pending":
        logger.info("ℹ️ Задача ещё в процессе")
        return None
    else:
        raise Exception(f"Ошибка получения результата: {data}")

def fetch_dalle_result(prompt, generation_id):
    # Заглушка для DALL·E 3, реализация будет в generate_media.py
    logger.info(f"ℹ️ Переключаемся на DALL·E 3 для {generation_id} с промптом: {prompt[:50]}...")
    return None  # Пока возвращаем None, реализацию добавим позже

def main():
    config_fetch = load_config(CONFIG_FETCH_PATH)
    if config_fetch.get("done"):
        logger.info("ℹ️ Задача уже завершена (done: true), завершаем работу")
        return

    config_midjourney = load_config_midjourney(b2_client)
    if "midjourney_task" not in config_midjourney or not config_midjourney["midjourney_task"]:
        logger.info("ℹ️ Нет задач для проверки в config_midjourney.json")
        sys.exit(0)
    task_info = config_midjourney["midjourney_task"]
    task_id = task_info["task_id"]
    sent_at = task_info["sent_at"]
    current_time = int(time.time())
    elapsed_time = current_time - sent_at
    fetch_attempts = config_fetch.get("fetch_attempts", 0)

    logger.info(f"ℹ️ Прошло {elapsed_time} секунд с момента отправки задачи {task_id}, попытка {fetch_attempts + 1}")

    check_intervals = [60, 120, 180, 300, 600]  # 15, 20, 30, 60, 300 минут
    if elapsed_time < check_intervals[0]:
        logger.info(f"ℹ️ Слишком рано ({elapsed_time} сек < 15 мин), ждём следующего запуска")
        return

    config_public = load_config_public(b2_client)  # Загружаем config_public для записи результатов

    if elapsed_time >= check_intervals[min(fetch_attempts, len(check_intervals) - 1)]:
        image_url = fetch_midjourney_result(task_id)
        if image_url:
            config_public["midjourney_results"] = {
                "task_id": task_id,
                "image_urls": [image_url]
            }
            save_config_public(b2_client, config_public)
            config_fetch["done"] = True
            config_fetch["fetch_attempts"] = 0
            save_config(CONFIG_FETCH_PATH, config_fetch)
            config_midjourney["midjourney_task"] = None  # Очищаем задачу
            save_config_midjourney(b2_client, config_midjourney)
            logger.info("✅ Задача завершена, config_midjourney.json очищен.")
            logger.info("🔄 Запускаем b2_storage_manager.py для обработки результата")
            subprocess.run(["python", "scripts/b2_storage_manager.py"])
        else:
            fetch_attempts += 1
            config_fetch["fetch_attempts"] = fetch_attempts

            if fetch_attempts >= 5:  # После 300 минут
                logger.warning("⚠️ MidJourney не ответил за 5 часов")
                if config.get("IMAGE_GENERATION.dalle_enabled", False):
                    logger.info("ℹ️ Пробуем DALL·E 3 как запасной вариант")
                    with open("config/config_gen.json", "r") as f:
                        gen_config = json.load(f)
                    generation_id = gen_config["generation_id"].split('.')[0]
                    with open("generated_content.json", "r") as f:
                        content = json.load(f)
                    prompt = content.get("first_frame_description", "Fallback prompt")
                    dalle_url = fetch_dalle_result(prompt, generation_id)
                    if dalle_url:
                        config_public["midjourney_results"] = {"task_id": task_id, "image_urls": [dalle_url]}
                        save_config_public(b2_client, config_public)
                        config_fetch["done"] = True
                    else:
                        logger.error("❌ DALL·E 3 тоже не сработал, сбрасываем задачу")
                        config_fetch["done"] = False
                    config_fetch["fetch_attempts"] = 0
                    config_midjourney["midjourney_task"] = None  # Очищаем задачу
                    save_config_midjourney(b2_client, config_midjourney)
                else:
                    logger.info("ℹ️ DALL·E 3 отключён, сбрасываем задачу")
                    config_fetch["done"] = False
                    config_fetch["fetch_attempts"] = 0
                    config_midjourney["midjourney_task"] = None  # Очищаем задачу
                    save_config_midjourney(b2_client, config_midjourney)
            save_config(CONFIG_FETCH_PATH, config_fetch)
            logger.info(f"ℹ️ Попытка {fetch_attempts}/5 завершена")

if __name__ == "__main__":
    main()