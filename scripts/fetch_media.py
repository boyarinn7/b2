import os
import requests
import json
import logging
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.config_manager import ConfigManager
from modules.api_clients import get_b2_client
from modules.utils import ensure_directory_exists  # Если есть, иначе os.makedirs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_media")

# Константы
# Установка путей
CONFIG_MIDJOURNEY_PATH = "config/config_midjourney.json"  # Путь в B2
CONFIG_MIDJOURNEY_LOCAL_PATH = "config/config_midjourney.json"  # Локальный путь
SCRIPTS_FOLDER = "scripts/"
B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
MIDJOURNEY_TASK_ENDPOINT = "https://api.piapi.ai/mj/v2/fetch"  # Реальный URL для PiAP

# Создание папки config/
ensure_directory_exists("config")


# Инициализация
config = ConfigManager()
b2_client = get_b2_client()

def load_config_midjourney(client):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        client.download_file(bucket_name, CONFIG_MIDJOURNEY_PATH, CONFIG_MIDJOURNEY_LOCAL_PATH)
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.warning(f"⚠️ Конфиг {CONFIG_MIDJOURNEY_PATH} не найден, создаём новый: {e}")
        return {"midjourney_task": None}

def save_config_midjourney(client, data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    try:
        with open(CONFIG_MIDJOURNEY_LOCAL_PATH, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
        client.upload_file(CONFIG_MIDJOURNEY_LOCAL_PATH, bucket_name, CONFIG_MIDJOURNEY_PATH)
        logger.info(f"✅ config_midjourney.json сохранён в B2: {json.dumps(data, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения config_midjourney.json: {e}")
        raise

def fetch_midjourney_result(task_id):
    headers = {"X-API-Key": MIDJOURNEY_API_KEY}
    endpoint = "https://api.piapi.ai/mj/v2/fetch"
    payload = {"task_id": task_id}
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        logger.info(f"ℹ️ Ответ от PiAPI: {response.status_code} - {response.text}")
        response.raise_for_status()
        data = response.json()
        if data["status"] in ["completed", "finished"]:
            task_result = data.get("task_result", {})
            if "image_url" in task_result and task_result["image_url"]:
                image_url = task_result["image_url"]
                logger.info(f"✅ Результат получен: {image_url}")
                return [image_url]
            elif "temporary_image_urls" in task_result and isinstance(task_result["temporary_image_urls"], list) and task_result["temporary_image_urls"]:
                image_urls = task_result["temporary_image_urls"]
                logger.info(f"✅ Получено {len(image_urls)} временных URL: {image_urls}")
                return image_urls
            else:
                logger.error(f"❌ Нет URL в task_result: {data}")
                return None
        elif data["status"] == "pending":
            logger.info("ℹ️ Задача ещё в процессе")
            return None
        else:
            logger.error(f"❌ Неожиданный статус задачи: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка запроса к PiAPI: {e}")
        return None
    except ValueError as e:
        logger.error(f"❌ Ошибка разбора JSON от PiAPI: {e}, ответ: {response.text}")
        return None
    
def main():
    logger.info("🔄 Начало проверки статуса задачи MidJourney...")
    try:
        config_midjourney = load_config_midjourney(b2_client)
        if "midjourney_task" not in config_midjourney or not config_midjourney["midjourney_task"]:
            logger.info("ℹ️ Нет задач для проверки в config_midjourney.json")
            return

        task_info = config_midjourney["midjourney_task"]
        task_id = task_info["task_id"]
        logger.info(f"ℹ️ Проверка статуса задачи: {task_id}")

        image_urls = fetch_midjourney_result(task_id)
        if image_urls:
            config_midjourney["midjourney_results"] = {"image_urls": image_urls}
            config_midjourney["midjourney_task"] = None
            save_config_midjourney(b2_client, config_midjourney)
            logger.info("✅ Задача завершена, midjourney_results обновлены, midjourney_task очищен.")
        else:
            logger.info("ℹ️ Задача ещё не завершена, результат не получен.")
            save_config_midjourney(b2_client, config_midjourney)

    except Exception as e:
        logger.error(f"❌ Ошибка в процессе проверки: {e}")
        raise

if __name__ == "__main__":
    main()