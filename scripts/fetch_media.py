import os
import requests
import json
import logging
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from modules.config_manager import ConfigManager
from modules.api_clients import get_b2_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_media")

# Константы
CONFIG_MIDJOURNEY_LOCAL_PATH = "config_midjourney.json"
CONFIG_MIDJOURNEY_REMOTE_PATH = "config/config_midjourney.json"
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
MIDJOURNEY_TASK_ENDPOINT = "https://api.piapi.ai/api/v1/task/"  # Предполагаемый эндпоинт, уточнить в конфиге

# Инициализация
config = ConfigManager()
b2_client = get_b2_client()

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

def fetch_midjourney_result(task_id):
    headers = {"X-API-Key": MIDJOURNEY_API_KEY}
    endpoint = f"{MIDJOURNEY_TASK_ENDPOINT}{task_id}"
    try:
        response = requests.get(endpoint, headers=headers, timeout=30)
        logger.info(f"ℹ️ Ответ от MidJourney: {response.status_code} - {response.text[:200]}")
        response.raise_for_status()
        data = response.json()
        if data["code"] == 200 and data["data"]["status"] == "completed":
            output = data["data"]["output"]
            if "image_urls" in output and isinstance(output["image_urls"], list):
                image_urls = output["image_urls"]
                logger.info(f"✅ Получено {len(image_urls)} URL: {image_urls}")
                return image_urls  # Возвращаем весь список
            elif "image_url" in output:
                image_url = output["image_url"]
                logger.info(f"✅ Результат получен: {image_url}")
                return [image_url]  # Преобразуем в список
            else:
                logger.error(f"❌ Нет URL в output: {output}")
                return None
        elif data["data"]["status"] == "pending":
            logger.info("ℹ️ Задача ещё в процессе")
            return None
        else:
            logger.error(f"❌ Неожиданный статус задачи: {data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка запроса к MidJourney: {e}")
        return None
    except ValueError as e:
        logger.error(f"❌ Ошибка разбора JSON от MidJourney: {e}, ответ: {response.text}")
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