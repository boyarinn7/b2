import os
import time
import requests
import json
import boto3
import subprocess
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_media")

B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")

b2_client = boto3.client(
    "s3",
    endpoint_url="https://s3.us-east-005.backblazeb2.com",
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

CONFIG_PUBLIC_PATH = "config/config_public.json"
CONFIG_FETCH_PATH = "config/config_fetch.json"

def load_config(file_path):
    try:
        config_obj = b2_client.get_object(Bucket=B2_BUCKET_NAME, Key=file_path)
        return json.loads(config_obj['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.warning(f"⚠️ Конфиг {file_path} не найден, создаём новый.")
            return {"done": False, "fetch_attempts": 0} if file_path == CONFIG_FETCH_PATH else {}
        logger.error(f"❌ Ошибка загрузки конфига {file_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при загрузке {file_path}: {e}")
        return {}

def save_config(file_path, config_data):
    json_str = json.dumps(config_data, ensure_ascii=False, indent=4)
    b2_client.put_object(
        Bucket=B2_BUCKET_NAME,
        Key=file_path,
        Body=json_str.encode('utf-8')
    )
    logger.info(f"✅ Конфигурация сохранена в {file_path}")

def fetch_midjourney_result(task_id):
    headers = {"X-API-Key": MIDJOURNEY_API_KEY}
    response = requests.get(f"https://api.piapi.ai/api/v1/task/{task_id}", headers=headers)
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

def main():
    config_fetch = load_config(CONFIG_FETCH_PATH)
    if config_fetch.get("done"):
        logger.info("ℹ️ Задача уже завершена (done: true), завершаем работу")
        return

    config_public = load_config(CONFIG_PUBLIC_PATH)
    if "midjourney_task" not in config_public:
        logger.info("ℹ️ Нет задач для проверки в config_public.json")
        return

    task_id = config_public["midjourney_task"]["task_id"]
    sent_at = config_public["midjourney_task"]["sent_at"]
    current_time = int(time.time())
    elapsed_time = current_time - sent_at

    logger.info(f"ℹ️ Прошло {elapsed_time} секунд с момента отправки задачи {task_id}")

    if elapsed_time >= 900:  # 15 минут
        image_url = fetch_midjourney_result(task_id)
        if image_url:
            config_public["midjourney_results"] = {
                "task_id": task_id,
                "image_urls": [image_url]
            }
            del config_public["midjourney_task"]
            save_config(CONFIG_PUBLIC_PATH, config_public)
            config_fetch["done"] = True
            config_fetch["fetch_attempts"] = 0
            save_config(CONFIG_FETCH_PATH, config_fetch)
            logger.info("🔄 Запускаем b2_storage_manager.py для обработки результата")
            subprocess.run(["python", "scripts/b2_storage_manager.py"])
        else:
            config_fetch["fetch_attempts"] = config_fetch.get("fetch_attempts", 0) + 1
            if config_fetch["fetch_attempts"] >= 3:
                logger.error("❌ Эй, MidJourney, где мой результат, капиталисты ленивые?!")
                config_fetch["done"] = False
                config_fetch["fetch_attempts"] = 0
                save_config(CONFIG_FETCH_PATH, config_fetch)
                logger.info("ℹ️ Сбрасываем fetch_attempts, оставляем midjourney_task для следующего цикла")
            else:
                save_config(CONFIG_FETCH_PATH, config_fetch)
                logger.info(f"ℹ️ Попытка {config_fetch['fetch_attempts']}/3, ждём следующего запуска")
    else:
        logger.info(f"ℹ️ Слишком рано ({elapsed_time} сек < 15 мин), ждём следующего запуска")

if __name__ == "__main__":
    main()