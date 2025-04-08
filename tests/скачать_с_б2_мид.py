import requests
import logging
import os
import sys

# Добавляем корень проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# API-ключ из переменной окружения или вручную
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY", "e053f7ff89ce552740e9c2256a34b76cb87362ff71277806be512c606423a088")


def test_fetch_v2(task_id):
    """Тестирует mj/v2/fetch с POST"""
    headers = {"X-API-Key": MIDJOURNEY_API_KEY}
    endpoint = "https://api.piapi.ai/mj/v2/fetch"
    payload = {"task_id": task_id}
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=30)
        logger.info(f"ℹ️ Ответ от mj/v2/fetch: {response.status_code} - {response.text}")
        return response.json()
    except Exception as e:
        logger.error(f"❌ Ошибка mj/v2/fetch: {e}")
        return None


def test_task_v1(task_id):
    """Тестирует /v1/task/<task_id> с GET"""
    headers = {"X-API-Key": MIDJOURNEY_API_KEY}
    endpoint = f"https://api.piapi.ai/v1/task/{task_id}"
    try:
        response = requests.get(endpoint, headers=headers, timeout=30)
        logger.info(f"ℹ️ Ответ от v1/task: {response.status_code} - {response.text}")
        return response.json()
    except Exception as e:
        logger.error(f"❌ Ошибка v1/task: {e}")
        return None


def main():
    # Твой task_id для теста
    task_id = "b8e497a4-35aa-4365-b0f1-2fa6543ec8b0"

    logger.info("🚀 Тестирование начато")

    # Тест 1: mj/v2/fetch
    logger.info("🔍 Тест 1: Проверка mj/v2/fetch")
    result_v2 = test_fetch_v2(task_id)
    if result_v2 and "output" in result_v2:
        logger.info(f"✅ URL изображения: {result_v2['output'].get('image_url')}")

    # Тест 2: /v1/task/<task_id>
    logger.info("🔍 Тест 2: Проверка v1/task")
    result_v1 = test_task_v1(task_id)
    if result_v1 and "data" in result_v1 and "output" in result_v1["data"]:
        logger.info(f"✅ URL изображения: {result_v1['data']['output'].get('image_url')}")

    logger.info("🏁 Тестирование завершено")


if __name__ == "__main__":
    main()