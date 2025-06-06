import os
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask, request, jsonify
import requests
import logging

# Настраиваем логи для Render
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, MIDJOURNEY_API_KEY, GITHUB_TOKEN]):
    app.logger.error("❌ Не заданы необходимые переменные окружения: B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, MIDJOURNEY_API_KEY, GITHUB_TOKEN")
    raise ValueError("❌ Не заданы необходимые переменные окружения")

# Инициализация клиента B2
b2_client = boto3.client(
    "s3",
    endpoint_url="https://s3.us-east-005.backblazeb2.com",
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def load_config_public():
    """Загружает config_public.json из B2 или возвращает дефолт при ошибке."""
    try:
        config_obj = b2_client.get_object(Bucket=B2_BUCKET_NAME, Key="config/config_public.json")
        config_data = json.loads(config_obj['Body'].read().decode('utf-8'))
        app.logger.info("✅ Конфигурация загружена из B2")
        return config_data
    except ClientError as e:
        app.logger.warning(f"⚠️ Config file not found in B2: {str(e)}")
        return {"publish": "", "empty": [], "processing_lock": False}
    except json.JSONDecodeError as e:
        app.logger.error(f"❌ Ошибка декодирования JSON: {e}")
        return {"publish": "", "empty": [], "processing_lock": False}

def save_config_public(config_data):
    """Сохраняет config_public.json в B2."""
    try:
        json_str = json.dumps(config_data, ensure_ascii=False, indent=4)
        json.loads(json_str)  # Проверка валидности JSON
        b2_client.put_object(
            Bucket=B2_BUCKET_NAME,
            Key="config/config_public.json",
            Body=json_str.encode('utf-8')
        )
        app.logger.info("✅ Конфигурация сохранена в B2")
    except Exception as e:
        app.logger.error(f"❌ Ошибка сохранения в B2: {e}")
        raise

@app.route('/healthz', methods=['GET'])
def health_check():
    """Health check для Render."""
    app.logger.info("Health check requested")
    return jsonify({"status": "healthy"}), 200

@app.route('/hook', methods=['POST'])
def webhook_handler():
    """Обрабатывает вебхук от MidJourney через PiAPI."""
    # Логируем заголовки и тело для отладки
    app.logger.info(f"Headers: {request.headers}")
    app.logger.info(f"Raw body: {request.get_data(as_text=True)}")

    api_key = request.headers.get("X-API-Key")
    app.logger.info(f"Получен запрос с API-ключом: {api_key}")
    if api_key != MIDJOURNEY_API_KEY:
        app.logger.warning("Неверный API-ключ")
        return jsonify({"error": "Unauthorized"}), 401

    if request.content_type != "application/json":
        app.logger.error(f"Неверный Content-Type: {request.content_type}")
        return jsonify({"error": "Unsupported Media Type"}), 415

    try:
        data = request.json
        app.logger.info(f"Получены данные: {json.dumps(data, ensure_ascii=False)}")
        task_id = data.get("task_id")
        image_url = data.get("output", {}).get("image_url")
        temp_image_urls = data.get("output", {}).get("temporary_image_urls", [])

        if not task_id or (not image_url and not temp_image_urls):
            app.logger.error("Неверный формат данных: отсутствует task_id или image_urls")
            return jsonify({"error": "Invalid data format"}), 400

        # Загружаем и обновляем конфигурацию
        config_public = load_config_public()
        config_public["midjourney_results"] = {
            "task_id": task_id,
            "image_urls": [image_url] if image_url else temp_image_urls[:1]
        }
        save_config_public(config_public)

        # Триггерим GitHub Actions
        github_url = "https://api.github.com/repos/boyarinn7/b2/dispatches"
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json"
        }
        payload = {"event_type": "midjourney-task-completed"}
        response = requests.post(github_url, json=payload, headers=headers)
        if response.status_code == 204:
            app.logger.info("✅ GitHub Actions успешно запущен")
        else:
            app.logger.error(f"❌ Ошибка GitHub Actions: {response.status_code} - {response.text}")

        return jsonify({"message": "Webhook processed"}), 200

    except Exception as e:
        app.logger.error(f"❌ Ошибка обработки вебхука: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port)