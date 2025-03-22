from flask import Flask, request, jsonify
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

app = Flask(__name__)

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, MIDJOURNEY_API_KEY]):
    raise ValueError("❌ Не заданы необходимые переменные окружения")

# Создаем клиент B2
b2_client = boto3.client(
    "s3",
    endpoint_url="https://s3.us-east-005.backblazeb2.com",
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def load_config_public():
    """Загружает config_public.json из B2."""
    try:
        config_obj = b2_client.get_object(Bucket=B2_BUCKET_NAME, Key="config/config_public.json")
        return json.loads(config_obj['Body'].read().decode('utf-8'))
    except ClientError as e:
        app.logger.warning(f"Config file not found, creating new: {str(e)}")
        return {"publish": "", "empty": [], "processing_lock": False}

def save_config_public(config_data):
    """Сохраняет config_public.json в B2."""
    try:
        b2_client.put_object(
            Bucket=B2_BUCKET_NAME,
            Key="config/config_public.json",
            Body=json.dumps(config_data, ensure_ascii=False).encode('utf-8')
        )
        app.logger.info("✅ Конфигурация успешно сохранена в B2.")
    except Exception as e:
        app.logger.error(f"❌ Ошибка сохранения конфигурации: {e}")

@app.route('/hook', methods=['POST'])
def webhook_handler():
    """Обрабатывает вебхук от Midjourney."""
    # Проверка подлинности запроса
    api_key = request.headers.get("X-API-Key")
    if api_key != MIDJOURNEY_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    # Обработка данных
    try:
        data = request.json
        task_id = data.get("data", {}).get("task_id")
        image_urls = data.get("data", {}).get("output", {}).get("image_urls", [])

        if not task_id or not image_urls:
            return jsonify({"error": "Invalid data format"}), 400

        # Загружаем текущую конфигурацию
        config_public = load_config_public()

        # Убедимся, что config_public — это словарь
        if not isinstance(config_public, dict):
            config_public = {}  # Инициализируем как пустой словарь, если это не словарь

        # Обновляем конфигурацию с результатами Midjourney
        config_public["midjourney_results"] = {
            "task_id": task_id,
            "image_urls": image_urls
        }

        # Сохраняем обновленную конфигурацию
        save_config_public(config_public)

        return jsonify({"message": "Webhook processed"}), 200

    except Exception as e:
        app.logger.error(f"❌ Ошибка обработки вебхука: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)