from flask import Flask, request, jsonify
import requests
from b2sdk.v2 import InMemoryAccountInfo, B2Api
import json
import os

app = Flask(__name__)

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME", "boyarinnbotbucket")  # Замените на имя бакета
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "boyarinn7/b2")  # Замените на ваш репозиторий

# Инициализация B2 с обработкой ошибок
info = InMemoryAccountInfo()
b2_api = B2Api(info)
try:
    b2_api.authorize_account("production", B2_ACCESS_KEY, B2_SECRET_KEY)
    bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)
except Exception as e:
    app.logger.error(f"B2 authorization failed: {str(e)}")
    bucket = None

@app.route('/hook', methods=['POST'])
def webhook_handler():
    if bucket is None:
        return jsonify({"error": "B2 not initialized"}), 500

    try:
        data = request.json
        task_id = data["data"]["task_id"]
        image_urls = data["data"]["output"]["image_urls"]
    except KeyError as e:
        return jsonify({"error": f"Invalid JSON: missing {str(e)}"}), 400

    # Читаем или создаём config_public.json
    try:
        file_info = bucket.get_file_info_by_name("config/config_public.json")
        current_config = json.loads(bucket.download_file_by_id(file_info.id_).read().decode())
    except Exception as e:
        app.logger.warning(f"Config file not found, creating new: {str(e)}")
        current_config = {"publish": "", "empty": [], "processing_lock": False}

    current_config["midjourney_results"] = {
        "task_id": task_id,
        "image_urls": image_urls
    }

    # Сохраняем в B2
    try:
        updated_config = json.dumps(current_config).encode()
        bucket.upload_bytes(updated_config, "config/config_public.json")
    except Exception as e:
        app.logger.error(f"Failed to upload to B2: {str(e)}")
        return jsonify({"error": "Failed to update config"}), 500

    # Отправляем в GitHub
    try:
        github_url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
        headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
        payload = {"event_type": "midjourney-task-completed", "client_payload": {"task_id": task_id}}
        response = requests.post(github_url, headers=headers, json=payload)
        response.raise_for_status()
    except Exception as e:
        app.logger.error(f"GitHub request failed: {str(e)}")
        return jsonify({"error": "Failed to trigger GitHub"}), 500

    return jsonify({"message": "Webhook processed"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)