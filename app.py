from flask import Flask, request, jsonify
import requests
import boto3
import json
import os
from io import StringIO

app = Flask(__name__)

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME", "boyarinnbotbucket")
B2_ENDPOINT = os.getenv("B2_ENDPOINT", "https://s3.us-east-005.backblazeb2.com")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "boyarinn7/b2")

# Проверяем переменные окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    app.logger.error("B2 environment variables are missing")
    b2_client = None
else:
    # Создаём клиент B2
    b2_client = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY  # Исправлено с aws_secret_key_id
    )

@app.route('/hook', methods=['POST'])
def webhook_handler():
    if b2_client is None:
        return jsonify({"error": "B2 not initialized"}), 500

    try:
        data = request.json
        task_id = data["data"]["task_id"]
        image_urls = data["data"]["output"]["image_urls"]
    except KeyError as e:
        return jsonify({"error": f"Invalid JSON: missing {str(e)}"}), 400

    # Работа с config_public.json
    remote_config = "config/config_public.json"
    try:
        # Скачиваем config в память
        config_obj = b2_client.get_object(Bucket=B2_BUCKET_NAME, Key=remote_config)
        current_config = json.loads(config_obj['Body'].read().decode('utf-8'))
    except Exception as e:
        app.logger.warning(f"Config file not found, creating new: {str(e)}")
        current_config = {"publish": "", "empty": [], "processing_lock": False}

    # Обновляем config
    current_config["midjourney_results"] = {
        "task_id": task_id,
        "image_urls": image_urls
    }

    # Сохраняем обратно в B2
    try:
        updated_config = json.dumps(current_config, ensure_ascii=False).encode('utf-8')
        b2_client.put_object(Bucket=B2_BUCKET_NAME, Key=remote_config, Body=updated_config)
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