from flask import Flask, request, jsonify
import requests
from b2sdk.v2 import InMemoryAccountInfo, B2Api
import json
import os

app = Flask(__name__)

B2_APPLICATION_KEY_ID = os.getenv("B2_APPLICATION_KEY_ID")
B2_APPLICATION_KEY = os.getenv("B2_APPLICATION_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME", "your-bucket-name")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO", "your-username/your-repo")

info = InMemoryAccountInfo()
b2_api = B2Api(info)
b2_api.authorize_account("production", B2_APPLICATION_KEY_ID, B2_APPLICATION_KEY)
bucket = b2_api.get_bucket_by_name(B2_BUCKET_NAME)

@app.route('/hook', methods=['POST'])
def webhook_handler():
    data = request.json
    task_id = data["data"]["task_id"]
    image_urls = data["data"]["output"]["image_urls"]

    file_info = bucket.get_file_info_by_name("config/config_public.json")
    current_config = json.loads(bucket.download_file_by_id(file_info.id_).read().decode())

    current_config["midjourney_results"] = {
        "task_id": task_id,
        "image_urls": image_urls
    }

    updated_config = json.dumps(current_config).encode()
    bucket.upload_bytes(updated_config, "config/config_public.json")

    github_url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    payload = {
        "event_type": "midjourney-task-completed",
        "client_payload": {"task_id": task_id}
    }
    response = requests.post(github_url, headers=headers, json=payload)
    response.raise_for_status()

    return jsonify({"message": "Webhook processed"}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)