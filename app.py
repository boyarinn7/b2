import subprocess
import os
import json
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask, request, jsonify

app = Flask(__name__)

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
MIDJOURNEY_API_KEY = os.getenv("MIDJOURNEY_API_KEY")

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, MIDJOURNEY_API_KEY]):
    app.logger.error("❌ Не заданы необходимые переменные окружения")
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
        config_data = json.loads(config_obj['Body'].read().decode('utf-8'))
        app.logger.info("✅ Конфигурация успешно загружена из B2")
        return config_data
    except ClientError as e:
        app.logger.warning(f"⚠️ Config file not found, creating new: {str(e)}")
        return {"publish": "", "empty": [], "processing_lock": False}
    except json.JSONDecodeError as e:
        app.logger.error(f"❌ Ошибка декодирования JSON в config_public.json: {e}")
        return {"publish": "", "empty": [], "processing_lock": False}

def save_config_public(config_data):
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        app.logger.error("❌ Переменная окружения B2_BUCKET_NAME не задана")
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    try:
        json_str = json.dumps(config_data, ensure_ascii=False, indent=4)
        json.loads(json_str)  # Проверяем валидность перед сохранением
        b2_client.put_object(
            Bucket=bucket_name,
            Key="config/config_public.json",
            Body=json_str.encode('utf-8')
        )
        app.logger.info("✅ Конфигурация успешно сохранена в B2")
    except json.JSONDecodeError as e:
        app.logger.error(f"❌ Ошибка валидации JSON перед сохранением: {e}")
        raise
    except NoCredentialsError:
        app.logger.error("❌ Ошибка аутентификации в B2: неверные учетные данные")
        raise
    except Exception as e:
        app.logger.error(f"❌ Ошибка сохранения конфигурации в B2: {e}")
        raise

@app.route('/hook', methods=['POST'])

def webhook_handler():
    """Обрабатывает вебхук от Midjourney."""
    # Проверка подлинности запроса
    api_key = request.headers.get("X-API-Key")
    app.logger.info(f"Получен запрос с API-ключом: {api_key}")
    if api_key != MIDJOURNEY_API_KEY:
        app.logger.warning("Неверный API-ключ")
        return jsonify({"error": "Unauthorized"}), 401

    # Проверка Content-Type
    if request.content_type != "application/json":
        app.logger.error(f"Неверный Content-Type: {request.content_type}, ожидается application/json")
        return jsonify({"error": "Unsupported Media Type: Content-Type must be application/json"}), 415

    # Обработка данных
    try:
        data = request.json
        app.logger.info(f"Получены данные: {json.dumps(data, ensure_ascii=False)}")
        task_id = data.get("data", {}).get("task_id")
        image_urls = data.get("data", {}).get("output", {}).get("image_urls", [])

        if not task_id or not image_urls:
            app.logger.error("Неверный формат данных: отсутствует task_id или image_urls")
            return jsonify({"error": "Invalid data format"}), 400

        # Загружаем текущую конфигурацию
        config_public = load_config_public()

        # Убедимся, что config_public — это словарь
        if not isinstance(config_public, dict):
            app.logger.warning("⚠️ config_public не является словарем, инициализируем пустой")
            config_public = {"publish": "", "empty": [], "processing_lock": False}

        # Обновляем конфигурацию с результатами Midjourney
        config_public["midjourney_results"] = {
            "task_id": task_id,
            "image_urls": image_urls
        }

        # Сохраняем обновленную конфигурацию
        save_config_public(config_public)

        # Новое: сразу загружаем обновленную конфигурацию и логируем её
        updated_config = load_config_public()
        app.logger.info("После обновления, config_public: " + json.dumps(updated_config, ensure_ascii=False))

        # Запуск b2_storage_manager.py
        script_path = "scripts/b2_storage_manager.py"
        app.logger.info(f"🔄 Запуск скрипта: {script_path}")
        try:
            subprocess.run(["python", script_path], check=True)
            app.logger.info(f"✅ Скрипт {script_path} успешно выполнен")
        except subprocess.CalledProcessError as e:
            app.logger.error(f"❌ Ошибка выполнения скрипта {script_path}: {e}")
            return jsonify({"error": f"Failed to run {script_path}: {str(e)}"}), 500
        except FileNotFoundError:
            app.logger.error(f"❌ Скрипт {script_path} не найден")
            return jsonify({"error": f"Script {script_path} not found"}), 500

        return jsonify({"message": "Webhook processed"}), 200

    except json.JSONDecodeError:
        app.logger.error("❌ Некорректный JSON в теле запроса")
        return jsonify({"error": "Invalid JSON"}), 400
    except Exception as e:
        app.logger.error(f"❌ Ошибка обработки вебхука: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)