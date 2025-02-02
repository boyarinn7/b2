import json
import os
import boto3
import io

# 🔹 Загружаем переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")
CONFIG_PUBLIC_PATH = "config/config_public.json"

# 🔹 Настраиваем B2-клиент через boto3 (S3 API)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,  # Используем S3-совместимый B2 endpoint
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)


def load_config_public():
    """Загружает config_public.json из B2."""
    try:
        response = s3.get_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_PATH)
        return json.load(response["Body"])
    except s3.exceptions.NoSuchKey:
        print("⚠️ Файл config_public.json отсутствует в B2, создаём новый.")
        return {}  # Если файла нет, создаём пустой JSON
    except Exception as e:
        print(f"❌ Ошибка загрузки config_public.json: {e}")
        return {}


def save_config_public(data):
    """Сохраняет config_public.json обратно в B2."""
    try:
        json_data = json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8")
        s3.put_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_PATH, Body=io.BytesIO(json_data))
        print(f"✅ config_public.json обновлён: {data}")
    except Exception as e:
        print(f"❌ Ошибка сохранения config_public.json: {e}")


def clear_generation_ids():
    """Очищает список generation_id в config_public.json."""
    config_data = load_config_public()

    # ❗ Полностью стираем все generation_id
    config_data["generation_id"] = []

    save_config_public(config_data)
    print(f"✅ Все generation_id удалены, теперь: {config_data}")


if __name__ == "__main__":
    clear_generation_ids()
