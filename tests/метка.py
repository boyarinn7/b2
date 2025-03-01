import os
import json
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")
CONFIG_FILE_PATH = "config/config_public.json"

# Проверяем переменные окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# Создаём клиент B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)


def update_config():
    """Загружает, обновляет и загружает обратно config_public.json."""
    try:
        print(f"🔄 Загружаем {CONFIG_FILE_PATH} из B2...")
        response = s3.get_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_FILE_PATH)
        config_data = json.loads(response['Body'].read().decode('utf-8'))

        # Обновляем ключ 'empty'
        if "empty" in config_data:
            config_data["empty"] = []

        # Сохраняем обновлённый конфиг локально
        local_filename = "updated_config.json"
        with open(local_filename, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4, ensure_ascii=False)

        # Загружаем обратно в B2
        print(f"🔼 Загружаем обновленный {CONFIG_FILE_PATH} в B2...")
        s3.upload_file(local_filename, B2_BUCKET_NAME, CONFIG_FILE_PATH)
        print("✅ Конфиг успешно обновлён.")

        # Удаляем локальный файл
        os.remove(local_filename)
    except Exception as e:
        print(f"❌ Ошибка при обновлении {CONFIG_FILE_PATH}: {e}")


if __name__ == "__main__":
    update_config()
