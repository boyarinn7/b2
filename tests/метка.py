import os
import boto3
import json

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Список файлов и нужные значения
CONFIG_FILES = {
    "config/config_gen.json": {"generation_id": None},
    "config/config_midjourney.json": {
        "midjourney_task": None,
        "midjourney_results": {},
        "generation": False
    },
    "config/config_public.json": {"processing_lock": False}
}

LOCAL_DIR = r"C:\Users\boyar\777"  # ты уж сам проверь, что папка существует

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# Клиент B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def process_config(file_key, desired_values):
    local_path = os.path.join(LOCAL_DIR, os.path.basename(file_key))

    try:
        print(f"\n⬇️ Скачиваем {file_key} → {local_path}")
        s3.download_file(B2_BUCKET_NAME, file_key, local_path)

        with open(local_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print("⚠️ Файл пустой или битый. Создаём новый.")
                data = {}

        updated = False
        for k, v in desired_values.items():
            if data.get(k) != v:
                data[k] = v
                updated = True

        if updated:
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print("📝 Обновлены нужные поля.")
        else:
            print("✅ Всё уже настроено правильно.")

        print(f"🔼 Загружаем обратно в {file_key}")
        s3.upload_file(local_path, B2_BUCKET_NAME, file_key)
        print("☑️ Готово.")

        print(f"\n📄 Финальный вид {file_key}:")
        print(json.dumps(data, ensure_ascii=False, indent=2))

    except Exception as e:
        print(f"❌ Ошибка обработки {file_key}: {e}")

if __name__ == "__main__":
    for config_path, values in CONFIG_FILES.items():
        process_config(config_path, values)
