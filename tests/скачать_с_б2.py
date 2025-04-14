import os
import boto3

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Папки для обработки
REMOTE_PREFIXES = ["666/", "555/", "444/"]
SYSTEM_FILES = ["666/placeholder.bzEmpty", "555/placeholder.bzEmpty", "444/placeholder.bzEmpty"]
LOCAL_BASE_DIR = r"C:\Users\boyar\777\555\444"

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# B2 клиент
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def ensure_local_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def sync_and_clean():
    for prefix in REMOTE_PREFIXES:
        print(f"\n📂 Обработка папки: {prefix}")

        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=prefix)
        contents = response.get("Contents", [])

        if not contents:
            print("⚠️ Нет файлов.")
            continue

        for obj in contents:
            key = obj["Key"]
            if key in SYSTEM_FILES or key.endswith("/"):
                print(f"⏭️ Пропускаем системный файл: {key}")
                continue

            filename = os.path.basename(key)
            local_path = os.path.join(LOCAL_BASE_DIR, filename)
            ensure_local_dir(os.path.dirname(local_path))

            try:
                print(f"⬇️ Скачиваем {key} → {local_path}")
                s3.download_file(B2_BUCKET_NAME, key, local_path)

                print(f"❌ Удаляем {key} из облака")
                s3.delete_object(Bucket=B2_BUCKET_NAME, Key=key)

            except Exception as e:
                print(f"💥 Ошибка с {key}: {e}")

if __name__ == "__main__":
    sync_and_clean()
