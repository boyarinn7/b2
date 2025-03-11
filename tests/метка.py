import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")
LOCAL_GROUP_PATH = r"C:\Users\boyar\777\555\55"
REMOTE_FOLDER = "444/"

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


def upload_group_files():
    """Загружает файлы группы (JSON, PNG, MP4) в B2."""
    try:
        for filename in os.listdir(LOCAL_GROUP_PATH):
            local_file = os.path.join(LOCAL_GROUP_PATH, filename)
            remote_file = REMOTE_FOLDER + filename

            if os.path.isfile(local_file) and any(filename.endswith(ext) for ext in [".json", ".png", ".mp4"]):
                print(f"🔄 Загружаем {local_file} -> {remote_file}")
                s3.upload_file(local_file, B2_BUCKET_NAME, remote_file)
                print(f"✅ {filename} загружен в {remote_file}")
    except Exception as e:
        print(f"❌ Ошибка при загрузке файлов группы: {e}")


if __name__ == "__main__":
    upload_group_files()
