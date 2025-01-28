import os
import boto3
import io

# 🔹 Загрузка переменных окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# 🔹 Настройка B2-клиента через boto3 (S3 API)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)

# 🔹 Файлы для загрузки
FILES_TO_UPLOAD = {
    "555/": ["20250124-0331.mp4", "20250124-0332.mp4"],
    "666/": ["20250124-0152.mp4", "20250124-0204.mp4", "20250124-0215.mp4"],
}

# 🔹 Создаём файл-заглушку (1MB)
def generate_mock_video():
    return io.BytesIO(b"0" * 1024 * 1024)  # 1MB пустых байтов

# 🔹 Загружаем файлы в B2
def upload_mock_videos():
    for folder, files in FILES_TO_UPLOAD.items():
        for file_name in files:
            s3_key = f"{folder}{file_name}"
            print(f"🚀 Загружаем {s3_key} в B2...")
            try:
                s3.upload_fileobj(generate_mock_video(), B2_BUCKET_NAME, s3_key)
                print(f"✅ Файл {s3_key} успешно загружен!")
            except Exception as e:
                print(f"❌ Ошибка загрузки {s3_key}: {e}")

if __name__ == "__main__":
    upload_mock_videos()
