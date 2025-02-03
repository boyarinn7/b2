import os
import boto3

# 🔹 Загружаем переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# 🔹 Настраиваем B2-клиент через boto3 (S3 API)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,  # Используем S3-совместимый B2 endpoint
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)


def delete_file():
    """Удаляет файл 666/20250123-1829.mp4 из B2."""
    file_path = "666/20250123-1829.mp4"

    try:
        s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file_path)
        print(f"✅ Файл {file_path} успешно удалён из B2.")
    except Exception as e:
        print(f"❌ Ошибка при удалении {file_path}: {e}")


if __name__ == "__main__":
    delete_file()
