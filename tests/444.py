import os
import boto3

# Настройки B2 из переменных окружения
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
LOCAL_SAVE_PATH = "C:\\Users\\boyar\\444\\"

# Создаем клиента B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)


def download_files_from_b2():
    """Скачивает все файлы из 444/ в B2 и сохраняет их локально."""
    response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix="444/")

    if "Contents" not in response:
        print("❌ Папка 444/ пуста или не найдена.")
        return

    os.makedirs(LOCAL_SAVE_PATH, exist_ok=True)

    for obj in response["Contents"]:
        file_key = obj["Key"]
        local_file_path = os.path.join(LOCAL_SAVE_PATH, os.path.basename(file_key))

        print(f"⬇️ Скачиваем {file_key} -> {local_file_path}")
        s3.download_file(B2_BUCKET_NAME, file_key, local_file_path)
        print(f"✅ Файл {file_key} скачан успешно.")


if __name__ == "__main__":
    download_files_from_b2()
