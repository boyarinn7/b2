import os
import boto3

# Настройки B2
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Папки в B2 для скачивания
B2_FOLDERS = ["666/", "555/"]
LOCAL_SAVE_DIR = r"C:\Users\boyar\hw\777"

# Инициализация клиента B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY,
)


def download_folder_from_b2():
    """Скачивает все файлы из указанных папок B2 в локальное хранилище."""
    try:
        # Получение списка объектов в бакете
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME)
        if 'Contents' not in response:
            print("❌ В указанных папках нет файлов.")
            return

        for obj in response['Contents']:
            key = obj['Key']
            if any(key.startswith(folder) for folder in B2_FOLDERS):
                local_file_path = os.path.join(LOCAL_SAVE_DIR, key.replace("/", os.sep))
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                print(f"🔄 Скачиваем {key} → {local_file_path}...")
                s3.download_file(B2_BUCKET_NAME, key, local_file_path)
                print(f"✅ Файл загружен: {local_file_path}")
    except Exception as e:
        print(f"❌ Ошибка загрузки файлов: {e}")


if __name__ == "__main__":
    download_folder_from_b2()
