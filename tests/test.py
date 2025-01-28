import os
import boto3
import logging

# === Конфигурация ===
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Проверка переменных окружения
if not all([B2_BUCKET_NAME, B2_ACCESS_KEY, B2_SECRET_KEY, B2_ENDPOINT]):
    raise ValueError("❌ Не все ключи B2 заданы в окружении!")

# Инициализация логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("B2_Uploader")

# Инициализация B2 клиента
s3_client = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

# Недостающие файлы
MISSING_FILES = {
    "444/": {"20250124-0358": [".pbl"]},
    "555/": {
        "20250124-0331": [".mp4", ".pbl"],
        "20250124-0332": [".mp4", ".pbl"],
        "20250124-1659": [".pbl"]
    },
    "666/": {
        "20250124-0152": [".mp4", ".pbl"],
        "20250124-0204": [".mp4", ".pbl"],
        "20250124-0215": [".mp4", ".pbl"],
        "20250126-0616": [".pbl"]
    }
}


def create_dummy_file(filepath, content=b"DUMMY DATA\n"):
    """Создает файл-заглушку с данными."""
    with open(filepath, "wb") as f:
        f.write(content)


def upload_file_to_b2(local_path, remote_path):
    """Загружает файл в B2."""
    try:
        s3_client.upload_file(local_path, B2_BUCKET_NAME, remote_path)
        logger.info(f"✅ Файл {remote_path} загружен в B2")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки {remote_path}: {e}")


def fill_missing_files():
    """Создает недостающие файлы и загружает их в B2."""
    temp_folder = "temp_files"
    os.makedirs(temp_folder, exist_ok=True)

    for folder, groups in MISSING_FILES.items():
        for group_id, extensions in groups.items():
            for ext in extensions:
                local_file = os.path.join(temp_folder, f"{group_id}{ext}")
                remote_file = f"{folder}{group_id}{ext}"

                if ext == ".mp4":
                    create_dummy_file(local_file, b"FAKE MP4 DATA\n")
                else:  # .pbl файлы
                    create_dummy_file(local_file, b"published\n")

                upload_file_to_b2(local_file, remote_file)
                os.remove(local_file)  # Удаляем локальный файл после загрузки


if __name__ == "__main__":
    fill_missing_files()
