import os
import boto3

# Проверка переменных окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

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

# Списки файлов
JSON_FILES_TO_CUT = [
    "666/20250426-0029.json",
    "666/20250426-0046.json"
]
PNG_FILES_TO_COPY = [
    "666/20250426-1436.png"
]

# Локальная папка назначения
LOCAL_DESTINATION_DIR = r"C:\Users\boyar\777\555"


def ensure_local_dir(path):
    """Убеждается, что локальная директория существует."""
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            print(f"Создана директория: {path}")
        except OSError as e:
            print(f"💥 Ошибка создания директории {path}: {e}")
            exit(1)


def cut_json_files():
    """Скачивает указанные JSON-файлы и удаляет их из B2."""
    print("\n--- Перенос JSON-файлов (cut) ---")
    for key in JSON_FILES_TO_CUT:
        filename = os.path.basename(key)
        local_path = os.path.join(LOCAL_DESTINATION_DIR, filename)
        try:
            print(f"⬇️  Скачиваем {key} -> {local_path}")
            s3.download_file(B2_BUCKET_NAME, key, local_path)
            print(f"❌ Удаляем из B2: {key}")
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=key)
            print(f"✅ {filename} перенесён успешно.")
        except Exception as e:
            print(f"💥 Ошибка при обработке {key}: {e}")


def copy_png_files():
    """Скачивает указанные PNG-файлы (copy) без удаления из B2."""
    print("\n--- Копирование PNG-файлов ---")
    for key in PNG_FILES_TO_COPY:
        filename = os.path.basename(key)
        local_path = os.path.join(LOCAL_DESTINATION_DIR, filename)
        try:
            print(f"⬇️  Скачиваем {key} -> {local_path}")
            s3.download_file(B2_BUCKET_NAME, key, local_path)
            print(f"✅ {filename} скопирован успешно.")
        except Exception as e:
            print(f"💥 Ошибка при копировании {key}: {e}")


if __name__ == "__main__":
    print("--- Запуск скрипта: вырезка JSON и копирование PNG ---")
    ensure_local_dir(LOCAL_DESTINATION_DIR)
    cut_json_files()
    copy_png_files()
    print("\n--- Скрипт завершил работу ---")
