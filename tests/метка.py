import os
import boto3

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Путь к файлу для примера (оставлен из исходного кода, если нужно)
REMOTE_PATH = "data/topics_tracker.json"


def get_file_from_b2():
    """Загружает содержимое файла из Backblaze B2 (пример из исходного кода)."""
    if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
        print("❌ Ошибка: не заданы переменные окружения B2.")
        return None

    s3 = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY
    )

    try:
        print(f"🔄 Загружаем {REMOTE_PATH} из B2...")
        response = s3.get_object(Bucket=B2_BUCKET_NAME, Key=REMOTE_PATH)
        file_data = response['Body'].read().decode('utf-8')
        print(f"✅ Файл {REMOTE_PATH} успешно загружен из B2.")
        return file_data
    except Exception as e:
        print(f"❌ Ошибка при загрузке файла: {e}")
        return None


def upload_dummy_videos():
    """
    Создаёт имитацию видеофайлов и загружает их в B2
    в соответствующие папки с заданными именами.
    """
    # Список имён файлов/папок, которые нужно загрузить
    video_paths = [
        "555/20250212-0710.mp4",
        "555/20250212-0713.mp4",
        "666/20250211-2124.mp4",
        "666/20250211-2158.mp4",
        "666/20250211-2350.mp4",
        "666/20250212-0026.mp4",
        "666/20250212-0111.mp4",
        "666/20250212-0125.mp4",
        "666/20250212-0712.mp4",
        "666/20250212-0715.mp4",
        "666/20250212-0717.mp4",
        "666/20250212-0718.mp4",
    ]

    # Проверяем, что все переменные окружения заданы
    if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
        print("❌ Ошибка: не заданы переменные окружения B2.")
        return

    # Создаём B2-клиент
    s3 = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY
    )

    for remote_path in video_paths:
        # Формируем локальное имя, чтобы избежать проблем с папками на локальном диске
        # Можно, к примеру, заменить '/' в имени на '_'
        local_filename = remote_path.replace("/", "_")

        # Создаём пустой файл (имитацию видео)
        with open(local_filename, "wb") as f:
            # Записываем просто несколько байт для наглядности
            f.write(b"FAKE_VIDEO_DATA")

        # Пытаемся загрузить файл в B2
        try:
            print(f"🔄 Загружаем файл {local_filename} в B2 -> {remote_path}")
            s3.upload_file(local_filename, B2_BUCKET_NAME, remote_path)
            print(f"✅ {local_filename} успешно загружен в {remote_path}")

        except Exception as e:
            print(f"❌ Ошибка при загрузке {local_filename} -> {remote_path}: {e}")

        finally:
            # После успешной/неуспешной попытки удаления файла, удаляем локальную копию
            if os.path.exists(local_filename):
                os.remove(local_filename)
                print(f"🗑️ Удалён локальный файл {local_filename}")


if __name__ == "__main__":
    # При необходимости можем сначала проверить, что get_file_from_b2() работает:
    content = get_file_from_b2()
    if content:
        print(content)

    # Затем вызываем загрузку имитации видеофайлов:
    upload_dummy_videos()
