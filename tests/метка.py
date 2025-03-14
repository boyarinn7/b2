import os
import boto3
import json

# Константы
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")
LOCAL_GROUP_PATH = r"C:\Users\boyar\777\555\55"

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


def process_files():
    """Скачивает файлы, убирает _mock, загружает обратно и обновляет config"""
    try:
        # Список файлов для обработки
        files_to_process = [
            "555/20250313-2338_mock.mp4",
            "555/20250313-2341_mock.mp4",
            "555/20250313-2342_mock.mp4",
            "555/20250313-2343_mock.mp4",
            "666/20250313-2340_mock.mp4"
        ]

        # Создаем временную папку, если не существует
        if not os.path.exists(LOCAL_GROUP_PATH):
            os.makedirs(LOCAL_GROUP_PATH)

        # 1. Скачиваем и переименовываем файлы
        for remote_file in files_to_process:
            # Убираем _mock из имени
            new_remote_file = remote_file.replace("_mock", "")
            local_file = os.path.join(LOCAL_GROUP_PATH, os.path.basename(new_remote_file))

            print(f"🔄 Скачиваем {remote_file}")
            s3.download_file(B2_BUCKET_NAME, remote_file, local_file)
            print(f"✅ Скачан как {local_file}")

            # 2. Загружаем обратно с новым именем
            print(f"🔄 Загружаем {local_file} -> {new_remote_file}")
            s3.upload_file(local_file, B2_BUCKET_NAME, new_remote_file)
            print(f"✅ Загружен как {new_remote_file}")

            # Удаляем локальный файл
            os.remove(local_file)

        # 3. Работа с config файлом
        config_path = os.path.join(LOCAL_GROUP_PATH, "config_public.json")
        remote_config = "config/config_public.json"

        # Скачиваем config
        print(f"🔄 Скачиваем конфиг {remote_config}")
        s3.download_file(B2_BUCKET_NAME, remote_config, config_path)

        # Читаем и модифицируем config
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)

        # Устанавливаем empty как пустой список
        config_data['empty'] = []

        # Сохраняем измененный config
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, ensure_ascii=False, indent=4)

        # Загружаем обновленный config обратно
        print(f"🔄 Загружаем обновленный конфиг")
        s3.upload_file(config_path, B2_BUCKET_NAME, remote_config)
        print(f"✅ Конфиг обновлен")

        # Удаляем локальный config
        os.remove(config_path)

    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    process_files()