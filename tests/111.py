import json
from botocore.exceptions import ClientError
from modules.api_clients import get_b2_client

# === Конфигурация ===
CONFIG_PATH = "C:\\Users\\boyar\\core\\config\\config.json"
config = json.load(open(CONFIG_PATH, "r", encoding="utf-8"))

B2_BUCKET_NAME = config["API_KEYS"]["b2"]["bucket_name"]
CONFIG_PUBLIC_PATH = config["FILE_PATHS"]["config_public"]
folders = ["444/", "555/", "666/"]
file_extensions = [".json", ".png", ".mp4"]

# === Клиент для B2 ===
s3 = get_b2_client()

def list_files(folder):
    """Возвращает список файлов в папке."""
    try:
        response = s3.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=folder)
        return [obj["Key"] for obj in response.get("Contents", [])]
    except ClientError as e:
        print(f"Ошибка при листинге {folder}: {e}")
        return []

def delete_non_bzempty_files(folder):
    """Удаляет файлы, кроме `.bzEmpty`."""
    files = list_files(folder)
    for file in files:
        if not file.endswith(".bzEmpty"):
            try:
                s3.delete_object(Bucket=B2_BUCKET_NAME, Key=file)
                print(f"Удалён файл: {file}")
            except ClientError as e:
                print(f"Ошибка удаления {file}: {e}")

def upload_mock_files(folder, group_id):
    """Загружает файлы в папку с точными именами."""
    for ext in file_extensions:
        file_key = f"{folder}{group_id}{ext}"
        try:
            s3.put_object(Bucket=B2_BUCKET_NAME, Key=file_key, Body=b"mock content")
            print(f"Загружен файл: {file_key}")
        except ClientError as e:
            print(f"Ошибка загрузки {file_key}: {e}")

def update_config_public(publish_folder, empty_folders):
    """Создаёт и загружает config_public.json в B2."""
    config_public = {
        "publish": publish_folder,
        "empty": empty_folders
    }
    local_file = "config_public.json"
    with open(local_file, "w", encoding="utf-8") as f:
        json.dump(config_public, f, indent=4, ensure_ascii=False)

    try:
        s3.upload_file(local_file, B2_BUCKET_NAME, CONFIG_PUBLIC_PATH)
        print("config_public.json успешно загружен.")
    except ClientError as e:
        print(f"Ошибка загрузки config_public.json: {e}")

def list_and_print_contents():
    """Выводит содержимое папок и config_public.json."""
    for folder in folders:
        print(f"Содержимое {folder}: {list_files(folder)}")

    try:
        response = s3.get_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_PATH)
        config_data = json.load(response["Body"])
        print("Содержимое config_public.json:")
        print(json.dumps(config_data, indent=4, ensure_ascii=False))
    except ClientError as e:
        print(f"Ошибка чтения config_public.json: {e}")

def main():
    # Шаг 1: Удаление файлов
    for folder in folders:
        delete_non_bzempty_files(folder)

    # Шаг 2: Загрузка групп файлов
    empty_folders = []
    for folder in folders:
        group_id = f"20250110-{int(folder.rstrip('/')) + 1000}"  # Формируем новый ID
        upload_mock_files(folder, group_id)
        # Если в папке остались только .bzEmpty
        if not list_files(folder):
            empty_folders.append(folder)

    # Шаг 3: Загрузка config_public.json
    update_config_public("444/", empty_folders)

    # Шаг 4: Листинг папок и вывод config_public.json
    list_and_print_contents()

if __name__ == "__main__":
    main()
