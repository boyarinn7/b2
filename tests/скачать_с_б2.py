import os
import boto3
import shutil  # Добавим shutil для переименования

# Переменные окружения (остаются без изменений)
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Константы для задач
FILES_TO_RENAME = [
    "666/20250423-1624.mp4.mp4",
    "666/20250423-1624.png.png"
]

JSON_FILES_TO_MOVE = [
    "666/20250423-2221.json",
    "666/20250423-2328.json",
    "666/20250424-0035.json",
    "666/20250424-0227.json",
    "666/20250424-0302.json",
    "666/20250424-0320.json",
    "666/20250424-0412.json"
]
LOCAL_DESTINATION_DIR_FOR_JSON = r"C:\Users\boyar\777\555\444"
LOCAL_TEMP_DIR = r"C:\temp_b2_downloads" # Временная папка для скачивания перед переименованием

# Проверка переменных окружения
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    print("❌ Ошибка: не заданы переменные окружения B2.")
    exit(1)

# B2 клиент (остается без изменений)
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def ensure_local_dir(path):
    """Убеждается, что локальная директория существует."""
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
            print(f"Создана директория: {dir_path}")
        except OSError as e:
            print(f"💥 Ошибка создания директории {dir_path}: {e}")
            raise # Прерываем выполнение, если не можем создать папку

# --- Задача 1: Переименование файлов с двойным расширением ---
def rename_double_extensions():
    """Скачивает файлы, убирает двойное расширение и загружает обратно."""
    print("\n--- ЗАДАЧА 1: Переименование файлов с двойным расширением ---")
    ensure_local_dir(os.path.join(LOCAL_TEMP_DIR, "dummy")) # Создаем временную папку

    for original_key in FILES_TO_RENAME:
        print(f"\n🔄 Обработка: {original_key}")
        try:
            # Определяем новое имя файла (ключа)
            base_name, first_ext = os.path.splitext(original_key)
            correct_base_name, second_ext = os.path.splitext(base_name)
            # Проверяем, действительно ли расширения совпадают (как .mp4.mp4)
            if first_ext.lower() == second_ext.lower() and first_ext:
                 new_key = base_name # Новое имя файла = имя без последнего расширения
                 print(f"   Новый ключ: {new_key}")
            else:
                 print(f"   ⚠️ Не удалось определить двойное расширение для {original_key}. Пропускаем.")
                 continue

            # Локальные пути
            local_original_path = os.path.join(LOCAL_TEMP_DIR, os.path.basename(original_key))
            local_renamed_path = os.path.join(LOCAL_TEMP_DIR, os.path.basename(new_key))

            # 1. Скачать оригинальный файл
            print(f"   ⬇️ Скачиваем {original_key} -> {local_original_path}")
            s3.download_file(B2_BUCKET_NAME, original_key, local_original_path)

            # 2. Переименовать локально
            print(f"   ✏️ Переименовываем {local_original_path} -> {local_renamed_path}")
            # shutil.move лучше подходит для переименования
            shutil.move(local_original_path, local_renamed_path)

            # 3. Загрузить переименованный файл обратно
            print(f"   ⬆️ Загружаем {local_renamed_path} -> {new_key}")
            s3.upload_file(local_renamed_path, B2_BUCKET_NAME, new_key)

            # 4. Удалить оригинальный файл из B2
            print(f"   ❌ Удаляем оригинал {original_key} из облака")
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=original_key)

            # 5. Удалить локальный переименованный файл
            if os.path.exists(local_renamed_path):
                os.remove(local_renamed_path)
                print(f"   🗑️ Локальный файл {local_renamed_path} удален.")

            print(f"   ✅ Успешно обработан: {original_key} -> {new_key}")

        except boto3.exceptions.S3UploadFailedError as upload_err:
             print(f"   💥 Ошибка ЗАГРУЗКИ {new_key}: {upload_err}. Возможно, файл с таким именем уже существует?")
             # Оставляем локальный файл для возможной ручной проверки
             if os.path.exists(local_renamed_path):
                 print(f"   ⚠️ Локальный файл {local_renamed_path} СОХРАНЕН для проверки.")
             elif os.path.exists(local_original_path):
                 print(f"   ⚠️ Локальный файл {local_original_path} СОХРАНЕН для проверки.")
        except Exception as e:
            print(f"   💥 Ошибка обработки {original_key}: {e}")
            # Попытка удалить временные файлы, если они остались
            if os.path.exists(local_original_path):
                try: os.remove(local_original_path)
                except OSError: pass
            if os.path.exists(local_renamed_path):
                try: os.remove(local_renamed_path)
                except OSError: pass


# --- Задача 2: Перемещение (скачивание с удалением) JSON файлов ---
def move_specific_jsons():
    """Скачивает указанные JSON файлы в локальную папку и удаляет их из B2."""
    print("\n--- ЗАДАЧА 2: Перемещение JSON файлов ---")
    ensure_local_dir(os.path.join(LOCAL_DESTINATION_DIR_FOR_JSON, "dummy")) # Создаем целевую папку

    for key in JSON_FILES_TO_MOVE:
        print(f"\n🔄 Обработка: {key}")
        filename = os.path.basename(key)
        local_path = os.path.join(LOCAL_DESTINATION_DIR_FOR_JSON, filename)

        try:
            # 1. Скачать файл
            print(f"   ⬇️ Скачиваем {key} -> {local_path}")
            s3.download_file(B2_BUCKET_NAME, key, local_path)

            # 2. Удалить файл из B2
            print(f"   ❌ Удаляем {key} из облака")
            s3.delete_object(Bucket=B2_BUCKET_NAME, Key=key)

            print(f"   ✅ Успешно перемещен: {key} -> {local_path}")

        except Exception as e:
            print(f"   💥 Ошибка обработки {key}: {e}")
            # Если скачали, но не удалили, оставим локальный файл
            if os.path.exists(local_path):
                 print(f"   ⚠️ Локальный файл {local_path} СОХРАНЕН после ошибки.")


# --- Основное выполнение ---
if __name__ == "__main__":
    print("--- Запуск скрипта ---")
    # Выполняем первую задачу
    rename_double_extensions()
    # Выполняем вторую задачу
    move_specific_jsons()
    print("\n--- Скрипт завершил работу ---")
    # Опционально: удалить временную папку после всех операций
    # if os.path.exists(LOCAL_TEMP_DIR):
    #     try:
    #         shutil.rmtree(LOCAL_TEMP_DIR)
    #         print(f"\n🗑️ Временная папка {LOCAL_TEMP_DIR} удалена.")
    #     except OSError as e:
    #         print(f"\n⚠️ Не удалось удалить временную папку {LOCAL_TEMP_DIR}: {e}")