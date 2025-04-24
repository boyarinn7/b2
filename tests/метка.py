import os
import boto3
import json
import logging # Добавлен импорт logging

# --- Настройка Логирования ---
# Настроим базовое логирование для вывода информации
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("reset_configs")

# --- Константы ---
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# --- Список файлов и значения для сброса ---
# Добавлено поле "status": null для config_midjourney.json
CONFIG_FILES_TO_RESET = {
    "config/config_gen.json": {
        "generation_id": None
    },
    "config/config_midjourney.json": {
        "midjourney_task": None,
        "midjourney_results": {},
        "generation": False,
        "status": None  # <-- ДОБАВЛЕНО: Сброс статуса
    },
    "config/config_public.json": {
        "processing_lock": False
        # Примечание: Список "generation_id" в config_public НЕ очищается этим скриптом.
        # Он должен очищаться либо менеджером после архивации, либо вручную,
        # либо внешним скриптом публикации.
    }
}

# Локальная папка для временных файлов (можно изменить)
# Используем временную папку системы или текущую директорию
LOCAL_TEMP_DIR = os.path.join(os.getcwd(), "temp_reset")
os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
logger.info(f"Локальная временная папка: {LOCAL_TEMP_DIR}")

# --- Проверка переменных окружения ---
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    logger.error("❌ Ошибка: не заданы переменные окружения B2 (B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT).")
    exit(1)

# --- Клиент B2 ---
try:
    s3 = boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT,
        aws_access_key_id=B2_ACCESS_KEY,
        aws_secret_access_key=B2_SECRET_KEY
    )
    logger.info("✅ Клиент B2 (boto3) успешно создан.")
except Exception as e:
    logger.error(f"❌ Не удалось создать клиент B2: {e}", exc_info=True)
    exit(1)

def reset_config_file(file_key, desired_values):
    """Скачивает, обновляет и загружает обратно конфигурационный файл."""
    # Используем уникальное имя для временного файла
    local_path = os.path.join(LOCAL_TEMP_DIR, f"temp_{os.path.basename(file_key)}")

    try:
        logger.info(f"\n--- Обработка файла: {file_key} ---")
        # 1. Скачивание
        logger.info(f"⬇️ Скачиваем {file_key} -> {local_path}")
        s3.download_file(B2_BUCKET_NAME, file_key, local_path)
        logger.info(f"✅ Файл скачан.")

        # 2. Чтение и обновление
        data = {}
        try:
            with open(local_path, "r", encoding="utf-8") as f:
                # Проверяем, не пустой ли файл перед загрузкой JSON
                content = f.read()
                if content.strip():
                    data = json.loads(content)
                    logger.info("Содержимое файла прочитано.")
                else:
                    logger.warning("⚠️ Файл пустой. Будет создан новый с нужными значениями.")
                    data = {} # Начинаем с пустого словаря
        except json.JSONDecodeError:
            logger.warning("⚠️ Файл содержит невалидный JSON. Будет перезаписан.")
            data = {} # Перезаписываем невалидный JSON
        except FileNotFoundError:
             logger.warning("⚠️ Локальный файл не найден после скачивания? Создаем новый.")
             data = {} # Если файл исчез

        updated = False
        current_values = data.copy() # Сохраняем текущие значения для лога

        for key_to_reset, reset_value in desired_values.items():
            # Сбрасываем значение, если ключ существует и значение не равно нужному,
            # или если ключа вообще нет (добавляем его)
            if key_to_reset not in data or data.get(key_to_reset) != reset_value:
                logger.info(f"🔄 Сброс поля '{key_to_reset}': {data.get(key_to_reset)} -> {reset_value}")
                data[key_to_reset] = reset_value
                updated = True

        # 3. Сохранение и загрузка (только если были изменения)
        if updated:
            logger.info("📝 Обнаружены изменения, сохраняем и загружаем...")
            try:
                with open(local_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4) # Используем indent=4 для читаемости
                logger.info("Локальный файл обновлен.")

                logger.info(f"🔼 Загружаем {local_path} обратно в {file_key}")
                s3.upload_file(local_path, B2_BUCKET_NAME, file_key)
                logger.info("✅ Файл успешно загружен в B2.")
            except Exception as save_err:
                 logger.error(f"❌ Ошибка при сохранении/загрузке {file_key}: {save_err}", exc_info=True)
                 # Не удаляем временный файл при ошибке сохранения для возможной отладки
                 return # Прерываем обработку этого файла
        else:
            logger.info("✅ Значения уже соответствуют требуемым. Загрузка не требуется.")

        logger.info(f"\n📄 Финальный вид {file_key} в B2:")
        logger.info(json.dumps(data, ensure_ascii=False, indent=4))

    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА при обработке {file_key}: {e}", exc_info=True)
    finally:
        # Удаляем временный локальный файл
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                logger.debug(f"Удален временный файл: {local_path}")
            except OSError as remove_err:
                logger.warning(f"Не удалось удалить временный файл {local_path}: {remove_err}")

if __name__ == "__main__":
    logger.info("--- Запуск скрипта сброса конфигураций ---")
    total_errors = 0
    for config_path, values_to_set in CONFIG_FILES_TO_RESET.items():
        try:
            reset_config_file(config_path, values_to_set)
        except Exception:
            total_errors += 1 # Считаем ошибки на верхнем уровне

    logger.info("\n--- Сброс конфигураций завершен ---")
    if total_errors > 0:
        logger.error(f"🔥 Обнаружено ошибок при обработке файлов: {total_errors}")
        exit(1) # Выход с кодом ошибки, если были проблемы
    else:
        logger.info("🎉 Все конфигурационные файлы успешно проверены/сброшены.")
