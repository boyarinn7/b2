import os
import json
import boto3
import logging
import re

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Переменные окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

REQUIRED_SUFFIXES = [".json", ".png", ".mp4", "_sarcasm.png"]
FOLDERS = ["444/", "555/", "666/"]
CONFIG_PUBLIC_PATH = "config/config_public.json"
GEN_ID_REGEX = re.compile(r"^(\d{8}-\d{4})")

# Проверка ключей
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    logging.error("❌ Не все ключи B2 заданы в окружении!")
    exit(1)

# Подключение к B2
s3 = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)

def clear_published_ids():
    try:
        obj = s3.get_object(Bucket=B2_BUCKET_NAME, Key=CONFIG_PUBLIC_PATH)
        content = json.loads(obj['Body'].read().decode('utf-8'))

        if 'generation_id' in content:
            logging.info(f"🧹 Очищаю {len(content['generation_id'])} ID из config_public.json...")
            content['generation_id'] = []
            s3.put_object(
                Bucket=B2_BUCKET_NAME,
                Key=CONFIG_PUBLIC_PATH,
                Body=json.dumps(content, ensure_ascii=False, indent=4).encode('utf-8')
            )
            logging.info("✅ config_public.json обновлён.")
        else:
            logging.info("ℹ️ Ключ 'generation_id' не найден в config_public.json.")
    except Exception as e:
        logging.error(f"❌ Ошибка при очистке config_public.json: {e}")

def list_all_objects(prefix):
    all_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=B2_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            all_keys.append(obj["Key"])
    return all_keys

def delete_incomplete_groups():
    for folder in FOLDERS:
        logging.info(f"🔍 Проверяю {folder} на неполные группы...")
        keys = list_all_objects(folder)

        group_map = {}

        for key in keys:
            filename = os.path.basename(key)
            matched = None

            # Проверка на обычные суффиксы
            for suffix in REQUIRED_SUFFIXES:
                if filename.endswith(suffix):
                    matched = suffix
                    break

            if matched:
                if matched == "_sarcasm.png":
                    base = filename[:-len("_sarcasm.png")]
                else:
                    base = filename.replace(matched, "")
                if GEN_ID_REGEX.match(base):
                    group_map.setdefault(base, set()).add(matched)

        for gen_id, found_suffixes in group_map.items():
            if set(found_suffixes) != set(REQUIRED_SUFFIXES):
                # Удаляем все найденные части
                for suffix in found_suffixes:
                    key_to_delete = f"{folder}{gen_id}{suffix}"
                    try:
                        s3.delete_object(Bucket=B2_BUCKET_NAME, Key=key_to_delete)
                        logging.info(f"🗑️ Удалён: {key_to_delete}")
                    except Exception as e:
                        logging.warning(f"⚠️ Не удалось удалить {key_to_delete}: {e}")

if __name__ == "__main__":
    clear_published_ids()
    delete_incomplete_groups()
