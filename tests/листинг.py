import os
import json
import boto3
import logging
from prettytable import PrettyTable

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Получение ключей из переменных окружения
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
B2_ENDPOINT = os.getenv("B2_ENDPOINT")

# Проверка, что все ключи заданы
if not all([B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME, B2_ENDPOINT]):
    logging.error("❌ Не все ключи B2 заданы в окружении!")
    exit(1)

# Подключение к B2
s3_client = boto3.client(
    "s3",
    endpoint_url=B2_ENDPOINT,
    aws_access_key_id=B2_ACCESS_KEY,
    aws_secret_access_key=B2_SECRET_KEY
)


def list_folder(bucket_name, prefix):
    """Выводит содержимое папки в виде таблицы"""
    table = PrettyTable(["Файл", "Размер (KB)"])
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if "Contents" in response:
            for obj in response["Contents"]:
                file_name = obj["Key"]
                file_size = round(obj["Size"] / 1024, 2)  # Размер в KB
                table.add_row([file_name, file_size])
            logging.info(f"\n📂 Содержимое папки {prefix}:\n{table}")
        else:
            logging.info(f"📂 Папка {prefix} пуста.")
    except Exception as e:
        logging.error(f"❌ Ошибка при листинге {prefix}: {e}")


def get_config(bucket_name, config_file="config/config_public.json"):
    """Проверяет и загружает config_public.json"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=config_file)
        config_data = json.loads(response["Body"].read().decode("utf-8"))

        table = PrettyTable(["Параметр", "Значение"])
        for key, value in config_data.items():
            table.add_row([key, json.dumps(value, ensure_ascii=False)])

        logging.info(f"\n📄 Содержимое {config_file}:\n{table}")
        return config_data

    except s3_client.exceptions.NoSuchKey:
        logging.warning(f"⚠️ Файл {config_file} отсутствует. Необходимо его создать.")
        return None
    except json.JSONDecodeError:
        logging.error(f"❌ Файл {config_file} поврежден! JSON некорректен.")
        return None
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке {config_file}: {e}")
        return None


# Выполняем листинг в виде таблицы
list_folder(B2_BUCKET_NAME, "444/")
list_folder(B2_BUCKET_NAME, "555/")
list_folder(B2_BUCKET_NAME, "666/")
list_folder(B2_BUCKET_NAME, "config/")  # Добавлен листинг папки config

# Проверяем и загружаем конфиг
get_config(B2_BUCKET_NAME)
