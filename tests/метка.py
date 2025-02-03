import os
import json
import boto3
from botocore.exceptions import ClientError

# 🔰 Имена переменных окружения (должны быть заданы)
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")  # ID ключа
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")  # Секретный ключ
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")  # Имя бакета
B2_ENDPOINT = os.getenv("B2_ENDPOINT")  # URL эндпоинта (например: https://s3.eu-central-003.backblazeb2.com)


def update_b2_config():
    """Обновляет config_public.json в B2"""
    try:
        # 🔰 1. Инициализация клиента B2
        s3 = boto3.client(
            "s3",
            endpoint_url=B2_ENDPOINT,
            aws_access_key_id=B2_ACCESS_KEY,
            aws_secret_access_key=B2_SECRET_KEY
        )

        # 🔰 2. Проверка существования бакета
        try:
            s3.head_bucket(Bucket=B2_BUCKET_NAME)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"❌ Бакет '{B2_BUCKET_NAME}' не найден!")
                return
            elif error_code == '403':
                print("❌ Ошибка доступа. Проверьте ключи!")
                return
            else:
                raise

        # 🔰 3. Загружаем текущий конфиг (если есть)
        config_key = "config/config_public.json"
        try:
            response = s3.get_object(Bucket=B2_BUCKET_NAME, Key=config_key)
            current_config = json.loads(response['Body'].read().decode('utf-8'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                current_config = {}
            else:
                raise

        # 🔰 4. Обновляем нужные поля
        updates = {
            "processing_lock": False,
            "empty": ["666/"],
            "generation_id": [],
            "publish": "444/, 555/, 666/"
        }
        merged_config = {**current_config, **updates}

        # 🔰 5. Загружаем обновленный конфиг обратно
        s3.put_object(
            Bucket=B2_BUCKET_NAME,
            Key=config_key,
            Body=json.dumps(merged_config, indent=4, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )

        print(f"✅ Конфиг успешно обновлен: {B2_BUCKET_NAME}/{config_key}")

    except ClientError as e:
        print(f"❌ Ошибка B2: {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Критическая ошибка: {str(e)}")


if __name__ == "__main__":
    # 🔰 Проверка переменных окружения
    required_vars = {
        "B2_ACCESS_KEY": B2_ACCESS_KEY,
        "B2_SECRET_KEY": B2_SECRET_KEY,
        "B2_BUCKET_NAME": B2_BUCKET_NAME,
        "B2_ENDPOINT": B2_ENDPOINT
    }

    missing_vars = [k for k, v in required_vars.items() if not v]

    if missing_vars:
        print("❌ Не заданы переменные окружения:")
        print("\n".join(f"- {var}" for var in missing_vars))
    else:
        update_b2_config()