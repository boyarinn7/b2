# core/modules/api_clients.py
import b2sdk.v2
import boto3
import openai
import os

from runwayml import RunwayML
from modules.config_manager import ConfigManager  # Исправлен импорт
from modules.error_handler import handle_error  # Исправлен импорт
from modules.logger import get_logger

logger = get_logger("api_clients")

# === Инициализация ConfigManager ===
config = ConfigManager()


# === OpenAI Client ===
def get_openai_client():
    """
    Возвращает клиент OpenAI с установленным API-ключом.
    """
    try:
        openai.api_key = config.get('API_KEYS.openai.api_key')
        return openai
    except Exception as e:
        handle_error("OpenAI Client Error", e)


# === RunwayML Client ===
def get_runwayml_client():
    """
    Возвращает клиент RunwayML с установленным API-ключом.
    """
    try:
        return RunwayML(api_key=config.get('API_KEYS.runwayml.api_key'))
    except Exception as e:
        handle_error("RunwayML Client Error", e)


# === B2 Client ===
def get_b2_client():
    """
    Возвращает клиент Backblaze B2, используя только переменные окружения из секретов GitHub.
    """
    endpoint = os.getenv("B2_ENDPOINT")
    access_key = os.getenv("B2_ACCESS_KEY")
    secret_key = os.getenv("B2_SECRET_KEY")
    
    if not all([endpoint, access_key, secret_key]):
        missing_vars = [var for var, val in [("B2_ENDPOINT", endpoint), ("B2_ACCESS_KEY", access_key), ("B2_SECRET_KEY", secret_key)] if not val]
        error_msg = f"❌ Не заданы обязательные переменные окружения для B2: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        logger.info("✅ Клиент B2 успешно создан")
        return client
    except Exception as e:
        handle_error("B2 Client Initialization Error", str(e), e)
        return None