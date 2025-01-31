# core/modules/api_clients.py
import b2sdk.v2
import boto3
import openai
from runwayml import RunwayML
from modules.config_manager import ConfigManager  # Исправлен импорт
from modules.error_handler import handle_error  # Исправлен импорт

# === Инициализация ConfigManager ===
config = ConfigManager()



import os
import b2sdk.v2


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
    Возвращает клиент Backblaze B2 с установленными учетными данными.
    """
    try:
        return boto3.client(
            's3',
            endpoint_url=config.get('API_KEYS.b2.endpoint'),
            aws_access_key_id=config.get('API_KEYS.b2.access_key'),
            aws_secret_access_key=config.get('API_KEYS.b2.secret_key')
        )
    except Exception as e:
        handle_error("Backblaze B2 Client Error", e)
