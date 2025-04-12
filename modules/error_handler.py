# core/modules/error_handler.py

import logging
from modules.logger import get_logger

def handle_error(context, message, exception=None):
    """Обрабатывает и логирует ошибки."""
    error_message = f"❌ Ошибка в контексте '{context}': {message}"
    if exception:
        error_message += f" | Подробности: {str(exception)}"
    logger.error(error_message)
    return {"status": "error", "context": context, "message": message}