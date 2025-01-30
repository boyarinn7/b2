# core/modules/error_handler.py

import logging
from modules.logger import get_logger

def handle_error(logger, context, exception):
    """
    Обрабатывает ошибки и логирует их.
    """
    logger.error(f"❌ Ошибка в контексте '{context}': {exception}")
    raise SystemExit(f"❌ Критическая ошибка в '{context}'. Остановка программы.")
