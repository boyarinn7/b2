# core/modules/logger.py

import logging
import os
from modules.config_manager import ConfigManager

config = ConfigManager()


def get_logger(name):
    """
    Создаёт и возвращает логгер.
    """
    log_file = os.path.join(config.get('FILE_PATHS.log_folder'), f"{name}.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(config.get('LOGGING.level', 'INFO'))

    handler = logging.FileHandler(log_file, encoding='utf-8')
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    if not logger.handlers:
        logger.addHandler(handler)

    return logger
