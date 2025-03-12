import os
import logging
from modules.config_manager import ConfigManager

config = ConfigManager()


def get_logger(name):
    """Инициализирует логгер с указанным именем."""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Проверяем, чтобы не добавлять обработчики повторно
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Получаем путь к папке логов с дефолтным значением из ConfigManager
        log_folder = config.get('FILE_PATHS.log_folder', default='logs')
        if not log_folder:  # Дополнительная защита
            log_folder = 'logs'

        log_file = os.path.join(log_folder, f"{name}.log")
        os.makedirs(log_folder, exist_ok=True)  # Создаём папку, если её нет

        # Настройка обработчиков
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger