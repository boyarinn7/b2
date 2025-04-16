import os
import logging
# Относительный импорт для ConfigManager
try:
    from .config_manager import ConfigManager
except ImportError:
    # Фоллбэк, если запускается не как часть пакета
    from modules.config_manager import ConfigManager


config = ConfigManager()


def get_logger(name):
    """Инициализирует логгер с указанным именем."""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Проверяем, чтобы не добавлять обработчики повторно
        logger.setLevel(logging.INFO)
        # Устанавливаем форматтер один раз
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Получаем путь к папке логов с дефолтным значением из ConfigManager
        log_folder = config.get('FILE_PATHS.log_folder', default='logs')
        if not log_folder:
            log_folder = 'logs'
            print(f"Warning: log_folder path is empty, defaulting to '{log_folder}'") # Используем print, т.к. логгер еще не настроен

        log_file = os.path.join(log_folder, f"{name}.log")

        # Убедимся, что папка существует перед созданием FileHandler
        try:
            os.makedirs(log_folder, exist_ok=True)
        except OSError as e:
            print(f"Error creating log directory {log_folder}: {e}")
            # Можно либо выйти, либо продолжить без файлового логгера
            # Пока продолжим только с консольным

        # Настройка обработчиков
        try:
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
             print(f"Error setting up file handler for {log_file}: {e}")

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # --- ДОБАВЛЕНО: Предотвращаем "всплытие" сообщений к root логгеру ---
        # Это должно убрать дублирование в консоли (например, в логах GitHub Actions)
        logger.propagate = False
        # --- КОНЕЦ ДОБАВЛЕНИЯ ---

    return logger

# Пример использования, если этот файл запускается напрямую (для теста)
if __name__ == '__main__':
    test_logger = get_logger("test_logger_main")
    test_logger.info("Это тестовое сообщение от логгера.")
    # Если вы запустите этот файл, сообщение должно появиться один раз в консоли
    # и один раз в файле logs/test_logger_main.log
