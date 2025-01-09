# core/scripts/main.py

import os
import subprocess
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.config_manager import ConfigManager

# === Инициализация ===
config = ConfigManager()
logger = get_logger("main")

# === Константы ===
LOG_FILE = os.path.join(config.get('FILE_PATHS.log_folder'), 'main.log')
RETRY_ATTEMPTS = config.get('OTHER.retry_attempts', 3)
TIMEOUT = config.get('OTHER.timeout_seconds', 30)

# Пути к скриптам
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = {
    "generate_content": os.path.join(BASE_DIR, "generate_content.py"),
    "generate_media": os.path.join(BASE_DIR, "generate_media.py"),
    "feedback_analyzer": os.path.join(BASE_DIR, "feedback_analyzer.py"),
    "learning_cycle": os.path.join(BASE_DIR, "learning_cycle.py"),
    "b2_storage_manager": os.path.join(BASE_DIR, "b2_storage_manager.py"),
    "itself": os.path.join(BASE_DIR, "itself.py")
}


# === Вспомогательные функции ===
def run_script(script_name):
    """Запускает скрипт и обрабатывает ошибки."""
    script_path = SCRIPTS.get(script_name)
    if not script_path or not os.path.isfile(script_path):
        handle_error("Script Not Found", f"Скрипт {script_name} не найден по пути {script_path}")

    try:
        logger.info(f"🔄 Запуск скрипта: {script_name}")
        subprocess.run(['python', script_path], check=True, timeout=TIMEOUT)
        logger.info(f"✅ Скрипт {script_name} успешно выполнен.")
    except subprocess.CalledProcessError as e:
        handle_error("Script Execution Error", f"Ошибка при выполнении скрипта {script_name}: {e}")
    except subprocess.TimeoutExpired:
        handle_error("Script Timeout", f"Скрипт {script_name} превысил таймаут {TIMEOUT} секунд.")
    except Exception as e:
        handle_error("Script Unknown Error", e)


def check_dependencies():
    """Проверяет доступность всех скриптов перед запуском."""
    missing_scripts = [name for name, path in SCRIPTS.items() if not os.path.isfile(path)]
    if missing_scripts:
        handle_error("Missing Scripts", f"Отсутствуют следующие скрипты: {', '.join(missing_scripts)}")
    logger.info("✅ Все зависимости проверены. Все скрипты на месте.")


# === Основной процесс ===
def main():
    """Основной управляющий цикл системы."""
    logger.info("🚀 Запуск основного цикла системы...")
    check_dependencies()

    for script_name in SCRIPTS:
        logger.info(f"🔄 Выполнение компонента: {script_name}")
        attempt = 0
        while attempt < RETRY_ATTEMPTS:
            try:
                run_script(script_name)
                break
            except Exception as e:
                attempt += 1
                logger.warning(f"⚠️ Попытка {attempt}/{RETRY_ATTEMPTS} для {script_name} завершилась ошибкой.")
                if attempt == RETRY_ATTEMPTS:
                    handle_error("Retry Limit Reached",
                                 f"Не удалось выполнить {script_name} после {RETRY_ATTEMPTS} попыток.")

    logger.info("🏁 Основной цикл завершён успешно.")


# === Точка входа ===
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
