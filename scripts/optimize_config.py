# core/scripts/optimize_config.py

import json
import shutil
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from datetime import datetime
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists, validate_json_structure
from modules.config_manager import ConfigManager

print("🔄 sys.path:")
for path in sys.path:
    print(path)

print("🔄 Текущая директория:", os.getcwd())

# === Инициализация ===
config = ConfigManager()
logger = get_logger("optimize_config")

# === Константы ===
CORE_CONFIG = config.get('FILE_PATHS.core_config', 'core/config_core.json')
DYNAMIC_CONFIG = config.get('FILE_PATHS.dynamic_config', 'core/config_dynamic.json')
ARCHIVE_CONFIG = config.get('FILE_PATHS.archive_config', 'core/config_archive.json')

DEFAULT_FLESCH_THRESHOLD = config.get('OPTIMIZE.default_flesch_threshold', 70)
VALIDATION_RULES = config.get('OPTIMIZE.validation_rules', {})
MAX_RETRIES = config.get('OPTIMIZE.max_retries', 3)


class ConfigOptimizer:
    def __init__(self):
        self.core_config = CORE_CONFIG
        self.dynamic_config = DYNAMIC_CONFIG
        self.archive_config = ARCHIVE_CONFIG

    def backup_config(self):
        """Создание резервной копии конфигурации."""
        try:
            ensure_directory_exists(os.path.dirname(self.archive_config))
            backup_path = f"{self.archive_config}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
            shutil.copyfile(self.core_config, backup_path)
            logger.info(f"✅ Резервная копия создана: {backup_path}")
        except FileNotFoundError:
            handle_error("Config Backup Error", f"Файл {self.core_config} не найден.")
        except Exception as e:
            handle_error("Config Backup Error", e)

    def load_config(self):
        """Загрузка основной конфигурации."""
        try:
            with open(self.core_config, 'r', encoding='utf-8') as file:
                config_data = json.load(file)
            validate_json_structure(config_data, ['METRICS'])
            logger.info("✅ Конфигурация успешно загружена.")
            return config_data
        except FileNotFoundError:
            handle_error("Config Load Error", f"Файл {self.core_config} не найден.")
        except json.JSONDecodeError as e:
            handle_error("Config JSON Error", e)

    def validate_config(self, config_data):
        """Валидация конфигурации по правилам."""
        try:
            for key, rule in VALIDATION_RULES.items():
                if key not in config_data or not isinstance(config_data[key], rule):
                    logger.warning(f"⚠️ Ключ {key} не соответствует правилу {rule}")
                    return False
            logger.info("✅ Конфигурация прошла валидацию.")
            return True
        except Exception as e:
            handle_error("Config Validation Error", e)

    def optimize_parameters(self, config_data):
        """Оптимизация параметров конфигурации."""
        try:
            if 'METRICS' in config_data:
                config_data['METRICS']['flesch_threshold'] = DEFAULT_FLESCH_THRESHOLD
                logger.info("🔄 Flesch Threshold оптимизирован.")
        except KeyError as e:
            handle_error("Metrics Key Error", e)

    def save_config(self, config_data):
        """Сохранение оптимизированной конфигурации."""
        try:
            with open(self.core_config, 'w', encoding='utf-8') as file:
                json.dump(config_data, file, ensure_ascii=False, indent=4)
            logger.info("✅ Конфигурация успешно сохранена.")
        except Exception as e:
            handle_error("Config Save Error", e)

    def run(self):
        """Основной процесс оптимизации конфигурации."""
        logger.info("🔄 Запуск процесса оптимизации конфигурации...")
        self.backup_config()
        config_data = self.load_config()

        if not config_data:
            logger.warning("⚠️ Конфигурация не загружена. Оптимизация невозможна.")
            return

        if not self.validate_config(config_data):
            logger.warning("⚠️ Конфигурация не прошла валидацию. Завершение работы.")
            return

        self.optimize_parameters(config_data)
        self.save_config(config_data)
        logger.info("🏁 Оптимизация конфигурации завершена успешно.")


# === Точка входа ===
if __name__ == "__main__":
    try:
        optimizer = ConfigOptimizer()
        optimizer.run()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
