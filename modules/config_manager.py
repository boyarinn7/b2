import os
import json
import logging
import hashlib

# === Динамическое определение базовой директории ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.json')

# === Логирование ===
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'config_manager.log')
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class ConfigManager:
    def __init__(self, config_path=CONFIG_PATH):
        """Инициализация менеджера конфигурации."""
        self.config_path = config_path
        self.config_data = {}
        self.last_loaded_time = 0
        self.last_config_hash = ""
        self.load_config()

    def calculate_file_hash(self):
        """Вычисление хеша файла конфигурации."""
        try:
            with open(self.config_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception as e:
            logging.error(f"❌ Ошибка при вычислении хеша конфигурации: {e}")
            return ""

    def load_config(self):
        """Загрузка конфигурации из файла."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config_data = json.load(file)
            self.last_loaded_time = os.path.getmtime(self.config_path)
            self.last_config_hash = self.calculate_file_hash()
            logging.info("✅ Конфигурация успешно загружена.")
        except FileNotFoundError:
            logging.error(f"❌ Файл конфигурации не найден: {self.config_path}")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"❌ Ошибка декодирования JSON: {e}")
            raise
        except Exception as e:
            logging.error(f"❌ Ошибка при загрузке конфигурации: {e}")
            raise

    def reload_config(self):
        """Перезагрузка конфигурации при изменении файла."""
        try:
            current_hash = self.calculate_file_hash()
            if current_hash and current_hash != self.last_config_hash:
                self.load_config()
                logging.info("🔄 Конфигурация обновлена на лету.")
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении конфигурации: {e}")
            raise

    def validate_config(self):
        """Валидация конфигурации."""
        required_sections = {
            "API_KEYS": dict,
            "FILE_PATHS": dict,
            "METRICS": dict,
            "LOGGING": dict,
            "OTHER": dict,
            "CONTENT": dict
        }

        for section, expected_type in required_sections.items():
            if section not in self.config_data:
                logging.error(f"❌ Отсутствует обязательный раздел: {section}")
                raise ValueError(f"Отсутствует обязательный раздел: {section}")
            if not isinstance(self.config_data[section], expected_type):
                logging.error(f"❌ Раздел {section} должен быть типа {expected_type}")
                raise TypeError(f"Раздел {section} должен быть типа {expected_type}")
        logging.info("✅ Конфигурация успешно проверена на корректность.")

    def get(self, key, default=None):
        """Получение значения по ключу из переменных окружения с fallback на config.json."""
        # Значения по умолчанию для случаев, когда секреты не заданы
        defaults = {
            'FILE_PATHS.log_folder': 'logs',  # Дефолтная папка для логов
        }

        # Проверяем переменные окружения
        env_key = key.replace('.', '_').upper()  # Например, API_KEYS.b2.access_key -> API_KEYS_B2_ACCESS_KEY
        env_value = os.getenv(env_key)
        if env_value is not None:
            logging.info(f"🔑 Значение для ключа '{key}' взято из переменной окружения: {env_value}")
            return env_value

        # Если переменной окружения нет и это не обязательный ключ, используем дефолт
        if key in defaults:
            logging.info(
                f"ℹ️ Ключ '{key}' не найден в переменных окружения, используется дефолтное значение: {defaults[key]}")
            return defaults[key]

        # Проверяем config.json (опционально, для обратной совместимости)
        keys = key.split('.')
        value = self.config_data
        try:
            for k in keys:
                if k not in value:
                    logging.warning(
                        f"⚠️ Ключ '{key}' не найден в конфигурации и переменных окружения. Возвращается: {default}")
                    return default
                value = value[k]
            return value
        except Exception as e:
            logging.error(f"❌ Ошибка при получении ключа '{key}' из config.json: {e}")
            return default

    def set(self, key, value):
        """Установка значения по ключу в конфигурации."""
        keys = key.split('.')
        cfg = self.config_data
        try:
            for k in keys[:-1]:
                cfg = cfg.setdefault(k, {})
            cfg[keys[-1]] = value
            self.save_config()
            logging.info(f"✅ Значение для ключа '{key}' обновлено на '{value}'.")
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении ключа {key}: {e}")
            raise

    def save_config(self):
        """Сохранение конфигурации в файл."""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as file:
                json.dump(self.config_data, file, ensure_ascii=False, indent=4)
            self.last_config_hash = self.calculate_file_hash()
            logging.info("✅ Конфигурация успешно сохранена.")
        except Exception as e:
            logging.error(f"❌ Ошибка при сохранении конфигурации: {e}")
            raise


# === Пример использования ===
if __name__ == "__main__":
    try:
        config = ConfigManager()
        config.validate_config()
        logging.info("🔑 Пример получения данных из конфигурации:")
        api_key = config.get('API_KEYS.b2.access_key')
        logging.info(f"🔑 API Key: {api_key}")
        config.set('OTHER.new_key', 'test_value')
    except Exception as e:
        logging.critical(f"⛔ Критическая ошибка: {e}")
