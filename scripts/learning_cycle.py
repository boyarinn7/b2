import os
import sys
import json
import shutil
import datetime

# Добавляем путь к родительской директории
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from modules.config_manager import ConfigManager
from modules.logger import get_logger
from modules.utils import ensure_directory_exists

# === Инициализация ===
config = ConfigManager()
logger = get_logger("learning_cycle")


class LearningCycle:
    """Класс для управления циклом самообучения."""

    def __init__(self):
        logger.info("✅ LearningCycle инициализирован.")
        self.config_path = config.get('FILE_PATHS.core_config', 'config/config_core.json')
        self.archive_path = config.get('FILE_PATHS.archive_config', 'config/backup/config_archive.json')
        self.backup_retention_days = 30  # Хранить резервные копии за последние 30 дней
        self.max_backup_files = 10  # Максимальное количество резервных копий

    def ensure_initial_config(self):
        """Создание базовой конфигурации при её отсутствии."""
        if not os.path.exists(self.config_path) or os.path.getsize(self.config_path) == 0:
            logger.warning("⚠️ Конфигурация отсутствует или пуста. Создание базовой конфигурации...")
            ensure_directory_exists(os.path.dirname(self.config_path))
            initial_config = {
                "LEARNING": {
                    "success_threshold": 8,
                    "delete_threshold": 3
                },
                "METRICS": {
                    "flesch_threshold": 70
                }
            }
            try:
                with open(self.config_path, 'w', encoding='utf-8') as file:
                    json.dump(initial_config, file, ensure_ascii=False, indent=4)
                logger.info(f"✅ Базовая конфигурация создана: {self.config_path}")
            except Exception as e:
                logger.error(f"❌ Ошибка при создании базовой конфигурации: {e}")
                sys.exit(1)
            self.backup_config()

    def backup_config(self):
        """Создание резервной копии конфигурации."""
        ensure_directory_exists(os.path.dirname(self.archive_path))
        timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        backup_file = f"{self.archive_path}_{timestamp}.json"

        try:
            shutil.copyfile(self.config_path, backup_file)
            logger.info(f"✅ Конфигурация сохранена в резервную копию: {backup_file}")
        except Exception as e:
            logger.error(f"❌ Ошибка при создании резервной копии: {e}")
            sys.exit(1)

        self.cleanup_old_backups()

    def cleanup_old_backups(self):
        """Очистка устаревших резервных копий."""
        backup_folder = os.path.abspath(os.path.join(os.path.dirname(self.archive_path)))
        backups = sorted([
            os.path.join(backup_folder, f) for f in os.listdir(backup_folder)
            if f.startswith('config_archive.json') and f.endswith('.json')
        ], key=os.path.getmtime, reverse=True)

        if not backups:
            logger.warning("⚠️ Резервные копии отсутствуют для очистки.")
            return

        # Удаление копий старше определённого количества дней
        now = datetime.datetime.now()
        for backup in backups:
            backup_time = datetime.datetime.fromtimestamp(os.path.getmtime(backup))
            if (now - backup_time).days > self.backup_retention_days:
                os.remove(backup)
                logger.info(f"🗑️ Удалена старая резервная копия по дате: {backup}")

        # Удаление старых копий, если их больше максимального количества
        while len(backups) > self.max_backup_files:
            oldest_backup = backups.pop()
            os.remove(oldest_backup)
            logger.info(f"🗑️ Удалена старая резервная копия по количеству: {oldest_backup}")

    def load_config(self):
        """Загрузка конфигурации."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                config_data = json.load(file)
            logger.info("✅ Конфигурация успешно загружена.")
            return config_data
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке конфигурации: {e}")
            self.restore_from_backup()

    def optimize_parameters(self):
        """Оптимизация параметров на основе анализа."""
        logger.info("🔄 Оптимизация параметров...")
        config_data = self.load_config()
        if not config_data:
            logger.warning("⚠️ Конфигурационные данные отсутствуют.")
            return

        learning_settings = config_data.get('LEARNING', {})
        if 'success_threshold' in learning_settings:
            learning_settings['success_threshold'] += 1
            logger.info(f"✅ Обновлён success_threshold: {learning_settings['success_threshold']}")

        self.save_config(config_data)

    def save_config(self, updated_config):
        """Сохранение конфигурации."""
        try:
            with open(self.config_path, 'w', encoding='utf-8') as file:
                json.dump(updated_config, file, ensure_ascii=False, indent=4)
            logger.info(f"✅ Конфигурация успешно сохранена: {self.config_path}")
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении конфигурации: {e}")
            sys.exit(1)

    def run(self):
        """Запуск цикла самообучения."""
        self.ensure_initial_config()
        self.backup_config()
        self.optimize_parameters()
        logger.info("🏁 Цикл самообучения завершён.")


# === Точка входа ===
if __name__ == "__main__":
    cycle = LearningCycle()
    cycle.run()
