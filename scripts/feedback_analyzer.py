# core/scripts/feedback_analyzer.py

import json
import os
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import validate_json_structure, ensure_directory_exists
from modules.config_manager import ConfigManager

# === Инициализация ===
config = ConfigManager()
logger = get_logger("feedback_analyzer")

# === Константы ===
FEEDBACK_FILE = config.get('FILE_PATHS.feedback_file', 'data/feedback.json')
FEEDBACK_REPORT = config.get('FILE_PATHS.feedback_report', 'feedback_report.json')
SUCCESS_THRESHOLD = config.get('METRICS.success_threshold', 8)
DELETE_THRESHOLD = config.get('METRICS.delete_threshold', 3)
RATING_COEFFICIENTS = config.get('METRICS.rating_coefficients', {
    "topic": 0.2,
    "text": 0.3,
    "engagement": 0.2,
    "media_weight": 0.1,
    "ocp": 0.1,
    "seo": 0.1
})


# === Класс анализатора обратной связи ===
class FeedbackAnalyzer:
    def __init__(self):
        self.feedback_file = FEEDBACK_FILE
        self.feedback_report = FEEDBACK_REPORT

    def load_feedback(self):
        """Загрузка данных обратной связи из файла."""
        try:
            with open(self.feedback_file, 'r', encoding='utf-8') as file:
                data = json.load(file)
            logger.info("✅ Данные обратной связи успешно загружены.")

            if not isinstance(data, list):
                handle_error("Feedback Structure Error", "Ожидался массив данных в feedback.json")

            return data
        except FileNotFoundError:
            handle_error("Feedback File Not Found", f"Файл {self.feedback_file} не найден.")
        except json.JSONDecodeError as e:
            handle_error("Feedback JSON Error", f"Ошибка декодирования JSON: {e}")
        except Exception as e:
            handle_error("Feedback Load Error", e)

    def repair_feedback_structure(self, feedback_data):
        """Автоматическое добавление недостающих ключей в feedback.json."""
        repaired_data = []
        for index, entry in enumerate(feedback_data):
            missing_keys = []
            if "topic_score" not in entry:
                entry["topic_score"] = 0
                missing_keys.append("topic_score")
            if "text_score" not in entry:
                entry["text_score"] = 0
                missing_keys.append("text_score")
            if "engagement_score" not in entry:
                entry["engagement_score"] = 0
                missing_keys.append("engagement_score")

            if missing_keys:
                logger.warning(f"⚠️ Исправлены отсутствующие ключи в элементе {index + 1}: {missing_keys}")
            repaired_data.append(entry)
        return repaired_data

    def analyze_feedback(self, feedback_data):
        """Анализ данных обратной связи."""
        try:
            total_score = 0
            total_entries = len(feedback_data)

            if total_entries == 0:
                logger.warning("⚠️ Нет данных для анализа.")
                return {"success_rate": 0, "action": "No Data"}

            for entry in feedback_data:
                topic_score = entry.get("topic_score", 0) * RATING_COEFFICIENTS["topic"]
                text_score = entry.get("text_score", 0) * RATING_COEFFICIENTS["text"]
                engagement_score = entry.get("engagement_score", 0) * RATING_COEFFICIENTS["engagement"]
                total_score += topic_score + text_score + engagement_score

            average_score = total_score / total_entries
            logger.info(f"📊 Средний балл контента: {average_score:.2f}")

            if average_score >= SUCCESS_THRESHOLD:
                action = "Keep"
            elif average_score <= DELETE_THRESHOLD:
                action = "Delete"
            else:
                action = "Optimize"

            return {"success_rate": average_score, "action": action}
        except Exception as e:
            handle_error("Feedback Analysis Error", e)

    def save_report(self, analysis_result):
        """Сохранение отчёта анализа."""
        try:
            ensure_directory_exists(os.path.dirname(self.feedback_report))
            with open(self.feedback_report, 'w', encoding='utf-8') as file:
                json.dump(analysis_result, file, ensure_ascii=False, indent=4)
            logger.info(f"✅ Отчёт сохранён в {self.feedback_report}")
        except Exception as e:
            handle_error("Feedback Report Save Error", e)

    def backup_feedback_file(self):
        """Создание резервной копии feedback.json."""
        import shutil
        backup_path = f"{self.feedback_file}.backup"
        try:
            shutil.copyfile(self.feedback_file, backup_path)
            logger.info(f"✅ Создана резервная копия файла: {backup_path}")
        except Exception as e:
            handle_error("Feedback Backup Error", e)

    def run(self):
        """Основной процесс анализа обратной связи."""
        logger.info("🔄 Запуск анализа обратной связи...")
        self.backup_feedback_file()

        feedback_data = self.load_feedback()
        if not feedback_data:
            handle_error("No Feedback Data", "Файл обратной связи пуст или невалиден.")
            return

        feedback_data = self.repair_feedback_structure(feedback_data)
        analysis_result = self.analyze_feedback(feedback_data)
        self.save_report(analysis_result)
        logger.info(f"🏁 Результат анализа: {analysis_result}")


# === Точка входа ===
if __name__ == "__main__":
    try:
        analyzer = FeedbackAnalyzer()
        analyzer.run()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
