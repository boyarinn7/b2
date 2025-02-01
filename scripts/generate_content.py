import json
import os
import sys
import requests
import openai
import textstat
import spacy
import re
import subprocess
import boto3
import io

from modules.config_manager import ConfigManager
from modules.logger import get_logger
from modules.error_handler import handle_error
from datetime import datetime
from modules.utils import ensure_directory_exists  # Если get_b2_client не определён здесь, он может быть импортирован отдельно
from PIL import Image, ImageDraw

# Добавляем путь к директории modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))

# === Инициализация ===
logger = get_logger("generate_content")
config = ConfigManager()

# Дополнительная инициализация логгера (если нужно, можно оставить один)
logger = get_logger("generate_media_launcher")


def create_and_upload_image(folder, generation_id):
    """Создает имитацию изображения и загружает его в ту же папку в B2."""
    try:
        file_name = generation_id.replace(".json", ".png")
        local_file_path = file_name

        # Создаем имитацию изображения
        img = Image.new('RGB', (800, 600), color=(73, 109, 137))
        draw = ImageDraw.Draw(img)
        draw.text((10, 10), f"ID: {generation_id}", fill=(255, 255, 255))
        img.save(local_file_path)
        logger.info(f"✅ Изображение '{local_file_path}' успешно создано.")

        # Загрузка изображения в ту же папку в B2
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        s3_key = f"{folder.rstrip('/')}/{file_name}"
        s3.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"✅ Изображение успешно загружено в B2: {s3_key}")

        os.remove(local_file_path)
    except Exception as e:
        handle_error("Image Upload Error", str(e))


def get_b2_client():
    """Создает клиент для работы с Backblaze B2."""
    try:
        return boto3.client(
            's3',
            endpoint_url=config.get("API_KEYS.b2.endpoint"),
            aws_access_key_id=config.get("API_KEYS.b2.access_key"),
            aws_secret_access_key=config.get("API_KEYS.b2.secret_key")
        )
    except Exception as e:
        handle_error("B2 Client Initialization Error", str(e))


def download_config_public():
    """Загружает файл config_public.json из B2 в локальное хранилище."""
    try:
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        config_public_path = config.get("FILE_PATHS.config_public")
        os.makedirs(os.path.dirname(config_public_path), exist_ok=True)
        s3.download_file(bucket_name, config_public_path, config_public_path)
        logger.info(f"✅ Файл config_public.json успешно загружен из B2 в {config_public_path}")
    except Exception as e:
        handle_error("Download Config Public Error", str(e))


def generate_file_id():
    """Создает уникальный ID генерации в формате YYYYMMDD-HHmm."""
    now = datetime.utcnow()
    date_part = now.strftime("%Y%m%d")
    time_part = now.strftime("%H%M")
    return f"{date_part}-{time_part}.json"


def save_generation_id_to_config(file_id):
    """Сохраняет ID генерации в файл config_gen.json."""
    config_gen_path = os.path.join("config", "config_gen.json")
    os.makedirs(os.path.dirname(config_gen_path), exist_ok=True)
    try:
        with open(config_gen_path, "w", encoding="utf-8") as file:
            json.dump({"generation_id": file_id}, file, ensure_ascii=False, indent=4)
        logger.info(f"✅ ID генерации '{file_id}' успешно сохранён в config_gen.json")
    except Exception as e:
        handle_error("Save Generation ID Error", str(e))


def save_to_b2(folder, content):
    """Сохраняет контент в B2 без двойного кодирования JSON."""
    try:
        file_id = generate_file_id()
        save_generation_id_to_config(file_id)
        logger.info(f"🔄 Сохранение контента в папку B2: {folder} с именем файла {file_id}")

        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        s3_key = f"{folder.rstrip('/')}/{file_id}"

        if not isinstance(content, dict):
            logger.error("❌ Ошибка: Контент должен быть словарём!")
            return

        sarcasm_data = content.get("sarcasm", {})
        if isinstance(sarcasm_data, str):
            try:
                sarcasm_data = json.loads(sarcasm_data)
                logger.warning("⚠️ Поле 'sarcasm' было строкой, исправляем...")
            except json.JSONDecodeError:
                logger.error("❌ Ошибка: Поле 'sarcasm' имеет неверный формат!")
                return

        if "poll" in sarcasm_data and isinstance(sarcasm_data["poll"], str):
            try:
                sarcasm_data["poll"] = json.loads(sarcasm_data["poll"])
                logger.warning("⚠️ Поле 'poll' было строкой, исправляем...")
            except json.JSONDecodeError:
                logger.error("❌ Ошибка: Поле 'poll' имеет неверный формат!")
                sarcasm_data["poll"] = {}

        content["sarcasm"] = sarcasm_data

        json_bytes = io.BytesIO(json.dumps(content, ensure_ascii=False, indent=4).encode("utf-8"))
        s3.upload_fileobj(json_bytes, bucket_name, s3_key)

        logger.info(f"✅ Контент успешно сохранён в B2: {s3_key}")

    except Exception as e:
        handle_error("B2 Upload Error", str(e))


class ContentGenerator:
    def __init__(self):
        self.topic_threshold = config.get('GENERATE.topic_threshold', 7)
        self.text_threshold = config.get('GENERATE.text_threshold', 8)
        self.max_attempts = config.get('GENERATE.max_attempts', 3)
        self.adaptation_enabled = config.get('GENERATE.adaptation_enabled', False)
        self.adaptation_params = config.get('GENERATE.adaptation_parameters', {})
        self.content_output_path = config.get('FILE_PATHS.content_output_path', 'generated_content.json')
        self.before_critique_path = config.get('FILE_PATHS.before_critique_path', 'before_critique.json')
        self.after_critique_path = config.get('FILE_PATHS.after_critique_path', 'after_critique.json')

        self.logger = logger
        self.config = config

        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4")
        self.temperature = float(os.getenv("OPENAI_TEMPERATURE", 0.7))

        if not self.openai_api_key:
            logger.error("❌ Переменная окружения OPENAI_API_KEY не задана!")
            raise EnvironmentError("Переменная окружения OPENAI_API_KEY отсутствует.")

    def adapt_prompts(self):
        if not self.adaptation_enabled:
            logger.info("🔄 Адаптация промптов отключена.")
            return
        logger.info("🔄 Применяю адаптацию промптов на основе обратной связи...")
        for key, value in self.adaptation_params.items():
            logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def clear_generated_content(self):
        try:
            logger.info("🧹 Полная очистка файла с результатами перед записью новой темы.")
            if not self.content_output_path:
                raise ValueError("❌ Ошибка: content_output_path пустой!")
            folder = os.path.dirname(self.content_output_path)
            if folder and not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"📁 Папка для сохранения данных создана: {folder}")
            logger.info(f"🔎 Debug: Очистка файла {self.content_output_path}")
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump({}, file, ensure_ascii=False, indent=4)
            logger.info("✅ Файл успешно очищен.")
        except PermissionError:
            handle_error("Clear Content Error", f"Нет прав на запись в файл: {self.content_output_path}")
        except Exception as e:
            handle_error("Clear Content Error", str(e))

    def generate_topic(self):
        try:
            prompt_template = config.get('CONTENT.topic.prompt_template')
            prompt = prompt_template.format(focus_areas="новая тема")
            logger.info("🔄 Запрос к OpenAI для генерации темы...")
            topic = self.request_openai(prompt)
            self.save_to_generated_content("topic", {"topic": topic})
            logger.info(f"✅ Тема успешно сгенерирована: {topic}")
            return topic
        except Exception as e:
            handle_error("Topic Generation Error", str(e))

    def request_openai(self, prompt):
        try:
            openai.api_key = self.openai_api_key
            max_tokens = self.config.get("API_KEYS.openai.max_tokens_text", 10)
            self.logger.info(f"🔎 Отправка запроса в OpenAI с max_tokens={max_tokens}")
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=self.temperature,
            )
            return response['choices'][0]['message']['content'].strip()
        except Exception as e:
            logger.error(f"❌ Ошибка при работе с OpenAI API: {e}")
            raise

    def generate_sarcastic_comment(self, text):
        self.logger.info(f"🔎 Debug: Промпт для саркастического комментария: {self.config.get('SARCASM.comment_prompt')}")
        self.logger.info(f"🔎 Debug: max_tokens_comment = {self.config.get('SARCASM.max_tokens_comment', 20)}")
        if not self.config.get('SARCASM.enabled', False):
            self.logger.info("🔕 Сарказм отключён в конфигурации.")
            return ""
        prompt = self.config.get('SARCASM.comment_prompt').format(text=text)
        max_tokens = self.config.get("SARCASM.max_tokens_comment", 20)
        self.logger.info(f"🔎 Debug: Используемый max_tokens_comment = {max_tokens}")
        self.logger.info(f"🔎 Отправка запроса в OpenAI с max_tokens={max_tokens}")
        try:
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=self.temperature
            )
            comment = response['choices'][0]['message']['content'].strip()
            self.logger.info(f"🔎 Debug: Ответ OpenAI: {comment}")
            return comment
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации саркастического комментария: {e}")
            return ""

    def generate_interactive_poll(self, text):
        if not self.config.get('SARCASM.enabled', False):
            self.logger.info("🔕 Сарказм отключён в конфигурации.")
            return {}
        prompt = self.config.get('SARCASM.question_prompt').format(text=text)
        try:
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.config.get('SARCASM.max_tokens_poll', 50),
                temperature=self.temperature
            )
            poll_text = response['choices'][0]['message']['content'].strip()
            self.logger.info(f"🛑 Сырой ответ OpenAI перед разбором: {poll_text}")
            try:
                poll_data = json.loads(poll_text)
                if "question" in poll_data and "options" in poll_data:
                    return poll_data
            except json.JSONDecodeError:
                self.logger.warning("⚠️ OpenAI вернул текст, а не JSON. Разбираем вручную...")
            match = re.findall(r"\d\.-\s*(.+)", poll_text)
            if len(match) >= 4:
                question = match[0].strip()
                options = [opt.strip() for opt in match[1:4]]
                return {"question": question, "options": options}
            self.logger.error("❌ OpenAI вернул некорректный формат! Возвращаем пустой объект.")
            return {}
        except Exception as e:
            handle_error("Sarcasm Poll Generation Error", str(e))
            return {}

    def save_to_generated_content(self, stage, data):
        try:
            if not self.content_output_path:
                raise ValueError("❌ Ошибка: self.content_output_path пустой!")
            logger.info(f"🔄 Обновление данных и сохранение в файл: {self.content_output_path}")
            folder = os.path.dirname(self.content_output_path) or "."
            logger.info(f"📁 Проверяем папку для сохранения: {folder}")
            if not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"📁 Папка создана: {folder}")
            if os.path.exists(self.content_output_path):
                logger.info(f"📄 Файл {self.content_output_path} найден, загружаем данные...")
                with open(self.content_output_path, 'r', encoding='utf-8') as file:
                    try:
                        result_data = json.load(file)
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️ Файл {self.content_output_path} поврежден, создаем новый.")
                        result_data = {}
            else:
                logger.warning(f"⚠️ Файл {self.content_output_path} не найден, создаем новый.")
                result_data = {}
            result_data["timestamp"] = datetime.utcnow().isoformat()
            result_data[stage] = data
            logger.info(f"💾 Записываем данные в {self.content_output_path}...")
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump(result_data, file, ensure_ascii=False, indent=4)
            logger.info(f"✅ Данные успешно обновлены и сохранены на этапе: {stage}")
        except FileNotFoundError:
            handle_error("Save to Generated Content Error", f"Файл не найден: {self.content_output_path}")
        except PermissionError:
            handle_error("Save to Generated Content Error", f"Нет прав на запись в файл: {self.content_output_path}")
        except Exception as e:
            handle_error("Save to Generated Content Error", str(e))

    def critique_content(self, content):
        try:
            self.logger.info("🔄 Выполняется критика текста через OpenAI...")
            prompt_template = self.config.get('CONTENT.critique.prompt_template')
            prompt = prompt_template.format(content=content)
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.config.get('CONTENT.critique.max_tokens', 200),
                temperature=self.temperature
            )
            critique = response['choices'][0]['message']['content'].strip()
            self.logger.info("✅ Критика успешно завершена.")
            return critique
        except Exception as e:
            handle_error("Critique Error", str(e))
            return "Критика текста завершилась ошибкой."

    def analyze_topic_generation(self):
        try:
            self.logger.info("🔍 Анализ архива успешных публикаций и обратной связи...")
            feedback_path = self.config.get('FILE_PATHS.feedback_file', 'data/feedback.json')
            positive_feedback_topics = []
            if os.path.exists(feedback_path):
                with open(feedback_path, 'r', encoding='utf-8') as file:
                    feedback_data = json.load(file)
                    positive_feedback_topics = [
                        entry['topic'] for entry in feedback_data if
                        entry.get('rating', 0) >= self.config.get('METRICS.success_threshold', 8)
                    ]
                self.logger.info(f"✅ Загрузили {len(positive_feedback_topics)} успешных тем из обратной связи.")
            else:
                self.logger.warning("⚠️ Файл обратной связи не найден.")
            archive_folder = self.config.get('FILE_PATHS.archive_folder', 'data/archive/')
            successful_topics = []
            if os.path.exists(archive_folder):
                for filename in os.listdir(archive_folder):
                    if filename.endswith('.json'):
                        with open(os.path.join(archive_folder, filename), 'r', encoding='utf-8') as file:
                            archive_data = json.load(file)
                            if archive_data.get('success', False):
                                successful_topics.append(archive_data.get('topic', ''))
                self.logger.info(f"✅ Загрузили {len(successful_topics)} успешных тем из архива.")
            else:
                self.logger.warning("⚠️ Папка архива не найдена.")
            valid_focus_areas = self.get_valid_focus_areas()
            combined_topics = list(set(positive_feedback_topics + successful_topics + valid_focus_areas))
            self.logger.info(f"📊 Итоговый список тем: {combined_topics}")
            return combined_topics
        except Exception as e:
            handle_error("Topic Analysis Error", str(e))
            return []

    def get_valid_focus_areas(self):
        try:
            tracker_file = self.config.get('FILE_PATHS.focus_tracker', 'data/focus_tracker.json')
            focus_areas = self.config.get('CONTENT.topic.focus_areas', [])
            if os.path.exists(tracker_file):
                with open(tracker_file, 'r', encoding='utf-8') as file:
                    focus_tracker = json.load(file)
                excluded_foci = focus_tracker[:10]
            else:
                excluded_foci = []
            valid_focus_areas = [focus for focus in focus_areas if focus not in excluded_foci]
            self.logger.info(f"✅ Доступные фокусы: {valid_focus_areas}")
            return valid_focus_areas
        except Exception as e:
            handle_error("Focus Area Filtering Error", str(e))
            return []

    def prioritize_focus_from_feedback_and_archive(self, valid_focus_areas):
        try:
            feedback_path = self.config.get('FILE_PATHS.feedback_file', 'data/feedback.json')
            feedback_foci = []
            if os.path.exists(feedback_path):
                with open(feedback_path, 'r', encoding='utf-8') as file:
                    feedback_data = json.load(file)
                    feedback_foci = [
                        entry['topic'] for entry in feedback_data if
                        entry.get('rating', 0) >= self.config.get('METRICS.success_threshold', 8)
                    ]
            archive_folder = self.config.get('FILE_PATHS.archive_folder', 'data/archive/')
            archive_foci = []
            if os.path.exists(archive_folder):
                for filename in os.listdir(archive_folder):
                    if filename.endswith('.json'):
                        with open(os.path.join(archive_folder, filename), 'r', encoding='utf-8') as file:
                            archive_data = json.load(file)
                            if archive_data.get('success', False):
                                archive_foci.append(archive_data.get('topic', ''))
            for focus in feedback_foci + archive_foci:
                if focus in valid_focus_areas:
                    self.logger.info(f"✅ Выбран приоритетный фокус: {focus}")
                    return focus
            if valid_focus_areas:
                self.logger.info(f"🔄 Используем первый доступный фокус: {valid_focus_areas[0]}")
                return valid_focus_areas[0]
            self.logger.warning("⚠️ Нет доступных фокусов для выбора.")
            return None
        except Exception as e:
            handle_error("Focus Prioritization Error", str(e))
            return None

    def run(self):
        """Основной процесс генерации контента."""
        try:
            download_config_public()
            with open(config.get("FILE_PATHS.config_public"), "r", encoding="utf-8") as file:
                config_public = json.load(file)
            empty_folders = config_public.get("empty", [])
            if not empty_folders:
                logger.info("✅ Нет пустых папок. Процесс завершён.")
                return

            self.adapt_prompts()
            self.clear_generated_content()

            valid_topics = self.analyze_topic_generation()
            chosen_focus = self.prioritize_focus_from_feedback_and_archive(valid_topics)
            if chosen_focus:
                self.logger.info(f"✅ Выбранный фокус: {chosen_focus}")
            else:
                self.logger.warning("⚠️ Фокус не найден, используем стандартный список.")

            topic = self.generate_topic()
            self.save_to_generated_content("topic", {"topic": topic})

            text_initial = self.request_openai(config.get('CONTENT.text.prompt_template').format(topic=topic))
            critique = self.critique_content(text_initial)
            self.save_to_generated_content("critique", {"critique": critique})

            sarcastic_comment = self.generate_sarcastic_comment(text_initial)
            sarcastic_poll = self.generate_interactive_poll(text_initial)
            self.save_to_generated_content("sarcasm", {
                "comment": sarcastic_comment,
                "poll": sarcastic_poll
            })

            final_text = text_initial.strip()
            target_folder = empty_folders[0]
            save_to_b2(target_folder, {"content": final_text})

            with open(os.path.join("config", "config_gen.json"), "r", encoding="utf-8") as gen_file:
                config_gen_content = json.load(gen_file)
                generation_id = config_gen_content["generation_id"]

            create_and_upload_image(target_folder, generation_id)

            logger.info(f"📄 Содержимое config_public.json: {json.dumps(config_public, ensure_ascii=False, indent=4)}")
            logger.info(f"📄 Содержимое config_gen.json: {json.dumps(config_gen_content, ensure_ascii=False, indent=4)}")

            run_generate_media()
            self.logger.info("✅ Генерация контента завершена.")

        except Exception as e:
            handle_error("Run Error", str(e))


def run_generate_media():
    """Запускает скрипт generate_media.py по локальному пути."""
    try:
        scripts_folder = config.get("FILE_PATHS.scripts_folder", "scripts")
        script_path = os.path.join(scripts_folder, "generate_media.py")
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт generate_media.py не найден по пути: {script_path}")
        logger.info(f"🔄 Запуск скрипта: {script_path}")
        subprocess.run(["python", script_path], check=True)
        logger.info(f"✅ Скрипт {script_path} выполнен успешно.")
    except subprocess.CalledProcessError as e:
        handle_error("Script Execution Error", f"Ошибка при выполнении скрипта {script_path}: {e}")
    except FileNotFoundError as e:
        handle_error("File Not Found Error", str(e))
    except Exception as e:
        handle_error("Unknown Error", f"Ошибка при запуске скрипта {script_path}: {e}")


if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()
