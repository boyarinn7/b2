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
from datetime import datetime
from PIL import Image, ImageDraw
import logging

# Добавляем путь к директории modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))

from modules.config_manager import ConfigManager
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists

# Инициализация
logger = get_logger("generate_content")
config = ConfigManager()

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_and_upload_image(folder, generation_id):
    """Создаёт изображение и загружает его в B2."""
    if not folder or not isinstance(folder, str):
        raise ValueError("folder должен быть непустой строкой")
    if not generation_id or not isinstance(generation_id, str):
        raise ValueError("generation_id должен быть непустой строкой")

    try:
        file_name = generation_id.replace(".json", ".png")
        local_file_path = file_name
        img = Image.new('RGB', (800, 600), color=(73, 109, 137))
        draw = ImageDraw.Draw(img)
        draw.text((10, 10), f"ID: {generation_id}", fill=(255, 255, 0))
        img.save(local_file_path)
        logger.info(f"✅ Изображение '{local_file_path}' успешно создано.")
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name не указан в конфигурации или переменных окружения")
        s3_key = f"{folder.rstrip('/')}/{file_name}"
        s3.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"✅ Изображение успешно загружено в B2: {s3_key}")
        os.remove(local_file_path)
    except ValueError as ve:
        handle_error("Image Upload Error", str(ve))
    except Exception as e:
        handle_error("Image Upload Error", str(e))

def get_b2_client():
    """Инициализирует клиент B2 с использованием ключей из конфигурации или переменных окружения."""
    endpoint = config.get("API_KEYS.b2.endpoint")
    access_key = config.get("API_KEYS.b2.access_key")
    secret_key = config.get("API_KEYS.b2.secret_key")
    bucket_name = config.get("API_KEYS.b2.bucket_name")

    logger.debug(f"B2 Config: endpoint={endpoint}, access_key={access_key[:4]}..., secret_key={secret_key[:4]}..., bucket_name={bucket_name}")

    if not all([endpoint, access_key, secret_key, bucket_name]):
        raise ValueError("Отсутствуют обязательные ключи для B2: endpoint, access_key, secret_key или bucket_name")

    try:
        return boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
    except Exception as e:
        handle_error("B2 Client Initialization Error", str(e))
        raise

def download_config_public():
    """Скачивает публичный конфиг из B2."""
    config_public_path = config.get("FILE_PATHS.config_public")
    if not config_public_path:
        raise ValueError("config_public_path не указан в конфигурации")
    try:
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name не указан в конфигурации или переменных окружения")
        os.makedirs(os.path.dirname(config_public_path), exist_ok=True)
        s3.download_file(bucket_name, config_public_path, config_public_path)
        logger.info(f"✅ Файл config_public.json успешно загружен из B2 в {config_public_path}")
    except ValueError as ve:
        handle_error("Download Config Public Error", str(ve))
    except Exception as e:
        handle_error("Download Config Public Error", str(e))

def generate_file_id():
    """Генерирует уникальный ID файла на основе текущей даты и времени."""
    try:
        now = datetime.utcnow()
        date_part = now.strftime("%Y%m%d")
        time_part = now.strftime("%H%M")
        return f"{date_part}-{time_part}.json"
    except Exception as e:
        handle_error("Generate File ID Error", str(e))
        raise

def save_generation_id_to_config(file_id):
    """Сохраняет ID генерации в config_gen.json."""
    if not file_id or not isinstance(file_id, str):
        raise ValueError("file_id должен быть непустой строкой")
    config_gen_path = os.path.join("config", "config_gen.json")
    os.makedirs(os.path.dirname(config_gen_path), exist_ok=True)
    try:
        with open(config_gen_path, "w", encoding="utf-8") as file:
            json.dump({"generation_id": file_id}, file, ensure_ascii=False, indent=4)
        logger.info(f"✅ ID генерации '{file_id}' успешно сохранён в config_gen.json")
    except Exception as e:
        handle_error("Save Generation ID Error", str(e))

def save_to_b2(folder, content):
    """Сохраняет контент в B2."""
    if not folder or not isinstance(folder, str):
        raise ValueError("folder должен быть непустой строкой")
    if not isinstance(content, dict):
        raise ValueError("content должен быть словарем")

    try:
        file_id = generate_file_id()
        save_generation_id_to_config(file_id)
        logger.info(f"🔄 Сохранение контента в папку B2: {folder} с именем файла {file_id}")
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name не указан в конфигурации или переменных окружения")
        s3_key = f"{folder.rstrip('/')}/{file_id}"
        json_bytes = io.BytesIO(json.dumps(content, ensure_ascii=False, indent=4).encode("utf-8"))
        s3.upload_fileobj(json_bytes, bucket_name, s3_key)
        logger.info(f"✅ Контент успешно сохранён в B2: {s3_key}")
    except ValueError as ve:
        handle_error("B2 Upload Error", str(ve))
    except Exception as e:
        handle_error("B2 Upload Error", str(e))

def run_generate_media():
    """Запускает скрипт generate_media.py."""
    try:
        scripts_folder = config.get("FILE_PATHS.scripts_folder", "scripts")
        script_path = os.path.join(scripts_folder, "generate_media.py")
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт generate_media.py не найден по пути: {script_path}")
        logger.info(f"🔄 Запуск скрипта: {script_path}")
        subprocess.run(["python", script_path], check=True)
        logger.info(f"✅ Скрипт {script_path} выполнен успешно.")
    except FileNotFoundError as e:
        handle_error("Script Execution Error", str(e))
    except subprocess.CalledProcessError as e:
        handle_error("Script Execution Error", str(e))
    except Exception as e:
        handle_error("Script Execution Error", str(e))

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
        self.used_short_topics = []  # Инициализация списка использованных тем
        if not self.openai_api_key:
            logger.error("❌ Переменная окружения OPENAI_API_KEY не задана!")
            raise EnvironmentError("Переменная окружения OPENAI_API_KEY отсутствует.")

    def adapt_prompts(self):
        """Адаптирует промпты на основе обратной связи, если включена адаптация."""
        if not self.adaptation_enabled:
            self.logger.info("🔄 Адаптация промптов отключена.")
            return
        if not isinstance(self.adaptation_params, dict):
            raise ValueError("adaptation_params должен быть словарем")
        self.logger.info("🔄 Применяю адаптацию промптов на основе обратной связи...")
        for key, value in self.adaptation_params.items():
            self.logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def clear_generated_content(self):
        """Очищает файл сгенерированного контента."""
        if not self.content_output_path:
            raise ValueError("content_output_path не указан")
        try:
            folder = os.path.dirname(self.content_output_path)
            if folder and not os.path.exists(folder):
                os.makedirs(folder)
                self.logger.info(f"📁 Папка для сохранения данных создана: {folder}")
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump({}, file, ensure_ascii=False, indent=4)
            self.logger.info("✅ Файл успешно очищен.")
        except Exception as e:
            handle_error("Clear Content Error", str(e))

    def generate_topic(self):
        """Генерирует новую тему на основе доступных фокусов."""
        logger.info("🔄 Генерация новой темы...")
        try:
            focus_areas = ", ".join(self.config.get('CONTENT.topic.focus_areas'))
            exclusions = ", ".join(self.used_short_topics)
            prompt = self.config.get('CONTENT.topic.prompt_template').format(
                focus_areas=focus_areas,
                exclusions=exclusions
            )
            logger.info(f"📝 Промпт для генерации темы: {prompt[:100]}...")
            response = openai.ChatCompletion.create(
                model=self.config.get('API_KEYS.openai.model'),
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.config.get('API_KEYS.openai.max_tokens_topic'),
                temperature=self.config.get('CONTENT.text.temperature')
            )
            result = response['choices'][0]['message']['content'].strip()
            topic_data = json.loads(result)
            return topic_data
        except Exception as e:
            handle_error("Topic Generation Error", str(e))
            return None

    def request_openai(self, prompt, max_tokens, temperature):
        """Отправляет запрос к OpenAI API."""
        if not prompt or not isinstance(prompt, str):
            raise ValueError("prompt должен быть непустой строкой")
        if not isinstance(max_tokens, int) or max_tokens <= 0:
            raise ValueError("max_tokens должен быть положительным целым числом")
        if not isinstance(temperature, float) or temperature < 0 or temperature > 1:
            raise ValueError("temperature должен быть числом от 0 до 1")
        try:
            openai.api_key = self.openai_api_key
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return response['choices'][0]['message']['content'].strip()
        except openai.error.OpenAIError as e:
            self.logger.error(f"❌ Ошибка OpenAI API: {e}")
            raise
        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка при работе с OpenAI: {e}")
            raise

    def generate_text(self, topic, content_data):
        """Генерирует текст на основе темы."""
        if not topic or not isinstance(topic, str):
            raise ValueError("topic должен быть непустой строкой")
        if not isinstance(content_data, dict):
            raise ValueError("content_data должен быть словарем")

        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt_template = self.config['CONTENT']['tragic_text']['prompt_template']
            temperature = self.config['CONTENT']['tragic_text']['temperature']
            max_tokens = self.config['CONTENT']['tragic_text']['max_length']
        else:
            prompt_template = self.config['CONTENT']['text']['prompt_template']
            temperature = self.config['CONTENT']['text']['temperature']
            max_tokens = self.config['CONTENT']['text']['max_length']

        prompt = prompt_template.format(topic=topic)
        text = self.request_openai(prompt, max_tokens, temperature)
        if text:
            self.logger.info(f"✅ Текст успешно сгенерирован: {text[:50]}...")
            return text
        else:
            self.logger.warning("⚠️ OpenAI вернул пустой ответ для текста.")
            return ""

    def generate_sarcastic_comment(self, text, content_data):
        """Генерирует саркастический комментарий."""
        if not self.config['SARCASM']['enabled']:
            self.logger.info("🔕 Сарказм отключён в конфигурации.")
            return ""
        if not text or not isinstance(text, str):
            raise ValueError("text должен быть непустой строкой")
        if not isinstance(content_data, dict):
            raise ValueError("content_data должен быть словарем")

        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt_template = self.config['SARCASM'].get('tragic_comment_prompt',
                                                         self.config['SARCASM']['comment_prompt'])
            temperature = self.config['SARCASM'].get('tragic_comment_temperature', 0.8)
        else:
            prompt_template = self.config['SARCASM']['comment_prompt']
            temperature = self.config['SARCASM'].get('comment_temperature', 0.8)

        prompt = prompt_template.format(text=text)
        comment = self.request_openai(prompt, self.config['SARCASM']['max_tokens_comment'], temperature)
        if comment:
            self.logger.info(f"✅ Саркастический комментарий: {comment}")
            return comment
        else:
            self.logger.warning("⚠️ OpenAI вернул пустой ответ для комментария.")
            return ""

    def generate_interactive_poll(self, text, content_data):
        """Генерирует интерактивный опрос."""
        if not self.config['SARCASM']['enabled']:
            self.logger.info("🔕 Саркастический опрос отключён в конфигурации.")
            return {}
        if not text or not isinstance(text, str):
            raise ValueError("text должен быть непустой строкой")
        if not isinstance(content_data, dict):
            raise ValueError("content_data должен быть словарем")

        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt_template = self.config['SARCASM'].get('tragic_question_prompt',
                                                         self.config['SARCASM']['question_prompt'])
            temperature = self.config['SARCASM'].get('tragic_poll_temperature', 0.9)
        else:
            prompt_template = self.config['SARCASM']['question_prompt']
            temperature = self.config['SARCASM'].get('poll_temperature', 0.9)

        prompt = prompt_template.format(text=text)
        poll_text = self.request_openai(prompt, self.config['SARCASM']['max_tokens_poll'], temperature)
        self.logger.info(f"🛑 Ответ OpenAI для опроса: {poll_text}")
        try:
            question_match = re.search(r"\[QUESTION\]:\s*(.+)", poll_text)
            option1_match = re.search(r"\[OPTION1\]:\s*(.+)", poll_text)
            option2_match = re.search(r"\[OPTION2\]:\s*(.+)", poll_text)
            option3_match = re.search(r"\[OPTION3\]:\s*(.+)", poll_text)
            if all([question_match, option1_match, option2_match, option3_match]):
                poll_data = {
                    "question": question_match.group(1).strip(),
                    "options": [
                        option1_match.group(1).strip(),
                        option2_match.group(1).strip(),
                        option3_match.group(1).strip()
                    ]
                }
                self.logger.info(f"✅ Опрос сгенерирован: {poll_data}")
                return poll_data
            else:
                self.logger.error("❌ Неверный формат опроса от OpenAI.")
                return {}
        except Exception as e:
            handle_error("Sarcasm Poll Generation Error", str(e))
            return {}

    def save_to_generated_content(self, stage, data):
        """Сохраняет данные в файл сгенерированного контента."""
        if not stage or not isinstance(stage, str):
            raise ValueError("stage должен быть непустой строкой")
        if not isinstance(data, dict):
            raise ValueError("data должен быть словарем")

        try:
            folder = os.path.dirname(self.content_output_path) or "."
            if not os.path.exists(folder):
                os.makedirs(folder)
                self.logger.info(f"📁 Папка создана: {folder}")
            if os.path.exists(self.content_output_path):
                with open(self.content_output_path, 'r', encoding='utf-8') as file:
                    result_data = json.load(file)
            else:
                result_data = {}
            result_data["timestamp"] = datetime.utcnow().isoformat()
            result_data[stage] = data
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump(result_data, file, ensure_ascii=False, indent=4)
            self.logger.info(f"✅ Данные сохранены на этапе {stage}")
        except Exception as e:
            handle_error("Save to Generated Content Error", str(e))

    def critique_content(self, content):
        """Критикует контент с помощью OpenAI."""
        if not content or not isinstance(content, str):
            raise ValueError("content должен быть непустой строкой")
        try:
            prompt_template = self.config.get('CONTENT.critique.prompt_template')
            if not prompt_template:
                raise ValueError("prompt_template для критики не указан")
            prompt = prompt_template.format(content=content)
            critique = self.request_openai(prompt, self.config.get('CONTENT.critique.max_tokens', 200),
                                          self.temperature)
            self.logger.info("✅ Критика успешно завершена.")
            return critique
        except ValueError as ve:
            handle_error("Critique Error", str(ve))
        except Exception as e:
            handle_error("Critique Error", str(e))
        return "Критика текста завершилась ошибкой."

    def analyze_topic_generation(self):
        """Анализирует темы из архива и обратной связи."""
        try:
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
            valid_focus_areas = self.get_valid_focus_areas()
            combined_topics = list(set(positive_feedback_topics + successful_topics + valid_focus_areas))
            self.logger.info(f"📊 Итоговый список тем: {combined_topics}")
            return combined_topics
        except Exception as e:
            handle_error("Topic Analysis Error", str(e))
            return []

    def get_valid_focus_areas(self):
        """Получает доступные фокусные области."""
        try:
            tracker_file = self.config.get('FILE_PATHS.focus_tracker', 'data/focus_tracker.json')
            focus_areas = self.config.get('CONTENT.topic.focus_areas', [])
            if not isinstance(focus_areas, list):
                raise ValueError("focus_areas должен быть списком")
            if os.path.exists(tracker_file):
                with open(tracker_file, 'r', encoding='utf-8') as file:
                    focus_tracker = json.load(file)
                excluded_foci = focus_tracker[:10]
            else:
                excluded_foci = []
            valid_focus_areas = [focus for focus in focus_areas if focus not in excluded_foci]
            self.logger.info(f"✅ Доступные фокусы: {valid_focus_areas}")
            return valid_focus_areas
        except ValueError as ve:
            handle_error("Focus Area Filtering Error", str(ve))
        except Exception as e:
            handle_error("Focus Area Filtering Error", str(e))
        return []

    def prioritize_focus_from_feedback_and_archive(self, valid_focus_areas):
        """Приоритизирует фокус на основе обратной связи и архива."""
        if not isinstance(valid_focus_areas, list):
            raise ValueError("valid_focus_areas должен быть списком")
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
        except ValueError as ve:
            handle_error("Focus Prioritization Error", str(ve))
        except Exception as e:
            handle_error("Focus Prioritization Error", str(e))
        return None

    def run(self):
        """Запускает процесс генерации контента."""
        try:
            download_config_public()
            with open(config.get("FILE_PATHS.config_public"), "r", encoding="utf-8") as file:
                config_public = json.load(file)
            empty_folders = config_public.get("empty", [])
            if not empty_folders:
                self.logger.info("✅ Нет пустых папок. Процесс завершён.")
                return

            self.adapt_prompts()
            self.clear_generated_content()

            valid_topics = self.analyze_topic_generation()
            chosen_focus = self.prioritize_focus_from_feedback_and_archive(valid_topics)
            if chosen_focus:
                self.logger.info(f"✅ Выбранный фокус: {chosen_focus}")
            topic = self.generate_topic()
            if not topic:
                self.logger.error("❌ Тема не сгенерирована, процесс остановлен.")
                return
            content_data = {"topic": topic}
            text = self.generate_text(topic["full_topic"], content_data)
            critique = self.critique_content(text)
            self.save_to_generated_content("critique", {"critique": critique})

            sarcastic_comment = self.generate_sarcastic_comment(text, content_data)
            sarcastic_poll = self.generate_interactive_poll(text, content_data)
            content_dict = {
                "topic": topic,
                "content": text,
                "sarcasm": {
                    "comment": sarcastic_comment,
                    "poll": sarcastic_poll
                }
            }
            target_folder = empty_folders[0]
            save_to_b2(target_folder, content_dict)

            with open(os.path.join("config", "config_gen.json"), "r", encoding="utf-8") as gen_file:
                config_gen_content = json.load(gen_file)
                generation_id = config_gen_content["generation_id"]

            create_and_upload_image(target_folder, generation_id)
            run_generate_media()
            self.logger.info("✅ Генерация контента завершена.")
        except Exception as e:
            handle_error("Run Error", str(e))

if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()