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
import random
from modules.config_manager import ConfigManager
from modules.logger import get_logger
from modules.error_handler import handle_error
from datetime import datetime
from modules.utils import ensure_directory_exists
from PIL import Image, ImageDraw

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))

logger = get_logger("generate_content")
config = ConfigManager()

logger = get_logger("generate_media_launcher")

def create_and_upload_image(folder, generation_id):
    """Создает имитацию изображения и загружает его в ту же папку в B2."""
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
        s3_key = f"{folder.rstrip('/')}/{file_name}"
        s3.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"✅ Изображение успешно загружено в B2: {s3_key}")
        os.remove(local_file_path)
    except Exception as e:
        handle_error("Image Upload Error", str(e), e)

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
        handle_error("B2 Client Initialization Error", str(e), e)

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
        handle_error("Download Config Public Error", str(e), e)

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
        handle_error("Save Generation ID Error", str(e), e)

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
        handle_error("B2 Upload Error", str(e), e)

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
            handle_error("Clear Content Error", f"Нет прав на запись в файл: {self.content_output_path}", PermissionError())
        except Exception as e:
            handle_error("Clear Content Error", str(e), e)

    def generate_topic(self):
        if not self.config.get('CONTENT.topic.enabled', True):
            logger.info("🔕 Генерация темы отключена.")
            return "", {}
        try:
            focus_areas = self.config.get("CONTENT.topic.focus_areas", [])
            if not focus_areas:
                logger.error("❌ Список focus_areas пуст!")
                raise ValueError("focus_areas не задан в конфигурации.")
            selected_focus = random.choice(focus_areas)
            prompt_template = self.config.get('CONTENT.topic.prompt_template')
            # Предполагаем, что exclusions может быть в конфиге или пустым списком
            exclusions = self.config.get('CONTENT.topic.exclusions', [])
            prompt = prompt_template.format(focus_areas=selected_focus, exclusions=', '.join(exclusions))
            logger.info(f"🔄 Запрос к OpenAI для генерации темы с фокусом: {selected_focus}")
            topic_response = self.request_openai(prompt)
            # Ожидаем JSON-формат из логов
            try:
                topic_data = json.loads(topic_response)
                topic = topic_data.get("full_topic", "")
                short_topic = topic_data.get("short_topic", "")
            except json.JSONDecodeError:
                logger.warning("⚠️ OpenAI вернул не JSON, используем как строку.")
                topic = topic_response
            self.save_to_generated_content("topic", {"topic": topic})
            is_tragic = selected_focus.endswith('(т)')
            content_data = {"theme": "tragic" if is_tragic else "normal"}
            logger.info(f"✅ Тема успешно сгенерирована: {topic}, трагическая: {is_tragic}")
            return topic, content_data
        except Exception as e:
            handle_error("Topic Generation Error", str(e), e)
            return "", {}

    def request_openai(self, prompt):
        try:
            openai.api_key = self.openai_api_key
            max_tokens = self.config.get("API_KEYS.openai.max_tokens_text", 750)
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

    def generate_sarcastic_comment(self, text, content_data={}):
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.comment_enabled', True):
            self.logger.info("🔕 Генерация саркастического комментария отключена.")
            return ""
        self.logger.info(f"🔎 Debug: Промпт для саркастического комментария: {self.config.get('SARCASM.comment_prompt')}")
        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt = self.config.get('SARCASM.tragic_comment_prompt').format(text=text)
            temperature = self.config.get('SARCASM.tragic_comment_temperature', 0.6)
        else:
            prompt = self.config.get('SARCASM.comment_prompt').format(text=text)
            temperature = self.config.get('SARCASM.comment_temperature', 0.8)
        max_tokens = self.config.get("SARCASM.max_tokens_comment", 150)
        self.logger.info(f"🔎 Debug: Используемый max_tokens_comment = {max_tokens}")
        try:
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=temperature
            )
            comment = response['choices'][0]['message']['content'].strip()
            self.logger.info(f"✅ Саркастический комментарий сгенерирован: {comment}")
            return comment
        except Exception as e:
            self.logger.error(f"❌ Ошибка генерации саркастического комментария: {e}")
            return ""

    def generate_interactive_poll(self, text, content_data={}):
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.poll_enabled', True):
            self.logger.info("🔕 Генерация саркастического опроса отключена.")
            return {}
        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt = self.config.get('SARCASM.tragic_question_prompt').format(text=text)
            temperature = self.config.get('SARCASM.tragic_poll_temperature', 0.6)
        else:
            prompt = self.config.get('SARCASM.question_prompt').format(text=text)
            temperature = self.config.get('SARCASM.poll_temperature', 0.9)
        try:
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.config.get('SARCASM.max_tokens_poll', 250),
                temperature=temperature
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
            handle_error("Sarcasm Poll Generation Error", str(e), e)
            return {}

    def save_to_generated_content(self, stage, data):
        try:
            if not self.content_output_path:
                raise ValueError("❌ Ошибка: self.content_output_path пустой!")
            logger.info(f"🔄 Обновление данных и сохранение в файл: {self.content_output_path}")
            folder = os.path.dirname(self.content_output_path) or "."
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
            handle_error("Save to Generated Content Error", f"Файл не найден: {self.content_output_path}", FileNotFoundError())
        except PermissionError:
            handle_error("Save to Generated Content Error", f"Нет прав на запись в файл: {self.content_output_path}", PermissionError())
        except Exception as e:
            handle_error("Save to Generated Content Error", str(e), e)

    def critique_content(self, content, topic):
        if not self.config.get('CONTENT.critique.enabled', True):
            self.logger.info("🔕 Критика контента отключена.")
            return "Критика отключена в конфигурации."
        try:
            self.logger.info("🔄 Выполняется критика текста через OpenAI...")
            prompt_template = self.config.get('CONTENT.critique.prompt_template')
            prompt = prompt_template.format(content=content, topic=topic)
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=self.config.get('CONTENT.critique.max_tokens', 300),
                temperature=self.config.get('CONTENT.critique.temperature', 0.3)
            )
            critique = response['choices'][0]['message']['content'].strip()
            self.logger.info("✅ Критика успешно завершена.")
            return critique
        except Exception as e:
            handle_error("Critique Error", str(e), e)
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
            handle_error("Topic Analysis Error", str(e), e)
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
            handle_error("Focus Area Filtering Error", str(e), e)
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
            handle_error("Focus Prioritization Error", str(e), e)
            return None

    def run(self):
        """Основной процесс генерации контента."""
        try:
            if not self.config.get('CONTENT.topic.enabled', True):
                logger.error("❌ Генерация темы отключена, дальнейшая работа невозможна.")
                sys.exit(1)
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
            topic, content_data = self.generate_topic()
            if not topic:
                logger.error("❌ Тема не сгенерирована, прерываем выполнение.")
                sys.exit(1)
            if self.config.get('CONTENT.text.enabled', True) or self.config.get('CONTENT.tragic_text.enabled', True):
                if "theme" in content_data and content_data["theme"] == "tragic" and self.config.get(
                        'CONTENT.tragic_text.enabled', True):
                    text_initial = self.request_openai(
                        self.config.get('CONTENT.tragic_text.prompt_template').format(topic=topic))
                else:
                    text_initial = self.request_openai(
                        self.config.get('CONTENT.text.prompt_template').format(topic=topic))
                critique = self.critique_content(text_initial, topic)
                self.save_to_generated_content("critique", {"critique": critique})
            else:
                text_initial = ""
                logger.info("🔕 Генерация текста отключена.")
            if text_initial:
                sarcastic_comment = self.generate_sarcastic_comment(text_initial, content_data)
                sarcastic_poll = self.generate_interactive_poll(text_initial, content_data)
                self.save_to_generated_content("sarcasm", {
                    "comment": sarcastic_comment,
                    "poll": sarcastic_poll
                })
            final_text = text_initial.strip()
            target_folder = empty_folders[0]
            content_dict = {
                "topic": topic,
                "content": final_text,
                "sarcasm": {
                    "comment": sarcastic_comment,
                    "poll": sarcastic_poll
                }
            }
            save_to_b2(target_folder, content_dict)
            with open(os.path.join("config", "config_gen.json"), "r", encoding="utf-8") as gen_file:
                config_gen_content = json.load(gen_file)
                generation_id = config_gen_content["generation_id"]
            create_and_upload_image(target_folder, generation_id)
            logger.info(f"📄 Содержимое config_public.json: {json.dumps(config_public, ensure_ascii=False, indent=4)}")
            logger.info(f"📄 Содержимое config_gen.json: {json.dumps(config_gen_content, ensure_ascii=False, indent=4)}")
            run_generate_media()  # Выполняется, но не прерывает процесс при ошибке
            self.logger.info("✅ Генерация контента завершена.")
        except Exception as e:
            handle_error("Run Error", "Ошибка в основном процессе генерации", e)
            logger.error("❌ Процесс генерации контента прерван из-за критической ошибки.")
            sys.exit(1)

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
        handle_error("Script Execution Error", "Ошибка при выполнении generate_media.py", e)
        logger.warning("⚠️ Генерация медиа не удалась, продолжаем без медиа.")
    except FileNotFoundError as e:
        handle_error("File Not Found Error", f"Скрипт не найден: {script_path}", e)
        logger.warning("⚠️ Скрипт generate_media.py отсутствует, продолжаем без медиа.")
    except Exception as e:
        handle_error("Unknown Error", "Неизвестная ошибка при запуске generate_media.py", e)
        logger.warning("⚠️ Неизвестная ошибка в generate_media, продолжаем без медиа.")

if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()