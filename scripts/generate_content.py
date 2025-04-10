import os
import json
import sys
import requests
import openai
import textstat
import spacy
import re
import subprocess
import random
from datetime import datetime
from modules.config_manager import ConfigManager
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists
from modules.api_clients import get_b2_client

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
logger = get_logger("generate_content")
config = ConfigManager()

FAILSAFE_PATH = "config/FailSafeVault.json"
TRACKER_PATH = "config/topics_tracker.json"  # Локальный путь
TRACKER_B2_PATH = "config/topics_tracker.json"  # Путь в B2
CONFIG_GEN_PATH = "config/config_gen.json"  # Путь в B2
CONFIG_GEN_LOCAL_PATH = "config/config_gen.json"  # Локальный путь
CONTENT_OUTPUT_PATH = "generated_content.json"  # Локальный путь для контента
TOPICS_TRACKER_PATH = "config/topics_tracker.json"  # Путь в B2
TOPICS_TRACKER_LOCAL_PATH = "config/topics_tracker.json"  # Локальный путь
SCRIPTS_FOLDER = "scripts/"
GENERATE_MEDIA_SCRIPT = os.path.join(SCRIPTS_FOLDER, "generate_media.py")
B2_STORAGE_MANAGER_SCRIPT = os.path.join(SCRIPTS_FOLDER, "b2_storage_manager.py")
TARGET_FOLDER = "666/"

ensure_directory_exists("config")

def download_config_public():
    bucket_name = os.getenv("B2_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
    local_path = config.get("FILE_PATHS.config_public", "config_public.json")
    try:
        b2_client = get_b2_client()
        bucket = b2_client.get_bucket_by_name(bucket_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        bucket.download_file_by_name("config/config_public.json", local_path)
        logger.info(f"✅ Файл config_public.json успешно загружен из B2 в {local_path}")
    except Exception as e:
        handle_error(logger, "Download Config Public Error", e)

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
    try:
        file_id = generate_file_id()
        save_generation_id_to_config(file_id)
        logger.info(f"🔄 Сохранение контента в папку B2: {folder} с именем файла {file_id}")
        b2_client = get_b2_client()
        bucket_name = os.getenv("B2_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
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
        temp_path = f"temp_{file_id}"
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
        bucket = b2_client.get_bucket_by_name(bucket_name)
        bucket.upload_local_file(local_file=temp_path, file_name=s3_key)
        os.remove(temp_path)
        logger.info(f"✅ Контент успешно сохранён в B2: {s3_key}")
    except Exception as e:
        handle_error(logger, "B2 Upload Error", e)
        failed_path = f"failed_{file_id}"
        with open(failed_path, "w", encoding="utf-8") as f:
            json.dump(content, f, ensure_ascii=False, indent=4)
        logger.warning(f"⚠️ Сохранена резервная копия в {failed_path}")

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

    def generate_topic(self, tracker):
        """
        Генерирует уникальную тему на основе доступных фокусов из трекера.

        Args:
            tracker (dict): Словарь с данными о предыдущих темах и фокусах.

        Returns:
            tuple: (full_topic, content_data) - сгенерированная тема и связанные данные.

        Raises:
            ValueError: Если фокусы недоступны или ответ OpenAI некорректен.
        """
        valid_focuses = self.get_valid_focus_areas(tracker)
        if not valid_focuses:
            self.logger.error("❌ Нет доступных фокусов")
            raise ValueError("Все фокусы использованы")

        selected_focus = random.choice(valid_focuses)
        used_labels = tracker["focus_data"].get(selected_focus, [])

        # Используем get() вместо прямого доступа
        prompt_template = self.config.get("CONTENT", {}).get("topic", {}).get("prompt_template", "")
        if not prompt_template:
            self.logger.error("❌ Шаблон промпта для темы не найден в конфигурации")
            raise ValueError("Шаблон промпта для темы не найден")

        prompt = prompt_template.format(
            focus_areas=selected_focus,
            exclusions=", ".join(used_labels)
        )
        topic_response = self.request_openai(prompt)

        # Парсинг ответа OpenAI (учитываем, что JSON не гарантирован)
        try:
            topic_data = json.loads(topic_response)
            full_topic = topic_data["full_topic"]
            short_topic = topic_data["short_topic"]
        except json.JSONDecodeError:
            self.logger.warning("⚠️ OpenAI вернул не JSON, парсим вручную")
            # Предполагаем формат: "Full topic: текст\nShort topic: текст" или просто строка
            lines = topic_response.strip().split("\n")
            full_topic = lines[0].replace("Full topic:", "").strip() if "Full topic:" in lines[0] else lines[0].strip()
            short_topic = lines[1].replace("Short topic:", "").strip() if len(lines) > 1 and "Short topic:" in lines[
                1] else full_topic[:50]

        self.update_tracker(selected_focus, short_topic)
        self.save_to_generated_content("topic", {"full_topic": full_topic, "short_topic": short_topic})
        return full_topic, {"theme": "tragic" if "(т)" in selected_focus else "normal"}

    def update_tracker(self, focus, short_topic):
        with open(TRACKER_PATH, 'r', encoding='utf-8') as f:
            tracker = json.load(f)
        used_focuses = tracker["used_focuses"]
        focus_data = tracker["focus_data"]
        if focus in used_focuses:
            used_focuses.remove(focus)
        used_focuses.insert(0, focus)
        if len(used_focuses) > 15:
            used_focuses.pop()
        focus_data.setdefault(focus, []).insert(0, short_topic)
        if len(focus_data[focus]) > 5:
            focus_data[focus].pop()
        tracker["used_focuses"] = used_focuses
        tracker["focus_data"] = focus_data
        with open(TRACKER_PATH, 'w', encoding='utf-8') as f:
            json.dump(tracker, f, ensure_ascii=False, indent=4)
        self.sync_tracker_to_b2()

    def sync_tracker_to_b2(self):
        b2_client = get_b2_client()
        bucket_name = os.getenv("B2_BUCKET_NAME")
        local_path = TRACKER_PATH  # "config/topics_tracker.json"
        b2_path = TRACKER_B2_PATH  # "config/topics_tracker.json"
        if not bucket_name:
            raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
        try:
            bucket = b2_client.get_bucket_by_name(bucket_name)
            bucket.upload_local_file(local_file=local_path, file_name=b2_path)
            self.logger.info(f"✅ {b2_path} синхронизирован с B2")
        except Exception as e:
            self.logger.warning(f"⚠️ Не удалось загрузить {b2_path} в B2: {e}")

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

    def generate_sarcasm(self, text, content_data={}):
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

    def generate_sarcasm_poll(self, text, content_data={}):
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

    def generate_script_and_frame(self, topic):
        """Генерация сценария и описания первого кадра."""
        creative_prompts = self.config.get("creative_prompts")
        if not creative_prompts or not isinstance(creative_prompts, list):
            logger.error(f"❌ Ошибка: 'creative_prompts' не найден или не является списком")
            raise ValueError("Список 'creative_prompts' не найден")
        selected_prompt = random.choice(creative_prompts)
        combined_prompt = self.config.get("PROMPTS.user_prompt_combined").replace("{topic}", topic).replace(
            "Затем выберите один творческий подход из 'creative_prompts' в конфиге",
            f"Затем используйте творческий подход: '{selected_prompt}'"
        )
        for attempt in range(self.max_attempts):
            try:
                logger.info(f"🔎 Генерация сценария для '{topic[:100]}' (попытка {attempt + 1}/{self.max_attempts})")
                response = openai.ChatCompletion.create(
                    model=self.config.get("OPENAI_SETTINGS.model", "gpt-4o"),
                    messages=[{"role": "user", "content": combined_prompt}],
                    max_tokens=self.config.get("OPENAI_SETTINGS.max_tokens", 1000),
                    temperature=self.config.get("OPENAI_SETTINGS.temperature", 0.7),
                )
                combined_response = response['choices'][0]['message']['content'].strip()
                if len(combined_response) < self.config.get("VISUAL_ANALYSIS.min_script_length", 200):
                    logger.error(f"❌ Ответ слишком короткий: {len(combined_response)} символов")
                    continue
                if "First Frame Description:" not in combined_response or "End of Description" not in combined_response:
                    logger.error("❌ Маркеры кадра не найдены в ответе!")
                    continue
                script_text = combined_response.split("First Frame Description:")[0].strip()
                first_frame_description = \
                    combined_response.split("First Frame Description:")[1].split("End of Description")[0].strip()
                return script_text, first_frame_description
            except Exception as e:
                handle_error(logger, f"Ошибка генерации сценария (попытка {attempt + 1}/{self.max_attempts})", e)
                if attempt == self.max_attempts - 1:
                    logger.error("❌ Превышено максимальное количество попыток генерации сценария.")
                    return None, None
        return None, None

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
            tracker = self.load_tracker()
            valid_focus_areas = self.get_valid_focus_areas(tracker)
            combined_topics = list(set(positive_feedback_topics + successful_topics + valid_focus_areas))
            self.logger.info(f"📊 Итоговый список тем: {combined_topics}")
            return combined_topics
        except Exception as e:
            handle_error("Topic Analysis Error", str(e), e)
            return []

    def get_valid_focus_areas(self, tracker):
        all_focuses = tracker["all_focuses"]
        used_focuses = tracker["used_focuses"]
        valid_focuses = [f for f in all_focuses if f not in used_focuses]
        self.logger.info(f"✅ Доступные фокусы: {valid_focuses}")
        return valid_focuses

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

    def load_tracker(self):
        bucket_name = os.getenv("B2_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("❌ Переменная окружения B2_BUCKET_NAME не задана")
        local_path = TRACKER_PATH  # "config/topics_tracker.json"
        b2_path = TRACKER_B2_PATH  # "config/topics_tracker.json"
        os.makedirs(os.path.dirname(local_path), exist_ok=True)  # Создаём папку config/
        b2_client = get_b2_client()
        tracker_updated = False
        try:
            bucket = b2_client.get_bucket_by_name(bucket_name)
            bucket.download_file_by_name(b2_path, local_path)
            self.logger.info(f"✅ Загружен {b2_path} из B2 в {local_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Не удалось загрузить {b2_path} из B2: {e}")
            if not os.path.exists(local_path):
                self.logger.info("Создаём новый topics_tracker.json из FailSafeVault")
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(FAILSAFE_PATH, 'r', encoding='utf-8') as f:
                    failsafe = json.load(f)
                tracker = {
                    "all_focuses": failsafe["focuses"],
                    "used_focuses": [],
                    "focus_data": {}
                }
                with open(local_path, 'w', encoding='utf-8') as f:
                    json.dump(tracker, f, ensure_ascii=False, indent=4)
                tracker_updated = True
        with open(local_path, 'r', encoding='utf-8') as f:
            tracker = json.load(f)
        if "all_focuses" not in tracker:
            self.logger.info("Обновляем старый трекер: добавляем all_focuses")
            with open(FAILSAFE_PATH, 'r', encoding='utf-8') as f:
                failsafe = json.load(f)
            tracker["all_focuses"] = failsafe["focuses"]
            tracker.setdefault("used_focuses", [])
            tracker.setdefault("focus_data", {})
            with open(local_path, 'w', encoding='utf-8') as f:
                json.dump(tracker, f, ensure_ascii=False, indent=4)
            tracker_updated = True
        if tracker_updated:
            self.sync_tracker_to_b2()
        return tracker

    def run(self):
        """Основной процесс генерации контента."""
        logger.info(">>> Начало генерации контента (метод run)")
        import argparse
        parser = argparse.ArgumentParser(description="Generate Content")
        parser.add_argument("--generation_id", type=str, help="ID for content generation")
        args = parser.parse_args()

        try:
            # Проверка и получение generation_id
            if args.generation_id:
                generation_id = args.generation_id
                logger.info(f"ℹ️ Используем переданный generation_id: {generation_id}")
            else:
                with open(os.path.join("config", "config_gen.json"), "r", encoding="utf-8") as gen_file:
                    config_gen_content = json.load(gen_file)
                    generation_id = config_gen_content.get("generation_id")
                if not generation_id:
                    generation_id = generate_file_id()
                    save_generation_id_to_config(generation_id)
                    logger.info(f"ℹ️ Сгенерирован новый generation_id: {generation_id}")
                else:
                    logger.info(f"ℹ️ Используем generation_id из config_gen.json: {generation_id}")

            self.adapt_prompts()
            tracker = self.load_tracker()  # Загрузка трекера для защиты от повторов
            topic, content_data = self.generate_topic(tracker)  # Генерация темы
            if not topic:
                logger.error("❌ Тема не сгенерирована, прерываем выполнение.")
                return

            # Генерация текста
            if self.config.get('CONTENT.text.enabled', True) or self.config.get('CONTENT.tragic_text.enabled', True):
                if "theme" in content_data and content_data["theme"] == "tragic" and self.config.get(
                        'CONTENT.tragic_text.enabled', True):
                    text_initial = self.request_openai(
                        self.config.get('CONTENT.tragic_text.prompt_template', "").format(topic=topic))
                else:
                    text_initial = self.request_openai(
                        self.config.get('CONTENT.text.prompt_template', "").format(topic=topic))
                critique = self.critique_content(text_initial, topic)
                self.save_to_generated_content("critique", {"critique": critique})
            else:
                text_initial = ""
                logger.info("🔕 Генерация текста отключена.")

            # Генерация сарказма
            sarcastic_comment = ""
            sarcastic_poll = {}
            if text_initial:
                sarcastic_comment = self.generate_sarcasm(text_initial, content_data)
                sarcastic_poll = self.generate_sarcasm_poll(text_initial, content_data)

            final_text = text_initial.strip()
            target_folder = "666/"

            # Генерация сценария и первого кадра
            script_text, first_frame_description = self.generate_script_and_frame(topic)
            if not script_text or not first_frame_description:
                logger.error("❌ Не удалось сгенерировать сценарий или описание кадра")
                sys.exit(1)
            logger.info(f"✅ Сценарий сгенерирован: {script_text[:100]}...")
            logger.info(f"✅ Описание первого кадра: {first_frame_description[:100]}...")

            # Сборка полного контента
            content_dict = {
                "topic": topic,
                "content": final_text,
                "sarcasm": {
                    "comment": sarcastic_comment,
                    "poll": sarcastic_poll
                },
                "script": script_text,
                "first_frame_description": first_frame_description
            }
            save_to_b2(target_folder, content_dict)
            logger.info(f"✅ Контент сохранен в B2: 666/{generation_id}.json")

            # Запуск generate_media.py
            scripts_folder = config.get("FILE_PATHS.scripts_folder", "scripts")
            generate_media_path = os.path.join(scripts_folder, "generate_media.py")
            if not os.path.isfile(generate_media_path):
                logger.error(f"❌ Скрипт не найден: {generate_media_path}")
                sys.exit(1)
            logger.info(f"🔄 Запуск generate_media.py с generation_id: {generation_id}")
            try:
                result = subprocess.run([sys.executable, generate_media_path, "--generation_id", generation_id],
                                        check=True)
                if result.returncode == 0:
                    logger.info("✅ generate_media.py выполнен успешно")
                    # Запуск b2_storage_manager.py
                    b2_manager_path = os.path.join(scripts_folder, "b2_storage_manager.py")
                    if not os.path.isfile(b2_manager_path):
                        logger.error(f"❌ Скрипт не найден: {b2_manager_path}")
                        sys.exit(1)
                    logger.info("🔄 Запуск b2_storage_manager.py для продолжения цикла")
                    subprocess.run([sys.executable, b2_manager_path], check=True)
                else:
                    logger.error("❌ Ошибка при выполнении generate_media.py")
                    sys.exit(1)
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Ошибка запуска generate_media.py: {e}")
                sys.exit(1)

            logger.info("✅ Генерация контента завершена.")
        except Exception as e:
            handle_error(self.logger, "Ошибка в основном процессе генерации", e)
            logger.error("❌ Процесс генерации контента прерван из-за критической ошибки.")
            sys.exit(1)

if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()