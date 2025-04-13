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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))
logger = get_logger("generate_content")
config = ConfigManager()

B2_BUCKET_NAME = "boyarinnbotbucket"  # Из конфига
FAILSAFE_PATH = "config/FailSafeVault.json"
TRACKER_PATH = "data/topics_tracker.json"
CONFIG_PUBLIC_LOCAL_PATH = "config/config_public.json"  # Фиксированный локальный путь

def run_generate_media(generation_id):
    try:
        scripts_folder = config.get("FILE_PATHS.scripts_folder", "scripts")
        script_path = os.path.join(scripts_folder, "generate_media.py")
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт generate_media.py не найден: {script_path}")
        logger.info(f"Запуск: {script_path} с generation_id: {generation_id}")
        subprocess.run(["python", script_path, generation_id], check=True)
        logger.info(f"Скрипт {script_path} выполнен.")
    except subprocess.CalledProcessError as e:
        handle_error("Script Error", "Ошибка generate_media.py", e)
        logger.warning("Генерация медиа не удалась, продолжаем.")
    except FileNotFoundError as e:
        handle_error("File Error", f"Скрипт не найден: {script_path}", e)
        logger.warning("generate_media.py отсутствует, продолжаем.")
    except Exception as e:
        handle_error("Unknown Error", "Ошибка запуска generate_media.py", e)
        logger.warning("Неизвестная ошибка, продолжаем.")

def get_b2_client():
    """Создает клиент для работы с Backblaze B2."""
    try:
        return boto3.client(
            's3',
            endpoint_url=os.getenv("B2_ENDPOINT"),
            aws_access_key_id=os.getenv("B2_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("B2_SECRET_KEY")
        )
    except Exception as e:
        handle_error("B2 Client Initialization Error", str(e), e)
        return None

def download_config_public():
    """Загружает файл config_public.json из B2 в локальное хранилище."""
    try:
        s3 = get_b2_client()
        if not s3:
            raise Exception("Не удалось создать клиент B2")
        bucket_name = config.get("API_KEYS.b2.bucket_name", B2_BUCKET_NAME)
        remote_path = "config/config_public.json"
        os.makedirs(os.path.dirname(CONFIG_PUBLIC_LOCAL_PATH), exist_ok=True)
        s3.download_file(bucket_name, remote_path, CONFIG_PUBLIC_LOCAL_PATH)
        logger.info(f"✅ Файл config_public.json успешно загружен из B2 в {CONFIG_PUBLIC_LOCAL_PATH}")
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
        if not s3:
            logger.error("❌ Не удалось создать клиент B2")
            return False

        bucket_name = config.get("API_KEYS.b2.bucket_name", B2_BUCKET_NAME)
        s3_key = f"{folder.rstrip('/')}/{file_id}"

        if not isinstance(content, dict):
            logger.error("❌ Ошибка: Контент должен быть словарём!")
            return False

        sarcasm_data = content.get("sarcasm", {})
        if isinstance(sarcasm_data, str):
            try:
                sarcasm_data = json.loads(sarcasm_data)
                logger.warning("⚠️ Поле 'sarcasm' было строкой, исправляем...")
            except json.JSONDecodeError:
                logger.error("❌ Ошибка: Поле 'sarcasm' имеет неверный формат!")
                return False

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
        return True

    except Exception as e:
        handle_error("B2 Upload Error", str(e), e)
        return False
    
def generate_script_and_frame(topic):
    """Генерирует сценарий и описание первого кадра для видео."""
    try:
        with open("config/config_gen.json", "r", encoding="utf-8") as f:
            config_data = json.load(f)
        USER_PROMPT_COMBINED = config_data.get("PROMPTS.user_prompt_combined")
        OPENAI_MODEL = config_data.get("OPENAI_SETTINGS.model", "gpt-4o")
        OPENAI_MAX_TOKENS = config_data.get("OPENAI_SETTINGS.max_tokens", 1000)
        OPENAI_TEMPERATURE = config_data.get("OPENAI_SETTINGS.temperature", 0.7)
        MIN_SCRIPT_LENGTH = config_data.get("VISUAL_ANALYSIS.min_script_length", 200)

        if not USER_PROMPT_COMBINED:
            logger.error("Промпт USER_PROMPT_COMBINED не найден в config_gen.json")
            return None, None

        for attempt in range(3):
            try:
                combined_prompt = (
                    USER_PROMPT_COMBINED.replace("{topic}", topic) +
                    "\n\n**Strict Format**:\n- Script (500 chars max).\n- 'First Frame Description:'\n- Description (500 chars max).\n- 'End of Description'."
                )
                logger.info(f"Попытка {attempt + 1}/3: Генерация для '{topic[:100]}'...")

                response = openai.ChatCompletion.create(
                    model=OPENAI_MODEL,
                    messages=[{"role": "user", "content": combined_prompt}],
                    max_tokens=OPENAI_MAX_TOKENS,
                    temperature=OPENAI_TEMPERATURE + 0.1 * attempt
                )

                if not response or not response.get("choices") or not response["choices"][0].get("message"):
                    logger.warning("⚠️ OpenAI не вернул валидный ответ")
                    continue

                combined_response = response["choices"][0]["message"]["content"]
                if not combined_response:
                    logger.warning("⚠️ OpenAI вернул пустой контент")
                    continue

                combined_response = combined_response.strip()
                logger.debug(f"OpenAI response: {combined_response}")

                # Сохраняем ответ для отладки
                with open(f"logs/openai_response_{topic[:50].replace(' ', '_')}_{attempt+1}.txt", "w", encoding="utf-8") as f:
                    f.write(combined_response)

                if len(combined_response) < MIN_SCRIPT_LENGTH:
                    logger.error(f"Ответ короткий: {len(combined_response)}")
                    continue

                if "First Frame Description:" not in combined_response or "End of Description" not in combined_response:
                    logger.error("Маркеры не найдены!")
                    continue

                script_text = combined_response.split("First Frame Description:")[0].strip()
                first_frame_description = (
                    combined_response.split("First Frame Description:")[1].split("End of Description")[0].strip()
                )

                if not script_text or not first_frame_description:
                    logger.error("Сценарий или описание пусты")
                    continue

                logger.info(f"Сценарий: {script_text[:100]}...")
                logger.info(f"Описание: {first_frame_description[:100]}...")
                return script_text, first_frame_description

            except Exception as e:
                logger.error(f"Ошибка (попытка {attempt + 1}/3): {str(e)}")
                continue

        logger.error("Превышено число попыток.")
        return None, None

    except Exception as e:
        logger.error(f"Ошибка загрузки config_gen.json: {str(e)}")
        return None, None

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
        valid_focuses = self.get_valid_focus_areas(tracker)
        if not valid_focuses:
            self.logger.error("❌ Нет доступных фокусов")
            raise ValueError("Все фокусы использованы")
        selected_focus = random.choice(valid_focuses)
        used_labels = tracker["focus_data"].get(selected_focus, [])
        prompt = self.config.get("CONTENT.topic.prompt_template").format(
            focus_areas=selected_focus, exclusions=", ".join(used_labels)
        )
        topic_response = self.request_openai(prompt)
        try:
            topic_data = json.loads(topic_response)
            full_topic = topic_data["full_topic"]
            short_topic = topic_data["short_topic"]
        except json.JSONDecodeError:
            self.logger.error("❌ OpenAI вернул не JSON")
            raise ValueError("Ошибка формата ответа OpenAI")
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
        s3 = get_b2_client()
        if not s3:
            self.logger.warning("⚠️ Не удалось создать клиент B2 для синхронизации трекера")
            return
        try:
            s3.upload_file(TRACKER_PATH, B2_BUCKET_NAME, "data/topics_tracker.json")
            self.logger.info("✅ topics_tracker.json синхронизирован с B2")
        except Exception as e:
            self.logger.warning(f"⚠️ Не удалось загрузить в B2: {e}")

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
        os.makedirs(os.path.dirname(TRACKER_PATH), exist_ok=True)
        s3 = get_b2_client()
        tracker_updated = False
        if not s3:
            self.logger.warning("⚠️ Не удалось создать клиент B2 для загрузки трекера")
        else:
            try:
                s3.download_file(B2_BUCKET_NAME, "data/topics_tracker.json", TRACKER_PATH)
                self.logger.info("✅ Загружен topics_tracker.json из B2")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось загрузить из B2: {e}")
                if not os.path.exists(TRACKER_PATH):
                    self.logger.info("Создаём новый topics_tracker.json из FailSafeVault")
                    with open(FAILSAFE_PATH, 'r', encoding='utf-8') as f:
                        failsafe = json.load(f)
                    tracker = {
                        "all_focuses": failsafe["focuses"],
                        "used_focuses": [],
                        "focus_data": {}
                    }
                    with open(TRACKER_PATH, 'w', encoding='utf-8') as f:
                        json.dump(tracker, f, ensure_ascii=False, indent=4)
                    tracker_updated = True
        with open(TRACKER_PATH, 'r', encoding='utf-8') as f:
            tracker = json.load(f)
        if "all_focuses" not in tracker:
            self.logger.info("Обновляем старый трекер: добавляем all_focuses")
            with open(FAILSAFE_PATH, 'r', encoding='utf-8') as f:
                failsafe = json.load(f)
            tracker["all_focuses"] = failsafe["focuses"]
            tracker.setdefault("used_focuses", [])
            tracker.setdefault("focus_data", {})
            with open(TRACKER_PATH, 'w', encoding='utf-8') as f:
                json.dump(tracker, f, ensure_ascii=False, indent=4)
            tracker_updated = True
        if tracker_updated and s3:
            self.sync_tracker_to_b2()
        return tracker

    def run(self):
        """Основной процесс генерации контента."""
        lock_file = "config/processing.lock"

        # Проверяем наличие лок-файла
        if os.path.exists(lock_file):
            logger.info("🔒 Процесс уже выполняется. Завершаем работу.")
            return

        try:
            # Создаём лок-файл
            os.makedirs(os.path.dirname(lock_file), exist_ok=True)
            with open(lock_file, "w") as f:
                f.write("")

            # Загружаем config_public.json из B2
            download_config_public()
            if not os.path.exists(CONFIG_PUBLIC_LOCAL_PATH):
                logger.error(f"❌ Файл {CONFIG_PUBLIC_LOCAL_PATH} не загружен из B2, создаём пустой config_public")
                config_public = {"empty": ["666/"]}
                os.makedirs(os.path.dirname(CONFIG_PUBLIC_LOCAL_PATH), exist_ok=True)
                with open(CONFIG_PUBLIC_LOCAL_PATH, "w", encoding="utf-8") as file:
                    json.dump(config_public, file, ensure_ascii=False, indent=4)
            else:
                with open(CONFIG_PUBLIC_LOCAL_PATH, "r", encoding="utf-8") as file:
                    config_public = json.load(file)

            empty_folders = config_public.get("empty", [])
            if len(empty_folders) > 1:
                config_public["empty"] = [empty_folders[0]]
                with open(CONFIG_PUBLIC_LOCAL_PATH, "w", encoding="utf-8") as file:
                    json.dump(config_public, file, ensure_ascii=False, indent=4)
                logger.info("Лимит: одна генерация, взята папка %s", empty_folders[0])

            if not self.config.get('CONTENT.topic.enabled', True):
                logger.error("❌ Генерация темы отключена, дальнейшая работа невозможна.")
                return

            if not empty_folders:
                logger.info("✅ Нет пустых папок. Процесс завершён.")
                return

            self.adapt_prompts()
            self.clear_generated_content()
            tracker = self.load_tracker()
            topic, content_data = self.generate_topic(tracker)
            if not topic:
                logger.error("❌ Тема не сгенерирована, прерываем выполнение.")
                return

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

            sarcastic_comment = None
            sarcastic_poll = None
            if text_initial:
                sarcastic_comment = self.generate_sarcasm(text_initial, content_data)
                sarcastic_poll = self.generate_sarcasm_poll(text_initial, content_data)
                self.save_to_generated_content("sarcasm", {"comment": sarcastic_comment, "poll": sarcastic_poll})

            final_text = text_initial.strip()
            target_folder = empty_folders[0]
            content_dict = {
                "topic": topic,
                "content": final_text,
                "sarcasm": {"comment": sarcastic_comment, "poll": sarcastic_poll}
            }

            # Сохраняем в B2 однократно
            if not save_to_b2(target_folder, content_dict):
                logger.error(f"❌ Не удалось сохранить контент в B2: {target_folder}")
                return

            script_text, first_frame_description = generate_script_and_frame(content_dict["topic"])
            if script_text and first_frame_description:
                content_dict["script"] = script_text
                content_dict["first_frame_description"] = first_frame_description
                with open(self.content_output_path, 'w', encoding='utf-8') as f:
                    json.dump(content_dict, f, ensure_ascii=False, indent=4)
                logger.info("✅ Сценарий и описание кадра сохранены в %s", self.content_output_path)
            else:
                logger.warning("⚠️ Не удалось сгенерировать сценарий или описание, продолжаем без них")

            # Читаем generation_id
            with open(os.path.join("config", "config_gen.json"), "r", encoding="utf-8") as gen_file:
                config_gen_content = json.load(gen_file)
                generation_id = config_gen_content["generation_id"]

            # Логируем конфиги однократно
            logger.info(f"📄 Содержимое config_public.json: {json.dumps(config_public, ensure_ascii=False, indent=4)}")
            logger.info(f"📄 Содержимое config_gen.json: {json.dumps(config_gen_content, ensure_ascii=False, indent=4)}")

            # Запускаем generate_media.py
            run_generate_media(generation_id)
            logger.info("✅ Генерация контента завершена.")

        except Exception as e:
            handle_error("Run Error", "Ошибка в основном процессе генерации", e)
            logger.error("❌ Процесс генерации контента прерван из-за критической ошибки.")
        finally:
            # Удаляем лок-файл
            if os.path.exists(lock_file):
                os.remove(lock_file)
                logger.info("🔓 Лок-файл удалён.")

if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()