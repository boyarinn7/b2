# -*- coding: utf-8 -*-
# В файле scripts/generate_content.py

import json
import os
import sys
import requests
import openai
import re
import subprocess
import boto3
import io
import random
import argparse
from datetime import datetime # Импортируем datetime
import shutil # <--- ДОБАВЛЕН ИМПОРТ

# Импортируем ClientError из botocore (часть boto3)
try:
    from botocore.exceptions import ClientError
except ImportError:
    # Заглушка на случай, если boto3/botocore не установлены полностью,
    # хотя основной импорт boto3 выше должен был бы упасть раньше.
    ClientError = Exception # Ловим общее исключение, если ClientError недоступен
    print("Warning: Could not import ClientError from botocore. B2 error handling might be less specific.")


# Добавляем путь к модулям, если скрипт запускается напрямую
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# --- Импорт кастомных модулей ---
try:
    from modules.config_manager import ConfigManager
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    # Импортируем нужные утилиты из utils.py
    from modules.utils import ensure_directory_exists, load_b2_json, save_b2_json, get_b2_client
except ModuleNotFoundError as e:
     print(f"Критическая Ошибка: Не найдены модули проекта в generate_content: {e}", file=sys.stderr)
     sys.exit(1)
except ImportError as e:
     print(f"Критическая Ошибка: Не найдена функция/класс в модулях: {e}", file=sys.stderr)
     sys.exit(1)


# --- Инициализация логгера и конфига ---
logger = get_logger("generate_content")
config = ConfigManager()

# --- Константы ---
# Получаем значения из ConfigManager с дефолтами
B2_BUCKET_NAME = config.get("API_KEYS.b2.bucket_name", "boyarinnbotbucket") # Убедитесь, что имя бакета правильное
FAILSAFE_PATH_REL = config.get("FILE_PATHS.failsafe_path", "config/FailSafeVault.json")
TRACKER_PATH_REL = config.get("FILE_PATHS.tracker_path", "data/topics_tracker.json")
# Полные пути для локальных операций
FAILSAFE_PATH_ABS = os.path.join(BASE_DIR, FAILSAFE_PATH_REL)
TRACKER_PATH_ABS = os.path.join(BASE_DIR, TRACKER_PATH_REL)
# Путь к локальному файлу для сохранения промежуточных результатов генерации
CONTENT_OUTPUT_PATH = config.get('FILE_PATHS.content_output_path', 'generated_content.json')

# --- ФУНКЦИЯ СОХРАНЕНИЯ КОНТЕНТА В B2 (ВНУТРИ ЭТОГО ФАЙЛА) ---
# Она использует переданный generation_id и не генерирует новый
def save_content_to_b2(folder, content_dict, generation_id):
    """
    Сохраняет словарь content_dict как JSON в указанную папку B2,
    используя переданный generation_id для имени файла.
    НЕ генерирует новый ID и НЕ обновляет config_gen.json.
    Возвращает True при успехе, False при ошибке.
    """
    logger.info(f"Вызов save_content_to_b2 для ID: {generation_id}")

    # Получаем B2 клиент (можно передавать как аргумент или получать здесь)
    s3 = get_b2_client()
    if not s3:
        logger.error("❌ Не удалось создать клиент B2 внутри save_content_to_b2")
        return False

    # Получаем имя бакета
    bucket_name = config.get("API_KEYS.b2.bucket_name", B2_BUCKET_NAME)
    if not bucket_name:
         logger.error("❌ Имя бакета B2 не найдено в save_content_to_b2")
         return False

    if not generation_id:
        logger.error("❌ Generation ID не предоставлен для save_content_to_b2.")
        return False
    if not isinstance(content_dict, dict):
         logger.error("❌ Данные для сохранения в save_content_to_b2 не являются словарем.")
         return False

    # Используем переданный ID, очищенный от возможного .json
    clean_base_id = generation_id.replace(".json", "")
    file_extension = ".json" # Мы сохраняем JSON
    s3_key = f"{folder.rstrip('/')}/{clean_base_id}{file_extension}"

    # Используем временный локальный файл для сохранения JSON перед загрузкой
    timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    # Сохраняем временный файл в корне проекта (или можно создать подпапку temp)
    local_temp_path = f"{clean_base_id}_content_temp_{timestamp_suffix}.json"

    logger.info(f"Попытка сохранения данных для ID {clean_base_id} в B2 как {s3_key} через {local_temp_path}...")

    try:
        # Создаем директорию для временного файла, если она указана (здесь не указана)
        # ensure_directory_exists(local_temp_path) # Не нужно, если файл в корне

        # Проверка содержимого перед сохранением (опционально, но полезно)
        required_keys = ["topic", "content", "sarcasm", "script", "first_frame_description"]
        missing_keys = [key for key in required_keys if key not in content_dict]
        if missing_keys:
             logger.warning(f"⚠️ В сохраняемых данных для ID {clean_base_id} отсутствуют ключи: {missing_keys}. Содержимое: {list(content_dict.keys())}")
        # Можно добавить более строгую проверку, если нужно

        # Сохраняем словарь во временный JSON файл
        with open(local_temp_path, 'w', encoding='utf-8') as f:
            json.dump(content_dict, f, ensure_ascii=False, indent=4)
        logger.debug(f"Временный файл {local_temp_path} создан.")

        # Загружаем временный файл в B2
        s3.upload_file(local_temp_path, bucket_name, s3_key)
        logger.info(f"✅ Данные для ID {clean_base_id} успешно сохранены в B2: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Не удалось сохранить данные для ID {clean_base_id} в B2 как {s3_key}: {e}", exc_info=True)
        return False
    finally:
        # Удаляем временный локальный файл после попытки загрузки
        if os.path.exists(local_temp_path):
            try:
                os.remove(local_temp_path)
                logger.debug(f"Временный файл {local_temp_path} удален.")
            except OSError as remove_err:
                 logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")
# --- КОНЕЦ ФУНКЦИИ save_content_to_b2 ---


# --- КЛАСС ГЕНЕРАТОРА КОНТЕНТА ---
class ContentGenerator:
    def __init__(self):
        """Инициализация генератора контента."""
        self.logger = logger
        self.config = config
        # Загрузка настроек генерации
        self.topic_threshold = self.config.get('GENERATE.topic_threshold', 7)
        self.text_threshold = self.config.get('GENERATE.text_threshold', 8)
        self.max_attempts = self.config.get('GENERATE.max_attempts', 1) # Возможно, не используется?
        self.adaptation_enabled = self.config.get('GENERATE.adaptation_enabled', False)
        self.adaptation_params = self.config.get('GENERATE.adaptation_parameters', {})
        # Путь к локальному файлу для сохранения промежуточных результатов
        self.content_output_path = CONTENT_OUTPUT_PATH
        # Настройки OpenAI
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_model = self.config.get("OPENAI_SETTINGS.model", "gpt-4o")
        self.temperature = float(self.config.get("OPENAI_SETTINGS.temperature", 0.7))
        if not self.openai_api_key:
            self.logger.error("❌ Переменная окружения OPENAI_API_KEY не задана!")
            raise EnvironmentError("Переменная окружения OPENAI_API_KEY отсутствует.")
        openai.api_key = self.openai_api_key # Устанавливаем ключ для библиотеки
        # Инициализация B2 клиента один раз
        self.b2_client = get_b2_client()
        if not self.b2_client:
             self.logger.warning("⚠️ Не удалось инициализировать B2 клиент в ContentGenerator.")

    def adapt_prompts(self):
        """Применяет адаптацию промптов (если включено)."""
        if not self.adaptation_enabled:
            self.logger.info("🔄 Адаптация промптов отключена.")
            return
        self.logger.info("🔄 Применяю адаптацию промптов на основе обратной связи...")
        # Логика адаптации (если она есть) должна быть здесь
        # Сейчас просто логирует параметры
        for key, value in self.adaptation_params.items():
            self.logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def clear_generated_content(self):
        """Очищает локальный файл с промежуточными результатами."""
        try:
            self.logger.info(f"🧹 Очистка локального файла: {self.content_output_path}")
            # Создаем папку, если ее нет
            ensure_directory_exists(self.content_output_path)
            # Открываем файл на запись, что очистит его или создаст, если его нет
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump({}, file, ensure_ascii=False, indent=4)
            self.logger.info("✅ Локальный файл успешно очищен/создан.")
        except PermissionError:
            handle_error("Clear Content Error", f"Нет прав на запись в файл: {self.content_output_path}", PermissionError())
        except Exception as e:
            handle_error("Clear Content Error", str(e), e)

    def load_tracker(self):
        """Загружает трекер тем из B2 или локального файла."""
        os.makedirs(os.path.dirname(TRACKER_PATH_ABS), exist_ok=True) # Убедимся, что папка data существует
        tracker_updated_locally = False
        if self.b2_client:
            try:
                self.logger.info(f"Попытка загрузки {TRACKER_PATH_REL} из B2...")
                # Используем уникальный временный путь для загрузки
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                local_temp_tracker = f"tracker_temp_{timestamp_suffix}.json"
                self.b2_client.download_file(B2_BUCKET_NAME, TRACKER_PATH_REL, local_temp_tracker)
                # Копируем загруженный файл в основное место
                # Используем shutil, который теперь импортирован
                shutil.copyfile(local_temp_tracker, TRACKER_PATH_ABS)
                os.remove(local_temp_tracker) # Удаляем временный файл
                self.logger.info(f"✅ Загружен {TRACKER_PATH_REL} из B2 в {TRACKER_PATH_ABS}")
            except ClientError as e: # Используем импортированный ClientError
                 error_code = e.response.get('Error', {}).get('Code')
                 if error_code == 'NoSuchKey' or '404' in str(e):
                      self.logger.warning(f"⚠️ {TRACKER_PATH_REL} не найден в B2. Проверяем локальную копию.")
                 else:
                      self.logger.error(f"⚠️ Ошибка B2 при загрузке трекера: {e}")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось загрузить трекер из B2: {e}")
        else:
             self.logger.warning("⚠️ B2 клиент недоступен, используем только локальный трекер.")

        # Если локальный файл не существует после попытки загрузки из B2
        if not os.path.exists(TRACKER_PATH_ABS):
            self.logger.warning(f"{TRACKER_PATH_ABS} не найден. Попытка создания из {FAILSAFE_PATH_ABS}.")
            try:
                with open(FAILSAFE_PATH_ABS, 'r', encoding='utf-8') as f_failsafe:
                    failsafe_data = json.load(f_failsafe)
                # Создаем структуру трекера
                tracker = {
                    "all_focuses": failsafe_data.get("focuses", []),
                    "used_focuses": [],
                    "focus_data": {}
                }
                with open(TRACKER_PATH_ABS, 'w', encoding='utf-8') as f_tracker:
                    json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                self.logger.info(f"✅ Создан новый {TRACKER_PATH_ABS} из FailSafeVault.")
                tracker_updated_locally = True # Помечаем, что создали новый локально
            except FileNotFoundError:
                 self.logger.error(f"❌ Файл {FAILSAFE_PATH_ABS} не найден! Невозможно создать трекер.")
                 return {"all_focuses": [], "used_focuses": [], "focus_data": {}} # Возвращаем пустую структуру
            except Exception as e:
                 self.logger.error(f"❌ Ошибка при создании трекера из FailSafe: {e}")
                 return {"all_focuses": [], "used_focuses": [], "focus_data": {}}

        # Читаем трекер из локального файла
        try:
            with open(TRACKER_PATH_ABS, 'r', encoding='utf-8') as f:
                tracker = json.load(f)
            # Проверяем и обновляем структуру, если нужно (для совместимости со старыми версиями)
            if "all_focuses" not in tracker:
                self.logger.info("Обновляем структуру старого трекера: добавляем all_focuses.")
                if os.path.exists(FAILSAFE_PATH_ABS):
                     with open(FAILSAFE_PATH_ABS, 'r', encoding='utf-8') as f_failsafe:
                         failsafe_data = json.load(f_failsafe)
                     tracker["all_focuses"] = failsafe_data.get("focuses", [])
                else:
                     tracker["all_focuses"] = [] # Пустой список, если FailSafe нет
                tracker.setdefault("used_focuses", [])
                tracker.setdefault("focus_data", {})
                # Сохраняем обновленный локальный файл
                with open(TRACKER_PATH_ABS, 'w', encoding='utf-8') as f_tracker:
                    json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                tracker_updated_locally = True
            # Синхронизируем с B2, если локальный файл был создан или обновлен
            if tracker_updated_locally:
                self.sync_tracker_to_b2(tracker_path_abs=TRACKER_PATH_ABS, tracker_path_rel=TRACKER_PATH_REL)

            return tracker
        except json.JSONDecodeError:
            self.logger.error(f"❌ Ошибка JSON в файле трекера: {TRACKER_PATH_ABS}. Возвращаем пустой.")
            return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения трекера {TRACKER_PATH_ABS}: {e}")
            return {"all_focuses": [], "used_focuses": [], "focus_data": {}}

    def get_valid_focus_areas(self, tracker):
        """Возвращает список доступных фокусов."""
        all_focuses = tracker.get("all_focuses", [])
        used_focuses = tracker.get("used_focuses", [])
        # Используем set для быстрой проверки
        used_set = set(used_focuses)
        valid_focuses = [f for f in all_focuses if f not in used_set]
        self.logger.info(f"✅ Доступные фокусы: {valid_focuses}")
        return valid_focuses

    def generate_topic(self, tracker):
        """Генерирует новую тему, используя доступные фокусы."""
        valid_focuses = self.get_valid_focus_areas(tracker)
        if not valid_focuses:
            self.logger.error("❌ Нет доступных фокусов для генерации темы.")
            # Можно либо выбросить исключение, либо вернуть None
            raise ValueError("Все фокусы использованы, невозможно сгенерировать тему.")
            # return None, {} # Альтернативный вариант

        selected_focus = random.choice(valid_focuses)
        self.logger.info(f"Выбран фокус для генерации темы: {selected_focus}")
        # Получаем список уже использованных ярлыков для этого фокуса, чтобы избежать повторов
        used_labels_for_focus = tracker.get("focus_data", {}).get(selected_focus, [])
        exclusions_str = ", ".join(used_labels_for_focus) if used_labels_for_focus else "нет"

        # Формируем промпт
        prompt_template = self.config.get("CONTENT.topic.prompt_template")
        if not prompt_template:
             self.logger.error("Промпт CONTENT.topic.prompt_template не найден!")
             raise ValueError("Отсутствует промпт для генерации темы.")
        prompt = prompt_template.format(focus_areas=selected_focus, exclusions=exclusions_str)

        try:
            # Запрашиваем JSON ответ
            topic_response_str = self.request_openai(prompt, use_json_mode=True)
            topic_data = json.loads(topic_response_str)

            full_topic = topic_data.get("full_topic")
            short_topic = topic_data.get("short_topic")

            if not full_topic or not short_topic:
                self.logger.error(f"❌ OpenAI вернул неполные данные для темы: {topic_data}")
                raise ValueError("Ответ OpenAI для темы не содержит full_topic или short_topic.")

            self.logger.info(f"Сгенерирована тема: '{full_topic}' (Ярлык: '{short_topic}')")
            # Обновляем трекер с новым ярлыком
            self.update_tracker(selected_focus, short_topic, tracker) # Передаем tracker для обновления
            # Сохраняем в локальный файл
            self.save_to_generated_content("topic", {"full_topic": full_topic, "short_topic": short_topic})

            # Определяем тему (tragic/normal) по фокусу
            content_metadata = {"theme": "tragic" if "(т)" in selected_focus else "normal"}
            return full_topic, content_metadata

        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Ошибка парсинга JSON ответа OpenAI для темы: {e}. Ответ: {topic_response_str[:500]}")
            raise ValueError("Ошибка формата ответа OpenAI при генерации темы.") from e
        except Exception as e:
            self.logger.error(f"❌ Ошибка при генерации темы: {e}", exc_info=True)
            raise # Пробрасываем ошибку дальше

    def update_tracker(self, focus, short_topic, tracker):
        """Обновляет данные трекера в памяти и сохраняет его."""
        used_focuses = tracker.get("used_focuses", [])
        focus_data = tracker.get("focus_data", {})

        # Обновляем список использованных фокусов (последние 15)
        if focus in used_focuses:
            used_focuses.remove(focus)
        used_focuses.insert(0, focus)
        if len(used_focuses) > 15:
            used_focuses.pop()

        # Обновляем список ярлыков для данного фокуса (последние 5)
        focus_labels = focus_data.setdefault(focus, [])
        if short_topic in focus_labels:
             focus_labels.remove(short_topic) # Убираем, чтобы вставить в начало
        focus_labels.insert(0, short_topic)
        if len(focus_labels) > 5:
            focus_labels.pop()

        # Обновляем основной словарь tracker (переданный по ссылке)
        tracker["used_focuses"] = used_focuses
        tracker["focus_data"] = focus_data

        # Сохраняем обновленный трекер локально и в B2
        self.save_topics_tracker(tracker) # Сохраняет локально
        self.sync_tracker_to_b2(tracker_path_abs=TRACKER_PATH_ABS, tracker_path_rel=TRACKER_PATH_REL) # Синхронизирует с B2

    def save_topics_tracker(self, tracker):
        """Сохраняет трекер в локальный файл."""
        try:
            ensure_directory_exists(TRACKER_PATH_ABS) # Убедимся, что папка есть
            with open(TRACKER_PATH_ABS, "w", encoding="utf-8") as file:
                json.dump(tracker, file, ensure_ascii=False, indent=4)
            self.logger.info(f"Трекер тем сохранен локально: {TRACKER_PATH_ABS}")
        except Exception as e:
             self.logger.error(f"Ошибка сохранения трекера локально: {e}")

    def sync_tracker_to_b2(self, tracker_path_abs, tracker_path_rel):
        """Синхронизирует локальный трекер с B2."""
        if not self.b2_client:
            self.logger.warning("⚠️ B2 клиент недоступен, синхронизация трекера невозможна.")
            return
        if not os.path.exists(tracker_path_abs):
             self.logger.warning(f"⚠️ Локальный файл трекера {tracker_path_abs} не найден для синхронизации.")
             return
        try:
            self.logger.info(f"Синхронизация {tracker_path_abs} с B2 как {tracker_path_rel}...")
            self.b2_client.upload_file(tracker_path_abs, B2_BUCKET_NAME, tracker_path_rel)
            self.logger.info(f"✅ {tracker_path_rel} синхронизирован с B2.")
        except Exception as e:
            self.logger.error(f"⚠️ Не удалось загрузить трекер {tracker_path_rel} в B2: {e}")

    def request_openai(self, prompt, use_json_mode=False, temperature_override=None):
        """Отправляет запрос к OpenAI, опционально запрашивая JSON и переопределяя температуру."""
        try:
            # Определяем параметры по умолчанию
            max_tokens = 750 # Дефолт для темы/текста
            temp = temperature_override if temperature_override is not None else self.temperature # Используем переданную или дефолтную

            # Корректируем параметры в зависимости от типа запроса (эвристика по ключам в промпте)
            prompt_lower = prompt.lower()
            # Используем более надежные проверки для типа запроса
            if use_json_mode: # Если явно запрошен JSON (для темы, опроса, скрипта)
                 if "script" in prompt_lower or "frame_description" in prompt_lower:
                      max_tokens = self.config.get("OPENAI_SETTINGS.max_tokens_script", 1000)
                      if temperature_override is None: temp = self.config.get("OPENAI_SETTINGS.temperature_script", 0.7)
                 elif "poll" in prompt_lower or "опрос" in prompt_lower:
                      max_tokens = self.config.get("SARCASM.max_tokens_poll", 250)
                      # Температура уже должна быть в temperature_override
                 else: # Вероятно, тема
                      max_tokens = self.config.get("OPENAI_SETTINGS.max_tokens_text", 750) # Используем max_tokens для текста для темы
                      # Температура уже должна быть в temperature_override или self.temperature
            elif "comment" in prompt_lower or "комментарий" in prompt_lower: # Комментарий - не JSON
                 max_tokens = self.config.get("SARCASM.max_tokens_comment", 150)
                 # Температура уже должна быть в temperature_override
            else: # Обычный текст
                 max_tokens = self.config.get("OPENAI_SETTINGS.max_tokens_text", 750)
                 # Температура уже должна быть в temperature_override или self.temperature

            self.logger.info(f"🔎 Отправка запроса в OpenAI (JSON={use_json_mode}): max_tokens={max_tokens}, temp={temp:.1f}")

            request_args = {
                "model": self.openai_model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": temp,
            }
            # Добавляем формат JSON, если требуется
            if use_json_mode:
                request_args["response_format"] = {"type": "json_object"}

            response = openai.ChatCompletion.create(**request_args)
            return response['choices'][0]['message']['content'].strip()

        except openai.error.OpenAIError as e:
            logger.error(f"❌ Ошибка при работе с OpenAI API: {e}")
            raise # Пробрасываем ошибку выше
        except Exception as e:
             logger.error(f"❌ Неизвестная ошибка в request_openai: {e}", exc_info=True)
             raise # Пробрасываем ошибку

    def generate_sarcasm(self, text, content_data={}):
        """Генерирует саркастический комментарий."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.comment_enabled', True):
            self.logger.info("🔕 Генерация саркастического комментария отключена.")
            return None # Возвращаем None, если отключено

        # Выбор промпта и температуры
        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt_template = self.config.get('SARCASM.tragic_comment_prompt')
            temperature = self.config.get('SARCASM.tragic_comment_temperature', 0.6)
            prompt_type = "tragic"
        else:
            prompt_template = self.config.get('SARCASM.comment_prompt')
            temperature = self.config.get('SARCASM.comment_temperature', 0.8)
            prompt_type = "normal"

        if not prompt_template:
            self.logger.error(f"Промпт для комментария ({prompt_type}) не найден!")
            return None

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос к OpenAI для генерации комментария (тип: {prompt_type}, temp: {temperature:.1f})...")

        try:
            # Вызываем request_openai с переопределением температуры
            comment = self.request_openai(prompt, temperature_override=temperature)
            self.logger.info(f"✅ Саркастический комментарий сгенерирован: {comment}")
            return comment
        except Exception as e:
            # request_openai уже залогировал ошибку
            self.logger.error(f"❌ Ошибка генерации саркастического комментария.")
            return None # Возвращаем None при ошибке

    def generate_sarcasm_poll(self, text, content_data={}):
        """Генерирует саркастический опрос, ожидая JSON."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.poll_enabled', True):
            self.logger.info("🔕 Генерация саркастического опроса отключена.")
            return {} # Возвращаем пустой словарь

        # Выбор промпта и температуры
        if "theme" in content_data and content_data["theme"] == "tragic":
            prompt_template = self.config.get('SARCASM.tragic_question_prompt')
            temperature = self.config.get('SARCASM.tragic_poll_temperature', 0.6)
            prompt_type = "tragic"
        else:
            prompt_template = self.config.get('SARCASM.question_prompt')
            temperature = self.config.get('SARCASM.poll_temperature', 0.9)
            prompt_type = "normal"

        if not prompt_template:
             self.logger.error(f"Промпт для опроса ({prompt_type}) не найден в конфигурации!")
             return {}

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос к OpenAI для генерации опроса (тип: {prompt_type}, temp: {temperature:.1f})... Ожидаем JSON.")
        response_content = "" # Инициализируем на случай ошибки до присваивания
        try:
            # Запрашиваем JSON ответ от модели, передавая температуру
            response_content = self.request_openai(prompt, use_json_mode=True, temperature_override=temperature)
            self.logger.debug(f"Сырой ответ OpenAI для опроса: {response_content[:500]}")

            # Пытаемся распарсить JSON
            poll_data = json.loads(response_content)

            # Проверяем структуру полученного JSON
            if isinstance(poll_data, dict) and "question" in poll_data and "options" in poll_data and isinstance(poll_data["options"], list) and len(poll_data["options"]) == 3:
                self.logger.info("✅ Опрос успешно сгенерирован и распарсен (JSON).")
                # Очищаем строки от лишних пробелов
                poll_data["question"] = str(poll_data["question"]).strip()
                poll_data["options"] = [str(opt).strip() for opt in poll_data["options"]]
                return poll_data
            else:
                self.logger.error(f"❌ OpenAI вернул JSON, но структура неверна: {poll_data}")
                return {}

        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Ошибка парсинга JSON ответа OpenAI для опроса: {e}. Ответ: {response_content[:500]}")
            return {}
        except Exception as e:
            # request_openai уже залогировал ошибку
            self.logger.error(f"❌ Ошибка генерации саркастического опроса.")
            return {}

    def save_to_generated_content(self, stage, data):
        """Сохраняет промежуточные данные в локальный JSON файл."""
        try:
            if not self.content_output_path:
                raise ValueError("❌ Ошибка: self.content_output_path не задан!")
            self.logger.debug(f"🔄 Обновление локального файла: {self.content_output_path}, этап: {stage}")
            # Создаем папку, если ее нет
            ensure_directory_exists(self.content_output_path)
            # Читаем текущее содержимое или создаем пустой словарь
            result_data = {}
            if os.path.exists(self.content_output_path):
                try:
                    # Добавим проверку на размер файла перед чтением
                    if os.path.getsize(self.content_output_path) > 0:
                        with open(self.content_output_path, 'r', encoding='utf-8') as file:
                            result_data = json.load(file)
                    else:
                         self.logger.warning(f"⚠️ Файл {self.content_output_path} пуст, начинаем с {{}}")
                         result_data = {}
                except json.JSONDecodeError:
                    self.logger.warning(f"⚠️ Файл {self.content_output_path} поврежден, создаем новый.")
                    result_data = {}
                except Exception as read_err:
                     self.logger.error(f"Ошибка чтения {self.content_output_path}: {read_err}")
                     result_data = {} # Начинаем с чистого листа при ошибке чтения

            # Обновляем данные для текущего этапа и добавляем метку времени
            result_data["timestamp"] = datetime.utcnow().isoformat()
            result_data[stage] = data
            # Записываем обновленные данные
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump(result_data, file, ensure_ascii=False, indent=4)
            self.logger.debug(f"✅ Данные локально обновлены для этапа: {stage}")
        except Exception as e:
            handle_error("Save to Generated Content Error", f"Ошибка при сохранении в {self.content_output_path}: {str(e)}", e)

    def critique_content(self, content, topic):
        """Выполняет критику текста (если включено)."""
        if not self.config.get('CONTENT.critique.enabled', False): # По умолчанию выключено
            self.logger.info("🔕 Критика контента отключена.")
            return "Критика отключена в конфигурации."
        if not content:
             self.logger.warning("Нет текста для критики.")
             return "Нет текста для критики."
        try:
            self.logger.info("🔄 Выполняется критика текста через OpenAI...")
            prompt_template = self.config.get('CONTENT.critique.prompt_template')
            if not prompt_template:
                 self.logger.error("Промпт для критики не найден!")
                 return "Промпт для критики не найден."

            prompt = prompt_template.format(content=content, topic=topic)
            # Используем request_openai с температурой для критики
            temperature = self.config.get('CONTENT.critique.temperature', 0.3)
            critique = self.request_openai(prompt, temperature_override=temperature)
            self.logger.info("✅ Критика успешно завершена.")
            return critique
        except Exception as e:
            self.logger.error(f"❌ Ошибка при выполнении критики.")
            # Ошибка уже залогирована в request_openai
            return "Критика текста завершилась ошибкой."

    def run(self, generation_id):
        """Основной процесс генерации контента для заданного ID."""
        self.logger.info(f"--- Запуск ContentGenerator.run для ID: {generation_id} ---")
        if not generation_id:
             self.logger.error("❌ В ContentGenerator.run не передан generation_id!")
             raise ValueError("generation_id не может быть пустым.")

        try:
            # --- Шаг 1: Подготовка ---
            self.adapt_prompts()
            self.clear_generated_content() # Очищает локальный файл generated_content.json

            # --- Шаг 2: Генерация Темы ---
            tracker = self.load_tracker()
            topic, content_data = self.generate_topic(tracker)
            # generate_topic выбросит исключение, если не сможет сгенерировать тему

            # --- Шаг 3: Генерация Текста ---
            text_initial = ""
            if self.config.get('CONTENT.text.enabled', True) or self.config.get('CONTENT.tragic_text.enabled', True):
                prompt_key = 'CONTENT.tragic_text.prompt_template' if content_data.get("theme") == "tragic" else 'CONTENT.text.prompt_template'
                prompt_template = self.config.get(prompt_key)
                if prompt_template:
                     text_initial = self.request_openai(prompt_template.format(topic=topic))
                     self.logger.info(f"Сгенерирован текст (длина: {len(text_initial)}): {text_initial[:100]}...")
                     self.save_to_generated_content("text", {"text": text_initial})
                else:
                     self.logger.warning(f"Промпт {prompt_key} не найден, генерация текста пропущена.")
            else:
                self.logger.info("🔕 Генерация текста отключена.")

            # --- Шаг 4: Критика ---
            critique_result = self.critique_content(text_initial, topic)
            self.save_to_generated_content("critique", {"critique": critique_result})

            # --- Шаг 5: Генерация Сарказма ---
            sarcastic_comment = None
            sarcastic_poll = {}
            if text_initial: # Генерируем сарказм только если есть текст
                sarcastic_comment = self.generate_sarcasm(text_initial, content_data)
                sarcastic_poll = self.generate_sarcasm_poll(text_initial, content_data) # Используем исправленный метод
            self.save_to_generated_content("sarcasm", {"comment": sarcastic_comment, "poll": sarcastic_poll})

            # --- Шаг 6: Генерация Сценария и Кадра ---
            script_text = None
            first_frame_description = None
            try:
                self.logger.info("Запрос к OpenAI (JSON Mode) для генерации сценария и описания кадра...")
                restrictions_list = self.config.get("restrictions", [])
                chosen_restriction = random.choice(restrictions_list) if restrictions_list else "No specific restrictions."
                self.logger.info(f"Выбрано ограничение: {chosen_restriction}")

                prompt_template = self.config.get('PROMPTS.user_prompt_combined')
                if not prompt_template:
                    raise ValueError("Промпт PROMPTS.user_prompt_combined не найден!")
                prompt_combined = prompt_template.format(topic=topic, restriction=chosen_restriction)

                # Используем request_openai с флагом JSON
                response_content = self.request_openai(prompt_combined, use_json_mode=True)
                self.logger.debug(f"Raw OpenAI JSON response for script/frame: {response_content[:500]}")
                script_data = json.loads(response_content)

                script_text = script_data.get("script")
                first_frame_description = script_data.get("first_frame_description")

                if not script_text or not first_frame_description:
                    raise ValueError(f"Ключи 'script' или 'first_frame_description' отсутствуют/пусты в JSON от OpenAI: {script_data}")

                self.logger.info("✅ Сценарий и описание кадра успешно извлечены (JSON Mode).")
                self.save_to_generated_content("script", {"script": script_text, "first_frame_description": first_frame_description})

            except (json.JSONDecodeError, ValueError) as parse_err:
                self.logger.error(f"❌ Ошибка парсинга/валидации JSON сценария/описания: {parse_err}.")
                # Не прерываем весь процесс, но логируем и оставляем script/description пустыми
                script_text = None
                first_frame_description = None
            except Exception as script_err:
                self.logger.error(f"❌ Неожиданная ошибка при генерации сценария/описания: {script_err}", exc_info=True)
                script_text = None
                first_frame_description = None

            # --- Шаг 7: Формирование и Сохранение Итогового Контента в B2 ---
            self.logger.info("Формирование итогового словаря для B2...")
            complete_content_dict = {
                "topic": topic,
                "content": text_initial.strip() if text_initial else "",
                "sarcasm": {
                    "comment": sarcastic_comment,
                    "poll": sarcastic_poll # Сохраняем результат (может быть пустым словарем)
                },
                "script": script_text, # Будет None, если генерация не удалась
                "first_frame_description": first_frame_description # Будет None, если генерация не удалась
            }
            self.logger.debug(f"Итоговый словарь: {json.dumps(complete_content_dict, ensure_ascii=False, indent=2)}")

            # Вызываем функцию save_content_to_b2 (определенную выше в этом файле), передавая ID
            self.logger.info(f"Сохранение итогового контента в B2 для ID {generation_id}...")
            success = save_content_to_b2(
                 "666/", # Целевая папка
                 complete_content_dict, # Словарь с данными
                 generation_id # <--- Передаем правильный ID!
            )
            if not success:
                # Если сохранение не удалось, это критично
                raise Exception(f"Не удалось сохранить итоговый контент в B2 для ID {generation_id}")

            # --- Шаг 8: Обновление config_midjourney.json ---
            self.logger.info(f"Обновление config_midjourney.json для ID: {generation_id} (установка generation: true)")
            try:
                s3_client_mj = self.b2_client
                if not s3_client_mj:
                     raise ConnectionError("B2 клиент недоступен для обновления config_midjourney")

                config_mj_remote_path = "config/config_midjourney.json"
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"
                bucket_name = B2_BUCKET_NAME

                # Загрузка текущего config_midjourney.json из B2
                config_mj = load_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, default_value={})
                if config_mj is None: config_mj = {}

                # Обновление данных
                config_mj['generation'] = True
                config_mj['midjourney_task'] = None
                config_mj['midjourney_results'] = {}
                config_mj['status'] = None
                self.logger.info("Данные для config_midjourney.json подготовлены: generation=True, task/results очищены.")

                # Сохранение обратно в B2 (используем save_b2_json из utils)
                if not save_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, config_mj):
                     raise Exception("Не удалось сохранить config_mj после установки generation=True!")
                else:
                     self.logger.info(f"✅ Обновленный {config_mj_remote_path} (generation=True) загружен в B2.")

            except Exception as e:
                self.logger.error(f"❌ Не удалось обновить config_midjourney.json: {str(e)}", exc_info=True)
                # Эта ошибка критична
                raise Exception("Критическая ошибка: не удалось установить флаг generation: true") from e

            self.logger.info(f"✅ ContentGenerator.run успешно завершен для ID {generation_id}.")

        except Exception as e:
            self.logger.error(f"❌ Ошибка в ContentGenerator.run для ID {generation_id}: {str(e)}", exc_info=True)
            # Пробрасываем исключение, чтобы точка входа могла его поймать и вернуть ненулевой код
            raise

# --- Точка входа ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate content for a specific ID.')
    # Аргумент generation_id обязателен
    parser.add_argument('--generation_id', type=str, required=True,
                        help='The generation ID for the content file (Mandatory).')
    args = parser.parse_args()
    generation_id_main = args.generation_id

    # Дополнительная проверка на пустой ID на всякий случай
    if not generation_id_main:
         logger.critical("Критическая ошибка: generation_id не был передан или пуст!")
         sys.exit(1) # Выход с ошибкой

    logger.info(f"--- Запуск скрипта generate_content.py для ID: {generation_id_main} ---")
    exit_code = 1 # По умолчанию код выхода - ошибка
    try:
        # Создаем экземпляр генератора
        generator = ContentGenerator()
        # Запускаем основной метод run, передавая ID
        generator.run(generation_id_main)
        logger.info(f"--- Скрипт generate_content.py успешно завершен для ID: {generation_id_main} ---")
        exit_code = 0 # Успешное завершение
    except Exception as main_err:
         # Логгер внутри generator.run уже должен был залогировать детали
         logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА ВЫПОЛНЕНИЯ generate_content.py для ID {generation_id_main} !!!")
         # exit_code остается 1
    finally:
         logger.info(f"--- Завершение generate_content.py с кодом выхода: {exit_code} ---")
         sys.exit(exit_code) # Выход с соответствующим кодом

