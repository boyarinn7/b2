# -*- coding: utf-8 -*-
# В файле scripts/generate_content.py

import json
import os
import sys
import requests
import openai # Импортируем основной модуль
import re
import subprocess
import boto3
import io
import random
import argparse
from datetime import datetime, timezone # Обновленный импорт
import shutil
from pathlib import Path # Добавляем pathlib

# Импортируем ClientError из botocore (часть boto3)
try:
    from botocore.exceptions import ClientError
except ImportError:
    ClientError = Exception
    print("Warning: Could not import ClientError from botocore.")

# Добавляем путь к модулям, если скрипт запускается напрямую
BASE_DIR = Path(__file__).resolve().parent.parent # Используем pathlib
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# --- Импорт кастомных модулей ---
try:
    from modules.config_manager import ConfigManager
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.utils import ensure_directory_exists, load_b2_json, save_b2_json, load_json_config # Добавляем load_json_config
    from modules.api_clients import get_b2_client
except ModuleNotFoundError as e:
     print(f"Критическая Ошибка: Не найдены модули проекта в generate_content: {e}", file=sys.stderr)
     sys.exit(1)
except ImportError as e:
     print(f"Критическая Ошибка: Не найдена функция/класс в модулях: {e}", file=sys.stderr)
     sys.exit(1)

# --- Инициализация логгера и основного конфига ---
logger = get_logger("generate_content")
# Основной config загружается в __init__ генератора

# --- Константы (Перенесены в __init__ или используются через self.config) ---
# B2_BUCKET_NAME = ...
# FAILSAFE_PATH_REL = ...
# TRACKER_PATH_REL = ...
# CONTENT_OUTPUT_PATH = ...

# --- ФУНКЦИЯ СОХРАНЕНИЯ КОНТЕНТА В B2 (Остается без изменений по логике сохранения) ---
def save_content_to_b2(folder, content_dict, generation_id, config_manager_instance):
    """
    Сохраняет словарь content_dict как JSON в указанную папку B2,
    используя переданный generation_id для имени файла.
    НЕ генерирует новый ID и НЕ обновляет config_gen.json.
    Возвращает True при успехе, False при ошибке.
    """
    logger.info(f"Вызов save_content_to_b2 для ID: {generation_id}")

    # Используем переданный экземпляр ConfigManager
    config = config_manager_instance

    # Получаем B2 клиент
    s3 = get_b2_client()
    if not s3:
        logger.error("❌ Не удалось создать клиент B2 внутри save_content_to_b2")
        return False

    # Получаем имя бакета
    bucket_name = config.get("API_KEYS.b2.bucket_name") # Убрал дефолт, ConfigManager должен его вернуть
    if not bucket_name:
         logger.error("❌ Имя бакета B2 не найдено в save_content_to_b2")
         return False

    if not generation_id:
        logger.error("❌ Generation ID не предоставлен для save_content_to_b2.")
        return False
    if not isinstance(content_dict, dict):
         logger.error("❌ Данные для сохранения в save_content_to_b2 не являются словарем.")
         return False

    clean_base_id = generation_id.replace(".json", "")
    file_extension = ".json"
    s3_key = f"{folder.rstrip('/')}/{clean_base_id}{file_extension}"

    timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    local_temp_path = f"{clean_base_id}_content_temp_{timestamp_suffix}.json"

    logger.info(f"Попытка сохранения данных для ID {clean_base_id} в B2 как {s3_key} через {local_temp_path}...")

    try:
        # Проверка содержимого перед сохранением (ОБНОВЛЕНО для новых ключей)
        required_keys = ["topic", "content", "sarcasm", "script", "first_frame_description",
                         "creative_brief", "final_mj_prompt", "final_runway_prompt"]
        missing_keys = [key for key in required_keys if key not in content_dict]
        null_keys = [key for key in required_keys if key in content_dict and content_dict[key] is None]

        if missing_keys:
             logger.warning(f"⚠️ В сохраняемых данных для ID {clean_base_id} отсутствуют ключи: {missing_keys}. Содержимое: {list(content_dict.keys())}")
        if null_keys:
             logger.warning(f"⚠️ В сохраняемых данных для ID {clean_base_id} есть ключи с null: {null_keys}.")
        # Можно добавить более строгую проверку, если нужно

        with open(local_temp_path, 'w', encoding='utf-8') as f:
            json.dump(content_dict, f, ensure_ascii=False, indent=4)
        logger.debug(f"Временный файл {local_temp_path} создан.")

        s3.upload_file(local_temp_path, bucket_name, s3_key)
        logger.info(f"✅ Данные для ID {clean_base_id} успешно сохранены в B2: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Не удалось сохранить данные для ID {clean_base_id} в B2 как {s3_key}: {e}", exc_info=True)
        return False
    finally:
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
        self.config = ConfigManager() # Загружаем основной config.json

        # --- Загрузка дополнительных конфигураций ---
        self.creative_config_data = self._load_additional_config('FILE_PATHS.creative_config', 'Creative Config')
        self.prompts_config_data = self._load_additional_config('FILE_PATHS.prompts_config', 'Prompts Config')

        # Загрузка настроек генерации из основного конфига
        self.topic_threshold = self.config.get('GENERATE.topic_threshold', 7)
        self.text_threshold = self.config.get('GENERATE.text_threshold', 8)
        self.max_attempts = self.config.get('GENERATE.max_attempts', 1)
        self.adaptation_enabled = self.config.get('GENERATE.adaptation_enabled', False)
        self.adaptation_params = self.config.get('GENERATE.adaptation_parameters', {})
        self.content_output_path = self.config.get('FILE_PATHS.content_output_path', 'generated_content.json')

        # --- Настройки и инициализация OpenAI (v > 1.0) ---
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_model = self.config.get("OPENAI_SETTINGS.model", "gpt-4o")
        self.openai_client = None # Инициализируем как None
        if not self.openai_api_key:
            self.logger.error("❌ Переменная окружения OPENAI_API_KEY не задана!")
            # Не выбрасываем исключение сразу, дадим шанс методам проверить self.openai_client
        else:
            try:
                # Используем новый способ инициализации клиента
                self.openai_client = openai.OpenAI(api_key=self.openai_api_key)
                self.logger.info("✅ Клиент OpenAI (>1.0) успешно инициализирован.")
            except Exception as e:
                 self.logger.error(f"❌ Ошибка инициализации клиента OpenAI: {e}")
                 # self.openai_client останется None

        # Инициализация B2 клиента
        self.b2_client = get_b2_client()
        if not self.b2_client:
             self.logger.warning("⚠️ Не удалось инициализировать B2 клиент в ContentGenerator.")

        # Получаем пути к трекеру и failsafe из основного конфига
        self.tracker_path_rel = self.config.get("FILE_PATHS.tracker_path", "data/topics_tracker.json")
        self.failsafe_path_rel = self.config.get("FILE_PATHS.failsafe_path", "config/FailSafeVault.json")
        self.tracker_path_abs = BASE_DIR / self.tracker_path_rel
        self.failsafe_path_abs = BASE_DIR / self.failsafe_path_rel
        self.b2_bucket_name = self.config.get("API_KEYS.b2.bucket_name", "default-bucket") # Получаем имя бакета

    def _load_additional_config(self, config_key, config_name):
        """Вспомогательный метод для загрузки доп. конфигов."""
        config_path_str = self.config.get(config_key)
        if not config_path_str:
            self.logger.error(f"❌ Путь к {config_name} не найден в основном конфиге (ключ: {config_key}).")
            return None
        config_path = BASE_DIR / config_path_str
        data = load_json_config(str(config_path)) # Используем load_json_config из utils
        if data:
            self.logger.info(f"✅ {config_name} успешно загружен из {config_path}.")
        else:
            self.logger.error(f"❌ Не удалось загрузить {config_name} из {config_path}.")
        return data

    def adapt_prompts(self):
        """Применяет адаптацию промптов (если включено)."""
        # (Логика без изменений)
        if not self.adaptation_enabled:
            self.logger.info("🔄 Адаптация промптов отключена.")
            return
        self.logger.info("🔄 Применяю адаптацию промптов на основе обратной связи...")
        for key, value in self.adaptation_params.items():
            self.logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def clear_generated_content(self):
        """Очищает локальный файл с промежуточными результатами."""
        # (Логика без изменений)
        try:
            self.logger.info(f"🧹 Очистка локального файла: {self.content_output_path}")
            ensure_directory_exists(self.content_output_path)
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump({}, file, ensure_ascii=False, indent=4)
            self.logger.info("✅ Локальный файл успешно очищен/создан.")
        except PermissionError:
            handle_error("Clear Content Error", f"Нет прав на запись в файл: {self.content_output_path}", PermissionError())
        except Exception as e:
            handle_error("Clear Content Error", str(e), e)

    def load_tracker(self):
        """Загружает трекер тем из B2 или локального файла."""
        # Используем пути, определенные в __init__
        tracker_path_abs = self.tracker_path_abs
        tracker_path_rel = self.tracker_path_rel
        failsafe_path_abs = self.failsafe_path_abs
        bucket_name = self.b2_bucket_name

        os.makedirs(tracker_path_abs.parent, exist_ok=True)
        tracker_updated_locally = False

        if self.b2_client:
            try:
                self.logger.info(f"Попытка загрузки {tracker_path_rel} из B2...")
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                local_temp_tracker = f"tracker_temp_{timestamp_suffix}.json"
                self.b2_client.download_file(bucket_name, tracker_path_rel, local_temp_tracker)
                shutil.copyfile(local_temp_tracker, tracker_path_abs)
                os.remove(local_temp_tracker)
                self.logger.info(f"✅ Загружен {tracker_path_rel} из B2 в {tracker_path_abs}")
            except ClientError as e:
                 error_code = e.response.get('Error', {}).get('Code')
                 if error_code == 'NoSuchKey' or '404' in str(e):
                      self.logger.warning(f"⚠️ {tracker_path_rel} не найден в B2. Проверяем локальную копию.")
                 else:
                      self.logger.error(f"⚠️ Ошибка B2 при загрузке трекера: {e}")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось загрузить трекер из B2: {e}")
        else:
             self.logger.warning("⚠️ B2 клиент недоступен, используем только локальный трекер.")

        if not tracker_path_abs.exists():
            self.logger.warning(f"{tracker_path_abs} не найден. Попытка создания из {failsafe_path_abs}.")
            try:
                ensure_directory_exists(str(failsafe_path_abs)) # Убедимся, что папка для FailSafe существует
                with open(failsafe_path_abs, 'r', encoding='utf-8') as f_failsafe:
                    failsafe_data = json.load(f_failsafe)
                tracker = {
                    "all_focuses": failsafe_data.get("focuses", []),
                    "used_focuses": [],
                    "focus_data": {}
                }
                with open(tracker_path_abs, 'w', encoding='utf-8') as f_tracker:
                    json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                self.logger.info(f"✅ Создан новый {tracker_path_abs} из FailSafeVault.")
                tracker_updated_locally = True
            except FileNotFoundError:
                 self.logger.error(f"❌ Файл {failsafe_path_abs} не найден! Невозможно создать трекер.")
                 return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
            except Exception as e:
                 self.logger.error(f"❌ Ошибка при создании трекера из FailSafe: {e}")
                 return {"all_focuses": [], "used_focuses": [], "focus_data": {}}

        try:
            with open(tracker_path_abs, 'r', encoding='utf-8') as f:
                tracker = json.load(f)
            if "all_focuses" not in tracker:
                self.logger.info("Обновляем структуру старого трекера: добавляем all_focuses.")
                if failsafe_path_abs.exists():
                     with open(failsafe_path_abs, 'r', encoding='utf-8') as f_failsafe:
                         failsafe_data = json.load(f_failsafe)
                     tracker["all_focuses"] = failsafe_data.get("focuses", [])
                else:
                     tracker["all_focuses"] = []
                tracker.setdefault("used_focuses", [])
                tracker.setdefault("focus_data", {})
                with open(tracker_path_abs, 'w', encoding='utf-8') as f_tracker:
                    json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                tracker_updated_locally = True
            if tracker_updated_locally:
                self.sync_tracker_to_b2(tracker_path_abs=tracker_path_abs, tracker_path_rel=tracker_path_rel)

            return tracker
        except json.JSONDecodeError:
            self.logger.error(f"❌ Ошибка JSON в файле трекера: {tracker_path_abs}. Возвращаем пустой.")
            return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения трекера {tracker_path_abs}: {e}")
            return {"all_focuses": [], "used_focuses": [], "focus_data": {}}

    def get_valid_focus_areas(self, tracker):
        """Возвращает список доступных фокусов."""
        # (Логика без изменений)
        all_focuses = tracker.get("all_focuses", [])
        used_focuses = tracker.get("used_focuses", [])
        used_set = set(used_focuses)
        valid_focuses = [f for f in all_focuses if f not in used_set]
        self.logger.info(f"✅ Доступные фокусы: {len(valid_focuses)} шт.")
        self.logger.debug(f"Полный список доступных фокусов: {valid_focuses}")
        return valid_focuses

    def generate_topic(self, tracker):
        """Генерирует новую тему, используя доступные фокусы."""
        valid_focuses = self.get_valid_focus_areas(tracker)
        if not valid_focuses:
            self.logger.error("❌ Нет доступных фокусов для генерации темы.")
            raise ValueError("Все фокусы использованы, невозможно сгенерировать тему.")

        selected_focus = random.choice(valid_focuses)
        self.logger.info(f"Выбран фокус для генерации темы: {selected_focus}")
        used_labels_for_focus = tracker.get("focus_data", {}).get(selected_focus, [])
        exclusions_str = ", ".join(used_labels_for_focus) if used_labels_for_focus else "нет"

        # --- Чтение промпта из prompts_config_data ---
        prompt_template = self.prompts_config_data.get("content", {}).get("topic")
        if not prompt_template:
             self.logger.error("Промпт content.topic не найден в prompts_config.json!")
             raise ValueError("Отсутствует промпт для генерации темы.")
        prompt = prompt_template.format(focus_areas=selected_focus, exclusions=exclusions_str)

        topic_response_str = ""
        try:
            # Используем request_openai, который теперь использует новый API
            topic_response_str = self.request_openai(prompt, use_json_mode=True)
            if not topic_response_str: # Проверка на None
                 raise ValueError("OpenAI не вернул ответ для генерации темы.")

            topic_data = json.loads(topic_response_str)

            full_topic = topic_data.get("full_topic")
            short_topic = topic_data.get("short_topic")

            if not full_topic or not short_topic:
                self.logger.error(f"❌ OpenAI вернул неполные данные для темы: {topic_data}")
                raise ValueError("Ответ OpenAI для темы не содержит full_topic или short_topic.")

            self.logger.info(f"Сгенерирована тема: '{full_topic}' (Ярлык: '{short_topic}')")
            self.update_tracker(selected_focus, short_topic, tracker)
            self.save_to_generated_content("topic", {"full_topic": full_topic, "short_topic": short_topic})

            content_metadata = {"theme": "tragic" if "(т)" in selected_focus else "normal"}
            return full_topic, content_metadata

        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Ошибка парсинга JSON ответа OpenAI для темы: {e}. Ответ: {topic_response_str[:500]}")
            raise ValueError("Ошибка формата ответа OpenAI при генерации темы.") from e
        except Exception as e:
            self.logger.error(f"❌ Ошибка при генерации темы: {e}", exc_info=True)
            raise

    def update_tracker(self, focus, short_topic, tracker):
        """Обновляет данные трекера в памяти и сохраняет его."""
        # (Логика без изменений)
        used_focuses = tracker.get("used_focuses", [])
        focus_data = tracker.get("focus_data", {})
        if focus in used_focuses: used_focuses.remove(focus)
        used_focuses.insert(0, focus)
        tracker["used_focuses"] = used_focuses[:15]
        focus_labels = focus_data.setdefault(focus, [])
        if short_topic in focus_labels: focus_labels.remove(short_topic)
        focus_labels.insert(0, short_topic)
        focus_data[focus] = focus_labels[:5]
        tracker["focus_data"] = focus_data
        self.save_topics_tracker(tracker)
        self.sync_tracker_to_b2(tracker_path_abs=self.tracker_path_abs, tracker_path_rel=self.tracker_path_rel)

    def save_topics_tracker(self, tracker):
        """Сохраняет трекер в локальный файл."""
        # (Логика без изменений)
        try:
            ensure_directory_exists(str(self.tracker_path_abs))
            with open(self.tracker_path_abs, "w", encoding="utf-8") as file:
                json.dump(tracker, file, ensure_ascii=False, indent=4)
            self.logger.info(f"Трекер тем сохранен локально: {self.tracker_path_abs}")
        except Exception as e:
             self.logger.error(f"Ошибка сохранения трекера локально: {e}")

    def sync_tracker_to_b2(self, tracker_path_abs, tracker_path_rel):
        """Синхронизирует локальный трекер с B2."""
        # (Логика без изменений)
        if not self.b2_client: self.logger.warning("⚠️ B2 клиент недоступен, синхронизация трекера невозможна."); return
        if not tracker_path_abs.exists(): self.logger.warning(f"⚠️ Локальный файл трекера {tracker_path_abs} не найден для синхронизации."); return
        try:
            self.logger.info(f"Синхронизация {tracker_path_abs} с B2 как {tracker_path_rel}...")
            self.b2_client.upload_file(str(tracker_path_abs), self.b2_bucket_name, tracker_path_rel)
            self.logger.info(f"✅ {tracker_path_rel} синхронизирован с B2.")
        except Exception as e: self.logger.error(f"⚠️ Не удалось загрузить трекер {tracker_path_rel} в B2: {e}")

    def request_openai(self, prompt, use_json_mode=False, temperature_override=None, max_tokens_override=None):
        """
        Отправляет запрос к OpenAI (v > 1.0), опционально запрашивая JSON,
        переопределяя температуру и макс. токены.
        Возвращает строку ответа или None при ошибке.
        """
        if not self.openai_client:
            self.logger.error("❌ OpenAI клиент не инициализирован.")
            return None

        try:
            # --- Логика определения параметров (можно вынести в отдельный метод) ---
            default_temp = self.config.get("OPENAI_SETTINGS.temperature", 0.7)
            default_max_tokens = 1500 # Общий дефолт

            # Эвристика для определения типа запроса и параметров по умолчанию
            prompt_lower = prompt.lower()
            if use_json_mode:
                if "script" in prompt_lower or "frame_description" in prompt_lower or "final_mj_prompt" in prompt_lower or "final_runway_prompt" in prompt_lower:
                    default_max_tokens = self.config.get("OPENAI_SETTINGS.max_tokens_script", 1000)
                    default_temp = self.config.get("OPENAI_SETTINGS.temperature_script", 0.7)
                elif "poll" in prompt_lower or "опрос" in prompt_lower:
                    default_max_tokens = self.config.get("SARCASM.max_tokens_poll", 250)
                    # Температура для опроса должна передаваться через temperature_override
                elif "translate" in prompt_lower or "перевод" in prompt_lower:
                     default_max_tokens = 2000 # Больше токенов для перевода
                     default_temp = 0.3 # Низкая температура для точности перевода
                else: # Вероятно, тема или креативный бриф (шаги 1, 2, 3)
                    default_max_tokens = self.config.get("OPENAI_SETTINGS.max_tokens_text", 750)
            elif "comment" in prompt_lower or "комментарий" in prompt_lower:
                default_max_tokens = self.config.get("SARCASM.max_tokens_comment", 150)
                # Температура для комментария должна передаваться через temperature_override
            else: # Обычный текст (основной контент)
                default_max_tokens = self.config.get("OPENAI_SETTINGS.max_tokens_text", 750)

            # Применяем переопределения, если они есть
            temp = temperature_override if temperature_override is not None else default_temp
            max_tokens = max_tokens_override if max_tokens_override is not None else default_max_tokens
            # ---------------------------------------------------------------------

            self.logger.info(f"🔎 Вызов OpenAI (Модель: {self.openai_model}, JSON={use_json_mode}, t={temp:.2f}, max_tokens={max_tokens})...")

            messages = [
                {"role": "system", "content": "Ты - AI ассистент, который строго следует инструкциям пользователя и всегда отвечает ТОЛЬКО в указанном формате (например, JSON), без какого-либо дополнительного текста."},
                {"role": "user", "content": prompt}
            ]

            request_params = {
                "model": self.openai_model,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": temp,
            }

            if use_json_mode:
                request_params["response_format"] = {"type": "json_object"}

            # Используем новый синтаксис
            response = self.openai_client.chat.completions.create(**request_params)

            # Обработка ответа
            if response.choices and response.choices[0].message and response.choices[0].message.content:
                response_content = response.choices[0].message.content.strip()
                # Очистка от ```json, если модель их добавляет
                if use_json_mode and response_content.startswith("```json"):
                     response_content = response_content[7:]
                     if response_content.endswith("```"):
                         response_content = response_content[:-3]
                     response_content = response_content.strip()
                self.logger.debug(f"Сырой ответ OpenAI: {response_content[:500]}...")
                return response_content
            else:
                self.logger.error("❌ OpenAI API вернул пустой или некорректный ответ.")
                return None

        except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI: {e}"); return None
        except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI: {e}"); return None
        except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI: {e}"); return None
        except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}"); return None
        except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI: {e}"); return None
        except Exception as e: logger.exception(f"Неизвестная ошибка в request_openai: {e}"); return None

    def generate_sarcasm(self, text, content_data={}):
        """Генерирует саркастический комментарий."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.comment_enabled', True):
            self.logger.info("🔕 Генерация саркастического комментария отключена.")
            return None

        prompt_key = "tragic_comment" if content_data.get("theme") == "tragic" else "comment"
        prompt_template = self.prompts_config_data.get("sarcasm", {}).get(prompt_key)
        temperature_key = "tragic_comment_temperature" if content_data.get("theme") == "tragic" else "comment_temperature"
        temperature = self.config.get(f'SARCASM.{temperature_key}', 0.8) # Получаем температуру из config.json

        if not prompt_template:
            self.logger.error(f"Промпт sarcasm.{prompt_key} не найден в prompts_config.json!")
            return None

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос к OpenAI для генерации комментария (тип: {prompt_key}, temp: {temperature:.1f})...")

        try:
            comment = self.request_openai(prompt, temperature_override=temperature) # Не JSON
            if comment: self.logger.info(f"✅ Саркастический комментарий сгенерирован: {comment}")
            else: self.logger.error(f"❌ Ошибка генерации саркастического комментария (OpenAI вернул None).")
            return comment
        except Exception as e:
            self.logger.error(f"❌ Исключение при генерации саркастического комментария: {e}")
            return None

    def generate_sarcasm_poll(self, text, content_data={}):
        """Генерирует саркастический опрос, ожидая JSON."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.poll_enabled', True):
            self.logger.info("🔕 Генерация саркастического опроса отключена.")
            return {}

        prompt_key = "tragic_poll" if content_data.get("theme") == "tragic" else "poll"
        prompt_template = self.prompts_config_data.get("sarcasm", {}).get(prompt_key)
        temperature_key = "tragic_poll_temperature" if content_data.get("theme") == "tragic" else "poll_temperature"
        temperature = self.config.get(f'SARCASM.{temperature_key}', 0.9) # Получаем температуру из config.json

        if not prompt_template:
             self.logger.error(f"Промпт sarcasm.{prompt_key} не найден в prompts_config.json!")
             return {}

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос к OpenAI для генерации опроса (тип: {prompt_key}, temp: {temperature:.1f})... Ожидаем JSON.")
        response_content = ""
        try:
            response_content = self.request_openai(prompt, use_json_mode=True, temperature_override=temperature)
            if not response_content:
                 self.logger.error("❌ Ошибка генерации саркастического опроса (OpenAI вернул None).")
                 return {}

            self.logger.debug(f"Сырой ответ OpenAI для опроса: {response_content[:500]}")
            poll_data = json.loads(response_content)

            if isinstance(poll_data, dict) and "question" in poll_data and "options" in poll_data and isinstance(poll_data["options"], list) and len(poll_data["options"]) == 3:
                self.logger.info("✅ Опрос успешно сгенерирован и распарсен (JSON).")
                poll_data["question"] = str(poll_data["question"]).strip()
                poll_data["options"] = [str(opt).strip() for opt in poll_data["options"]]
                return poll_data
            else:
                self.logger.error(f"❌ OpenAI вернул JSON для опроса, но структура неверна: {poll_data}")
                return {}

        except json.JSONDecodeError as e:
            self.logger.error(f"❌ Ошибка парсинга JSON ответа OpenAI для опроса: {e}. Ответ: {response_content[:500]}")
            return {}
        except Exception as e:
            self.logger.error(f"❌ Исключение при генерации саркастического опроса: {e}")
            return {}

    def save_to_generated_content(self, stage, data):
        """Сохраняет промежуточные данные в локальный JSON файл."""
        # (Логика без изменений)
        try:
            if not self.content_output_path: raise ValueError("❌ Ошибка: self.content_output_path не задан!")
            self.logger.debug(f"🔄 Обновление локального файла: {self.content_output_path}, этап: {stage}")
            ensure_directory_exists(self.content_output_path)
            result_data = {}
            if os.path.exists(self.content_output_path):
                try:
                    if os.path.getsize(self.content_output_path) > 0:
                        with open(self.content_output_path, 'r', encoding='utf-8') as file: result_data = json.load(file)
                    else: self.logger.warning(f"⚠️ Файл {self.content_output_path} пуст, начинаем с {{}}"); result_data = {}
                except json.JSONDecodeError: self.logger.warning(f"⚠️ Файл {self.content_output_path} поврежден, создаем новый."); result_data = {}
                except Exception as read_err: self.logger.error(f"Ошибка чтения {self.content_output_path}: {read_err}"); result_data = {}
            result_data["timestamp"] = datetime.utcnow().isoformat()
            result_data[stage] = data
            with open(self.content_output_path, 'w', encoding='utf-8') as file: json.dump(result_data, file, ensure_ascii=False, indent=4)
            self.logger.debug(f"✅ Данные локально обновлены для этапа: {stage}")
        except Exception as e: handle_error("Save to Generated Content Error", f"Ошибка при сохранении в {self.content_output_path}: {str(e)}", e)

    def critique_content(self, content, topic):
        """Выполняет критику текста (если включено)."""
        if not self.config.get('CONTENT.critique.enabled', False):
            self.logger.info("🔕 Критика контента отключена.")
            return "Критика отключена в конфигурации."
        if not content: self.logger.warning("Нет текста для критики."); return "Нет текста для критики."
        try:
            self.logger.info("🔄 Выполняется критика текста через OpenAI...")
            # --- Чтение промпта из prompts_config_data ---
            prompt_template = self.prompts_config_data.get("content", {}).get("critique")
            if not prompt_template or prompt_template == "...": # Проверка на заглушку
                 self.logger.error("Промпт content.critique не найден или не заполнен в prompts_config.json!")
                 return "Промпт для критики не найден."

            prompt = prompt_template.format(content=content, topic=topic)
            temperature = self.config.get('CONTENT.critique.temperature', 0.3)
            critique = self.request_openai(prompt, temperature_override=temperature) # Не JSON
            if critique: self.logger.info("✅ Критика успешно завершена.")
            else: self.logger.error("❌ Ошибка при выполнении критики (OpenAI вернул None).")
            return critique if critique else "Критика текста завершилась ошибкой."
        except Exception as e:
            self.logger.error(f"❌ Исключение при выполнении критики: {e}")
            return "Критика текста завершилась ошибкой."

    # --- Вспомогательная функция форматирования списков (из тестера) ---
    def format_list_for_prompt(self, items: list | dict, use_weights=False) -> str:
        """Форматирует список или словарь списков для вставки в промпт."""
        lines = []
        if isinstance(items, list):
            if not items: return "- (Список пуст)"
            for item in items:
                if use_weights and isinstance(item, dict) and 'value' in item and 'weight' in item:
                    lines.append(f"* {item['value']} (Вес: {item['weight']})")
                elif isinstance(item, str): lines.append(f"* {item}")
                elif isinstance(item, dict) and 'value' in item: lines.append(f"* {item['value']}")
        elif isinstance(items, dict):
             if not items: return "- (Словарь пуст)"
             for category, cat_items in items.items():
                 if isinstance(cat_items, list):
                     # Не добавляем заголовок категории, если это не main с весами
                     # или если это не словарь со списками внутри
                     is_main_with_weights = use_weights and category == 'main'
                     is_nested_list_dict = isinstance(items.get(next(iter(items))), list) # Проверка, что это словарь списков

                     if not is_main_with_weights and is_nested_list_dict:
                          if lines: lines.append("")
                          lines.append(f"  Категория '{category}':")

                     formatted_sublist = self.format_list_for_prompt(cat_items, use_weights=is_main_with_weights)
                     if formatted_sublist != "- (Список пуст)":
                         # Отступ только для подсписков в словаре списков
                         indent = "    " if is_nested_list_dict else ""
                         indented_lines = [f"{indent}{line}" if line.strip() else line for line in formatted_sublist.split('\n')]
                         lines.extend(indented_lines)
                 else:
                     lines.append(f"* {category}: {cat_items}") # Для простых пар ключ-значение
        else: return "- (Неверный формат данных)"
        return "\n".join(lines).strip()
    # --- Конец вспомогательной функции ---

    def run(self, generation_id):
        """Основной процесс генерации контента для заданного ID."""
        self.logger.info(f"--- Запуск ContentGenerator.run для ID: {generation_id} ---")
        if not generation_id:
             self.logger.error("❌ В ContentGenerator.run не передан generation_id!")
             raise ValueError("generation_id не может быть пустым.")
        if not self.creative_config_data or not self.prompts_config_data:
             self.logger.error("❌ Не загружены creative_config или prompts_config. Прерывание.")
             raise RuntimeError("Конфигурационные файлы не загружены.")
        if not self.openai_client:
             self.logger.error("❌ OpenAI клиент не инициализирован. Прерывание.")
             raise RuntimeError("OpenAI клиент не инициализирован.")

        try:
            # --- Шаг 1: Подготовка ---
            self.adapt_prompts()
            self.clear_generated_content()

            # --- Шаг 2: Генерация Темы ---
            tracker = self.load_tracker()
            topic, content_data = self.generate_topic(tracker)

            # --- Шаг 3: Генерация Текста (основного) ---
            text_initial = ""
            generate_text_enabled = self.config.get('CONTENT.text.enabled', True)
            generate_tragic_text_enabled = self.config.get('CONTENT.tragic_text.enabled', True)

            if (content_data.get("theme") == "tragic" and generate_tragic_text_enabled) or \
               (content_data.get("theme") != "tragic" and generate_text_enabled):

                prompt_key = "tragic_text" if content_data.get("theme") == "tragic" else "text"
                prompt_template = self.prompts_config_data.get("content", {}).get(prompt_key)

                if prompt_template:
                     # Получаем температуру из основного config.json
                     temp_key = "temperature" # Общий ключ температуры в config.json
                     temperature = self.config.get(f'CONTENT.{prompt_key}.{temp_key}', 0.7)

                     text_initial = self.request_openai(prompt_template.format(topic=topic), temperature_override=temperature) # Не JSON
                     if text_initial:
                         self.logger.info(f"Сгенерирован текст (длина: {len(text_initial)}): {text_initial[:100]}...")
                         self.save_to_generated_content("text", {"text": text_initial})
                     else:
                          self.logger.warning(f"Генерация текста ({prompt_key}) не удалась (OpenAI вернул None).")
                else:
                     self.logger.warning(f"Промпт content.{prompt_key} не найден, генерация текста пропущена.")
            else:
                self.logger.info(f"🔕 Генерация текста (тема: {content_data.get('theme')}) отключена.")

            # --- Шаг 4: Критика (если включена) ---
            critique_result = self.critique_content(text_initial, topic)
            self.save_to_generated_content("critique", {"critique": critique_result})

            # --- Шаг 5: Генерация Сарказма ---
            sarcastic_comment = None
            sarcastic_poll = {}
            if text_initial:
                sarcastic_comment = self.generate_sarcasm(text_initial, content_data)
                sarcastic_poll = self.generate_sarcasm_poll(text_initial, content_data)
            self.save_to_generated_content("sarcasm", {"comment": sarcastic_comment, "poll": sarcastic_poll})

            # --- Шаг 6: Многошаговая Генерация Брифа и Промптов (НОВАЯ ЛОГИКА) ---
            self.logger.info("--- Запуск многошаговой генерации креативного брифа и промптов ---")
            creative_brief = None
            script_en = None
            frame_description_en = None
            final_mj_prompt_en = None
            final_runway_prompt_en = None
            script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru = None, None, None, None
            translations = None
            enable_russian_translation = self.config.get("WORKFLOW.enable_russian_translation", False)

            try:
                # --- Подготовка списков для промптов ---
                moods_list_str = self.format_list_for_prompt(self.creative_config_data.get("moods", []), use_weights=True)
                arcs_list_str = self.format_list_for_prompt(self.creative_config_data.get("emotional_arcs", []))
                # Используем структуру creative_prompts из creative_config.json
                main_prompts_list = self.creative_config_data.get("creative_prompts", {}).get("main", [])
                prompts_list_str = self.format_list_for_prompt(main_prompts_list, use_weights=True)
                perspectives_list_str = self.format_list_for_prompt(self.creative_config_data.get("perspective_types", []))
                metaphors_list_str = self.format_list_for_prompt(self.creative_config_data.get("visual_metaphor_types", []))
                directors_list_str = self.format_list_for_prompt(self.creative_config_data.get("director_styles", []))
                artists_list_str = self.format_list_for_prompt(self.creative_config_data.get("artist_styles", []))

                # --- Шаг 6.1: Эмоциональное Ядро ---
                self.logger.info("--- Шаг 6.1: Выбор Эмоционального Ядра ---")
                prompt1_tmpl = self.prompts_config_data.get("multi_step", {}).get("step1_core")
                if not prompt1_tmpl: raise ValueError("Промпт multi_step.step1_core не найден.")
                prompt1_text = prompt1_tmpl.format(input_text=topic, moods_list_str=moods_list_str, arcs_list_str=arcs_list_str) # Используем topic как input_text
                core_brief_str = self.request_openai(prompt1_text, use_json_mode=True)
                if not core_brief_str: raise ValueError("Шаг 6.1 не удался (OpenAI вернул None).")
                core_brief = json.loads(core_brief_str)
                if not core_brief or not all(k in core_brief for k in ["chosen_type", "chosen_value", "justification"]): raise ValueError(f"Шаг 6.1: неверный JSON {core_brief}.")

                # --- Шаг 6.2: Креативный Драйвер ---
                self.logger.info("--- Шаг 6.2: Выбор Основного Креативного Драйвера ---")
                prompt2_tmpl = self.prompts_config_data.get("multi_step", {}).get("step2_driver")
                if not prompt2_tmpl: raise ValueError("Промпт multi_step.step2_driver не найден.")
                prompt2_text = prompt2_tmpl.format(input_text=topic, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), prompts_list_str=prompts_list_str, perspectives_list_str=perspectives_list_str, metaphors_list_str=metaphors_list_str)
                driver_brief_str = self.request_openai(prompt2_text, use_json_mode=True)
                if not driver_brief_str: raise ValueError("Шаг 6.2 не удался (OpenAI вернул None).")
                driver_brief = json.loads(driver_brief_str)
                if not driver_brief or not all(k in driver_brief for k in ["chosen_driver_type", "chosen_driver_value", "justification"]): raise ValueError(f"Шаг 6.2: неверный JSON {driver_brief}.")

                # --- Шаг 6.3: Эстетический Фильтр ---
                self.logger.info("--- Шаг 6.3: Выбор Эстетического Фильтра ---")
                prompt3_tmpl = self.prompts_config_data.get("multi_step", {}).get("step3_aesthetic")
                if not prompt3_tmpl: raise ValueError("Промпт multi_step.step3_aesthetic не найден.")
                prompt3_text = prompt3_tmpl.format(input_text=topic, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), chosen_driver_json=json.dumps(driver_brief, ensure_ascii=False, indent=2), directors_list_str=directors_list_str, artists_list_str=artists_list_str)
                aesthetic_brief_str = self.request_openai(prompt3_text, use_json_mode=True)
                if not aesthetic_brief_str: raise ValueError("Шаг 6.3 не удался (OpenAI вернул None).")
                aesthetic_brief = json.loads(aesthetic_brief_str)
                # Валидация aesthetic_brief (как в тестере)
                valid_step3 = False
                if isinstance(aesthetic_brief, dict):
                    style_needed = aesthetic_brief.get("style_needed", False)
                    base_keys_exist = all(k in aesthetic_brief for k in ["style_needed", "chosen_style_type", "chosen_style_value", "style_keywords", "justification"])
                    if base_keys_exist:
                        if not style_needed:
                            if (aesthetic_brief.get("chosen_style_type") is None and aesthetic_brief.get("chosen_style_value") is None and aesthetic_brief.get("style_keywords") is None and aesthetic_brief.get("justification") is None): valid_step3 = True
                            else: self.logger.warning(f"Шаг 6.3: style_needed=false, но ключи не null: {aesthetic_brief}. Исправляем."); aesthetic_brief.update({"chosen_style_type":None, "chosen_style_value":None, "style_keywords":None, "justification":None}); valid_step3 = True
                        else:
                            if (aesthetic_brief.get("chosen_style_type") and aesthetic_brief.get("chosen_style_value") and isinstance(aesthetic_brief.get("style_keywords"), list) and aesthetic_brief.get("justification")): valid_step3 = True
                            else: logger.error(f"Шаг 6.3: style_needed=true, но значения некорректны. {aesthetic_brief}")
                    else: logger.error(f"Шаг 6.3: Отсутствуют ключи: {aesthetic_brief}")
                else: logger.error(f"Шаг 6.3: Ответ не словарь: {aesthetic_brief}")
                if not valid_step3: raise ValueError("Шаг 6.3: неверный JSON.")

                # --- Сборка Брифа ---
                creative_brief = {"core": core_brief, "driver": driver_brief, "aesthetic": aesthetic_brief}
                self.logger.info("--- Шаг 6.4: Креативный Бриф Собран ---")
                self.logger.debug(f"Бриф: {json.dumps(creative_brief, ensure_ascii=False, indent=2)}")
                self.save_to_generated_content("creative_brief", creative_brief) # Сохраняем локально

                # --- Шаг 6.5: Сценарий и Описание Кадра ---
                self.logger.info("--- Шаг 6.5: Генерация Сценария и Описания Кадра ---")
                prompt5_tmpl = self.prompts_config_data.get("multi_step", {}).get("step5_script_frame")
                if not prompt5_tmpl: raise ValueError("Промпт multi_step.step5_script_frame не найден.")
                prompt5_text = prompt5_tmpl.format(input_text=topic, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2))
                script_frame_data_str = self.request_openai(prompt5_text, use_json_mode=True, max_tokens_override=1000)
                if not script_frame_data_str: raise ValueError("Шаг 6.5 не удался (OpenAI вернул None).")
                script_frame_data = json.loads(script_frame_data_str)
                if not script_frame_data or not all(k in script_frame_data for k in ["script", "first_frame_description"]): raise ValueError(f"Шаг 6.5: неверный JSON {script_frame_data}.")
                script_en = script_frame_data["script"]
                frame_description_en = script_frame_data["first_frame_description"]
                self.logger.info(f"Сценарий (EN): {script_en[:100]}...")
                self.logger.info(f"Описание кадра (EN): {frame_description_en[:100]}...")
                self.save_to_generated_content("script_frame_en", {"script": script_en, "first_frame_description": frame_description_en}) # Сохраняем локально

                # --- Шаг 6.6a: Адаптация под MJ V7 ---
                self.logger.info("--- Шаг 6.6a: Адаптация Описания под Midjourney V7 ---")
                mj_params_cfg = self.config.get("IMAGE_GENERATION", {})
                aspect_ratio = mj_params_cfg.get("output_size", "16:9").replace('x', ':').replace('×', ':') # Получаем AR из config
                version = mj_params_cfg.get("midjourney_version", "7.0") # Используем версию из config
                style = mj_params_cfg.get("midjourney_style", None)
                mj_parameters_json_for_prompt = json.dumps({"aspect_ratio": aspect_ratio, "version": version, "style": style}, ensure_ascii=False)
                style_parameter_str = f" --style {style}" if style else ""
                prompt6a_tmpl = self.prompts_config_data.get("multi_step", {}).get("step6a_mj_adapt")
                if not prompt6a_tmpl: raise ValueError("Промпт multi_step.step6a_mj_adapt не найден.")
                prompt6a_text = prompt6a_tmpl.format(
                    first_frame_description=frame_description_en,
                    creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2),
                    script=script_en,
                    input_text=topic,
                    mj_parameters_json=mj_parameters_json_for_prompt,
                    aspect_ratio=aspect_ratio,
                    version=version, # Передаем версию
                    style_parameter_str=style_parameter_str
                )
                mj_prompt_data_str = self.request_openai(prompt6a_text, use_json_mode=True, max_tokens_override=1000)
                if not mj_prompt_data_str: raise ValueError("Шаг 6.6a не удался (OpenAI вернул None).")
                mj_prompt_data = json.loads(mj_prompt_data_str)
                if not mj_prompt_data or "final_mj_prompt" not in mj_prompt_data: raise ValueError(f"Шаг 6.6a: неверный JSON {mj_prompt_data}.")
                final_mj_prompt_en = mj_prompt_data["final_mj_prompt"]
                self.logger.info(f"MJ промпт (EN, V{version}): {final_mj_prompt_en}")
                self.save_to_generated_content("final_mj_prompt_en", {"final_mj_prompt": final_mj_prompt_en}) # Сохраняем локально

                # --- Шаг 6.6b: Адаптация под Runway ---
                self.logger.info("--- Шаг 6.6b: Адаптация Сценария под Runway ---")
                prompt6b_tmpl = self.prompts_config_data.get("multi_step", {}).get("step6b_runway_adapt")
                if not prompt6b_tmpl: raise ValueError("Промпт multi_step.step6b_runway_adapt не найден.")
                prompt6b_text = prompt6b_tmpl.format(script=script_en, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2), input_text=topic)
                runway_prompt_data_str = self.request_openai(prompt6b_text, use_json_mode=True, max_tokens_override=1000)
                if not runway_prompt_data_str: raise ValueError("Шаг 6.6b не удался (OpenAI вернул None).")
                runway_prompt_data = json.loads(runway_prompt_data_str)
                if not runway_prompt_data or "final_runway_prompt" not in runway_prompt_data: raise ValueError(f"Шаг 6.6b: неверный JSON {runway_prompt_data}.")
                final_runway_prompt_en = runway_prompt_data["final_runway_prompt"]
                self.logger.info(f"Runway промпт (EN): {final_runway_prompt_en}")
                self.save_to_generated_content("final_runway_prompt_en", {"final_runway_prompt": final_runway_prompt_en}) # Сохраняем локально

                # --- Шаг 6.6c: Перевод на русский (Опционально) ---
                if enable_russian_translation:
                    self.logger.info("--- Шаг 6.6c: Перевод результатов на русский язык ---")
                    if all([script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en]):
                        prompt6c_tmpl = self.prompts_config_data.get("multi_step", {}).get("step6c_translate")
                        if not prompt6c_tmpl: raise ValueError("Промпт multi_step.step6c_translate не найден.")
                        prompt6c_text = prompt6c_tmpl.format(script_en=script_en, frame_description_en=frame_description_en, mj_prompt_en=final_mj_prompt_en, runway_prompt_en=final_runway_prompt_en)
                        translations_str = self.request_openai(prompt6c_text, use_json_mode=True, max_tokens_override=2000, temperature_override=0.3) # Низкая температура для перевода
                        if translations_str:
                            translations = json.loads(translations_str)
                            script_ru = translations.get("script_ru")
                            frame_description_ru = translations.get("first_frame_description_ru")
                            final_mj_prompt_ru = translations.get("final_mj_prompt_ru")
                            final_runway_prompt_ru = translations.get("final_runway_prompt_ru")
                            if all([script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru]):
                                self.logger.info("✅ Перевод на русский выполнен.")
                                self.save_to_generated_content("translations_ru", translations) # Сохраняем локально
                            else:
                                self.logger.error(f"Шаг 6.6c: Не все поля переведены. {translations}")
                                translations = None # Сбрасываем, если перевод неполный
                        else:
                            self.logger.error("Шаг 6.6c (Перевод) не удался (OpenAI вернул None).")
                            translations = None
                    else:
                         self.logger.error("Недостаточно данных для запуска Шага 6.6c (Перевод).")
                         translations = None
                else:
                     self.logger.info("Шаг 6.6c (Перевод) пропущен согласно настройке.")

            except (json.JSONDecodeError, ValueError) as parse_err:
                self.logger.error(f"❌ Ошибка парсинга/валидации/выполнения шага 6: {parse_err}.")
                # Не прерываем весь процесс, но логируем и оставляем None
                creative_brief, script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en = None, None, None, None, None
                script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru = None, None, None, None
            except Exception as script_err:
                self.logger.error(f"❌ Неожиданная ошибка при генерации шага 6: {script_err}", exc_info=True)
                creative_brief, script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en = None, None, None, None, None
                script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru = None, None, None, None

            # --- Шаг 7: Формирование и Сохранение Итогового Контента в B2 ---
            self.logger.info("Формирование итогового словаря для B2...")
            complete_content_dict = {
                "topic": topic,
                "content": text_initial.strip() if text_initial else "", # Основной текст (RU)
                "sarcasm": {"comment": sarcastic_comment, "poll": sarcastic_poll}, # Сарказм (RU)
                "script": script_en, # Сценарий (EN)
                "first_frame_description": frame_description_en, # Описание кадра (EN)
                "creative_brief": creative_brief, # Бриф (EN/RU в зависимости от содержимого)
                "final_mj_prompt": final_mj_prompt_en, # Промпт MJ (EN)
                "final_runway_prompt": final_runway_prompt_en, # Промпт Runway (EN)
                # Добавляем переводы, если они есть
                "script_ru": script_ru if enable_russian_translation else None,
                "first_frame_description_ru": frame_description_ru if enable_russian_translation else None,
                "final_mj_prompt_ru": final_mj_prompt_ru if enable_russian_translation else None,
                "final_runway_prompt_ru": final_runway_prompt_ru if enable_russian_translation else None,
            }
            # Удаляем ключи с None, если перевод отключен
            if not enable_russian_translation:
                complete_content_dict = {k: v for k, v in complete_content_dict.items() if not k.endswith('_ru')}

            self.logger.debug(f"Итоговый словарь: {json.dumps(complete_content_dict, ensure_ascii=False, indent=2)}")

            self.logger.info(f"Сохранение итогового контента в B2 для ID {generation_id}...")
            # Передаем экземпляр ConfigManager в функцию сохранения
            success = save_content_to_b2("666/", complete_content_dict, generation_id, self.config)
            if not success:
                raise Exception(f"Не удалось сохранить итоговый контент в B2 для ID {generation_id}")

            # --- Шаг 8: Обновление config_midjourney.json ---
            self.logger.info(f"Обновление config_midjourney.json для ID: {generation_id} (установка generation: true)")
            try:
                s3_client_mj = self.b2_client
                if not s3_client_mj: raise ConnectionError("B2 клиент недоступен для обновления config_midjourney")

                config_mj_remote_path = self.config.get('FILE_PATHS.config_midjourney', 'config/config_midjourney.json')
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"
                bucket_name = self.b2_bucket_name

                config_mj = load_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, default_value={})
                if config_mj is None: config_mj = {}

                config_mj['generation'] = True
                config_mj['midjourney_task'] = None
                config_mj['midjourney_results'] = {}
                config_mj['status'] = None
                self.logger.info("Данные для config_midjourney.json подготовлены: generation=True, task/results очищены.")

                if not save_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, config_mj):
                     raise Exception("Не удалось сохранить config_mj после установки generation=True!")
                else:
                     self.logger.info(f"✅ Обновленный {config_mj_remote_path} (generation=True) загружен в B2.")

            except Exception as e:
                self.logger.error(f"❌ Не удалось обновить config_midjourney.json: {str(e)}", exc_info=True)
                raise Exception("Критическая ошибка: не удалось установить флаг generation: true") from e

            self.logger.info(f"✅ ContentGenerator.run успешно завершен для ID {generation_id}.")

        except Exception as e:
            self.logger.error(f"❌ Ошибка в ContentGenerator.run для ID {generation_id}: {str(e)}", exc_info=True)
            raise

# --- Точка входа ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate content for a specific ID.')
    parser.add_argument('--generation_id', type=str, required=True, help='The generation ID for the content file (Mandatory).')
    args = parser.parse_args()
    generation_id_main = args.generation_id

    if not generation_id_main:
         logger.critical("Критическая ошибка: generation_id не был передан или пуст!")
         sys.exit(1)

    logger.info(f"--- Запуск скрипта generate_content.py для ID: {generation_id_main} ---")
    exit_code = 1
    try:
        generator = ContentGenerator()
        generator.run(generation_id_main)
        logger.info(f"--- Скрипт generate_content.py успешно завершен для ID: {generation_id_main} ---")
        exit_code = 0
    except Exception as main_err:
         logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА ВЫПОЛНЕНИЯ generate_content.py для ID {generation_id_main} !!!")
         # Логгер внутри generator.run уже залогировал детали
    finally:
         logger.info(f"--- Завершение generate_content.py с кодом выхода: {exit_code} ---")
         sys.exit(exit_code)
