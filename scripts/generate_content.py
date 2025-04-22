# -*- coding: utf-8 -*-
# В файле scripts/generate_content.py

import json
import os
import sys
# import requests # Не используется напрямую
import openai # Импортируем основной модуль
import re
# import subprocess # Не используется напрямую
import boto3
import io
import random
import argparse
from datetime import datetime, timezone
import shutil
from pathlib import Path # Используем pathlib

# Импортируем ClientError из botocore
try:
    from botocore.exceptions import ClientError
except ImportError:
    ClientError = Exception
    print("Warning: Could not import ClientError from botocore.")

# Добавляем путь к модулям
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# --- Импорт кастомных модулей ---
try:
    from modules.config_manager import ConfigManager
    from modules.logger import get_logger
    from modules.error_handler import handle_error
    from modules.utils import ensure_directory_exists, load_b2_json, save_b2_json, load_json_config
    from modules.api_clients import get_b2_client
except ModuleNotFoundError as e:
     print(f"Критическая Ошибка: Не найдены модули проекта в generate_content: {e}", file=sys.stderr)
     sys.exit(1)
except ImportError as e:
     print(f"Критическая Ошибка: Не найдена функция/класс в модулях: {e}", file=sys.stderr)
     sys.exit(1)

# --- Инициализация логгера ---
logger = get_logger("generate_content")

# --- Функция сохранения в B2 (без изменений) ---
def save_content_to_b2(folder, content_dict, generation_id, config_manager_instance):
    """Сохраняет словарь content_dict как JSON в указанную папку B2."""
    logger.info(f"Вызов save_content_to_b2 для ID: {generation_id}")
    config = config_manager_instance
    s3 = get_b2_client()
    if not s3:
        logger.error("❌ Не удалось создать клиент B2 внутри save_content_to_b2")
        return False
    bucket_name = config.get("API_KEYS.b2.bucket_name")
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
        # Проверка ключей (оставляем как было в v1 рефакторинга)
        required_keys = ["topic", "content", "sarcasm", "script", "first_frame_description",
                         "creative_brief", "final_mj_prompt", "final_runway_prompt"]
        optional_ru_keys = ["script_ru", "first_frame_description_ru", "final_mj_prompt_ru", "final_runway_prompt_ru"]
        missing_keys = [key for key in required_keys if key not in content_dict]
        null_keys = [key for key in required_keys if key in content_dict and content_dict[key] is None]
        if missing_keys: logger.warning(f"⚠️ В сохраняемых данных для ID {clean_base_id} отсутствуют обязательные ключи: {missing_keys}.")
        if null_keys: logger.warning(f"⚠️ В сохраняемых данных для ID {clean_base_id} есть обязательные ключи со значением null: {null_keys}.")

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
            try: os.remove(local_temp_path); logger.debug(f"Временный файл {local_temp_path} удален.")
            except OSError as remove_err: logger.warning(f"Не удалось удалить временный файл {local_temp_path}: {remove_err}")

# --- КЛАСС ГЕНЕРАТОРА КОНТЕНТА ---
class ContentGenerator:
    def __init__(self):
        """Инициализация генератора контента."""
        self.logger = logger
        self.config = ConfigManager() # Основной config

        # --- Загрузка дополнительных конфигураций ---
        self.creative_config_data = self._load_additional_config('FILE_PATHS.creative_config', 'Creative Config')
        self.prompts_config_data = self._load_additional_config('FILE_PATHS.prompts_config', 'Prompts Config') # Загружаем конфиг промптов

        # Настройки генерации из основного конфига
        self.topic_threshold = self.config.get('GENERATE.topic_threshold', 7)
        self.text_threshold = self.config.get('GENERATE.text_threshold', 8)
        self.max_attempts = self.config.get('GENERATE.max_attempts', 1)
        self.adaptation_enabled = self.config.get('GENERATE.adaptation_enabled', False)
        self.adaptation_params = self.config.get('GENERATE.adaptation_parameters', {})
        self.content_output_path = self.config.get('FILE_PATHS.content_output_path', 'generated_content.json')

        # --- Настройки и инициализация OpenAI (v > 1.0) ---
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.openai_model = self.config.get("OPENAI_SETTINGS.model", "gpt-4o") # Модель берем из основного конфига
        self.openai_client = None
        if not self.openai_api_key:
            self.logger.error("❌ Переменная окружения OPENAI_API_KEY не задана!")
        else:
            try:
                if hasattr(openai, 'OpenAI'):
                    self.openai_client = openai.OpenAI(api_key=self.openai_api_key)
                    self.logger.info("✅ Клиент OpenAI (>1.0) успешно инициализирован.")
                else:
                    self.logger.error("❌ Класс openai.OpenAI не найден. Убедитесь, что установлена версия библиотеки OpenAI >= 1.0.")
            except Exception as e:
                 self.logger.error(f"❌ Ошибка инициализации клиента OpenAI: {e}")

        # Инициализация B2 клиента
        self.b2_client = get_b2_client()
        if not self.b2_client:
             self.logger.warning("⚠️ Не удалось инициализировать B2 клиент в ContentGenerator.")

        # Пути к трекеру и failsafe
        self.tracker_path_rel = self.config.get("FILE_PATHS.tracker_path", "data/topics_tracker.json")
        self.failsafe_path_rel = self.config.get("FILE_PATHS.failsafe_path", "config/FailSafeVault.json")
        self.tracker_path_abs = BASE_DIR / self.tracker_path_rel
        self.failsafe_path_abs = BASE_DIR / self.failsafe_path_rel
        self.b2_bucket_name = self.config.get("API_KEYS.b2.bucket_name", "default-bucket")

    def _load_additional_config(self, config_key, config_name):
        """Вспомогательный метод для загрузки доп. конфигов."""
        config_path_str = self.config.get(config_key)
        if not config_path_str:
            self.logger.error(f"❌ Путь к {config_name} не найден в основном конфиге (ключ: {config_key}).")
            return None
        config_path = BASE_DIR / config_path_str
        data = load_json_config(str(config_path))
        if data:
            self.logger.info(f"✅ {config_name} успешно загружен из {config_path}.")
        else:
            self.logger.error(f"❌ Не удалось загрузить {config_name} из {config_path}.")
        return data

    def adapt_prompts(self):
        """Применяет адаптацию промптов (если включено)."""
        if not self.adaptation_enabled: self.logger.info("🔄 Адаптация промптов отключена."); return
        self.logger.info("🔄 Применяю адаптацию промптов...")
        for key, value in self.adaptation_params.items(): self.logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def clear_generated_content(self):
        """Очищает локальный файл с промежуточными результатами."""
        try:
            self.logger.info(f"🧹 Очистка локального файла: {self.content_output_path}")
            ensure_directory_exists(self.content_output_path)
            with open(self.content_output_path, 'w', encoding='utf-8') as file: json.dump({}, file, ensure_ascii=False, indent=4)
            self.logger.info("✅ Локальный файл успешно очищен/создан.")
        except PermissionError: handle_error("Clear Content Error", f"Нет прав на запись в файл: {self.content_output_path}", PermissionError())
        except Exception as e: handle_error("Clear Content Error", str(e), e)

    def load_tracker(self):
        """Загружает трекер тем из B2 или локального файла."""
        # (Логика без изменений по сравнению с v1 рефакторинга)
        tracker_path_abs = self.tracker_path_abs; tracker_path_rel = self.tracker_path_rel
        failsafe_path_abs = self.failsafe_path_abs; bucket_name = self.b2_bucket_name
        os.makedirs(tracker_path_abs.parent, exist_ok=True); tracker_updated_locally = False
        if self.b2_client:
            try:
                self.logger.info(f"Попытка загрузки {tracker_path_rel} из B2...")
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f"); local_temp_tracker = f"tracker_temp_{timestamp_suffix}.json"
                self.b2_client.download_file(bucket_name, tracker_path_rel, local_temp_tracker)
                shutil.copyfile(local_temp_tracker, str(tracker_path_abs)); os.remove(local_temp_tracker)
                self.logger.info(f"✅ Загружен {tracker_path_rel} из B2 в {tracker_path_abs}")
            except ClientError as e:
                 error_code = e.response.get('Error', {}).get('Code')
                 if error_code == 'NoSuchKey' or '404' in str(e): self.logger.warning(f"⚠️ {tracker_path_rel} не найден в B2.")
                 else: self.logger.error(f"⚠️ Ошибка B2 при загрузке трекера: {e}")
            except Exception as e: self.logger.warning(f"⚠️ Не удалось загрузить трекер из B2: {e}")
        else: self.logger.warning("⚠️ B2 клиент недоступен, используем только локальный трекер.")
        if not tracker_path_abs.exists():
            self.logger.warning(f"{tracker_path_abs} не найден. Попытка создания из {failsafe_path_abs}.")
            try:
                ensure_directory_exists(str(failsafe_path_abs))
                with open(failsafe_path_abs, 'r', encoding='utf-8') as f_failsafe: failsafe_data = json.load(f_failsafe)
                tracker = {"all_focuses": failsafe_data.get("focuses", []), "used_focuses": [], "focus_data": {}}
                with open(tracker_path_abs, 'w', encoding='utf-8') as f_tracker: json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                self.logger.info(f"✅ Создан новый {tracker_path_abs} из FailSafeVault."); tracker_updated_locally = True
            except FileNotFoundError: self.logger.error(f"❌ Файл {failsafe_path_abs} не найден!"); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
            except Exception as e: self.logger.error(f"❌ Ошибка при создании трекера из FailSafe: {e}"); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
        try:
            with open(tracker_path_abs, 'r', encoding='utf-8') as f: tracker = json.load(f)
            if "all_focuses" not in tracker:
                self.logger.info("Обновляем структуру старого трекера."); failsafe_data = {}
                if failsafe_path_abs.exists():
                     with open(failsafe_path_abs, 'r', encoding='utf-8') as f_failsafe: failsafe_data = json.load(f_failsafe)
                tracker["all_focuses"] = failsafe_data.get("focuses", []); tracker.setdefault("used_focuses", []); tracker.setdefault("focus_data", {})
                with open(tracker_path_abs, 'w', encoding='utf-8') as f_tracker: json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                tracker_updated_locally = True
            if tracker_updated_locally: self.sync_tracker_to_b2(tracker_path_abs=tracker_path_abs, tracker_path_rel=tracker_path_rel)
            return tracker
        except json.JSONDecodeError: self.logger.error(f"❌ Ошибка JSON в файле трекера: {tracker_path_abs}."); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
        except Exception as e: self.logger.error(f"❌ Ошибка чтения трекера {tracker_path_abs}: {e}"); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}

    def get_valid_focus_areas(self, tracker):
        """Возвращает список доступных фокусов."""
        all_focuses = tracker.get("all_focuses", []); used_focuses = tracker.get("used_focuses", [])
        used_set = set(used_focuses); valid_focuses = [f for f in all_focuses if f not in used_set]
        self.logger.info(f"✅ Доступные фокусы: {len(valid_focuses)} шт."); self.logger.debug(f"Полный список: {valid_focuses}")
        return valid_focuses

    def generate_topic(self, tracker):
        """Генерирует новую тему, используя доступные фокусы."""
        valid_focuses = self.get_valid_focus_areas(tracker)
        if not valid_focuses: raise ValueError("Все фокусы использованы.")
        selected_focus = random.choice(valid_focuses)
        self.logger.info(f"Выбран фокус: {selected_focus}")
        used_labels = tracker.get("focus_data", {}).get(selected_focus, [])
        exclusions_str = ", ".join(used_labels) if used_labels else "нет"

        # --- ИЗМЕНЕНО: Используем request_openai с ключом промпта ---
        prompt_config_key = "content.topic"
        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template: raise ValueError(f"Промпт {prompt_config_key} не найден.")
        prompt = prompt_template.format(focus_areas=selected_focus, exclusions=exclusions_str)

        topic_response_str = ""
        try:
            # Передаем ключ для получения настроек температуры/токенов
            topic_response_str = self.request_openai(prompt, prompt_config_key=prompt_config_key, use_json_mode=True)
            if not topic_response_str: raise ValueError("OpenAI не вернул ответ для темы.")
            topic_data = json.loads(topic_response_str)
            full_topic = topic_data.get("full_topic"); short_topic = topic_data.get("short_topic")
            if not full_topic or not short_topic: raise ValueError(f"Ответ для темы не содержит ключи: {topic_data}")
            self.logger.info(f"Сгенерирована тема: '{full_topic}' (Ярлык: '{short_topic}')")
            self.update_tracker(selected_focus, short_topic, tracker)
            self.save_to_generated_content("topic", {"full_topic": full_topic, "short_topic": short_topic})
            content_metadata = {"theme": "tragic" if "(т)" in selected_focus else "normal"}
            return full_topic, content_metadata
        except json.JSONDecodeError as e: self.logger.error(f"Ошибка JSON ответа темы: {e}. Ответ: {topic_response_str[:500]}"); raise ValueError("Ошибка формата ответа темы.") from e
        except Exception as e: self.logger.error(f"Ошибка генерации темы: {e}", exc_info=True); raise

    def update_tracker(self, focus, short_topic, tracker):
        """Обновляет данные трекера в памяти и сохраняет его."""
        used_focuses = tracker.get("used_focuses", []); focus_data = tracker.get("focus_data", {})
        if focus in used_focuses: used_focuses.remove(focus)
        used_focuses.insert(0, focus); tracker["used_focuses"] = used_focuses[:15]
        focus_labels = focus_data.setdefault(focus, [])
        if short_topic in focus_labels: focus_labels.remove(short_topic)
        focus_labels.insert(0, short_topic); focus_data[focus] = focus_labels[:5]
        tracker["focus_data"] = focus_data
        self.save_topics_tracker(tracker)
        self.sync_tracker_to_b2(tracker_path_abs=self.tracker_path_abs, tracker_path_rel=self.tracker_path_rel)

    def save_topics_tracker(self, tracker):
        """Сохраняет трекер в локальный файл."""
        try:
            ensure_directory_exists(str(self.tracker_path_abs))
            with open(self.tracker_path_abs, "w", encoding="utf-8") as file: json.dump(tracker, file, ensure_ascii=False, indent=4)
            self.logger.info(f"Трекер тем сохранен: {self.tracker_path_abs}")
        except Exception as e: self.logger.error(f"Ошибка сохранения трекера: {e}")

    def sync_tracker_to_b2(self, tracker_path_abs, tracker_path_rel):
        """Синхронизирует локальный трекер с B2."""
        if not self.b2_client: self.logger.warning("B2 клиент недоступен."); return
        if not tracker_path_abs.exists(): self.logger.warning(f"Локальный трекер {tracker_path_abs} не найден."); return
        try:
            self.logger.info(f"Синхронизация {tracker_path_abs} с B2 как {tracker_path_rel}...")
            self.b2_client.upload_file(str(tracker_path_abs), self.b2_bucket_name, tracker_path_rel)
            self.logger.info(f"✅ {tracker_path_rel} синхронизирован с B2.")
        except Exception as e: self.logger.error(f"⚠️ Не удалось загрузить трекер {tracker_path_rel} в B2: {e}")

    # --- ИЗМЕНЕНО: request_openai теперь принимает prompt_config_key ---
    def request_openai(self, prompt_text: str, prompt_config_key: str, use_json_mode=False, temperature_override=None, max_tokens_override=None):
        """
        Отправляет запрос к OpenAI (v > 1.0), используя настройки из prompts_config.json.
        Возвращает строку ответа или None при ошибке.
        """
        if not self.openai_client: self.logger.error("❌ OpenAI клиент не инициализирован."); return None
        if not self.prompts_config_data: self.logger.error("❌ Конфигурация промптов не загружена."); return None

        try:
            # --- Получение настроек из prompts_config_data ---
            keys = prompt_config_key.split('.')
            prompt_settings = self.prompts_config_data
            for key in keys:
                prompt_settings = prompt_settings.get(key, {})
            if not isinstance(prompt_settings, dict):
                self.logger.warning(f"Настройки для ключа '{prompt_config_key}' не найдены или не являются словарем. Используем дефолты.")
                prompt_settings = {}

            # Получаем температуру и токены из настроек промпта, с дефолтами
            default_temp = 0.7 # Глобальный дефолт
            default_max_tokens = 1500 # Глобальный дефолт
            temp = float(temperature_override if temperature_override is not None else prompt_settings.get('temperature', default_temp))
            max_tokens = int(max_tokens_override if max_tokens_override is not None else prompt_settings.get('max_tokens', default_max_tokens))
            # --------------------------------------------------

            self.logger.info(f"🔎 Вызов OpenAI (Ключ: {prompt_config_key}, Модель: {self.openai_model}, JSON={use_json_mode}, t={temp:.2f}, max_tokens={max_tokens})...")

            messages = [
                {"role": "system", "content": "Ты - AI ассистент, который строго следует инструкциям пользователя и всегда отвечает ТОЛЬКО в указанном формате (например, JSON), без какого-либо дополнительного текста."},
                {"role": "user", "content": prompt_text}
            ]
            request_params = { "model": self.openai_model, "messages": messages, "max_tokens": max_tokens, "temperature": temp }
            if use_json_mode: request_params["response_format"] = {"type": "json_object"}

            response = self.openai_client.chat.completions.create(**request_params)

            if response.choices and response.choices[0].message and response.choices[0].message.content:
                response_content = response.choices[0].message.content.strip()
                if use_json_mode and response_content.startswith("```json"):
                     response_content = response_content[7:]; response_content = response_content[:-3] if response_content.endswith("```") else response_content; response_content = response_content.strip()
                self.logger.debug(f"Сырой ответ OpenAI: {response_content[:500]}...")
                return response_content
            else:
                self.logger.error("❌ OpenAI API вернул пустой или некорректный ответ."); self.logger.debug(f"Запрос: {messages}"); return None
        # Обработка ошибок OpenAI (без изменений)
        except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI: {e}"); return None
        except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI: {e}"); return None
        except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI: {e}"); return None
        except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}"); return None
        except openai.BadRequestError as e: logger.exception(f"Ошибка неверного запроса OpenAI: {e}"); return None
        except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI: {e}"); return None
        except Exception as e: logger.exception(f"Неизвестная ошибка в request_openai: {e}"); return None

    def _get_prompt_template(self, prompt_config_key: str) -> str | None:
        """Вспомогательный метод для получения шаблона промпта."""
        if not self.prompts_config_data: self.logger.error("❌ Конфигурация промптов не загружена."); return None
        keys = prompt_config_key.split('.')
        prompt_settings = self.prompts_config_data
        try:
            for key in keys: prompt_settings = prompt_settings[key]
            template = prompt_settings.get('template')
            if not template: self.logger.error(f"Шаблон 'template' не найден для ключа '{prompt_config_key}'")
            return template
        except KeyError: self.logger.error(f"Ключ '{prompt_config_key}' не найден в prompts_config.json"); return None
        except TypeError: self.logger.error(f"Неверная структура для ключа '{prompt_config_key}' в prompts_config.json"); return None

    def generate_sarcasm(self, text, content_data={}):
        """Генерирует саркастический комментарий."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.comment_enabled', True):
            self.logger.info("🔕 Генерация саркастического комментария отключена."); return None

        prompt_key_suffix = "tragic_comment" if content_data.get("theme") == "tragic" else "comment"
        prompt_config_key = f"sarcasm.{prompt_key_suffix}" # Собираем полный ключ

        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template: return None # Ошибка уже залогирована в _get_prompt_template

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос к OpenAI для генерации комментария (ключ: {prompt_config_key})...")
        try:
            # Передаем ключ для получения настроек, use_json_mode=False
            comment = self.request_openai(prompt, prompt_config_key=prompt_config_key, use_json_mode=False)
            if comment: self.logger.info(f"✅ Саркастический комментарий сгенерирован: {comment}")
            else: self.logger.error(f"❌ Ошибка генерации комментария (ключ: {prompt_config_key}).")
            return comment
        except Exception as e: self.logger.error(f"❌ Исключение при генерации комментария: {e}"); return None

    def generate_sarcasm_poll(self, text, content_data={}):
        """Генерирует саркастический опрос, ожидая JSON."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.poll_enabled', True):
            self.logger.info("🔕 Генерация саркастического опроса отключена."); return {}

        prompt_key_suffix = "tragic_poll" if content_data.get("theme") == "tragic" else "poll"
        prompt_config_key = f"sarcasm.{prompt_key_suffix}" # Собираем полный ключ

        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template: return {}

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос к OpenAI для генерации опроса (ключ: {prompt_config_key})... Ожидаем JSON.")
        response_content = ""
        try:
            # Передаем ключ, use_json_mode=True
            response_content = self.request_openai(prompt, prompt_config_key=prompt_config_key, use_json_mode=True)
            if not response_content: self.logger.error(f"❌ Ошибка генерации опроса (ключ: {prompt_config_key})."); return {}
            self.logger.debug(f"Сырой ответ OpenAI для опроса: {response_content[:500]}")
            poll_data = json.loads(response_content)
            if isinstance(poll_data, dict) and "question" in poll_data and "options" in poll_data and isinstance(poll_data["options"], list) and len(poll_data["options"]) == 3:
                self.logger.info("✅ Опрос успешно сгенерирован и распарсен (JSON).")
                poll_data["question"] = str(poll_data["question"]).strip(); poll_data["options"] = [str(opt).strip() for opt in poll_data["options"]]
                return poll_data
            else: self.logger.error(f"❌ Структура JSON для опроса неверна: {poll_data}"); return {}
        except json.JSONDecodeError as e: self.logger.error(f"❌ Ошибка парсинга JSON опроса: {e}. Ответ: {response_content[:500]}"); return {}
        except Exception as e: self.logger.error(f"❌ Исключение при генерации опроса: {e}"); return {}

    def save_to_generated_content(self, stage, data):
        """Сохраняет промежуточные данные в локальный JSON файл."""
        # (Логика без изменений)
        try:
            if not self.content_output_path: raise ValueError("❌ self.content_output_path не задан!")
            self.logger.debug(f"🔄 Обновление {self.content_output_path}, этап: {stage}")
            ensure_directory_exists(self.content_output_path); result_data = {}
            if os.path.exists(self.content_output_path):
                try:
                    if os.path.getsize(self.content_output_path) > 0:
                        with open(self.content_output_path, 'r', encoding='utf-8') as file: result_data = json.load(file)
                    else: self.logger.warning(f"⚠️ Файл {self.content_output_path} пуст."); result_data = {}
                except json.JSONDecodeError: self.logger.warning(f"⚠️ Файл {self.content_output_path} поврежден."); result_data = {}
                except Exception as read_err: self.logger.error(f"Ошибка чтения {self.content_output_path}: {read_err}"); result_data = {}
            result_data["timestamp"] = datetime.utcnow().isoformat(); result_data[stage] = data
            with open(self.content_output_path, 'w', encoding='utf-8') as file: json.dump(result_data, file, ensure_ascii=False, indent=4)
            self.logger.debug(f"✅ Локально обновлено для этапа: {stage}")
        except Exception as e: handle_error("Save to Generated Content Error", f"Ошибка при сохранении в {self.content_output_path}: {str(e)}", e)

    def critique_content(self, content, topic):
        """Выполняет критику текста (если включено)."""
        if not self.config.get('CONTENT.critique.enabled', False):
            self.logger.info("🔕 Критика контента отключена."); return "Критика отключена."
        if not content: self.logger.warning("Нет текста для критики."); return "Нет текста для критики."
        try:
            self.logger.info("🔄 Выполняется критика текста...")
            prompt_config_key = "content.critique"
            prompt_template = self._get_prompt_template(prompt_config_key)
            if not prompt_template or prompt_template == "...":
                 self.logger.error(f"Промпт {prompt_config_key} не найден/заполнен."); return "Промпт критики не найден."
            prompt = prompt_template.format(content=content, topic=topic)
            # Передаем ключ для получения настроек, use_json_mode=False
            critique = self.request_openai(prompt, prompt_config_key=prompt_config_key, use_json_mode=False)
            if critique: self.logger.info("✅ Критика завершена.")
            else: self.logger.error(f"❌ Ошибка критики (ключ: {prompt_config_key}).")
            return critique if critique else "Критика завершилась ошибкой."
        except Exception as e: self.logger.error(f"❌ Исключение при критике: {e}"); return "Критика завершилась ошибкой."

    def format_list_for_prompt(self, items: list | dict, use_weights=False) -> str:
        """Форматирует список или словарь списков для вставки в промпт."""
        # (Логика без изменений по сравнению с v1 рефакторинга)
        lines = [];
        if isinstance(items, list):
            if not items: return "- (Список пуст)"
            for item in items:
                if use_weights and isinstance(item, dict) and 'value' in item and 'weight' in item: lines.append(f"* {item['value']} (Вес: {item['weight']})")
                elif isinstance(item, str): lines.append(f"* {item}")
                elif isinstance(item, dict) and 'value' in item: lines.append(f"* {item['value']}")
        elif isinstance(items, dict):
             if not items: return "- (Словарь пуст)"; is_dict_of_lists = all(isinstance(v, list) for v in items.values())
             for category, cat_items in items.items():
                 if is_dict_of_lists:
                     if lines: lines.append(""); lines.append(f"  Категория '{category}':")
                     formatted_sublist = self.format_list_for_prompt(cat_items, use_weights=(use_weights and category == 'main'))
                     if formatted_sublist != "- (Список пуст)": indented_lines = [f"    {line}" if line.strip() else line for line in formatted_sublist.split('\n')]; lines.extend(indented_lines)
                 elif isinstance(cat_items, list):
                      lines.append(f"* {category}:")
                      formatted_sublist = self.format_list_for_prompt(cat_items, use_weights=False)
                      if formatted_sublist != "- (Список пуст)": indented_lines = [f"    {line}" if line.strip() else line for line in formatted_sublist.split('\n')]; lines.extend(indented_lines)
                 else: lines.append(f"* {category}: {cat_items}")
        else: return "- (Неверный формат данных)"
        return "\n".join(lines).strip()

    def run(self, generation_id):
        """Основной процесс генерации контента для заданного ID."""
        self.logger.info(f"--- Запуск ContentGenerator.run для ID: {generation_id} ---")
        if not generation_id: raise ValueError("generation_id не может быть пустым.")
        if not self.creative_config_data or not self.prompts_config_data: raise RuntimeError("Конфиги не загружены.")
        if not self.openai_client: raise RuntimeError("OpenAI клиент не инициализирован.")

        try:
            # --- Шаг 1: Подготовка ---
            self.adapt_prompts(); self.clear_generated_content()

            # --- Шаг 2: Генерация Темы ---
            tracker = self.load_tracker(); topic, content_data = self.generate_topic(tracker)

            # --- Шаг 3: Генерация Текста (RU) ---
            text_initial = ""; generate_text_enabled = self.config.get('CONTENT.text.enabled', True)
            generate_tragic_text_enabled = self.config.get('CONTENT.tragic_text.enabled', True)
            if (content_data.get("theme") == "tragic" and generate_tragic_text_enabled) or (content_data.get("theme") != "tragic" and generate_text_enabled):
                prompt_key_suffix = "tragic_text" if content_data.get("theme") == "tragic" else "text"
                prompt_config_key = f"content.{prompt_key_suffix}"
                prompt_template = self._get_prompt_template(prompt_config_key)
                if prompt_template:
                     # Передаем ключ для получения настроек, use_json_mode=False
                     text_initial = self.request_openai(prompt_template.format(topic=topic), prompt_config_key=prompt_config_key, use_json_mode=False)
                     if text_initial: self.logger.info(f"Сгенерирован текст: {text_initial[:100]}..."); self.save_to_generated_content("text", {"text": text_initial})
                     else: self.logger.warning(f"Генерация текста ({prompt_config_key}) не удалась.")
                else: self.logger.warning(f"Промпт {prompt_config_key} не найден.")
            else: self.logger.info(f"Генерация текста (тема: {content_data.get('theme')}) отключена.")

            # --- Шаг 4: Критика ---
            critique_result = self.critique_content(text_initial, topic); self.save_to_generated_content("critique", {"critique": critique_result})

            # --- Шаг 5: Генерация Сарказма (RU) ---
            sarcastic_comment = None; sarcastic_poll = {}
            if text_initial:
                sarcastic_comment = self.generate_sarcasm(text_initial, content_data)
                sarcastic_poll = self.generate_sarcasm_poll(text_initial, content_data)
            self.save_to_generated_content("sarcasm", {"comment": sarcastic_comment, "poll": sarcastic_poll})

            # --- Шаг 6: Многошаговая Генерация Брифа и Промптов (EN) + Перевод (RU) ---
            self.logger.info("--- Запуск многошаговой генерации ---")
            creative_brief, script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en = None, None, None, None, None
            script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru = None, None, None, None
            enable_russian_translation = self.config.get("WORKFLOW.enable_russian_translation", False)
            self.logger.info(f"Генерация русского перевода {'ВКЛЮЧЕНА' if enable_russian_translation else 'ОТКЛЮЧЕНА'}.")

            try:
                # Подготовка списков для промптов
                moods_list_str = self.format_list_for_prompt(self.creative_config_data.get("moods", []), use_weights=True)
                arcs_list_str = self.format_list_for_prompt(self.creative_config_data.get("emotional_arcs", []))
                main_prompts_list = self.creative_config_data.get("creative_prompts", {}).get("main", [])
                prompts_list_str = self.format_list_for_prompt(main_prompts_list, use_weights=True)
                perspectives_list_str = self.format_list_for_prompt(self.creative_config_data.get("perspective_types", []))
                metaphors_list_str = self.format_list_for_prompt(self.creative_config_data.get("visual_metaphor_types", []))
                directors_list_str = self.format_list_for_prompt(self.creative_config_data.get("director_styles", []))
                artists_list_str = self.format_list_for_prompt(self.creative_config_data.get("artist_styles", []))

                # Шаг 6.1: Ядро
                self.logger.info("--- Шаг 6.1: Эмоциональное Ядро ---")
                prompt_key1 = "multi_step.step1_core"; tmpl1 = self._get_prompt_template(prompt_key1);
                if not tmpl1: raise ValueError(f"{prompt_key1} не найден.")
                prompt1 = tmpl1.format(input_text=topic, moods_list_str=moods_list_str, arcs_list_str=arcs_list_str)
                core_brief_str = self.request_openai(prompt1, prompt_config_key=prompt_key1, use_json_mode=True)
                if not core_brief_str: raise ValueError("Шаг 6.1 не удался."); core_brief = json.loads(core_brief_str)
                if not core_brief or not all(k in core_brief for k in ["chosen_type", "chosen_value", "justification"]): raise ValueError(f"Шаг 6.1: неверный JSON {core_brief}.")

                # Шаг 6.2: Драйвер
                self.logger.info("--- Шаг 6.2: Креативный Драйвер ---")
                prompt_key2 = "multi_step.step2_driver"; tmpl2 = self._get_prompt_template(prompt_key2);
                if not tmpl2: raise ValueError(f"{prompt_key2} не найден.")
                prompt2 = tmpl2.format(input_text=topic, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), prompts_list_str=prompts_list_str, perspectives_list_str=perspectives_list_str, metaphors_list_str=metaphors_list_str)
                driver_brief_str = self.request_openai(prompt2, prompt_config_key=prompt_key2, use_json_mode=True)
                if not driver_brief_str: raise ValueError("Шаг 6.2 не удался."); driver_brief = json.loads(driver_brief_str)
                if not driver_brief or not all(k in driver_brief for k in ["chosen_driver_type", "chosen_driver_value", "justification"]): raise ValueError(f"Шаг 6.2: неверный JSON {driver_brief}.")

                # Шаг 6.3: Эстетика
                self.logger.info("--- Шаг 6.3: Эстетический Фильтр ---")
                prompt_key3 = "multi_step.step3_aesthetic"; tmpl3 = self._get_prompt_template(prompt_key3);
                if not tmpl3: raise ValueError(f"{prompt_key3} не найден.")
                prompt3 = tmpl3.format(input_text=topic, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), chosen_driver_json=json.dumps(driver_brief, ensure_ascii=False, indent=2), directors_list_str=directors_list_str, artists_list_str=artists_list_str)
                aesthetic_brief_str = self.request_openai(prompt3, prompt_config_key=prompt_key3, use_json_mode=True)
                if not aesthetic_brief_str: raise ValueError("Шаг 6.3 не удался."); aesthetic_brief = json.loads(aesthetic_brief_str)
                valid_step3 = False # Валидация (без изменений)
                if isinstance(aesthetic_brief, dict):
                    style_needed = aesthetic_brief.get("style_needed", False); base_keys_exist = all(k in aesthetic_brief for k in ["style_needed", "chosen_style_type", "chosen_style_value", "style_keywords", "justification"])
                    if base_keys_exist:
                        if not style_needed:
                            if all(aesthetic_brief.get(k) is None for k in ["chosen_style_type", "chosen_style_value", "style_keywords", "justification"]): valid_step3 = True
                            else: self.logger.warning(f"Шаг 6.3: style_needed=false, но ключи не null. Исправляем."); aesthetic_brief.update({k:None for k in ["chosen_style_type", "chosen_style_value", "style_keywords", "justification"]}); valid_step3 = True
                        else:
                            if all([aesthetic_brief.get("chosen_style_type"), aesthetic_brief.get("chosen_style_value"), isinstance(aesthetic_brief.get("style_keywords"), list), aesthetic_brief.get("justification")]): valid_step3 = True
                            else: logger.error(f"Шаг 6.3: style_needed=true, но значения некорректны.")
                    else: logger.error(f"Шаг 6.3: Отсутствуют базовые ключи.")
                else: logger.error(f"Шаг 6.3: Ответ не словарь.")
                if not valid_step3: raise ValueError("Шаг 6.3: неверный JSON.")

                # Сборка Брифа
                creative_brief = {"core": core_brief, "driver": driver_brief, "aesthetic": aesthetic_brief}; self.logger.info("--- Шаг 6.4: Бриф Собран ---"); self.logger.debug(f"Бриф: {json.dumps(creative_brief, ensure_ascii=False, indent=2)}"); self.save_to_generated_content("creative_brief", creative_brief)

                # Шаг 6.5: Сценарий и Описание (EN)
                self.logger.info("--- Шаг 6.5: Сценарий и Описание (EN) ---")
                prompt_key5 = "multi_step.step5_script_frame"; tmpl5 = self._get_prompt_template(prompt_key5);
                if not tmpl5: raise ValueError(f"{prompt_key5} не найден.")
                prompt5 = tmpl5.format(input_text=topic, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2))
                script_frame_data_str = self.request_openai(prompt5, prompt_config_key=prompt_key5, use_json_mode=True)
                if not script_frame_data_str: raise ValueError("Шаг 6.5 не удался."); script_frame_data = json.loads(script_frame_data_str)
                if not script_frame_data or not all(k in script_frame_data for k in ["script", "first_frame_description"]): raise ValueError(f"Шаг 6.5: неверный JSON {script_frame_data}.")
                script_en = script_frame_data["script"]; frame_description_en = script_frame_data["first_frame_description"]
                self.logger.info(f"Сценарий (EN): {script_en[:100]}..."); self.logger.info(f"Описание (EN): {frame_description_en[:100]}..."); self.save_to_generated_content("script_frame_en", {"script": script_en, "first_frame_description": frame_description_en})

                # Шаг 6.6a: MJ Промпт (EN)
                self.logger.info("--- Шаг 6.6a: MJ Промпт (EN) ---")
                mj_params_cfg = self.config.get("IMAGE_GENERATION", {}); aspect_ratio_str = mj_params_cfg.get("output_size", "16:9").replace('x', ':').replace('×', ':'); version_str = str(mj_params_cfg.get("midjourney_version", "7.0")); style_str = mj_params_cfg.get("midjourney_style", None)
                mj_parameters_json_for_prompt = json.dumps({"aspect_ratio": aspect_ratio_str, "version": version_str, "style": style_str}, ensure_ascii=False); style_parameter_str_for_prompt = f" --style {style_str}" if style_str else ""
                prompt_key6a = "multi_step.step6a_mj_adapt"; tmpl6a = self._get_prompt_template(prompt_key6a);
                if not tmpl6a: raise ValueError(f"{prompt_key6a} не найден.")
                prompt6a = tmpl6a.format(first_frame_description=frame_description_en, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2), script=script_en, input_text=topic, mj_parameters_json=mj_parameters_json_for_prompt, aspect_ratio=aspect_ratio_str, version=version_str, style_parameter_str=style_parameter_str_for_prompt)
                mj_prompt_data_str = self.request_openai(prompt6a, prompt_config_key=prompt_key6a, use_json_mode=True)
                if not mj_prompt_data_str: raise ValueError("Шаг 6.6a не удался."); mj_prompt_data = json.loads(mj_prompt_data_str)
                if not mj_prompt_data or "final_mj_prompt" not in mj_prompt_data: raise ValueError(f"Шаг 6.6a: неверный JSON {mj_prompt_data}.")
                final_mj_prompt_en = mj_prompt_data["final_mj_prompt"]; self.logger.info(f"MJ промпт (EN, V{version_str}): {final_mj_prompt_en}"); self.save_to_generated_content("final_mj_prompt_en", {"final_mj_prompt": final_mj_prompt_en})

                # Шаг 6.6b: Runway Промпт (EN)
                self.logger.info("--- Шаг 6.6b: Runway Промпт (EN) ---")
                prompt_key6b = "multi_step.step6b_runway_adapt"; tmpl6b = self._get_prompt_template(prompt_key6b);
                if not tmpl6b: raise ValueError(f"{prompt_key6b} не найден.")
                prompt6b = tmpl6b.format(script=script_en, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2), input_text=topic)
                runway_prompt_data_str = self.request_openai(prompt6b, prompt_config_key=prompt_key6b, use_json_mode=True)
                if not runway_prompt_data_str: raise ValueError("Шаг 6.6b не удался."); runway_prompt_data = json.loads(runway_prompt_data_str)
                if not runway_prompt_data or "final_runway_prompt" not in runway_prompt_data: raise ValueError(f"Шаг 6.6b: неверный JSON {runway_prompt_data}.")
                final_runway_prompt_en = runway_prompt_data["final_runway_prompt"]; self.logger.info(f"Runway промпт (EN): {final_runway_prompt_en}"); self.save_to_generated_content("final_runway_prompt_en", {"final_runway_prompt": final_runway_prompt_en})

                # Шаг 6.6c: Перевод (RU)
                if enable_russian_translation:
                    self.logger.info("--- Шаг 6.6c: Перевод (RU) ---")
                    if all([script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en]):
                        prompt_key6c = "multi_step.step6c_translate"; tmpl6c = self._get_prompt_template(prompt_key6c);
                        if not tmpl6c: raise ValueError(f"{prompt_key6c} не найден.")
                        prompt6c = tmpl6c.format(script_en=script_en, frame_description_en=frame_description_en, mj_prompt_en=final_mj_prompt_en, runway_prompt_en=final_runway_prompt_en)
                        translations_str = self.request_openai(prompt6c, prompt_config_key=prompt_key6c, use_json_mode=True) # Получаем настройки из конфига
                        if translations_str:
                            translations = json.loads(translations_str)
                            script_ru = translations.get("script_ru"); frame_description_ru = translations.get("first_frame_description_ru"); final_mj_prompt_ru = translations.get("final_mj_prompt_ru"); final_runway_prompt_ru = translations.get("final_runway_prompt_ru")
                            if all([script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru]): self.logger.info("✅ Перевод выполнен."); self.save_to_generated_content("translations_ru", translations)
                            else: self.logger.error(f"Шаг 6.6c: Не все поля переведены. {translations}"); translations = None
                        else: self.logger.error("Шаг 6.6c не удался."); translations = None
                    else: self.logger.error("Недостаточно данных для перевода."); translations = None
                else: self.logger.info("Перевод пропущен.")

            except (json.JSONDecodeError, ValueError) as parse_err: self.logger.error(f"❌ Ошибка шага 6: {parse_err}.") # Оставляем None
            except Exception as script_err: self.logger.error(f"❌ Ошибка шага 6: {script_err}", exc_info=True) # Оставляем None

            # --- Шаг 7: Сохранение в B2 ---
            self.logger.info("Формирование итогового словаря для B2...")
            complete_content_dict = {
                "topic": topic, "content": text_initial.strip() if text_initial else "",
                "sarcasm": {"comment": sarcastic_comment, "poll": sarcastic_poll},
                "script": script_en, "first_frame_description": frame_description_en,
                "creative_brief": creative_brief, "final_mj_prompt": final_mj_prompt_en,
                "final_runway_prompt": final_runway_prompt_en,
                "script_ru": script_ru, "first_frame_description_ru": frame_description_ru,
                "final_mj_prompt_ru": final_mj_prompt_ru, "final_runway_prompt_ru": final_runway_prompt_ru,
            }
            complete_content_dict = {k: v for k, v in complete_content_dict.items() if v is not None} # Убираем None
            self.logger.debug(f"Итоговый словарь: {json.dumps(complete_content_dict, ensure_ascii=False, indent=2)}")
            self.logger.info(f"Сохранение в B2 для ID {generation_id}...")
            if not save_content_to_b2("666/", complete_content_dict, generation_id, self.config):
                raise Exception(f"Не удалось сохранить итоговый контент в B2 для ID {generation_id}")

            # --- Шаг 8: Обновление config_midjourney.json ---
            self.logger.info(f"Обновление config_midjourney.json для ID: {generation_id}...")
            try:
                s3_client_mj = self.b2_client
                if not s3_client_mj: raise ConnectionError("B2 клиент недоступен.")
                config_mj_remote_path = self.config.get('FILE_PATHS.config_midjourney', 'config/config_midjourney.json')
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"
                bucket_name = self.b2_bucket_name
                config_mj = load_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, default_value={})
                if config_mj is None: config_mj = {}
                config_mj['generation'] = True; config_mj['midjourney_task'] = None; config_mj['midjourney_results'] = {}; config_mj['status'] = None
                self.logger.info("Данные для config_midjourney.json подготовлены.")
                if not save_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, config_mj):
                     raise Exception("Не удалось сохранить config_mj!")
                else: self.logger.info(f"✅ Обновленный {config_mj_remote_path} загружен в B2.")
            except Exception as e: self.logger.error(f"❌ Не удалось обновить config_midjourney.json: {e}", exc_info=True); raise Exception("Критическая ошибка: не удалось установить флаг generation: true") from e

            self.logger.info(f"✅ ContentGenerator.run успешно завершен для ID {generation_id}.")

        except Exception as e: self.logger.error(f"❌ Ошибка в ContentGenerator.run для ID {generation_id}: {e}", exc_info=True); raise

# --- Точка входа ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate content for a specific ID.')
    parser.add_argument('--generation_id', type=str, required=True, help='The generation ID.')
    args = parser.parse_args()
    generation_id_main = args.generation_id
    if not generation_id_main: logger.critical("generation_id не передан!"); sys.exit(1)
    logger.info(f"--- Запуск generate_content.py для ID: {generation_id_main} ---")
    exit_code = 1
    try:
        generator = ContentGenerator(); generator.run(generation_id_main)
        logger.info(f"--- Скрипт generate_content.py успешно завершен для ID: {generation_id_main} ---")
        exit_code = 0
    except Exception as main_err: logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА generate_content.py для ID {generation_id_main} !!!")
    finally: logger.info(f"--- Завершение generate_content.py с кодом выхода: {exit_code} ---"); sys.exit(exit_code)

