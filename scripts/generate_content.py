# -*- coding: utf-8 -*-
# В файле scripts/generate_content.py

import json
import os
import sys
import openai # Импортируем основной модуль
import re
import boto3
import io
import random
import argparse
from datetime import datetime, timezone # Добавлен timezone
import shutil
from pathlib import Path
import logging # Добавляем logging
import httpx # <-- Добавляем импорт httpx

from modules.utils import (
        # ... другие функции ...
        upload_to_b2 # <<< УБЕДИТЕСЬ, ЧТО ЭТА СТРОКА ЕСТЬ
    )

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
    from modules.utils import (
        ensure_directory_exists, load_b2_json, save_b2_json,
        load_json_config, save_error_to_b2, generate_file_id,
        validate_output_json # Добавлен импорт validate_output_json (если он там)
    )
    from modules.api_clients import get_b2_client
except ModuleNotFoundError as e:
     print(f"Критическая Ошибка: Не найдены модули проекта в generate_content: {e}", file=sys.stderr)
     sys.exit(1)
except ImportError as e:
     # Проверяем конкретно load_json_config
     if 'load_json_config' in str(e):
        print(f"Критическая Ошибка: Функция 'load_json_config' не найдена в 'modules.utils'.", file=sys.stderr)
     elif 'save_error_to_b2' in str(e):
        print(f"Критическая Ошибка: Функция 'save_error_to_b2' не найдена в 'modules.utils'.", file=sys.stderr)
     elif 'generate_file_id' in str(e):
        print(f"Критическая Ошибка: Функция 'generate_file_id' не найдена в 'modules.utils'.", file=sys.stderr)
     elif 'validate_output_json' in str(e):
        print(f"Критическая Ошибка: Функция 'validate_output_json' не найдена в 'modules.utils'.", file=sys.stderr)
     else:
        print(f"Критическая Ошибка: Не найдена функция/класс в модулях: {e}", file=sys.stderr)
     sys.exit(1)

# --- Инициализация логгера ---
logger = get_logger("generate_content")

# --- Глобальная переменная для клиента OpenAI ---
openai_client_instance = None

# --- Функция вызова OpenAI API (без изменений) ---
def call_openai(prompt_text: str, prompt_config_key: str, use_json_mode=False, temperature_override=None, max_tokens_override=None, config_manager_instance=None, prompts_config_data_instance=None):
    """
    Выполняет вызов OpenAI API (версии >=1.0), инициализируя клиент при необходимости,
    и возвращает распарсенный JSON или строку.
    Использует настройки из prompts_config.json.
    """
    global openai_client_instance # Используем глобальную переменную для клиента

    # --- Инициализация клиента при первом вызове ---
    if not openai_client_instance:
        api_key_local = os.getenv("OPENAI_API_KEY")
        if not api_key_local:
            logger.error("❌ Переменная окружения OPENAI_API_KEY не задана!")
            raise RuntimeError("OpenAI API key not found.") # Прерываем выполнение

        try:
            if openai and hasattr(openai, 'OpenAI'):
                # Проверяем наличие прокси в переменных окружения
                http_proxy = os.getenv("HTTP_PROXY")
                https_proxy = os.getenv("HTTPS_PROXY")
                proxies_dict = {}
                if http_proxy: proxies_dict["http://"] = http_proxy
                if https_proxy: proxies_dict["https://"] = https_proxy

                # Создаем httpx_client ВСЕГДА, но передаем proxies только если они есть
                if proxies_dict:
                    logger.info(f"Обнаружены настройки прокси для OpenAI: {proxies_dict}")
                    http_client = httpx.Client(proxies=proxies_dict)
                else:
                    logger.info("Прокси не обнаружены, создаем httpx.Client без аргумента proxies.")
                    http_client = httpx.Client() # Инициализируем без proxies

                # Передаем созданный http_client в OpenAI
                openai_client_instance = openai.OpenAI(api_key=api_key_local, http_client=http_client)
                logger.info("✅ Клиент OpenAI (>1.0) инициализирован.")

            else:
                logger.error("❌ Модуль/класс openai.OpenAI не найден. Убедитесь, что установлена версия >= 1.0.")
                raise ImportError("openai.OpenAI class not found.")
        except Exception as init_err:
            logger.error(f"❌ Ошибка инициализации клиента OpenAI: {init_err}", exc_info=True)
            # Проверяем, не связана ли ошибка с 'proxies' снова
            if "got an unexpected keyword argument 'proxies'" in str(init_err):
                logger.error("!!! Повторная ошибка 'unexpected keyword argument proxies'. Проблема глубже, возможно, в httpx или окружении.")
            raise RuntimeError(f"Failed to initialize OpenAI client: {init_err}") from init_err
    # --- Конец инициализации клиента ---

    if not config_manager_instance:
        logger.error("❌ Экземпляр ConfigManager не передан в call_openai.")
        return None # Или raise exception
    if not prompts_config_data_instance:
        logger.error("❌ Данные prompts_config не переданы в call_openai.")
        return None # Или raise exception

    openai_model = config_manager_instance.get("OPENAI_SETTINGS.model", "gpt-4o")

    try:
        # Получение настроек промпта
        keys = prompt_config_key.split('.')
        prompt_settings = prompts_config_data_instance
        for key in keys: prompt_settings = prompt_settings.get(key, {})
        if not isinstance(prompt_settings, dict):
            logger.warning(f"Настройки для '{prompt_config_key}' не найдены/не словарь. Дефолты."); prompt_settings = {}

        default_temp = 0.7; default_max_tokens = 1500
        temp = float(temperature_override if temperature_override is not None else prompt_settings.get('temperature', default_temp))
        max_tokens = int(max_tokens_override if max_tokens_override is not None else prompt_settings.get('max_tokens', default_max_tokens))

        logger.info(f"🔎 Вызов OpenAI (Ключ: {prompt_config_key}, Модель: {openai_model}, JSON={use_json_mode}, t={temp:.2f}, max_tokens={max_tokens})...")

        messages = [{"role": "system", "content": "You are a helpful AI assistant specializing in historical content generation. Follow user instructions precisely and respond ONLY in the specified format (e.g., JSON) without any extra text."},
                    {"role": "user", "content": prompt_text}]

        request_params = { "model": openai_model, "messages": messages, "max_tokens": max_tokens, "temperature": temp }
        if use_json_mode: request_params["response_format"] = {"type": "json_object"}

        response = openai_client_instance.chat.completions.create(**request_params)

        if response.choices and response.choices[0].message and response.choices[0].message.content:
            response_content = response.choices[0].message.content.strip()
            logger.debug(f"Сырой ответ OpenAI: {response_content[:500]}...")
            # Удаляем возможную Markdown обертку JSON
            if response_content.startswith("```json"):
                logger.debug("Обнаружена обертка ```json в ответе, удаляем...")
                response_content = response_content[7:]
                if response_content.endswith("```"):
                    response_content = response_content[:-3]
                response_content = response_content.strip()
                logger.debug(f"Ответ после удаления обертки: {response_content[:500]}...")
            elif response_content.startswith("```") and response_content.endswith("```"):
                logger.debug("Обнаружена обертка ``` в ответе, удаляем...")
                response_content = response_content[3:-3].strip()
                logger.debug(f"Ответ после удаления обертки ```: {response_content[:500]}...")

            # Если нужен JSON, пытаемся распарсить
            if use_json_mode:
                try:
                    parsed_json = json.loads(response_content)
                    logger.debug("Ответ OpenAI успешно распарсен как JSON.")
                    return parsed_json
                except json.JSONDecodeError as json_e:
                    logger.error(f"Ошибка декодирования JSON из ответа OpenAI: {json_e}\nОтвет: {response_content}")
                    return None # Возвращаем None при ошибке парсинга JSON
            else:
                # Если JSON не нужен, возвращаем как строку
                return response_content
        else:
            logger.error("❌ OpenAI API вернул пустой/некорректный ответ.");
            logger.debug(f"Запрос: {messages}")
            return None

    # Обработка специфичных ошибок OpenAI
    except openai.AuthenticationError as e: logger.exception(f"Ошибка аутентификации OpenAI: {e}"); return None
    except openai.RateLimitError as e: logger.exception(f"Превышен лимит запросов OpenAI: {e}"); return None
    except openai.APIConnectionError as e: logger.exception(f"Ошибка соединения с API OpenAI: {e}"); return None
    except openai.APIStatusError as e: logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}"); return None
    except openai.BadRequestError as e: logger.exception(f"Ошибка неверного запроса OpenAI: {e}"); return None
    except openai.OpenAIError as e: logger.exception(f"Произошла ошибка API OpenAI: {e}"); return None
    # Обработка других исключений
    except Exception as e: logger.exception(f"Неизвестная ошибка в call_openai: {e}"); return None


# --- Функция сохранения в B2 ---
# <<< ИЗМЕНЕНИЕ: Добавлено ensure_ascii=False, indent=4 при записи во временный файл >>>
def save_content_to_b2(folder, content_dict, generation_id, config_manager_instance):
    """Сохраняет словарь content_dict как JSON в указанную папку B2."""
    logger.info(f"Вызов save_content_to_b2 для ID: {generation_id}")
    config = config_manager_instance
    s3 = get_b2_client()
    if not s3: logger.error("❌ Не удалось создать клиент B2."); return False
    bucket_name = config.get("API_KEYS.b2.bucket_name")
    if not bucket_name: logger.error("❌ Имя бакета B2 не найдено."); return False
    if not generation_id: logger.error("❌ Generation ID не предоставлен."); return False
    if not isinstance(content_dict, dict): logger.error("❌ Данные не словарь."); return False

    clean_base_id = generation_id.replace(".json", "")
    s3_key = f"{folder.rstrip('/')}/{clean_base_id}.json"
    timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
    local_temp_path = f"{clean_base_id}_content_temp_{timestamp_suffix}.json"
    logger.info(f"Сохранение {clean_base_id} в B2 как {s3_key} через {local_temp_path}...")

    try:
        # Проверка ключей
        required_keys = ["topic", "content", "sarcasm", "script", "first_frame_description",
                         "creative_brief", "final_mj_prompt", "final_runway_prompt", "hashtags"]
        missing_keys = [key for key in required_keys if key not in content_dict]
        null_keys = [key for key in required_keys if key in content_dict and content_dict[key] is None]
        if missing_keys: logger.warning(f"⚠️ Отсутствуют ключи: {missing_keys}.")
        if null_keys: logger.warning(f"⚠️ Ключи с null: {null_keys}.")

        ensure_directory_exists(local_temp_path) # Создаем папку перед записью
        with open(local_temp_path, 'w', encoding='utf-8') as f:
            # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
            json.dump(content_dict, f, ensure_ascii=False, indent=4)
            # --- КОНЕЦ ИЗМЕНЕНИЯ ---
        logger.debug(f"Временный файл {local_temp_path} создан.")
        s3.upload_file(local_temp_path, bucket_name, s3_key)
        logger.info(f"✅ Данные для {clean_base_id} сохранены в B2: {s3_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Не удалось сохранить {clean_base_id} в B2: {e}", exc_info=True)
        return False
    finally:
        if os.path.exists(local_temp_path):
            try: os.remove(local_temp_path); logger.debug(f"Временный файл {local_temp_path} удален.")
            except OSError as remove_err: logger.warning(f"Не удалить {local_temp_path}: {remove_err}")
# --- КОНЕЦ ИЗМЕНЕНИЯ ---


# --- КЛАСС ГЕНЕРАТОРА КОНТЕНТА ---
class ContentGenerator:
    def __init__(self):
        """Инициализация генератора контента."""
        self.logger = logger
        self.config = ConfigManager()

        self.creative_config_data = self._load_additional_config('FILE_PATHS.creative_config', 'Creative Config')
        self.prompts_config_data = self._load_additional_config('FILE_PATHS.prompts_config', 'Prompts Config')

        self.topic_threshold = self.config.get('GENERATE.topic_threshold', 7)
        self.text_threshold = self.config.get('GENERATE.text_threshold', 8)
        self.max_attempts = self.config.get('GENERATE.max_attempts', 1)
        self.adaptation_enabled = self.config.get('GENERATE.adaptation_enabled', False)
        self.adaptation_params = self.config.get('GENERATE.adaptation_parameters', {})
        self.content_output_path = self.config.get('FILE_PATHS.content_output_path', 'generated_content.json')

        self.b2_client = get_b2_client()
        if not self.b2_client: self.logger.warning("⚠️ Не удалось инициализировать B2 клиент.")

        self.tracker_path_rel = self.config.get("FILE_PATHS.tracker_path", "data/topics_tracker.json")
        self.failsafe_path_rel = self.config.get("FILE_PATHS.failsafe_path", "config/FailSafeVault.json")
        self.error_folder_b2 = self.config.get("FILE_PATHS.error_folder", "000/")
        self.max_error_files = int(self.config.get("WORKFLOW.max_error_files", 20))


        self.tracker_path_abs = BASE_DIR / self.tracker_path_rel
        self.failsafe_path_abs = BASE_DIR / self.failsafe_path_rel
        self.b2_bucket_name = self.config.get("API_KEYS.b2.bucket_name", "default-bucket")
        if not self.b2_bucket_name or self.b2_bucket_name == "default-bucket":
            logger.warning("Имя бакета B2 не задано или используется значение по умолчанию!")

    def _load_additional_config(self, config_key, config_name):
        """Вспомогательный метод для загрузки доп. конфигов."""
        config_path_str = self.config.get(config_key)
        if not config_path_str: self.logger.error(f"❌ Путь к {config_name} не найден (ключ: {config_key})."); return None
        config_path = BASE_DIR / config_path_str
        data = load_json_config(str(config_path))
        if data: self.logger.info(f"✅ {config_name} загружен из {config_path}.")
        else: self.logger.error(f"❌ Не удалось загрузить {config_name} из {config_path}.")
        return data

    def adapt_prompts(self):
        """Применяет адаптацию промптов (если включено)."""
        if not self.adaptation_enabled: self.logger.info("🔄 Адаптация промптов отключена."); return
        self.logger.info("🔄 Применяю адаптацию промптов...");
        for key, value in self.adaptation_params.items(): self.logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def clear_generated_content(self):
        """Очищает локальный файл с промежуточными результатами."""
        try:
            content_path_obj = Path(self.content_output_path)
            self.logger.info(f"🧹 Очистка {content_path_obj.resolve()}") # Логируем абсолютный путь
            ensure_directory_exists(str(content_path_obj)) # Передаем строку
            with open(content_path_obj, 'w', encoding='utf-8') as file: json.dump({}, file)
            self.logger.info("✅ Локальный файл очищен/создан.")
        except PermissionError: handle_error("Clear Content Error", f"Нет прав на запись: {self.content_output_path}", PermissionError())
        except Exception as e: handle_error("Clear Content Error", str(e), e)


    def load_tracker(self):
        """Загружает трекер тем из B2 или локального файла."""
        tracker_path_abs = self.tracker_path_abs; tracker_path_rel = self.tracker_path_rel
        failsafe_path_abs = self.failsafe_path_abs; bucket_name = self.b2_bucket_name
        os.makedirs(tracker_path_abs.parent, exist_ok=True); tracker_updated_locally = False
        if self.b2_client:
            try:
                self.logger.info(f"Загрузка {tracker_path_rel} из B2...")
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f"); local_temp_tracker = f"tracker_temp_{timestamp_suffix}.json"
                ensure_directory_exists(local_temp_tracker) # Убедимся, что папка для temp есть
                self.b2_client.download_file(bucket_name, tracker_path_rel, local_temp_tracker)
                shutil.copyfile(local_temp_tracker, str(tracker_path_abs)); os.remove(local_temp_tracker)
                self.logger.info(f"✅ Загружен {tracker_path_rel} из B2.")
            except ClientError as e:
                 error_code = e.response.get('Error', {}).get('Code')
                 if error_code == 'NoSuchKey' or '404' in str(e): self.logger.warning(f"⚠️ {tracker_path_rel} не найден в B2.")
                 else: self.logger.error(f"⚠️ Ошибка B2 при загрузке трекера: {e}")
            except Exception as e: self.logger.warning(f"⚠️ Не удалось загрузить трекер из B2: {e}")
        else: self.logger.warning("⚠️ B2 клиент недоступен.")
        if not tracker_path_abs.exists():
            self.logger.warning(f"{tracker_path_abs} не найден. Создание из {failsafe_path_abs}.")
            try:
                ensure_directory_exists(str(failsafe_path_abs.parent)) # Проверка папки для failsafe
                if not failsafe_path_abs.is_file(): raise FileNotFoundError(f"Failsafe файл не найден: {failsafe_path_abs}")
                with open(failsafe_path_abs, 'r', encoding='utf-8') as f_failsafe: failsafe_data = json.load(f_failsafe)
                tracker = {"all_focuses": failsafe_data.get("focuses", []), "used_focuses": [], "focus_data": {}}
                with open(tracker_path_abs, 'w', encoding='utf-8') as f_tracker: json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                self.logger.info(f"✅ Создан новый {tracker_path_abs}."); tracker_updated_locally = True
            except FileNotFoundError: self.logger.error(f"❌ {failsafe_path_abs} не найден!"); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
            except Exception as e: self.logger.error(f"❌ Ошибка создания трекера: {e}"); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
        try:
            with open(tracker_path_abs, 'r', encoding='utf-8') as f: tracker = json.load(f)
            if "all_focuses" not in tracker: # Обновление структуры старого трекера
                self.logger.info("Обновляем структуру трекера."); failsafe_data = {}
                if failsafe_path_abs.exists():
                     with open(failsafe_path_abs, 'r', encoding='utf-8') as f_failsafe: failsafe_data = json.load(f_failsafe)
                tracker["all_focuses"] = failsafe_data.get("focuses", []); tracker.setdefault("used_focuses", []); tracker.setdefault("focus_data", {})
                with open(tracker_path_abs, 'w', encoding='utf-8') as f_tracker: json.dump(tracker, f_tracker, ensure_ascii=False, indent=4)
                tracker_updated_locally = True
            if tracker_updated_locally: self.sync_tracker_to_b2(tracker_path_abs=tracker_path_abs, tracker_path_rel=tracker_path_rel)
            return tracker
        except json.JSONDecodeError: self.logger.error(f"❌ Ошибка JSON в трекере: {tracker_path_abs}."); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}
        except Exception as e: self.logger.error(f"❌ Ошибка чтения трекера {tracker_path_abs}: {e}"); return {"all_focuses": [], "used_focuses": [], "focus_data": {}}

    def get_valid_focus_areas(self, tracker):
        """Возвращает список доступных фокусов."""
        all_focuses = tracker.get("all_focuses", []); used_focuses = tracker.get("used_focuses", [])
        used_set = set(used_focuses); valid_focuses = [f for f in all_focuses if f not in used_set]
        self.logger.info(f"✅ Доступные фокусы: {len(valid_focuses)} шт."); self.logger.debug(f"Полный список: {valid_focuses}")
        return valid_focuses

    def generate_topic(self, tracker):
        """Генерирует новую тему."""
        valid_focuses = self.get_valid_focus_areas(tracker)
        if not valid_focuses: raise ValueError("Все фокусы использованы.")
        selected_focus = random.choice(valid_focuses)
        self.logger.info(f"Выбран фокус: {selected_focus}")
        used_labels = tracker.get("focus_data", {}).get(selected_focus, [])
        exclusions_str = ", ".join(used_labels) if used_labels else "нет"

        prompt_config_key = "content.topic"
        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template: raise ValueError(f"Промпт {prompt_config_key} не найден.")
        prompt = prompt_template.format(focus_areas=selected_focus, exclusions=exclusions_str)

        try:
            topic_data = call_openai(prompt,
                                     prompt_config_key=prompt_config_key,
                                     use_json_mode=True,
                                     config_manager_instance=self.config,
                                     prompts_config_data_instance=self.prompts_config_data)

            if not topic_data: raise ValueError("call_openai не вернул ответ для темы.")

            full_topic = topic_data.get("full_topic"); short_topic = topic_data.get("short_topic")
            if not full_topic or not short_topic: raise ValueError(f"Ответ для темы не содержит ключи: {topic_data}")
            self.logger.info(f"Сгенерирована тема: '{full_topic}' (Ярлык: '{short_topic}')")
            self.update_tracker(selected_focus, short_topic, tracker)
            self.save_to_generated_content("topic", {"full_topic": full_topic, "short_topic": short_topic})
            content_metadata = {"theme": "tragic" if "(т)" in selected_focus else "normal"}
            return full_topic, content_metadata, selected_focus
        except Exception as e:
            self.logger.error(f"Ошибка генерации темы: {e}", exc_info=True)
            return None, None, None
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
            ensure_directory_exists(str(self.tracker_path_abs.parent)) # Используем parent
            with open(self.tracker_path_abs, "w", encoding="utf-8") as file: json.dump(tracker, file, ensure_ascii=False, indent=4)
            self.logger.info(f"Трекер тем сохранен: {self.tracker_path_abs}")
        except Exception as e: self.logger.error(f"Ошибка сохранения трекера: {e}")

    def sync_tracker_to_b2(self, tracker_path_abs, tracker_path_rel):
        """Синхронизирует локальный трекер с B2."""
        if not self.b2_client: self.logger.warning("B2 клиент недоступен."); return
        if not tracker_path_abs.is_file(): self.logger.warning(f"Локальный трекер {tracker_path_abs} не найден."); return # Проверка is_file
        try:
            self.logger.info(f"Синхронизация {tracker_path_abs} с B2 как {tracker_path_rel}...")
            self.b2_client.upload_file(str(tracker_path_abs), self.b2_bucket_name, tracker_path_rel)
            self.logger.info(f"✅ {tracker_path_rel} синхронизирован с B2.")
        except Exception as e: self.logger.error(f"⚠️ Не удалось загрузить трекер {tracker_path_rel} в B2: {e}")

    def _get_prompt_template(self, prompt_config_key: str) -> str | None:
        """Вспомогательный метод для получения шаблона промпта."""
        if not self.prompts_config_data: self.logger.error("❌ Конфигурация промптов не загружена."); return None
        keys = prompt_config_key.split('.')
        prompt_settings = self.prompts_config_data
        try:
            for key in keys: prompt_settings = prompt_settings[key]
            template = prompt_settings.get('template')
            if not template: self.logger.error(f"Шаблон 'template' не найден для '{prompt_config_key}'")
            return template
        except (KeyError, TypeError): self.logger.error(f"Ошибка доступа к ключу/структуре '{prompt_config_key}'"); return None

    def generate_sarcasm(self, text, content_data={}):
        """
        Генерирует саркастический комментарий и возвращает его как СТРОКУ
        или None при ошибке/отключении.
        """
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.comment_enabled', True):
            self.logger.info("🔕 Генерация комментария отключена.");
            return None  # Возвращаем None

        prompt_key_suffix = "tragic_comment" if content_data.get("theme") == "tragic" else "comment"
        prompt_config_key = f"sarcasm.{prompt_key_suffix}"
        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template: return None  # Возвращаем None

        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос комментария (ключ: {prompt_config_key})...")
        try:
            # Просим OpenAI вернуть ПРОСТОЙ ТЕКСТ
            comment_text = call_openai(prompt,
                                       prompt_config_key=prompt_config_key,
                                       use_json_mode=False,  # Просим НЕ JSON
                                       config_manager_instance=self.config,
                                       prompts_config_data_instance=self.prompts_config_data)

            if comment_text and isinstance(comment_text, str):
                # Просто возвращаем полученный текст
                self.logger.info(f"✅ Сгенерирован текст комментария: {comment_text}")
                # Убираем возможные кавычки по краям, если OpenAI их добавил
                return comment_text.strip().strip('"')
            elif comment_text:
                self.logger.warning(
                    f"Ответ OpenAI для сарказма не является строкой: {type(comment_text)}. Возвращаем None.")
                return None
            else:
                self.logger.error(f"❌ Ошибка генерации текста комментария ({prompt_config_key}).")
                return None  # Возвращаем None при ошибке
        except Exception as e:
            self.logger.error(f"❌ Исключение при генерации комментария: {e}");
            return None  # Возвращаем None при исключении

    def generate_sarcasm_poll(self, text, content_data={}):
        """Генерирует саркастический опрос."""
        if not self.config.get('SARCASM.enabled', True) or not self.config.get('SARCASM.poll_enabled', True):
            self.logger.info("🔕 Генерация опроса отключена."); return {}
        prompt_key_suffix = "tragic_poll" if content_data.get("theme") == "tragic" else "poll"
        prompt_config_key = f"sarcasm.{prompt_key_suffix}"
        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template: return {}
        prompt = prompt_template.format(text=text)
        self.logger.info(f"Запрос опроса (ключ: {prompt_config_key})... JSON.")
        try:
            poll_data = call_openai(prompt,
                                    prompt_config_key=prompt_config_key,
                                    use_json_mode=True, # Опрос - JSON
                                    config_manager_instance=self.config,
                                    prompts_config_data_instance=self.prompts_config_data)

            if not poll_data: self.logger.error(f"❌ Ошибка генерации опроса ({prompt_config_key})."); return {}

            if isinstance(poll_data, dict) and "question" in poll_data and "options" in poll_data and isinstance(poll_data["options"], list) and len(poll_data["options"]) == 3:
                self.logger.info("✅ Опрос сгенерирован."); poll_data["question"] = str(poll_data["question"]).strip(); poll_data["options"] = [str(opt).strip() for opt in poll_data["options"]]
                return poll_data
            else: self.logger.error(f"❌ Структура JSON опроса неверна: {poll_data}"); return {}
        except Exception as e: self.logger.error(f"❌ Исключение опроса: {e}"); return {}

    def generate_hashtags(self, topic: str, main_text: str) -> list[str] | None:
        """
        Генерирует список хештегов на основе темы и текста поста.
        Возвращает список строк (хештегов) или None при ошибке.
        """
        self.logger.info("Запрос хештегов...")
        if not topic or not main_text:
            self.logger.warning("Тема или текст поста отсутствуют, генерация хештегов невозможна.")
            return None

        prompt_config_key = "content.hashtags" # Используем новый ключ
        prompt_template = self._get_prompt_template(prompt_config_key)
        if not prompt_template:
            self.logger.error(f"Промпт {prompt_config_key} не найден.")
            return None

        prompt = prompt_template.format(topic=topic, main_text=main_text)

        try:
            hashtags_data = call_openai(prompt,
                                        prompt_config_key=prompt_config_key,
                                        use_json_mode=True, # Ожидаем JSON
                                        config_manager_instance=self.config,
                                        prompts_config_data_instance=self.prompts_config_data)

            if not hashtags_data:
                self.logger.error(f"❌ Ошибка генерации хештегов ({prompt_config_key}).")
                return None

            # Валидация ответа
            if isinstance(hashtags_data, dict) and "hashtags" in hashtags_data:
                hashtags_list = hashtags_data["hashtags"]
                if isinstance(hashtags_list, list) and all(isinstance(tag, str) for tag in hashtags_list):
                    # Очистка хештегов (удаление #, пробелов по краям)
                    cleaned_hashtags = [tag.strip().replace('#', '').lower() for tag in hashtags_list if tag.strip()]
                    self.logger.info(f"✅ Сгенерированы хештеги: {cleaned_hashtags}")
                    return cleaned_hashtags
                else:
                    self.logger.error(f"❌ Неверный формат списка хештегов в ответе: {hashtags_list}")
                    return None
            else:
                self.logger.error(f"❌ Ответ OpenAI для хештегов не содержит ключ 'hashtags' или не является словарем: {hashtags_data}")
                return None
        except Exception as e:
            self.logger.error(f"❌ Исключение при генерации хештегов: {e}", exc_info=True)
            return None

    # <<< ИЗМЕНЕНИЕ: Добавлено ensure_ascii=False, indent=4 при записи в локальный файл >>>
    def save_to_generated_content(self, stage, data):
        """Сохраняет промежуточные данные в локальный JSON файл."""
        try:
            if not self.content_output_path: raise ValueError("❌ self.content_output_path не задан!")
            content_path_obj = Path(self.content_output_path)
            self.logger.debug(f"🔄 Обновление {content_path_obj.resolve()}, этап: {stage}")
            ensure_directory_exists(str(content_path_obj)); result_data = {}
            if content_path_obj.exists():
                try:
                    if content_path_obj.stat().st_size > 0:
                        with open(content_path_obj, 'r', encoding='utf-8') as file: result_data = json.load(file)
                    else: self.logger.warning(f"⚠️ Файл {content_path_obj} пуст."); result_data = {}
                except json.JSONDecodeError: self.logger.warning(f"⚠️ Файл {content_path_obj} поврежден."); result_data = {}
                except Exception as read_err: self.logger.error(f"Ошибка чтения {content_path_obj}: {read_err}"); result_data = {}
            result_data["timestamp"] = datetime.utcnow().isoformat(); result_data[stage] = data
            with open(content_path_obj, 'w', encoding='utf-8') as file:
                # --- ИЗМЕНЕНИЕ ЗДЕСЬ ---
                json.dump(result_data, file, ensure_ascii=False, indent=4)
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---
            self.logger.debug(f"✅ Локально обновлено для этапа: {stage}")
        except Exception as e: handle_error("Save Content Error", f"Ошибка при сохранении в {self.content_output_path}: {str(e)}", e)
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    def critique_content(self, content, topic):
        """Выполняет критику текста (если включено)."""
        if not self.config.get('CONTENT.critique.enabled', False): self.logger.info("🔕 Критика отключена."); return "Критика отключена."
        if not content: self.logger.warning("Нет текста для критики."); return "Нет текста для критики."
        try:
            self.logger.info("🔄 Выполняется критика...")
            prompt_config_key = "content.critique"
            prompt_template = self._get_prompt_template(prompt_config_key)
            if not prompt_template or prompt_template == "...": self.logger.error(f"Промпт {prompt_config_key} не найден."); return "Промпт критики не найден."
            prompt = prompt_template.format(content=content, topic=topic)
            critique = call_openai(prompt,
                                   prompt_config_key=prompt_config_key,
                                   use_json_mode=False, # Критика - строка
                                   config_manager_instance=self.config,
                                   prompts_config_data_instance=self.prompts_config_data)
            if critique: self.logger.info("✅ Критика завершена.")
            else: self.logger.error(f"❌ Ошибка критики ({prompt_config_key}).")
            return critique if critique else "Критика завершилась ошибкой."
        except Exception as e: self.logger.error(f"❌ Исключение при критике: {e}"); return "Критика завершилась ошибкой."

    def format_list_for_prompt(self, items: list | dict, use_weights=False) -> str:
        """Форматирует список или словарь списков для вставки в промпт."""
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

        # Переменная для хранения текста с абзацами
        text_initial_with_paragraphs = ""
        # Переменные для многошаговой генерации
        creative_brief, script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en = None, None, None, None, None
        script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru = None, None, None, None
        # Переменные для сарказма
        sarcastic_comment_text = None
        sarcastic_poll = {}
        # Переменная для хештегов
        generated_hashtags = []

        # --- ИЗМЕНЕНИЕ: Обертка try/except/finally для логирования креативных выборов ---
        try:
            # Шаг 1: Подготовка
            self.adapt_prompts();
            self.clear_generated_content()
            # Шаг 2: Генерация Темы
            tracker = self.load_tracker()
            topic, content_data, selected_focus = self.generate_topic(tracker)
            if topic is None or selected_focus is None:
                self.logger.error("Не удалось сгенерировать тему или получить фокус. Прерывание.")
                raise RuntimeError("Ошибка генерации темы")

            # Шаг 3: Генерация Текста (RU)
            generate_text_enabled = self.config.get('CONTENT.text.enabled', True);
            generate_tragic_text_enabled = self.config.get('CONTENT.tragic_text.enabled', True)
            if (content_data.get("theme") == "tragic" and generate_tragic_text_enabled) or (
                    content_data.get("theme") != "tragic" and generate_text_enabled):
                prompt_key_suffix = "tragic_text" if content_data.get("theme") == "tragic" else "text";
                prompt_config_key = f"content.{prompt_key_suffix}"
                prompt_template = self._get_prompt_template(prompt_config_key)
                if prompt_template:
                    self.logger.info(
                        f"Запрос текста (ключ: {prompt_config_key}). Ожидается текст с абзацами ('\\n\\n').")
                    text_initial_with_paragraphs = call_openai(prompt_template.format(topic=topic),
                                                               prompt_config_key=prompt_config_key,
                                                               use_json_mode=False,  # Текст - строка
                                                               config_manager_instance=self.config,
                                                               prompts_config_data_instance=self.prompts_config_data)
                    if text_initial_with_paragraphs:
                        self.logger.info(f"Текст: {text_initial_with_paragraphs[:100]}...");
                        self.save_to_generated_content("text", {"text": text_initial_with_paragraphs})
                    else:
                        self.logger.warning(f"Генерация текста ({prompt_config_key}) не удалась.")
                        text_initial_with_paragraphs = ""  # Устанавливаем пустую строку при ошибке
                else:
                    self.logger.warning(f"Промпт {prompt_config_key} не найден.")
                    text_initial_with_paragraphs = ""
            else:
                self.logger.info(f"Генерация текста (тема: {content_data.get('theme')}) отключена.")
                text_initial_with_paragraphs = ""

            # Шаг 4: Критика (используем текст с абзацами)
            critique_result = self.critique_content(text_initial_with_paragraphs, topic);
            self.save_to_generated_content("critique", {"critique": critique_result})

            # Шаг 5: Генерация Сарказма (RU)
            if text_initial_with_paragraphs:
                sarcastic_comment_text = self.generate_sarcasm(text_initial_with_paragraphs,
                                                               content_data)
                sarcastic_poll = self.generate_sarcasm_poll(text_initial_with_paragraphs, content_data)
            self.save_to_generated_content("sarcasm", {"comment_text": sarcastic_comment_text,
                                                       "poll": sarcastic_poll})

            # Шаг 5.5: Генерация хештегов
            if text_initial_with_paragraphs:
                generated_hashtags = self.generate_hashtags(topic, text_initial_with_paragraphs) or []
            self.save_to_generated_content("hashtags", generated_hashtags)

            # Шаг 6: Многошаговая Генерация Брифа и Промптов (EN) + Перевод (RU)
            self.logger.info("--- Запуск многошаговой генерации ---")
            enable_russian_translation = self.config.get("WORKFLOW.enable_russian_translation", False)
            self.logger.info(f"Перевод {'ВКЛЮЧЕН' if enable_russian_translation else 'ОТКЛЮЧЕН'}.")
            try:
                # Подготовка списков для промптов шага 6
                moods_list_str = self.format_list_for_prompt(self.creative_config_data.get("moods", []),
                                                             use_weights=True)
                arcs_list_str = self.format_list_for_prompt(self.creative_config_data.get("emotional_arcs", []))
                main_prompts_list = self.creative_config_data.get("creative_prompts", {}).get("main", [])
                prompts_list_str = self.format_list_for_prompt(main_prompts_list, use_weights=True)
                perspectives_list_str = self.format_list_for_prompt(
                    self.creative_config_data.get("perspective_types", []))
                metaphors_list_str = self.format_list_for_prompt(
                    self.creative_config_data.get("visual_metaphor_types", []))
                directors_list_str = self.format_list_for_prompt(self.creative_config_data.get("director_styles", []))
                artists_list_str = self.format_list_for_prompt(self.creative_config_data.get("artist_styles", []))

                # Шаг 6.1: Ядро
                self.logger.info("--- Шаг 6.1: Ядро ---");
                prompt_key1 = "multi_step.step1_core";
                tmpl1 = self._get_prompt_template(prompt_key1);
                if not tmpl1: raise ValueError(f"{prompt_key1} не найден.")
                prompt1 = tmpl1.format(input_text=topic, moods_list_str=moods_list_str, arcs_list_str=arcs_list_str)
                core_brief = call_openai(prompt1, prompt_config_key=prompt_key1, use_json_mode=True,
                                         config_manager_instance=self.config,
                                         prompts_config_data_instance=self.prompts_config_data)
                if not core_brief or not all(
                    k in core_brief for k in ["chosen_type", "chosen_value", "justification"]): raise ValueError(
                    f"Шаг 6.1: неверный JSON {core_brief}.")

                # Шаг 6.2: Драйвер
                self.logger.info("--- Шаг 6.2: Драйвер ---");
                prompt_key2 = "multi_step.step2_driver";
                tmpl2 = self._get_prompt_template(prompt_key2);
                if not tmpl2: raise ValueError(f"{prompt_key2} не найден.")
                prompt2 = tmpl2.format(input_text=topic,
                                       chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2),
                                       prompts_list_str=prompts_list_str, perspectives_list_str=perspectives_list_str,
                                       metaphors_list_str=metaphors_list_str)
                driver_brief = call_openai(prompt2, prompt_config_key=prompt_key2, use_json_mode=True,
                                           config_manager_instance=self.config,
                                           prompts_config_data_instance=self.prompts_config_data)
                if not driver_brief or not all(k in driver_brief for k in ["chosen_driver_type", "chosen_driver_value",
                                                                           "justification"]): raise ValueError(
                    f"Шаг 6.2: неверный JSON {driver_brief}.")

                # Шаг 6.3: Эстетика
                self.logger.info("--- Шаг 6.3: Эстетика ---");
                prompt_key3 = "multi_step.step3_aesthetic";
                tmpl3 = self._get_prompt_template(prompt_key3);
                if not tmpl3: raise ValueError(f"{prompt_key3} не найден.")
                prompt3 = tmpl3.format(input_text=topic,
                                       chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2),
                                       chosen_driver_json=json.dumps(driver_brief, ensure_ascii=False, indent=2),
                                       directors_list_str=directors_list_str, artists_list_str=artists_list_str)
                aesthetic_brief = call_openai(prompt3, prompt_config_key=prompt_key3, use_json_mode=True,
                                              config_manager_instance=self.config,
                                              prompts_config_data_instance=self.prompts_config_data)
                # Валидация aesthetic_brief
                valid_step3 = False
                if isinstance(aesthetic_brief, dict):
                    style_needed = aesthetic_brief.get("style_needed", False);
                    base_keys_exist = all(k in aesthetic_brief for k in
                                          ["style_needed", "chosen_style_type", "chosen_style_value", "style_keywords",
                                           "justification"])
                    if base_keys_exist:
                        if not style_needed:
                            if all(aesthetic_brief.get(k) is None for k in
                                   ["chosen_style_type", "chosen_style_value", "style_keywords", "justification"]):
                                valid_step3 = True
                            else:
                                self.logger.warning(
                                    f"Шаг 6.3: style_needed=false, но ключи не null. Исправляем."); aesthetic_brief.update(
                                    {k: None for k in ["chosen_style_type", "chosen_style_value", "style_keywords",
                                                       "justification"]}); valid_step3 = True
                        else:
                            if all([aesthetic_brief.get("chosen_style_type"), aesthetic_brief.get("chosen_style_value"),
                                    isinstance(aesthetic_brief.get("style_keywords"), list),
                                    aesthetic_brief.get("justification")]):
                                valid_step3 = True
                            else:
                                logger.error(f"Шаг 6.3: style_needed=true, но значения некорректны.")
                    else:
                        logger.error(f"Шаг 6.3: Отсутствуют базовые ключи.")
                else:
                    logger.error(f"Шаг 6.3: Ответ не словарь.")
                if not valid_step3: raise ValueError("Шаг 6.3: неверный JSON.")

                # Сборка Брифа
                creative_brief = {"core": core_brief, "driver": driver_brief, "aesthetic": aesthetic_brief};
                self.logger.info("--- Шаг 6.4: Бриф Собран ---");
                self.logger.debug(f"Бриф: {json.dumps(creative_brief, ensure_ascii=False, indent=2)}");
                self.save_to_generated_content("creative_brief", creative_brief)

                # Шаг 6.5: Сценарий и Описание (EN)
                self.logger.info("--- Шаг 6.5: Сценарий и Описание (EN) ---");
                prompt_key5 = "multi_step.step5_script_frame";
                tmpl5 = self._get_prompt_template(prompt_key5);
                if not tmpl5: raise ValueError(f"{prompt_key5} не найден.")
                prompt5 = tmpl5.format(input_text=topic,
                                       creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2))
                script_frame_data = call_openai(prompt5, prompt_config_key=prompt_key5, use_json_mode=True,
                                                config_manager_instance=self.config,
                                                prompts_config_data_instance=self.prompts_config_data)
                if not script_frame_data or not all(
                    k in script_frame_data for k in ["script", "first_frame_description"]): raise ValueError(
                    f"Шаг 6.5: неверный JSON {script_frame_data}.")
                script_en = script_frame_data["script"];
                frame_description_en = script_frame_data["first_frame_description"]
                self.logger.info(f"Сценарий (EN): {script_en[:100]}...");
                self.logger.info(f"Описание (EN): {frame_description_en[:100]}...");
                self.save_to_generated_content("script_frame_en",
                                               {"script": script_en, "first_frame_description": frame_description_en})

                # Шаг 6.6a: MJ Промпт (EN)
                self.logger.info("--- Шаг 6.6a: MJ Промпт (EN) ---");
                mj_params_cfg = self.config.get("IMAGE_GENERATION", {});
                aspect_ratio_str = mj_params_cfg.get("output_size", "16:9").replace('x', ':').replace('×', ':');
                version_str = str(mj_params_cfg.get("midjourney_version", "7.0"));
                style_str = mj_params_cfg.get("midjourney_style", None)
                mj_parameters_json_for_prompt = json.dumps(
                    {"aspect_ratio": aspect_ratio_str, "version": version_str, "style": style_str}, ensure_ascii=False);
                style_parameter_str_for_prompt = f" --style {style_str}" if style_str else ""
                prompt_key6a = "multi_step.step6a_mj_adapt";
                tmpl6a = self._get_prompt_template(prompt_key6a);
                if not tmpl6a: raise ValueError(f"{prompt_key6a} не найден.")
                prompt6a = tmpl6a.format(first_frame_description=frame_description_en,
                                         creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2),
                                         script=script_en, input_text=topic,
                                         mj_parameters_json=mj_parameters_json_for_prompt,
                                         aspect_ratio=aspect_ratio_str, version=version_str,
                                         style_parameter_str=style_parameter_str_for_prompt)
                mj_prompt_data = call_openai(prompt6a, prompt_config_key=prompt_key6a, use_json_mode=True,
                                             config_manager_instance=self.config,
                                             prompts_config_data_instance=self.prompts_config_data)
                if not mj_prompt_data or "final_mj_prompt" not in mj_prompt_data: raise ValueError(
                    f"Шаг 6.6a: неверный JSON {mj_prompt_data}.")
                final_mj_prompt_en = mj_prompt_data["final_mj_prompt"];
                self.logger.info(f"MJ промпт (EN, V{version_str}): {final_mj_prompt_en}");
                self.save_to_generated_content("final_mj_prompt_en", {"final_mj_prompt": final_mj_prompt_en})

                # Шаг 6.6b: Runway Промпт (EN)
                self.logger.info("--- Шаг 6.6b: Runway Промпт (EN) ---");
                prompt_key6b = "multi_step.step6b_runway_adapt";
                tmpl6b = self._get_prompt_template(prompt_key6b);
                if not tmpl6b: raise ValueError(f"{prompt_key6b} не найден.")
                prompt6b = tmpl6b.format(script=script_en,
                                         creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2),
                                         input_text=topic)
                runway_prompt_data = call_openai(prompt6b, prompt_config_key=prompt_key6b, use_json_mode=True,
                                                 config_manager_instance=self.config,
                                                 prompts_config_data_instance=self.prompts_config_data)
                if not runway_prompt_data or "final_runway_prompt" not in runway_prompt_data: raise ValueError(
                    f"Шаг 6.6b: неверный JSON {runway_prompt_data}.")
                final_runway_prompt_en = runway_prompt_data["final_runway_prompt"];
                self.logger.info(f"Runway промпт (EN): {final_runway_prompt_en}");
                self.save_to_generated_content("final_runway_prompt_en",
                                               {"final_runway_prompt": final_runway_prompt_en})

                # Шаг 6.6c: Перевод (RU)
                if enable_russian_translation:
                    self.logger.info("--- Шаг 6.6c: Перевод (RU) ---")
                    if all([script_en, frame_description_en, final_mj_prompt_en, final_runway_prompt_en]):
                        prompt_key6c = "multi_step.step6c_translate";
                        tmpl6c = self._get_prompt_template(prompt_key6c);
                        if not tmpl6c: raise ValueError(f"{prompt_key6c} не найден.")
                        prompt6c = tmpl6c.format(script_en=script_en, frame_description_en=frame_description_en,
                                                 mj_prompt_en=final_mj_prompt_en,
                                                 runway_prompt_en=final_runway_prompt_en)
                        translations = call_openai(prompt6c, prompt_config_key=prompt_key6c, use_json_mode=True,
                                                   config_manager_instance=self.config,
                                                   prompts_config_data_instance=self.prompts_config_data)
                        if translations:  # translations уже словарь
                            script_ru = translations.get("script_ru");
                            frame_description_ru = translations.get("first_frame_description_ru");
                            final_mj_prompt_ru = translations.get("final_mj_prompt_ru");
                            final_runway_prompt_ru = translations.get("final_runway_prompt_ru")
                            if all([script_ru, frame_description_ru, final_mj_prompt_ru, final_runway_prompt_ru]):
                                self.logger.info("✅ Перевод выполнен."); self.save_to_generated_content(
                                    "translations_ru", translations)
                            else:
                                self.logger.error(
                                    f"Шаг 6.6c: Не все поля переведены. {translations}"); translations = None
                        else:
                            self.logger.error("Шаг 6.6c не удался."); translations = None
                    else:
                        self.logger.error("Недостаточно данных для перевода."); translations = None
                else:
                    self.logger.info("Перевод пропущен.")

            except (json.JSONDecodeError, ValueError, RuntimeError) as step6_err:
                self.logger.error(f"❌ Ошибка шага 6: {step6_err}.")
                if isinstance(step6_err, RuntimeError) and "OpenAI client" in str(step6_err):
                    raise  # Пробрасываем ошибку инициализации клиента
            except Exception as script_err:
                self.logger.error(f"❌ Ошибка шага 6: {script_err}", exc_info=True)

            # Шаг 7: Формирование итогового словаря
            self.logger.info("Формирование итогового словаря для сохранения...")
            # <<< ИЗМЕНЕНИЕ: Формируем JSON-строки с ensure_ascii=False >>>
            content_json_str = json.dumps({"текст": text_initial_with_paragraphs.strip()}, ensure_ascii=False, indent=2)
            sarcasm_comment_json_str_final = None
            if sarcastic_comment_text:
                try:
                    sarcasm_comment_dict = {"комментарий": sarcastic_comment_text}
                    sarcasm_comment_json_str_final = json.dumps(sarcasm_comment_dict, ensure_ascii=False, indent=2)
                    self.logger.debug(
                        f"Сформирована финальная JSON-строка для сарказма: {sarcasm_comment_json_str_final}")
                except Exception as json_err:
                    self.logger.error(f"Ошибка кодирования текста сарказма в JSON: {json_err}. Сарказм будет null.")
                    sarcasm_comment_json_str_final = None
            else:
                self.logger.debug("Текст комментария сарказма пуст, финальная JSON-строка будет null.")
            # <<< КОНЕЦ ИЗМЕНЕНИЯ >>>

            complete_content_dict = {
                "topic": topic,
                "content": content_json_str,
                "selected_focus": selected_focus,
                "sarcasm": {
                    "comment": sarcasm_comment_json_str_final,
                    "poll": sarcastic_poll if sarcastic_poll else None
                },
                "script": script_en, "first_frame_description": frame_description_en,
                "creative_brief": creative_brief, "final_mj_prompt": final_mj_prompt_en,
                "final_runway_prompt": final_runway_prompt_en,
                "script_ru": script_ru, "first_frame_description_ru": frame_description_ru,
                "final_mj_prompt_ru": final_mj_prompt_ru, "final_runway_prompt_ru": final_runway_prompt_ru,
                "hashtags": generated_hashtags if generated_hashtags else None,
            }
            # Очистка None значений
            complete_content_dict = {k: v for k, v in complete_content_dict.items() if v is not None}
            if isinstance(complete_content_dict.get("sarcasm"), dict):
                complete_content_dict["sarcasm"] = {k: v for k, v in complete_content_dict["sarcasm"].items() if
                                                    v is not None}
                if not complete_content_dict["sarcasm"]:
                    del complete_content_dict["sarcasm"]

            self.logger.debug(
                f"Итоговый словарь перед валидацией: {json.dumps(complete_content_dict, ensure_ascii=False, indent=2)}")

            # Шаг 7.1: Валидация выходного JSON
            # Используем функцию validate_output_json, если она импортирована
            if 'validate_output_json' in globals() and callable(globals()['validate_output_json']):
                is_valid, validation_message = validate_output_json(complete_content_dict, self.logger)
            else:
                self.logger.warning("Функция validate_output_json не найдена, валидация пропускается.")
                is_valid = True # Пропускаем валидацию

            if not is_valid:
                self.logger.error(f"❌ ВАЛИДАЦИЯ НЕ ПРОЙДЕНА для ID {generation_id}: {validation_message}")
                error_filename = f"error_{generation_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
                local_error_path = f"temp_error_{error_filename}"
                error_data_to_save = {"validation_error": validation_message, "generation_id": generation_id,
                                      "timestamp_utc": datetime.utcnow().isoformat(),
                                      "invalid_data": complete_content_dict}
                if not save_error_to_b2(s3_client=self.b2_client, bucket_name=self.b2_bucket_name,
                                        error_folder=self.error_folder_b2, local_file_path_str=local_error_path,
                                        error_data_dict=error_data_to_save, max_error_files=self.max_error_files):
                    self.logger.error(
                        f"!!! КРИТИЧЕСКАЯ ОШИБКА: Не удалось сохранить файл ошибки для ID {generation_id} в B2 !!!")
                raise ValueError(f"Validation failed for {generation_id}: {validation_message}")
            else:
                self.logger.info(f"✅ Валидация успешно пройдена для ID {generation_id}.")

            # Шаг 8: Сохранение в B2 (папка 666/)
            self.logger.info(f"Сохранение валидного контента в B2 для ID {generation_id}...")
            if not save_content_to_b2("666/", complete_content_dict, generation_id, self.config):
                raise Exception(f"Не удалось сохранить итоговый контент в B2 для ID {generation_id}")

            # Шаг 9: Обновление config_midjourney.json
            self.logger.info(f"Обновление config_midjourney.json для ID: {generation_id}...")
            try:
                s3_client_mj = self.b2_client
                if not s3_client_mj: raise ConnectionError("B2 клиент недоступен.")
                config_mj_remote_path = self.config.get('FILE_PATHS.config_midjourney', 'config/config_midjourney.json')
                timestamp_suffix = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
                config_mj_local_path = f"config_midjourney_{generation_id}_temp_{timestamp_suffix}.json"
                bucket_name = self.b2_bucket_name
                ensure_directory_exists(config_mj_local_path)
                config_mj = load_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path,
                                         default_value={})
                if config_mj is None: config_mj = {}
                config_mj['generation'] = True;
                config_mj['midjourney_task'] = None;
                config_mj['midjourney_results'] = {};
                config_mj['status'] = None
                self.logger.info("Данные для config_midjourney.json подготовлены.")
                if not save_b2_json(s3_client_mj, bucket_name, config_mj_remote_path, config_mj_local_path, config_mj):
                    raise Exception("Не удалось сохранить config_mj!")
                else:
                    self.logger.info(f"✅ Обновленный {config_mj_remote_path} загружен в B2.")
            except Exception as e:
                self.logger.error(f"❌ Не удалось обновить config_midjourney.json: {e}", exc_info=True); raise Exception(
                    "Критическая ошибка: не удалось установить флаг generation: true") from e

            self.logger.info(f"✅ ContentGenerator.run успешно завершен для ID {generation_id}.")

        # --- ИЗМЕНЕНИЕ: Добавлен блок finally для логирования ---
        finally:
            # Фрагмент для блока finally в методе run класса ContentGenerator (generate_content.py)
            # ... (начало блока finally) ...
            # --- Логирование креативных выборов (РАСШИРЕННОЕ) ---
            if 'creative_brief' in locals() and creative_brief:
                try:
                    # --- Получаем пути (как и раньше) ---
                    log_path_rel = self.config.get('FILE_PATHS.creative_choices_log_path', 'data/creative_choices.csv')
                    local_temp_csv_path = BASE_DIR / (
                                log_path_rel + ".local_temp_v2")  # Используем новый суффикс для временного файла
                    b2_log_path_key = log_path_rel

                    ensure_directory_exists(str(local_temp_csv_path))

                    # --- Извлекаем ОСНОВНЫЕ данные для новой строки (как и раньше) ---
                    core_data = creative_brief.get('core', {})
                    driver_data = creative_brief.get('driver', {})
                    aesthetic_data = creative_brief.get('aesthetic', {})

                    core_choice_value = core_data.get('chosen_value', 'N/A')
                    driver_choice_value = driver_data.get('chosen_driver_value', 'N/A')

                    style_needed = aesthetic_data.get('style_needed', False)
                    aesthetic_choice_value = aesthetic_data.get('chosen_style_value', 'N/A') if style_needed else 'None'

                    timestamp_log = datetime.now(timezone.utc).isoformat()

                    # --- ДОПОЛНИТЕЛЬНЫЕ ДАННЫЕ для логирования ---
                    # Важно: эти переменные должны быть доступны в этой области видимости.
                    # Они должны были быть определены ранее в методе run().

                    # 1. InputTopic (переменная 'topic' из метода run)
                    input_topic_log = topic if 'topic' in locals() and topic else 'N/A'

                    # 2. CoreChoiceType
                    core_choice_type_log = core_data.get('chosen_type', 'N/A')

                    # 3. DriverChoiceType
                    driver_choice_type_log = driver_data.get('chosen_driver_type', 'N/A')

                    # 4. AestheticChoiceType
                    aesthetic_choice_type_log = aesthetic_data.get('chosen_style_type',
                                                                   'N/A') if style_needed else 'None'

                    # 5. StyleKeywords (список или None)
                    style_keywords_list = aesthetic_data.get('style_keywords') if style_needed else None
                    # Преобразуем список в строку для CSV, например, через точку с запятой, или оставляем пустым
                    style_keywords_log = ";".join(style_keywords_list) if isinstance(style_keywords_list,
                                                                                     list) else 'None'

                    # 6. FinalMJPrompt_EN (переменная 'final_mj_prompt_en' из метода run)
                    final_mj_prompt_en_log = final_mj_prompt_en if 'final_mj_prompt_en' in locals() and final_mj_prompt_en else 'N/A'

                    # 7. FinalRunwayPrompt_EN (переменная 'final_runway_prompt_en' из метода run)
                    final_runway_prompt_en_log = final_runway_prompt_en if 'final_runway_prompt_en' in locals() and final_runway_prompt_en else 'N/A'

                    # Очистка данных от запятых, чтобы не ломать CSV
                    def clean_csv_value(value):
                        if isinstance(value, str):
                            return value.replace(',', ';').replace('\n', ' ').replace('\r', '')
                        return value

                    input_topic_log = clean_csv_value(input_topic_log)
                    core_choice_value = clean_csv_value(core_choice_value)
                    driver_choice_value = clean_csv_value(driver_choice_value)
                    aesthetic_choice_value = clean_csv_value(aesthetic_choice_value)
                    style_keywords_log = clean_csv_value(style_keywords_log)
                    final_mj_prompt_en_log = clean_csv_value(final_mj_prompt_en_log)
                    final_runway_prompt_en_log = clean_csv_value(final_runway_prompt_en_log)

                    # --- ОБНОВЛЕННЫЙ ЗАГОЛОВОК CSV ---
                    header = (
                        "TimestampUTC,GenerationID,InputTopic,"
                        "CoreChoiceValue,CoreChoiceType,"
                        "DriverChoiceValue,DriverChoiceType,"
                        "AestheticChoiceValue,AestheticChoiceType,StyleKeywords,"
                        "FinalMJPrompt_EN,FinalRunwayPrompt_EN\n"
                    )

                    # --- ОБНОВЛЕННАЯ СТРОКА ДАННЫХ ---
                    new_log_line = (
                        f"{timestamp_log},{generation_id},{input_topic_log},"
                        f"{core_choice_value},{core_choice_type_log},"
                        f"{driver_choice_value},{driver_choice_type_log},"
                        f"{aesthetic_choice_value},{aesthetic_choice_type_log},{style_keywords_log},"
                        f"{final_mj_prompt_en_log},{final_runway_prompt_en_log}\n"
                    )

                    # --- Логика скачивания, записи и загрузки CSV (остается прежней, но проверяет новый header) ---
                    existing_content = ""
                    needs_header = True

                    if self.b2_client:
                        self.logger.info(f"Попытка скачать существующий лог из B2: {b2_log_path_key}")
                        try:
                            self.b2_client.download_file(self.b2_bucket_name, b2_log_path_key, str(local_temp_csv_path))
                            with open(local_temp_csv_path, 'r', encoding='utf-8') as temp_f:
                                existing_content = temp_f.read()
                            # Проверяем, есть ли уже НОВЫЙ заголовок
                            if existing_content.strip().startswith(
                                    "TimestampUTC,GenerationID,InputTopic"):  # Проверка по началу нового заголовка
                                needs_header = False
                            elif existing_content.strip().startswith(
                                    "TimestampUTC,GenerationID,CoreChoice"):  # Проверка на старый заголовок
                                self.logger.warning(
                                    "Обнаружен старый формат заголовка в CSV. Файл будет перезаписан с новым заголовком.")
                                existing_content = ""  # Перезаписываем, если заголовок старый
                                needs_header = True
                            else:  # Если заголовок вообще не найден или файл пуст
                                needs_header = True

                            self.logger.info(f"Существующий лог {b2_log_path_key} успешно скачан.")
                        except ClientError as e:
                            error_code = e.response.get('Error', {}).get('Code')
                            if error_code == 'NoSuchKey' or '404' in str(e):
                                self.logger.info(f"Файл лога {b2_log_path_key} не найден в B2. Будет создан новый.")
                                existing_content = ""
                                needs_header = True
                            else:
                                self.logger.error(
                                    f"Ошибка B2 при скачивании лога {b2_log_path_key}: {e}. Не удастся накопить данные.")
                                existing_content = None
                        except Exception as download_err:
                            self.logger.error(
                                f"Неизвестная ошибка при скачивании лога {b2_log_path_key}: {download_err}. Не удастся накопить данные.")
                            existing_content = None
                    else:
                        self.logger.error("B2 клиент недоступен, скачивание существующего лога невозможно.")
                        existing_content = None

                    if existing_content is not None:
                        try:
                            # Используем 'a' (append) если заголовок уже есть и он правильный, иначе 'w' (write)
                            write_mode = 'a' if not needs_header else 'w'
                            with open(local_temp_csv_path, write_mode, encoding='utf-8',
                                      newline='') as log_file:  # newline='' для корректной записи CSV
                                if needs_header:
                                    log_file.write(header)
                                # Если мы дописываем (append), а не перезаписываем, то старый контент уже в файле (если был скачан)
                                # или файл пуст и мы только что записали заголовок.
                                # Если existing_content был, но мы решили перезаписать (write_mode='w'), то он не пишется.
                                # Логика выше уже обработала existing_content для needs_header.
                                # Здесь просто дописываем новую строку.
                                log_file.write(new_log_line)
                            self.logger.info(
                                f"Обновленное содержимое лога записано в локальный файл: {local_temp_csv_path}")

                            if self.b2_client:
                                b2_log_folder = os.path.dirname(b2_log_path_key)
                                b2_log_filename = os.path.basename(b2_log_path_key)
                                self.logger.info(
                                    f"Попытка загрузки обновленного лога {local_temp_csv_path} в B2 как {b2_log_path_key}...")
                                if 'upload_to_b2' in globals() and callable(
                                        globals()['upload_to_b2']):  # Проверка наличия функции
                                    if upload_to_b2(self.b2_client, self.b2_bucket_name, b2_log_folder,
                                                    str(local_temp_csv_path), b2_log_filename):
                                        self.logger.info(
                                            f"✅ Накопительный лог креативных выборов успешно загружен в B2: {b2_log_path_key}")
                                    else:
                                        self.logger.error(
                                            f"❌ Не удалось загрузить накопительный лог в B2: {b2_log_path_key}")
                                else:
                                    self.logger.error("Функция upload_to_b2 не найдена, загрузка лога в B2 невозможна.")
                            else:
                                self.logger.error("B2 клиент недоступен, загрузка обновленного лога в B2 невозможна.")
                        except Exception as write_upload_err:
                            self.logger.error(f"Ошибка при записи или загрузке обновленного лога: {write_upload_err}")
                    else:
                        self.logger.error(
                            "Пропускаем запись и загрузку лога из-за ошибки скачивания предыдущей версии.")

                except Exception as logging_err:
                    self.logger.error(f"Ошибка при попытке логирования/загрузки креативных выборов: {logging_err}",
                                      exc_info=True)
                finally:
                    if local_temp_csv_path.exists():  # Используем Path.exists()
                        try:
                            os.remove(local_temp_csv_path)
                            self.logger.debug(f"Удален временный CSV файл: {local_temp_csv_path}")
                        except OSError as rm_err:
                            self.logger.warning(
                                f"Не удалось удалить временный CSV файл {local_temp_csv_path}: {rm_err}")
            else:
                self.logger.warning(
                    f"Словарь creative_brief не был создан для ID {generation_id}, логирование/загрузка креативных выборов пропущено.")
        # ... (конец блока finally) ...

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
    except ValueError as val_err: # Ловим ошибку валидации
        logger.error(f"!!! ОШИБКА ВАЛИДАЦИИ generate_content.py для ID {generation_id_main}: {val_err}")
        exit_code = 1
    except Exception as main_err:
        logger.error(f"!!! КРИТИЧЕСКАЯ ОШИБКА generate_content.py для ID {generation_id_main} !!!")
        logger.exception(main_err)
        exit_code = 1 # Устанавливаем код ошибки
    finally: logger.info(f"--- Завершение generate_content.py с кодом выхода: {exit_code} ---"); sys.exit(exit_code)
