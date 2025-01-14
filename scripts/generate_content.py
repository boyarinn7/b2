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


from modules.config_manager import ConfigManager
from modules.logger import get_logger
from modules.error_handler import handle_error
from datetime import datetime
from modules.utils import ensure_directory_exists
from PIL import Image, ImageDraw


# Добавляем путь к директории modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))


# === Инициализация ===
logger = get_logger("generate_content")
config = ConfigManager()

# === Инициализация ===
config = ConfigManager()
logger = get_logger("generate_media_launcher")


def create_and_upload_image(folder, generation_id):
    """Создает имитацию изображения и загружает его в ту же папку в B2."""
    try:
        # Формирование имени файла с расширением .png
        file_name = generation_id.replace(".json", ".png")
        local_file_path = file_name

        # Создание имитации изображения
        img = Image.new('RGB', (800, 600), color=(73, 109, 137))
        draw = ImageDraw.Draw(img)
        draw.text((10, 10), f"ID: {generation_id}", fill=(255, 255, 255))
        img.save(local_file_path)
        logger.info(f"✅ Изображение '{local_file_path}' успешно создано.")

        # Загрузка изображения в ту же папку
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        s3_key = f"{folder.rstrip('/')}/{file_name}"
        s3.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"✅ Изображение успешно загружено в B2: {s3_key}")

        # Удаление локального файла
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
    config_gen_path = os.path.join("core", "config", "config_gen.json")
    os.makedirs(os.path.dirname(config_gen_path), exist_ok=True)
    try:
        with open(config_gen_path, "w", encoding="utf-8") as file:
            json.dump({"generation_id": file_id}, file, ensure_ascii=False, indent=4)
        logger.info(f"✅ ID генерации '{file_id}' успешно сохранён в config_gen.json")
    except Exception as e:
        handle_error("Save Generation ID Error", str(e))


def save_to_b2(folder, content):
    """Сохраняет контент в указанную папку B2 с уникальным именем файла."""
    try:
        file_id = generate_file_id()
        save_generation_id_to_config(file_id)
        logger.info(f"🔄 Сохранение контента в папку B2: {folder} с именем файла {file_id}")

        # Подготовка локального файла
        s3 = get_b2_client()
        bucket_name = config.get("API_KEYS.b2.bucket_name")
        local_file_path = file_id
        with open(local_file_path, "w", encoding="utf-8") as file:
            json.dump(content, file, ensure_ascii=False, indent=4)

        # Правильное формирование пути для загрузки
        s3_key = f"{folder.rstrip('/')}/{file_id}"
        s3.upload_file(local_file_path, bucket_name, s3_key)
        logger.info(f"✅ Контент успешно сохранён в B2: {s3_key}")

        # Удаление локального файла
        os.remove(local_file_path)
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

        self.openai_model = config.get('API_KEYS.openai.model', 'gpt-4')
        self.temperature = config.get('API_KEYS.openai.temperature', 0.7)

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
            folder = os.path.dirname(self.content_output_path)
            if not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"📁 Папка для сохранения данных создана: {folder}")
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
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=config.get('CONTENT.text.max_tokens', 750),
                temperature=config.get('API_KEYS.openai.temperature', 0.7)
            )
            return response['choices'][0]['message']['content'].strip()
        except Exception as e:
            handle_error("OpenAI API Error", e)

    def save_to_generated_content(self, stage, data):
        try:
            logger.info(f"🔄 Обновление данных и сохранение в файл: {self.content_output_path}")
            folder = os.path.dirname(self.content_output_path)
            if not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"📁 Папка для сохранения данных создана: {folder}")
            if os.path.exists(self.content_output_path):
                with open(self.content_output_path, 'r', encoding='utf-8') as file:
                    try:
                        result_data = json.load(file)
                    except json.JSONDecodeError:
                        result_data = {}
            else:
                result_data = {}
            result_data["timestamp"] = datetime.utcnow().isoformat()
            result_data[stage] = data
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump(result_data, file, ensure_ascii=False, indent=4)
            logger.info(f"✅ Данные успешно обновлены и сохранены на этапе: {stage}")
        except FileNotFoundError:
            handle_error("Save to Generated Content Error", f"Файл не найден: {self.content_output_path}")
        except PermissionError:
            handle_error("Save to Generated Content Error", f"Нет прав на запись в файл: {self.content_output_path}")
        except Exception as e:
            handle_error("Save to Generated Content Error", str(e))

    def run(self):
        try:
            # Основная логика генерации контента
            download_config_public()
            with open(config.get("FILE_PATHS.config_public"), "r", encoding="utf-8") as file:
                config_public = json.load(file)
                empty_folders = config_public.get("empty", [])

            if not empty_folders:
                logger.info("✅ Нет пустых папок. Процесс завершён.")
                return

            self.adapt_prompts()
            self.clear_generated_content()
            topic = self.generate_topic()
            self.save_to_generated_content("topic", {"topic": topic})
            text_initial = self.request_openai(config.get('CONTENT.text.prompt_template').format(topic=topic))
            self.save_to_generated_content("text_initial", {"content": text_initial})
            final_text = f"Сгенерированный текст на тему: {topic}\n{text_initial}"

            target_folder = empty_folders[0]
            save_to_b2(target_folder, {"topic": topic, "content": final_text})

            with open(os.path.join("core", "config", "config_gen.json"), "r", encoding="utf-8") as gen_file:
                config_gen_content = json.load(gen_file)
                generation_id = config_gen_content["generation_id"]

            create_and_upload_image(target_folder, generation_id)

            # Логирование содержимого конфигурационных файлов
            logger.info(f"📄 Содержимое config_public.json: {json.dumps(config_public, ensure_ascii=False, indent=4)}")
            logger.info(f"📄 Содержимое config_gen.json: {json.dumps(config_gen_content, ensure_ascii=False, indent=4)}")

            # Запускаем generate_media.py
            logger.info("🔄 Запуск скрипта generate_media.py...")
            run_generate_media()
            logger.info("✅ Скрипт generate_media.py успешно выполнен.")

        except Exception as e:
            handle_error("Run Error", str(e))


def run_generate_media():
    """Запускает скрипт generate_media.py по локальному пути."""
    try:
        # Получаем путь к папке скриптов из config.json
        scripts_folder = config.get("FILE_PATHS.scripts_folder", "core/scripts")
        script_path = os.path.join(scripts_folder, "generate_media.py")

        # Проверяем, что файл существует
        if not os.path.isfile(script_path):
            raise FileNotFoundError(f"Скрипт generate_media.py не найден по пути: {script_path}")

        logger.info(f"🔄 Запуск скрипта: {script_path}")

        # Запуск скрипта
        subprocess.run(["python", script_path], check=True)
        logger.info(f"✅ Скрипт {script_path} выполнен успешно.")
    except subprocess.CalledProcessError as e:
        handle_error("Script Execution Error", f"Ошибка при выполнении скрипта {script_path}: {e}")
    except FileNotFoundError as e:
        handle_error("File Not Found Error", str(e))
    except Exception as e:
        handle_error("Unknown Error", f"Ошибка при запуске скрипта {script_path}: {e}")

        if __name__ == "__main__":
            run_generate_media()


if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()