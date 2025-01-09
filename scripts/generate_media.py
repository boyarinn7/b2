# core/scripts/generate_media.py

import os
import time
import random
from modules.api_clients import get_runwayml_client
from modules.logger import get_logger
from modules.error_handler import handle_error
from modules.utils import ensure_directory_exists, encode_image_to_base64
from modules.config_manager import ConfigManager

# === Инициализация ===
config = ConfigManager()
logger = get_logger("generate_media")

# === Константы из конфигурации ===
DEFAULT_IMAGE_PATH = config.get('FILE_PATHS.default_image_path', 'core/media/input_image.jpg')
DEFAULT_VIDEO_PATH = config.get('FILE_PATHS.default_video_path', 'core/media/output_video.mp4')
RUNWAY_MODEL = config.get('API_KEYS.RUNWAYML.model', 'gen3a_turbo')
RUNWAY_DURATION = config.get('API_KEYS.RUNWAYML.duration', 5)
RUNWAY_RATIO = config.get('API_KEYS.RUNWAYML.ratio', '1280:768')
RUNWAY_SCENARIO = config.get('API_KEYS.RUNWAYML.default_scenario', "Атмосферная сцена с тёмной комнатой и старой картой.")
USE_MOCK_API = config.get('OTHER.use_mock_api', True)

# === Проверка API-ключа ===
RUNWAY_API_KEY = config.get('API_KEYS.RUNWAYML.api_key')
if not RUNWAY_API_KEY:
    logger.error("❌ API-ключ для RunwayML отсутствует в конфигурации.")
    raise ValueError("API-ключ для RunwayML отсутствует. Проверьте конфигурацию.")


# === Генерация видео через RunwayML ===
def generate_video_with_image_and_prompt(prompt, image_path):
    """
    Генерация видео с использованием RunwayML API
    """
    try:
        if USE_MOCK_API:
            logger.warning("⚠️ Используется заглушка для генерации видео (Runway API Mock).")
            mock_video_url = f"https://mock.runwayml.com/video_{random.randint(1000, 9999)}.mp4"
            time.sleep(2)  # Имитация задержки запроса
            logger.info(f"✅ Видео успешно сгенерировано (заглушка). Ссылка: {mock_video_url}")
            return mock_video_url

        base64_image = encode_image_to_base64(image_path)
        if not base64_image:
            handle_error("Image Encoding Error", f"Не удалось преобразовать изображение: {image_path}")

        logger.info("🔄 Создаю задачу на генерацию видео через Runway...")
        client = get_runwayml_client(api_key=RUNWAY_API_KEY)
        task = client.image_to_video.create(
            model=RUNWAY_MODEL,
            prompt_image=f"data:image/jpeg;base64,{base64_image}",
            prompt_text=prompt,
            duration=RUNWAY_DURATION,
            ratio=RUNWAY_RATIO
        )
        logger.info(f"✅ Задача успешно создана! ID задачи: {task.id}")

        # Ожидание завершения задачи
        while True:
            task_status = client.tasks.retrieve(task.id)
            logger.info(f"🔍 Текущий статус задачи: {task_status.status}")
            if task_status.status in ["SUCCEEDED", "FAILED"]:
                break
            time.sleep(5)  # Ждём 5 секунд перед проверкой статуса

        if task_status.status == "SUCCEEDED":
            video_url = task_status.output[0]
            logger.info(f"🏁 Видео успешно сгенерировано! Ссылка: {video_url}")
            save_video(video_url)
            return video_url
        else:
            handle_error("Runway Video Generation Error", "Задача завершилась с ошибкой.")

    except Exception as e:
        handle_error("RunwayML Video Generation Error", str(e))


def save_video(video_url):
    """
    Сохранение видео по указанному URL
    """
    try:
        import requests
        response = requests.get(video_url, timeout=30)
        response.raise_for_status()
        ensure_directory_exists(os.path.dirname(DEFAULT_VIDEO_PATH))
        with open(DEFAULT_VIDEO_PATH, 'wb') as file:
            file.write(response.content)
        logger.info(f"✅ Видео сохранено в {DEFAULT_VIDEO_PATH}")
    except Exception as e:
        handle_error("Video Save Error", str(e))


def main():
    """
    Основной процесс генерации медиа-контента
    """
    logger.info("🔄 Начинаем генерацию медиа-контента...")
    video_url = generate_video_with_image_and_prompt(RUNWAY_SCENARIO, DEFAULT_IMAGE_PATH)

    if video_url:
        logger.info(f"🏁 Медиа-контент успешно сгенерирован. Ссылка: {video_url}")
    else:
        handle_error("Runway Media Generation Error", "Генерация медиа-контента не удалась.")


# === Точка входа ===
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Программа остановлена пользователем.")
