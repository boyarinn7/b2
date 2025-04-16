import os
import base64
import time
import argparse
import logging
import json

# --- Используем библиотеку runwayml ---
try:
    from runwayml import RunwayML
    RUNWAY_SDK_AVAILABLE = True
    RunwayError = Exception
except ImportError as e:
    print(f"!!! ОШИБКА ИМПОРТА: {e} !!!")
    print("!!! Убедитесь, что библиотека runwayml и ее зависимости установлены: pip install runwayml !!!")
    Runway = None
    RunwayError = Exception
    RUNWAY_SDK_AVAILABLE = False
    exit(1)

# --- Настройка Логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("runway_test")

# --- Константы и Настройки по Умолчанию ---
DEFAULT_MODEL_NAME = "gen-2" # Оставляем gen-2 как дефолт, но пользователь передает gen3a_turbo
DEFAULT_DURATION = 5
# --- ИЗМЕНЕНО: Формат Ratio ---
DEFAULT_RATIO = "1280:768" # Используем формат, требуемый API
# --- КОНЕЦ ИЗМЕНЕНИЯ ---
DEFAULT_POLLING_TIMEOUT = 300
DEFAULT_POLLING_INTERVAL = 15

# --- Получение API ключа ---
RUNWAY_API_KEY = os.getenv("RUNWAY_API_KEY")

# --- Основная Функция Тестирования ---

def test_runway_generation(
    api_key: str,
    model: str,
    image_url: str,
    prompt_text: str,
    duration: int,
    ratio: str, # Теперь ожидаем "ШИРИНА:ВЫСОТА"
    poll_timeout: int,
    poll_interval: int
):
    """
    Тестирует генерацию видео через Runway API, используя URL изображения.
    """
    if not RUNWAY_SDK_AVAILABLE:
        logger.error("❌ SDK Runway недоступен (ошибка при импорте). Тест не может быть выполнен.")
        return
    if not api_key:
        logger.error("❌ RUNWAY_API_KEY не найден в переменных окружения.")
        return
    if not image_url or not image_url.startswith(('http://', 'https://')):
        logger.error(f"❌ Некорректный или отсутствующий URL изображения: {image_url}")
        return
    # --- ДОБАВЛЕНО: Проверка формата ratio ---
    if ratio not in ["1280:768", "768:1280"]:
         logger.warning(f"Нестандартное значение ratio '{ratio}'. API может его не принять. Ожидается '1280:768' или '768:1280'.")
    # --- КОНЕЦ ДОБАВЛЕНИЯ ---


    logger.info(f"Инициализация клиента Runway...")
    try:
        client = RunwayML(api_key=api_key)
        logger.info("✅ Клиент Runway инициализирован.")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации клиента Runway: {e}", exc_info=True)
        return

    try:
        generation_params = {
            "model": model,
            "prompt_image": image_url,
            "prompt_text": prompt_text,
            "duration": duration,
            "ratio": ratio # Передаем исправленный формат
        }
        logger.info(f"🚀 Создание задачи на генерацию видео с моделью '{model}'...")
        logger.debug(f"Параметры: {json.dumps(generation_params, indent=2)}")

        task = client.image_to_video.create(**generation_params)
        task_id = getattr(task, 'id', 'N/A')
        logger.info(f"✅ Задача успешно создана! ID задачи: {task_id}")

        logger.info(f"⏳ Начало опроса статуса задачи {task_id} (Таймаут: {poll_timeout} сек, Интервал: {poll_interval} сек)...")
        start_time = time.time()
        final_output_url = None
        while time.time() - start_time < poll_timeout:
            try:
                task_status = client.tasks.retrieve(task_id)
                current_status = getattr(task_status, 'status', 'UNKNOWN').upper()
                logger.info(f"Текущий статус задачи {task_id}: {current_status}")

                if current_status == "SUCCEEDED":
                    logger.info("✅ Задача успешно завершена!")
                    if hasattr(task_status, 'output') and isinstance(task_status.output, list) and len(task_status.output) > 0:
                        final_output_url = task_status.output[0]
                    else:
                         logger.warning("Статус SUCCEEDED, но результат (output) не найден или имеет неверный формат.")
                    break
                elif current_status == "FAILED":
                    logger.error("❌ Задача завершилась с ошибкой!")
                    error_details = getattr(task_status, 'error_message', 'Нет деталей')
                    logger.error(f"Детали ошибки: {error_details}")
                    break

                time.sleep(poll_interval)
            except Exception as poll_err:
                 logger.error(f"❌ Ошибка во время опроса (возможно, API Runway): {poll_err}", exc_info=True)
                 break
        else:
            logger.warning(f"⏰ Превышен таймаут ожидания ({poll_timeout} сек) результата от Runway для задачи {task_id}.")

        if final_output_url:
            logger.info("--- Финальный результат Runway ---")
            logger.info(f"URL Видео: {final_output_url}")
        else:
            logger.error("Не удалось получить финальный URL видео от Runway.")

    except Exception as e:
        logger.error(f"❌ Ошибка при создании или обработке задачи Runway: {e}", exc_info=True)


# === Точка входа ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Тестовый скрипт для Runway API (Image-to-Video).")
    parser.add_argument("--image_url",type=str, required=True, help="URL входного изображения.")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL_NAME, help=f"Название модели Runway (по умолчанию: {DEFAULT_MODEL_NAME}).")
    parser.add_argument("--prompt_text", type=str, default="Create a video based on this image", help="Текстовый промпт для Runway (на английском).")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION, help="Желаемая длительность видео (сек).")
    # --- ИЗМЕНЕНО: Обновлен help text и default ---
    parser.add_argument(
        "--ratio",
        type=str,
        default=DEFAULT_RATIO, # Теперь по умолчанию "1280:768"
        help="Соотношение сторон видео (формат ШИРИНА:ВЫСОТА, напр. 1280:768 или 768:1280)."
    )
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---
    parser.add_argument("--timeout", type=int, default=DEFAULT_POLLING_TIMEOUT, help="Общий таймаут опроса статуса (сек).")
    parser.add_argument("--interval", type=int, default=DEFAULT_POLLING_INTERVAL, help="Интервал между проверками статуса (сек).")

    args = parser.parse_args()

    if not RUNWAY_API_KEY:
        logger.critical("Переменная окружения RUNWAY_API_KEY не установлена! Завершение.")
        exit(1)

    logger.info(f"--- Запуск теста Runway для изображения: {args.image_url} ---")
    test_runway_generation(
        api_key=RUNWAY_API_KEY, model=args.model, image_url=args.image_url,
        prompt_text=args.prompt_text, duration=args.duration, ratio=args.ratio,
        poll_timeout=args.timeout, poll_interval=args.interval
    )
    logger.info("--- Тестирование Runway завершено ---")

