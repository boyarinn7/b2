# -*- coding: utf-8 -*-
import os
import sys
import requests
import json
import time
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
import uuid # Для уникальных имен файлов

# --- Попытка импорта Boto3 ---
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    ClientError = Exception # Fallback
    NoCredentialsError = Exception # Fallback
    logging.warning("Библиотека boto3 не найдена. Загрузка в B2 будет недоступна.")

# --- Настройка Логирования ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("mj_text_test_b2")

# --- Константы ---
MJ_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
MJ_IMAGINE_ENDPOINT = os.getenv("MJ_IMAGINE_ENDPOINT", "https://api.piapi.ai/api/v1/task")
MJ_FETCH_ENDPOINT = os.getenv("MJ_FETCH_ENDPOINT", "https://api.piapi.ai/mj/v2/fetch")

# --- B2 Константы (из переменных окружения) ---
B2_ENDPOINT_URL = os.getenv("B2_ENDPOINT")
B2_ACCESS_KEY = os.getenv("B2_ACCESS_KEY")
B2_SECRET_KEY = os.getenv("B2_SECRET_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")
# Папка в B2 для временных изображений (можно изменить)
B2_TEMP_FOLDER = "temp_mj_input_images/"

# --- Вспомогательные функции ---

def get_b2_client():
    """Инициализирует и возвращает клиент boto3 для B2."""
    if not BOTO3_AVAILABLE:
        logger.error("Boto3 недоступен.")
        return None
    if not all([B2_ENDPOINT_URL, B2_ACCESS_KEY, B2_SECRET_KEY]):
        logger.error("Не все переменные окружения B2 установлены (B2_ENDPOINT, B2_ACCESS_KEY, B2_SECRET_KEY).")
        return None
    try:
        client = boto3.client(
            's3',
            endpoint_url=B2_ENDPOINT_URL,
            aws_access_key_id=B2_ACCESS_KEY,
            aws_secret_access_key=B2_SECRET_KEY
        )
        logger.info("Клиент B2 (boto3) успешно создан.")
        return client
    except Exception as e:
        logger.error(f"Ошибка инициализации клиента B2: {e}", exc_info=True)
        return None

def upload_to_b2_public(s3_client, bucket_name, local_path: Path, b2_folder: str) -> str | None:
    """Загружает файл в B2 и возвращает публичный URL."""
    if not s3_client:
        logger.error("Клиент B2 не инициализирован для загрузки.")
        return None
    if not local_path.is_file():
        logger.error(f"Локальный файл не найден: {local_path}")
        return None
    if not bucket_name:
        logger.error("Имя бакета B2 не указано.")
        return None

    # Генерируем уникальное имя файла для B2
    unique_filename = f"{uuid.uuid4()}{local_path.suffix}"
    b2_key = f"{b2_folder.strip('/')}/{unique_filename}"

    logger.info(f"Загрузка {local_path.name} в B2 как {b2_key}...")
    try:
        # Загружаем файл, делая его публично читаемым (ACL='public-read')
        with open(local_path, "rb") as f:
            s3_client.upload_fileobj(
                f,
                bucket_name,
                b2_key,
                ExtraArgs={'ACL': 'public-read'} # Делаем файл публичным
            )

        # Формируем публичный URL (формат может зависеть от настроек бакета и эндпоинта)
        # Стандартный формат для B2: https://<bucket_name>.<endpoint_domain>/<file_key>
        # Или через fXX домен: https://f<XXX>.backblazeb2.com/file/<bucket_name>/<file_key>
        # Используем второй вариант как более надежный
        endpoint_domain = B2_ENDPOINT_URL.replace("https://", "")
        # Пытаемся извлечь fXXX часть, если она есть
        f_part = endpoint_domain.split('.')[0] # e.g., "s3.us-east-005" -> "s3" or "f005" -> "f005"
        if not f_part.startswith('f'): # Если эндпоинт вида s3.region... , URL будет другим
             # В этом случае лучше использовать кастомный домен или стандартный URL бакета, если он настроен
             # Для простоты теста, пока оставим URL через fXXX, но он может не сработать для s3.* эндпоинтов
             logger.warning(f"Формат эндпоинта ({endpoint_domain}) может не подходить для стандартного URL через fXXX. URL может быть неверным.")
             # Пытаемся угадать fXXX из региона (не надежно)
             region_part = endpoint_domain.split('.')[1] if '.' in endpoint_domain else '005' # Пример
             f_part = f"f{region_part[-3:]}"


        public_url = f"https://{f_part}.backblazeb2.com/file/{bucket_name}/{b2_key}"

        # Проверка доступности URL (опционально, но полезно)
        try:
            response = requests.head(public_url, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ Файл успешно загружен и доступен по URL: {public_url}")
                return public_url
            else:
                logger.error(f"Файл загружен, но URL {public_url} недоступен (статус: {response.status_code}). Проверьте права доступа к бакету или формат URL.")
                return None
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Ошибка проверки доступности URL {public_url}: {req_err}")
            logger.warning("Продолжаем работу, но URL может быть неверным.")
            return public_url # Возвращаем URL, даже если проверка не удалась

    except (ClientError, NoCredentialsError) as e:
        logger.error(f"Ошибка Boto3/Credentials при загрузке в B2: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при загрузке в B2: {e}", exc_info=True)
        return None

def initiate_mj_imagine_with_url(image_url: str, text_prompt: str, api_key: str, endpoint: str) -> dict | None:
    """Инициирует /imagine с URL изображения и текстовым промптом."""
    if not api_key: logger.error("Нет MIDJOURNEY_API_KEY."); return None
    if not endpoint: logger.error("Нет эндпоинта для /imagine."); return None
    if not image_url: logger.error("Нет URL изображения."); return None
    if not text_prompt: logger.error("Текстовый промпт пуст."); return None

    # Собираем полный промпт: URL картинки + текстовая часть
    full_prompt = f"{image_url} {text_prompt}"
    logger.info("Полный промпт для MJ (с URL):")
    logger.info(full_prompt) # Логируем весь промпт, т.к. URL не секретный

    payload = { "model": "midjourney", "task_type": "imagine", "input": { "prompt": full_prompt } }
    headers = {'X-API-Key': api_key, 'Content-Type': 'application/json'}
    request_time = datetime.now(timezone.utc)

    logger.info(f"Отправка запроса /imagine на {endpoint}...")
    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        logger.debug(f"Ответ PiAPI Imagine: Status={response.status_code}, Body={response.text[:500]}")
        response.raise_for_status()
        result = response.json()
        task_id = result.get("result", {}).get("task_id") or result.get("data", {}).get("task_id") or result.get("task_id")
        if task_id:
            logger.info(f"✅ Получен task_id MJ /imagine: {task_id} (запрошено в {request_time.isoformat()})")
            return {"task_id": str(task_id), "requested_at_utc": request_time.isoformat()}
        else:
            logger.error(f"❌ Ответ MJ API не содержит task_id: {result}")
            return None
    # ... (обработка ошибок как в предыдущей версии initiate_mj_imagine_with_image) ...
    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут MJ API ({60} сек) при запросе /imagine: {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса MJ API (/imagine): {e}")
        if e.response is not None:
            logger.error(f"    Status: {e.response.status_code}, Body: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка JSON MJ API (/imagine): {e}. Ответ: {response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка MJ (/imagine): {e}", exc_info=True)
        return None


def fetch_piapi_status(task_id: str, api_key: str, endpoint: str) -> dict | None:
    """Запрашивает статус задачи Midjourney по task_id через PiAPI."""
    # (Код этой функции остается без изменений)
    if not api_key: logger.error("Нет MIDJOURNEY_API_KEY для проверки статуса."); return None
    if not endpoint: logger.error("Нет эндпоинта для проверки статуса."); return None
    if not task_id: logger.error("Нет task_id для проверки статуса."); return None

    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
    payload = {"task_id": task_id}
    logger.debug(f"Проверка статуса {task_id} на {endpoint}...")

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        result = response.json()
        status = result.get("status", "UNKNOWN")
        progress = result.get("progress", "N/A")
        logger.info(f"Статус задачи {task_id}: {status} (Прогресс: {progress})")
        return result
    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут ({120} сек) при запросе статуса {task_id}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса при запросе статуса {task_id}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка декодирования JSON ответа статуса {task_id}: {e}. Ответ: {response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при проверке статуса {task_id}: {e}", exc_info=True)
        return None

# --- Основная логика ---
def main():
    parser = argparse.ArgumentParser(description="Тест генерации текста на изображении через Midjourney (PiAPI) с загрузкой в B2.")
    parser.add_argument("image_path", help="Путь к локальному PNG изображению.")
    parser.add_argument("-t", "--text", required=True, help="Текст для надписи (в кавычках).")
    # ... (остальные аргументы парсера без изменений) ...
    parser.add_argument("-s", "--style", default="Атмосфера Тайны, недосказанность", help="Описание стиля.")
    parser.add_argument("-v", "--version", default="7", help="Версия Midjourney (например, 6 или 7).")
    parser.add_argument("-ar", "--aspect_ratio", default="16:9", help="Соотношение сторон (например, 16:9, 1:1).")
    parser.add_argument("-q", "--quality", default="1", help="Качество (например, 0.5, 1, 2).")
    parser.add_argument("--timeout", type=int, default=600, help="Таймаут ожидания результата MJ в секундах.")
    parser.add_argument("--interval", type=int, default=20, help="Интервал проверки статуса MJ в секундах.")

    args = parser.parse_args()

    # --- Проверки ключей и эндпоинтов ---
    if not MJ_API_KEY: logger.critical("Переменная окружения MIDJOURNEY_API_KEY не установлена! Выход."); sys.exit(1)
    if not MJ_IMAGINE_ENDPOINT or not MJ_FETCH_ENDPOINT: logger.critical("Не установлены эндпоинты MJ_IMAGINE_ENDPOINT или MJ_FETCH_ENDPOINT! Выход."); sys.exit(1)
    if not BOTO3_AVAILABLE: logger.critical("Boto3 недоступен. Невозможно загрузить изображение в B2. Выход."); sys.exit(1)
    if not all([B2_ENDPOINT_URL, B2_ACCESS_KEY, B2_SECRET_KEY, B2_BUCKET_NAME]): logger.critical("Не установлены все переменные окружения для B2! Выход."); sys.exit(1)

    local_image_path = Path(args.image_path)
    # ... (получение остальных аргументов без изменений) ...
    text_to_add = args.text
    style_description = args.style
    mj_version = args.version
    aspect_ratio = args.aspect_ratio
    quality = args.quality
    poll_timeout = args.timeout
    poll_interval = args.interval

    logger.info(f"Используется изображение: {local_image_path}")
    logger.info(f"Текст для надписи: \"{text_to_add}\"")
    logger.info(f"Стиль: {style_description}")
    logger.info(f"Параметры MJ: --v {mj_version} --ar {aspect_ratio}")

    # 1. Инициализация клиента B2
    s3_client = get_b2_client()
    if not s3_client:
        sys.exit(1)

    # 2. Загрузка изображения в B2
    public_image_url = upload_to_b2_public(s3_client, B2_BUCKET_NAME, local_image_path, B2_TEMP_FOLDER)
    if not public_image_url:
        logger.error("Не удалось загрузить изображение в B2 или получить URL.")
        sys.exit(1)

    # 3. Формируем текстовую часть промпта
    text_en = f'"{text_to_add}"'
    style_en = "mysterious atmosphere, ambiguity, typography"
    context_en = "text overlay on the image, slightly blurred background if possible"
    text_prompt_part = f'typography of the russian text {text_en}, {style_en}, {context_en} --v {mj_version} --ar {aspect_ratio}'

    # 4. Запускаем /imagine с URL
    task_info = initiate_mj_imagine_with_url(public_image_url, text_prompt_part, MJ_API_KEY, MJ_IMAGINE_ENDPOINT)

    if not task_info or not task_info.get("task_id"):
        logger.error("Не удалось запустить задачу Midjourney.")
        # TODO: По-хорошему, здесь надо бы удалить временный файл из B2
        sys.exit(1)

    task_id = task_info["task_id"]
    logger.info(f"Задача запущена: {task_id}. Ожидание результата...")

    # 5. Ожидание результата (без изменений)
    start_time = time.time()
    final_result = None
    while time.time() - start_time < poll_timeout:
        status_result = fetch_piapi_status(task_id, MJ_API_KEY, MJ_FETCH_ENDPOINT)
        if status_result:
            status = status_result.get("status")
            if status in ["finished", "completed", "success"]:
                logger.info(f"✅ Задача {task_id} успешно завершена!")
                final_result = status_result
                break
            elif status in ["failed", "error"]:
                logger.error(f"❌ Задача {task_id} завершилась с ошибкой!")
                logger.error(f"Детали: {json.dumps(status_result, indent=2, ensure_ascii=False)}")
                final_result = status_result
                break
        else:
            logger.warning(f"Не удалось получить статус {task_id}. Повторная попытка через {poll_interval} сек.")
        time.sleep(poll_interval)
    else:
        logger.warning(f"⏰ Таймаут ({poll_timeout} сек) ожидания результата для задачи {task_id}.")

    # 6. Вывод результата (без изменений)
    if final_result:
        logger.info("--- Финальный результат ---")
        print(json.dumps(final_result, indent=4, ensure_ascii=False))
        image_url = None
        task_result_data = final_result.get("task_result", final_result.get("data", final_result))
        if isinstance(task_result_data, dict):
             possible_keys = ["image_url", "imageUrl", "discord_image_url", "url", "temporary_image_urls", "image_urls"]
             for key in possible_keys:
                 value = task_result_data.get(key)
                 if isinstance(value, str) and value.startswith("http"): image_url = value; break
                 elif isinstance(value, list) and value and isinstance(value[0], str) and value[0].startswith("http"): image_url = value[0]; logger.info(f"Взят первый URL из списка '{key}'."); break
        if image_url: logger.info(f"➡️ URL Результата (или первого из сетки): {image_url}")
        else: logger.warning("Не удалось извлечь URL изображения из финального результата.")
    else:
        logger.error("Финальный результат не был получен.")

    # 7. TODO: Опционально - удаление временного файла из B2
    # logger.info(f"Попытка удаления временного файла из B2: {b2_key}") # Нужно получить b2_key из upload_to_b2_public

if __name__ == "__main__":
    main()
