import os
import requests
import json
import logging
import sys
import time # Добавлен для возможного использования

# --- Путь к корневой папке проекта ---
# Это важно, чтобы правильно находить модули
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# --- Импорты из ваших модулей ---
try:
    from modules.config_manager import ConfigManager
    from modules.api_clients import get_b2_client
    # Используем utils для сохранения/загрузки JSON в/из B2
    from modules.utils import load_b2_json, save_b2_json, ensure_directory_exists
    from modules.logger import get_logger # Используем ваш стандартный логгер
except ModuleNotFoundError as import_err:
    print(f"[Workspace_media] Ошибка импорта модулей проекта: {import_err}")
    print(f"[Workspace_media] Убедитесь, что PYTHONPATH настроен правильно или скрипт запускается из папки scripts.")
    sys.exit(1)
except ImportError as import_err:
     print(f"[Workspace_media] Критическая Ошибка: Не найдена функция в модулях: {import_err}", file=sys.stderr)
     sys.exit(1)


# --- Настройка Логирования ---
# Используем ваш стандартный логгер
logger = get_logger("Workspace_media") # Используем имя файла для логгера

# --- Инициализация Конфигурации ---
try:
    config = ConfigManager()
    logger.info("ConfigManager инициализирован.")
except Exception as init_err:
    logger.error(f"Критическая ошибка инициализации ConfigManager: {init_err}", exc_info=True)
    sys.exit(1)

# --- Константы ---
try:
    # Пути к конфигам в B2
    CONFIG_MJ_REMOTE_PATH = config.get("FILE_PATHS.config_midjourney", "config/config_midjourney.json")
    # Локальный путь для временного хранения конфига
    CONFIG_MJ_LOCAL_PATH = "config_midjourney_workspace_temp.json"

    # Параметры API из конфига
    MJ_FETCH_ENDPOINT = config.get("API_KEYS.midjourney.task_endpoint", "https://api.piapi.ai/mj/v2/fetch") # Эндпоинт для проверки
    MJ_API_KEY = os.getenv("MIDJOURNEY_API_KEY")
    B2_BUCKET_NAME = config.get('API_KEYS.b2.bucket_name', os.getenv('B2_BUCKET_NAME'))

    # Таймаут из тестового скрипта
    FETCH_REQUEST_TIMEOUT = 120 # Секунд

    if not MJ_API_KEY:
        logger.critical("Переменная окружения MIDJOURNEY_API_KEY не установлена!")
        # Не выходим из скрипта сразу, дадим шанс main() обработать это
    if not B2_BUCKET_NAME:
         logger.critical("Имя B2 бакета не определено!")
         # Не выходим из скрипта сразу

except Exception as cfg_err:
    logger.error(f"Критическая ошибка при чтении настроек: {cfg_err}", exc_info=True)
    sys.exit(1)


# --- Функция для получения статуса (аналог fetch_mj_status из теста) ---
def fetch_piapi_status(task_id: str, api_key: str, endpoint: str) -> dict | None:
    """
    Запрашивает статус задачи Midjourney по task_id через PiAPI.
    Возвращает полный JSON ответа API при успехе, иначе None.
    """
    if not api_key:
        logger.error("❌ MIDJOURNEY_API_KEY не предоставлен функции.")
        return None
    if not task_id:
        logger.error("❌ Не передан task_id для проверки статуса.")
        return None

    headers = {"X-API-Key": api_key, "Content-Type": "application/json"} # Добавлен Content-Type на всякий случай
    payload = {"task_id": task_id}

    logger.info(f"🔍 Проверка статуса задачи {task_id} на {endpoint}... (Таймаут: {FETCH_REQUEST_TIMEOUT} сек)")

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=FETCH_REQUEST_TIMEOUT)
        logger.debug(f"Ответ от API (статус {response.status_code}): {response.text[:500]}")
        response.raise_for_status() # Проверка на HTTP ошибки (4xx, 5xx)

        result = response.json()
        logger.info(f"✅ Статус получен для задачи {task_id}.")
        # Логируем только основное, полный ответ сохраним в конфиг если нужно
        status = result.get("status")
        progress = result.get("progress")
        logger.info(f"Текущий статус: {status}, Прогресс: {progress}")

        return result # Возвращаем весь ответ

    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут ({FETCH_REQUEST_TIMEOUT} сек) при запросе статуса: {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса при запросе статуса: {e}")
        if e.response is not None:
            logger.error(f"    Статус ответа: {e.response.status_code}")
            logger.error(f"    Тело ответа: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка декодирования JSON ответа статуса: {e}. Ответ: {response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"❌ Неизвестная ошибка при проверке статуса: {e}", exc_info=True)
        return None

# --- Основная функция ---
def main():
    logger.info("🔄 Начало проверки статуса задачи MidJourney...")

    if not MJ_API_KEY:
        logger.error("Завершение работы: MIDJOURNEY_API_KEY не установлен.")
        return
    if not B2_BUCKET_NAME:
        logger.error("Завершение работы: B2_BUCKET_NAME не определен.")
        return

    b2_client = None
    config_midjourney = None
    config_changed = False

    try:
        # Получаем клиент B2
        b2_client = get_b2_client()
        if not b2_client:
            raise ConnectionError("Не удалось инициализировать B2 клиент.")

        # Загружаем текущий конфиг MJ из B2
        logger.info(f"Загрузка {CONFIG_MJ_REMOTE_PATH} из B2...")
        # Убедимся, что папка для временного файла существует
        ensure_directory_exists(os.path.dirname(CONFIG_MJ_LOCAL_PATH))
        config_midjourney = load_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, default_value=None)

        if config_midjourney is None:
            logger.error(f"Не удалось загрузить {CONFIG_MJ_REMOTE_PATH}. Проверка невозможна.")
            return

        # Проверяем наличие активной задачи
        task_info = config_midjourney.get("midjourney_task")
        task_id = None

        if isinstance(task_info, dict):
            task_id = task_info.get("task_id")
        elif isinstance(task_info, str): # Поддержка старого формата на всякий случай
            task_id = task_info
            logger.warning("Обнаружен старый формат midjourney_task (строка).")
        # else: task_info is None or invalid format

        if not task_id:
            logger.info("ℹ️ Нет активных задач Midjourney для проверки в config_midjourney.json.")
            return

        logger.info(f"Найдена активная задача: {task_id}")

        # --- Выполняем проверку статуса ---
        status_result = fetch_piapi_status(task_id, MJ_API_KEY, MJ_FETCH_ENDPOINT)

        if status_result:
            current_status = status_result.get("status")
            # Определяем финальные статусы (могут отличаться у PiAPI)
            final_success_statuses = ["completed", "finished"] # Добавили finished
            final_error_statuses = ["failed"] # Добавить другие статусы ошибок, если есть

            if current_status in final_success_statuses or current_status in final_error_statuses:
                logger.info(f"Задача {task_id} достигла финального статуса: {current_status}. Обновление конфига...")
                # Записываем ВЕСЬ результат в midjourney_results
                config_midjourney["midjourney_results"] = status_result
                # Очищаем задачу
                config_midjourney["midjourney_task"] = None
                # Сбрасываем статус и флаг генерации на всякий случай
                config_midjourney["status"] = None
                config_midjourney["generation"] = False
                config_changed = True # Помечаем, что конфиг нужно сохранить

                if current_status in final_error_statuses:
                     logger.error(f"Задача {task_id} завершилась с ошибкой (статус: {current_status}). Результат сохранен.")
                else:
                     logger.info(f"Задача {task_id} успешно завершена. Результат сохранен.")

            else:
                # Статус промежуточный (pending, processing, running и т.д.)
                logger.info(f"Задача {task_id} все еще в процессе (статус: {current_status}). Конфиг не изменен.")
                # Ничего не делаем, конфиг не меняем

        else:
            # Ошибка при получении статуса
            logger.error(f"Не удалось получить статус для задачи {task_id}. Конфиг не изменен.")
            # Ничего не делаем с конфигом

        # --- Сохраняем конфиг в B2, ТОЛЬКО если были изменения ---
        if config_changed:
            logger.info(f"Сохранение обновленного config_midjourney.json в B2...")
            if not save_b2_json(b2_client, B2_BUCKET_NAME, CONFIG_MJ_REMOTE_PATH, CONFIG_MJ_LOCAL_PATH, config_midjourney):
                logger.error("!!! Не удалось сохранить обновленный config_midjourney.json в B2!")
            else:
                logger.info("✅ Конфиг Midjourney успешно обновлен в B2.")
        else:
            logger.info("Изменений в статусе задачи, требующих сохранения конфига, не произошло.")


    except ConnectionError as conn_err:
         logger.error(f"❌ Ошибка соединения B2: {conn_err}")
    except Exception as e:
        logger.error(f"❌ Непредвиденная ошибка в Workspace_media: {e}", exc_info=True)
    finally:
        # Очистка временного локального файла конфига
        if os.path.exists(CONFIG_MJ_LOCAL_PATH):
            try:
                os.remove(CONFIG_MJ_LOCAL_PATH)
                logger.debug(f"Удален временный файл: {CONFIG_MJ_LOCAL_PATH}")
            except OSError as e:
                logger.warning(f"Не удалось удалить временный файл {CONFIG_MJ_LOCAL_PATH}: {e}")
        logger.info("✅ Проверка статуса задачи MidJourney завершена.")


# === Точка входа ===
if __name__ == "__main__":
    main()

