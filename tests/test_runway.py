# -*- coding: utf-8 -*-
# Импорты и Настройка Логгера (Imports and Logger Setup)
import openai
import os
import json
import sys
import argparse
from pathlib import Path
import random
import logging
from datetime import datetime, timezone
import time
import requests
import shutil
import re # <--- Импорт 're'

# --- Новые импорты для Runway ---
try:
    from runwayml import RunwayML # Попытка импорта SDK Runway
    RUNWAY_SDK_AVAILABLE = True
except ImportError:
    RUNWAY_SDK_AVAILABLE = False
    RunwayML = None # Определяем как None, если не найдено
    logging.warning("SDK RunwayML не найден. Функционал Runway будет недоступен.")

# --- Импорт необходимых функций из других скриптов проекта ---
try:
    # Добавляем путь к родительской папке (b2), чтобы найти папку modules
    # Определяем BASE_DIR относительно текущего файла (__file__)
    CURRENT_SCRIPT_PATH = Path(__file__).resolve()
    # Предполагаем, что скрипт находится в папке scripts, а modules - на уровень выше
    BASE_DIR = CURRENT_SCRIPT_PATH.parent.parent
    if str(BASE_DIR) not in sys.path:
        sys.path.append(str(BASE_DIR))
        logging.info(f"Добавлен путь в sys.path: {BASE_DIR}")

    # Импортируем функции из generate_media.py
    from scripts.generate_media import (
        initiate_midjourney_task,
        generate_runway_video,
        select_best_image,
    )
    # Импортируем функцию проверки статуса из Workspace_media.py
    # Убедитесь, что скрипт Workspace_media.py доступен по пути scripts/Workspace_media.py
    from scripts.Workspace_media import fetch_piapi_status

    # Импортируем утилиты
    from modules.utils import ensure_directory_exists, download_image, download_video
    from modules.config_manager import ConfigManager

except ImportError as e:
     # Логируем ошибку импорта с указанием пути
     logging.exception(f"Критическая ошибка импорта модулей/функций проекта: {e}. Проверьте структуру папок и наличие файлов.")
     logging.error(f"Текущая рабочая директория: {os.getcwd()}")
     logging.error(f"sys.path: {sys.path}")
     # Попытка определить ожидаемый BASE_DIR даже при ошибке импорта
     try:
         CURRENT_SCRIPT_PATH_FALLBACK = Path(__file__).resolve()
         BASE_DIR_FALLBACK = CURRENT_SCRIPT_PATH_FALLBACK.parent.parent
         logging.error(f"Ожидаемый BASE_DIR: {BASE_DIR_FALLBACK}")
     except NameError: # Если __file__ не определен
         logging.error("Не удалось определить путь к текущему скрипту (__file__).")
     sys.exit(1)
except FileNotFoundError as e:
     logging.exception(f"Критическая ошибка: Не найден файл скрипта для импорта: {e}. Убедитесь, что структура папок верна.")
     sys.exit(1)


# --- Настройка Логгирования ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger("iid_local_tester")

# --- Тексты Промптов ИИД ---
# ... (PROMPT_STEP1 - PROMPT_STEP5 остаются без изменений) ...
PROMPT_STEP1 = """
# ЗАДАЧА: Определение Эмоционального Ядра Видео
Ты — креативный стратег ИИД (Искусственный Интеллект Драматург).
Проанализируй входной текст и определи его основное ЭМОЦИОНАЛЬНОЕ ЯДРО.

ВХОДНОЙ ТЕКСТ:
{input_text}

ВАРИАНТЫ ЭМОЦИОНАЛЬНОГО ЯДРА (выбери ОДНО):
Типы настроений (moods) с весами (чем выше вес, тем более предпочтительно при прочих равных):
{moods_list_str}

ИЛИ

Типы эмоциональных дуг (arcs):
{arcs_list_str}

ТВОЙ ВЫБОР:
Определи, что лучше подходит — конкретное настроение или эмоциональная дуга.
Обоснуй свой выбор кратко (1-2 предложения), почему именно это ядро лучше всего отражает суть текста.

ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект со следующими ключами:
- "chosen_type": строка, либо "mood", либо "arc".
- "chosen_value": строка, выбранное настроение или дуга.
- "justification": строка, краткое обоснование твоего выбора.

ПРИМЕР JSON ОТВЕТА:
{{"chosen_type": "mood", "chosen_value": "Загадочное", "justification": "Текст содержит элементы тайны и нераскрытых событий."}}
ИЛИ
{{"chosen_type": "arc", "chosen_value": "Надежда -> Разочарование/Крушение", "justification": "Текст начинается с оптимизма, но заканчивается трагически."}}

JSON:
"""
PROMPT_STEP2 = """
# ЗАДАЧА: Выбор Основного Креативного Драйвера Видео
Ты — креативный стратег ИИД.
На основе входного текста и выбранного Эмоционального Ядра, определи ОДИН основной КРЕАТИВНЫЙ ДРАЙВЕР для будущего видеоролика (10 секунд).

ВХОДНОЙ ТЕКСТ:
{input_text}

ВЫБРАННОЕ ЭМОЦИОНАЛЬНОЕ ЯДРО:
{chosen_emotional_core_json}

ВАРИАНТЫ КРЕАТИВНЫХ ДРАЙВЕРОВ (выбери ОДИН):

1.  **Креативные Промпты (Creative Prompts)** - фокус на необычной идее или подходе (веса показывают предпочтительность):
    {prompts_list_str}

2.  **Экстремальная Перспектива (Perspective Types)** - фокус на необычной точке зрения камеры:
    {perspectives_list_str}

3.  **Визуальная Метафора (Visual Metaphor Types)** - фокус на символическом образе:
    {metaphors_list_str}

ТВОЙ ВЫБОР:
Выбери ОДИН драйвер (либо из 'Creative Prompts', либо 'Perspective Types', либо 'Visual Metaphor Types'), который наилучшим образом поможет раскрыть Эмоциональное Ядро через визуальный ряд в 10-секундном видео.
Кратко обоснуй (1-2 предложения), почему выбранный драйвер подходит лучше всего.

ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект со следующими ключами:
- "chosen_driver_type": строка, тип выбранного драйвера ("creative_prompts", "perspective_types" или "visual_metaphor_types").
- "chosen_driver_value": строка, название выбранного драйвера.
- "justification": строка, краткое обоснование выбора.

ПРИМЕР JSON ОТВЕТА:
{{"chosen_driver_type": "creative_prompts", "chosen_driver_value": "Атмосфера Тайны", "justification": "Подчеркнет загадочность ядра через визуальные намеки и недосказанность."}}
ИЛИ
{{"chosen_driver_type": "perspective_types", "chosen_driver_value": "`Через замочную скважину / щель` (Keyhole View)", "justification": "Создаст ощущение подглядывания и ограниченности информации, усиливая ядро."}}

JSON:
"""
PROMPT_STEP3 = """
# ЗАДАЧА: Выбор Эстетического Фильтра (Стиля) - Опционально
Ты — креативный стратег ИИД.
Нужно ли добавлять специфический ЭСТЕТИЧЕСКИЙ ФИЛЬТР (стиль режиссера или художника) к видео, чтобы усилить Эмоциональное Ядро и Креативный Драйвер?

ВХОДНОЙ ТЕКСТ:
{input_text}

ВЫБРАННОЕ ЭМОЦИОНАЛЬНОЕ ЯДРО:
{chosen_emotional_core_json}

ВЫБРАННЫЙ КРЕАТИВНЫЙ ДРАЙВЕР:
{chosen_driver_json}

ВАРИАНТЫ СТИЛЕЙ (если решишь добавить):

Стили Режиссеров:
{directors_list_str}

Стили Художников:
{artists_list_str}

ТВОЕ РЕШЕНИЕ:
1.  Определи, нужен ли вообще стиль (`style_needed`: true/false). Стиль нужен, если он ЗНАЧИТЕЛЬНО усилит восприятие, а не будет просто украшением.
2.  Если стиль нужен (`style_needed`: true):
    * Выбери тип стиля (`chosen_style_type`: "director" или "artist").
    * Выбери КОНКРЕТНЫЙ стиль (`chosen_style_value`: имя режиссера или художника из списков).
    * Кратко обоснуй (1-2 предложения), как этот стиль поможет раскрыть ядро и драйвер (`justification`).
3.  Если стиль НЕ нужен (`style_needed`: false):
    * `chosen_style_type`, `chosen_style_value`, `justification` должны быть `null`.

ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект со следующими ключами:
- "style_needed": булево значение (true или false).
- "chosen_style_type": строка ("director", "artist") или null.
- "chosen_style_value": строка (имя из списка) или null.
- "justification": строка (обоснование) или null.

ПРИМЕР JSON ОТВЕТА (стиль нужен):
{{"style_needed": true, "chosen_style_type": "director", "chosen_style_value": "Дэвид Линч", "justification": "Сюрреализм Линча усилит атмосферу тайны и создаст ощущение сна."}}
ПРИМЕР JSON ОТВЕТА (стиль не нужен):
{{"style_needed": false, "chosen_style_type": null, "chosen_style_value": null, "justification": null}}

JSON:
"""
PROMPT_STEP5 = """
# ЗАДАЧА: Генерация Сценария и Описания Первого Кадра
Ты — ИИД-сценарист и визионер. На основе Креативного Брифа создай:
1.  **Сценарий (Script):** Короткий (до 500 символов) сценарий для 10-секундного видео. Опиши ключевое действие, движение камеры и атмосферу, РЕАЛИЗУЯ выбранные Эмоциональное Ядро, Креативный Драйвер и (если есть) Эстетический Стиль. Фокусируйся на ВИЗУАЛЬНОМ повествовании.
2.  **Описание Первого Кадра (First Frame Description):** Детальное (до 500 символов) описание САМОГО ПЕРВОГО кадра видео. Опиши композицию, цвета, свет, ракурс камеры. Этот кадр должен быть квинтэссенцией всего ролика, задавать тон и передавать основную идею/настроение.

ИСХОДНЫЕ ДАННЫЕ:

Входной Текст (для контекста):
{input_text}

Креативный Бриф:
{creative_brief_json}

ТРЕБОВАНИЯ:
- Сценарий и Описание Кадра должны строго соответствовать Креативному Брифу.
- Общая длина ответа (сценарий + описание) не должна превышать ~1000 символов.
- Текст должен быть на английском языке, готов к использованию в Runway ML.

ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект с ДВУМЯ ключами:
- "script": строка, содержащая сгенерированный сценарий.
- "first_frame_description": строка, содержащая сгенерированное описание первого кадра.

ПРИМЕР JSON ОТВЕТА:
{{
"script": "Slow zoom out from a cracked pocket watch lying on dusty cobblestones. Rain begins to fall, reflecting neon signs. The watch hands spin backwards rapidly. Ends on a wide shot of a desolate, futuristic street. Mood: Melancholy mystery.",
"first_frame_description": "Extreme close-up on a cracked pocket watch face. Aged brass casing, intricate details. A single crack runs across the glass. Background is dark, out-of-focus cobblestones. Lighting is dim, focused on the watch. Colors: Muted brass, dark greys, a hint of reflected blue neon."
}}

JSON:
"""

# --- ОБНОВЛЕННЫЙ ПРОМПТ ДЛЯ ШАГА 6A ---
PROMPT_STEP6A = """
# ЗАДАЧА: Адаптация Описания Кадра для Промпта Midjourney

## 1. ТВОЯ РОЛЬ:
Ты - AI Промпт-Инженер, специализирующийся на Midjourney. Твоя задача - преобразовать концептуальное описание первого кадра и креативный бриф в максимально эффективный и детализированный промпт для Midjourney (v 6.0 и выше).

## 2. ВХОДНЫЕ ДАННЫЕ:

### 2.1. ОПИСАНИЕ ПЕРВОГО КАДРА (из Шага 5):
'''
{first_frame_description}
'''

### 2.2. КРЕАТИВНЫЙ БРИФ (Выбранные элементы с Шагов 1-3):
(Сюда Python-код подставит JSON объект с результатами Шагов 1-3)
Пример структуры JSON:
`{{{{ "core": {{"type": "mood"|"arc", "value": "..."}}, "driver": {{"type": "prompt"|"perspective"|"metaphor", "value": "..."}}, "aesthetic": {{"type": "director"|"artist"|null, "value": "..."|null}} }}}}`


### 2.3. ИСХОДНЫЙ ТЕКСТ (для дополнительного контекста, если нужно):
'''
{input_text}
'''

### 2.4. ТЕХНИЧЕСКИЕ ПАРАМЕТРЫ MJ (из конфига):
(Сюда Python-код подставит JSON объект с параметрами MJ из config.json)
Пример структуры JSON:
`{{{{ "aspect_ratio": "16:9", "version": "6.0", "style": "raw" | null }}}}`


## 3. ИНСТРУКЦИИ ПО ГЕНЕРАЦИИ ПРОМПТА ДЛЯ MIDJOURNEY:
1.  **Основа:** Используй `first_frame_description` как смысловую базу для промпта.
2.  **Детализация и Ключевые Слова:** Добавь конкретные детали, текстуры, элементы окружения. Используй сильные, образные прилагательные и существительные, которые хорошо понимает Midjourney. Опиши композицию кадра (например, "close-up", "wide angle", "rule of thirds").
3.  **Передача Эмоции/Дуги:** Убедись, что ключевые слова и описания в промпте явно передают `chosen_emotional_core` (настроение или *начальное* состояние дуги).
4.  **Отражение Драйвера:** Если `chosen_driver` - это перспектива, явно укажи ее (например, "ant's view", "extreme close-up"). Если метафора - опиши ее визуально. Если креативный подход - отрази его суть в описании сцены.
5.  **Интеграция Эстетики:** Если в брифе выбран `chosen_aesthetic` (стиль режиссера или художника), **обязательно** интегрируй его в промпт, используя характерные для этого стиля ключевые слова, описания техник или прямые указания (например, "in the style of Wes Anderson", "chiaroscuro lighting like Caravaggio", "surrealism like Dali").
6.  **Технические Параметры:** Аккуратно добавь в **конец** промпта технические параметры: `--ar {aspect_ratio} --v {version}{style_parameter_str}`. (Плейсхолдер `{style_parameter_str}` будет заменен кодом на пустую строку или на `--style <значение>`).
7.  **Избегание Запрещенных Слов:** **ВАЖНО!** При формулировании промпта избегай слов, которые могут быть восприняты системами модерации Midjourney как запрещенные или чувствительные (например, слова связанные с насилием, откровенным контентом, **интимностью**), даже если они используются в переносном или техническом смысле (например, замени "intimate camera angle" на "very close camera angle" или "personal perspective"). Используй безопасные синонимы.
8.  **Структура:** Сформируй единую, связную текстовую строку промпта.

## 4. ФОРМАТ ВЫВОДА:
Предоставь результат СТРОГО в формате JSON с одним ключом: "final_mj_prompt".
(Пример структуры JSON: `{{{{ "final_mj_prompt": "A detailed image prompt for Midjourney..." }}}}`)

"""
# --- КОНЕЦ ОБНОВЛЕННОГО ПРОМПТА ДЛЯ ШАГА 6A ---


PROMPT_STEP6B = """
# ЗАДАЧА: Адаптация Сценария для Промпта Runway (Image-to-Video)
Ты — AI-промпт инженер, эксперт по Runway ML (модель Image-to-Video). Твоя задача — адаптировать Сценарий видео в эффективный текстовый промпт для Runway, который будет использоваться ВМЕСТЕ с изображением первого кадра (сгенерированным Midjourney).

ИСХОДНЫЕ ДАННЫЕ:

Сценарий Видео:
{script}

Креативный Бриф (для контекста стиля и настроения):
{creative_brief_json}

Входной Текст (для контекста темы):
{input_text}

ТРЕБОВАНИЯ:
- Создай текстовый промпт для Runway (на английском языке).
- Промпт должен описывать ДЕЙСТВИЕ и ДВИЖЕНИЕ КАМЕРЫ, которые должны произойти в видео, НАЧИНАЯ с первого кадра (который будет подан как image input).
- Промпт должен четко передавать динамику, описанную в Сценарии.
- Учти Эмоциональное Ядро, Креативный Драйвер и Эстетический Стиль из Креативного Брифа для описания атмосферы и характера движения.
- Промпт должен быть единой строкой текста, лаконичным и понятным для Runway. Длина - до 1000 символов (в идеале меньше).

ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект с ОДНИМ ключом:
- "final_runway_prompt": строка, содержащая готовый текстовый промпт для Runway (Image-to-Video).

ПРИМЕР JSON ОТВЕТА:
{{
"final_runway_prompt": "Slow zoom out revealing the cracked pocket watch on wet cobblestones. Rain intensifies, neon lights reflect more vividly. The watch hands start spinning backwards rapidly. Camera continues zooming out to a wide shot of a desolate, futuristic street. Maintain a melancholic, mysterious Blade Runner style atmosphere throughout."
}}

JSON:
"""

# --- Вспомогательные Функции ---

def load_json_config(file_path: Path):
    """Загружает JSON конфиг из файла."""
    if not file_path.is_file():
        logger.error(f"Файл конфигурации не найден: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки JSON из {file_path}: {e}")
        return None

def format_list_for_prompt(items: list | dict, use_weights=False) -> str:
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
                 # Не добавляем заголовок категории для 'main', если используем веса
                 if not use_weights or category != 'main':
                     # Добавляем пустую строку перед заголовком категории, если это не первая строка
                     if lines: lines.append("")
                     lines.append(f"  Категория '{category}':")

                 formatted_sublist = format_list_for_prompt(cat_items, use_weights=(use_weights and category == 'main'))
                 if formatted_sublist != "- (Список пуст)":
                     # Добавляем отступ к каждой строке подсписка
                     indented_lines = [f"    {line}" if line.strip() else line for line in formatted_sublist.split('\n')]
                     lines.extend(indented_lines)
             else: # Если элемент словаря не список, просто выводим ключ-значение
                 lines.append(f"* {category}: {cat_items}")
    else: return "- (Неверный формат данных)"

    # Убираем пустые строки в начале и конце и возвращаем результат
    return "\n".join(lines).strip()


def call_openai(prompt_text: str, model: str = "gpt-4o", temperature: float = 0.7, max_tokens: int = 500) -> dict | None:
    """
    Выполняет вызов OpenAI API (версии >=1.0) и возвращает распарсенный JSON.
    """
    # --- ИСПРАВЛЕНИЕ OpenAI v1.x ---
    # Проверяем наличие клиента OpenAI (создается в run_iid_pipeline)
    global openai_client # Используем глобальный клиент, если он есть
    if not openai_client:
         # Попытка инициализировать, если его нет (хотя он должен быть создан раньше)
         api_key_local = os.getenv("OPENAI_API_KEY")
         if api_key_local and hasattr(openai, 'OpenAI'):
             try:
                 openai_client = openai.OpenAI(api_key=api_key_local)
                 logger.info("Клиент OpenAI (>1.0) был инициализирован внутри call_openai.")
             except Exception as init_err:
                 logger.error(f"Ошибка инициализации клиента OpenAI внутри call_openai: {init_err}")
                 return None
         else:
             logger.error("Клиент OpenAI не инициализирован и не удалось создать.")
             return None

    logger.info(f"Вызов OpenAI (Модель: {model}, Температура: {temperature}, Макс.токены: {max_tokens})...")
    try:
        # Используем синтаксис для OpenAI >= 1.0
        response = openai_client.chat.completions.create( # Используем openai_client
            model=model,
            messages=[
                {"role": "system", "content": "Ты - AI ассистент, который строго следует инструкциям пользователя и всегда отвечает ТОЛЬКО в указанном формате (например, JSON), без какого-либо дополнительного текста."},
                {"role": "user", "content": prompt_text}
            ],
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"} # Запрос JSON ответа
        )
        # Доступ к контенту в новой версии
        raw_response_text = response.choices[0].message.content.strip() if response.choices and response.choices[0].message else ""
        logger.debug(f"Сырой ответ от OpenAI:\n{raw_response_text}")

        if not raw_response_text:
            logger.error("OpenAI вернул пустой ответ.")
            return None
        try:
            # Убираем возможные артефакты перед парсингом JSON
            if raw_response_text.startswith("```json"):
                raw_response_text = raw_response_text[7:]
                if raw_response_text.endswith("```"):
                    raw_response_text = raw_response_text[:-3]
            raw_response_text = raw_response_text.strip()

            parsed_json = json.loads(raw_response_text)
            logger.info("Ответ OpenAI успешно распарсен как JSON.")
            return parsed_json
        except json.JSONDecodeError as json_e:
            logger.error(f"Ошибка декодирования JSON из ответа OpenAI: {json_e}\nОтвет: {raw_response_text}")
            return None
    # Используем новые классы исключений для openai >= 1.0
    except openai.AuthenticationError as e:
        logger.exception(f"Ошибка аутентификации OpenAI: {e}")
    except openai.RateLimitError as e:
        logger.exception(f"Превышен лимит запросов OpenAI: {e}")
    except openai.APIConnectionError as e:
         logger.exception(f"Ошибка соединения с API OpenAI: {e}")
    except openai.APIStatusError as e:
         logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}")
    except openai.OpenAIError as e: # Общее исключение OpenAI
        logger.exception(f"Произошла ошибка API OpenAI: {e}")
    except Exception as e:
        logger.exception(f"Произошла непредвиденная ошибка при вызове OpenAI: {e}")
    return None
    # --- КОНЕЦ ИСПРАВЛЕНИЯ OpenAI v1.x ---

def save_json_output(output_path: Path, data: dict):
    """Сохраняет словарь в JSON файл."""
    try:
        # Используем ensure_directory_exists из utils.py для создания папки
        ensure_directory_exists(str(output_path.parent)) # Исправлено: создаем родительскую папку файла
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Результаты успешно сохранены в файл {output_path.name}.") # Уточнено сообщение
    except Exception as e:
        logger.error(f"Не удалось сохранить JSON файл {output_path}: {e}")


# --- Функция для запуска действия PiAPI (Upscale/Variation) ---
def trigger_piapi_action(original_task_id: str, action: str, api_key: str, endpoint: str) -> dict | None:
    """Запускает действие (например, upscale) для задачи Midjourney через PiAPI."""
    if not api_key or not endpoint or not original_task_id or not action:
        logger.error("Недостаточно данных для запуска действия PiAPI (trigger_piapi_action).")
        return None

    # Определяем тип задачи на основе действия
    task_type = "upscale" if "upscale" in action else "variation" if "variation" in action else None # Убрали "unknown"
    if not task_type:
        logger.error(f"Не удалось определить тип задачи для действия '{action}'. Поддерживаются 'upscale' и 'variation'.")
        return None

    # Извлекаем индекс из действия (например, '1' из 'upscale1')
    index_match = re.search(r'\d+$', action) # Используем импортированный 're'
    if not index_match:
        logger.error(f"Не удалось извлечь индекс из действия '{action}'. Ожидался формат типа 'upscale1'.")
        return None
    index_str = index_match.group(0)

    # --- Используем правильный PAYLOAD (Согласно документации) ---
    payload = {
        "model": "midjourney",
        "task_type": task_type, # "upscale" или "variation"
        "input": {
            "origin_task_id": original_task_id, # ID исходной задачи (imagine)
            "index": index_str # Номер картинки (1-4) как строка
        }
    }
    # --- КОНЕЦ ---

    headers = {'X-API-Key': api_key, 'Content-Type': 'application/json'}
    request_time = datetime.now(timezone.utc)

    logger.info(f"Отправка запроса на действие '{action}' (тип: {task_type}) для задачи {original_task_id} на {endpoint}...")
    logger.debug(f"Payload действия PiAPI (согласно документации): {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=60) # Таймаут для запуска действия
        logger.debug(f"Ответ от PiAPI Action Trigger: Status={response.status_code}, Body={response.text[:500]}") # Логируем больше тела ответа
        response.raise_for_status() # Проверка на HTTP ошибки
        result = response.json()

        # Ищем новый task_id в ответе (структура может отличаться в зависимости от API)
        # Пробуем разные стандартные ключи
        new_task_id = result.get("result", result.get("task_id"))
        if not new_task_id and isinstance(result.get("data"), dict):
            new_task_id = result.get("data", {}).get("task_id")

        if new_task_id:
            timestamp_str = request_time.isoformat()
            logger.info(f"✅ Получен НОВЫЙ task_id для действия '{action}': {new_task_id} (запрошено в {timestamp_str})")
            return {"task_id": str(new_task_id), "requested_at_utc": timestamp_str}
        else:
            logger.warning(f"Ответ API на действие '{action}' не содержит нового task_id. Проверяем статус исходной задачи или детали ответа: {result}")
            # Возможно, API не возвращает новый ID для этого действия, а обновляет исходную задачу?
            # Или ошибка в ответе API. Возвращаем None, т.к. не получили ожидаемый ID.
            return None
    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут при запросе на действие '{action}' к PiAPI: {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса на действие '{action}' к PiAPI: {e}")
        if e.response is not None:
            logger.error(f"    Статус: {e.response.status_code}, Тело: {e.response.text}") # Логируем тело при ошибке
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка JSON ответа на действие '{action}' от PiAPI: {e}. Ответ: {response.text[:500]}")
        return None
    except Exception as e:
        logger.exception(f"❌ Неизвестная ошибка при запуске действия '{action}' PiAPI: {e}")
        return None
# --- КОНЕЦ ФУНКЦИИ ---

# --- НОВАЯ ФУНКЦИЯ: Генерация изображения через DALL-E 3 ---
def generate_dalle_image(prompt: str, size: str = "1792x1024", quality: str = "standard") -> str | None:
    """
    Генерирует изображение с помощью DALL-E 3 через OpenAI API.

    Args:
        prompt: Текстовый промпт для генерации изображения.
        size: Размер изображения ("1024x1024", "1792x1024" или "1024x1792").
        quality: Качество изображения ("standard" или "hd").

    Returns:
        URL сгенерированного изображения или None в случае ошибки.
    """
    global openai_client # Используем глобальный клиент OpenAI
    if not openai_client:
        logger.error("Клиент OpenAI не инициализирован. Невозможно вызвать DALL-E 3.")
        return None

    logger.info(f"Попытка генерации изображения через DALL-E 3 (Размер: {size}, Качество: {quality})...")
    logger.debug(f"Промпт для DALL-E 3: {prompt}")

    try:
        response = openai_client.images.generate(
            model="dall-e-3",
            prompt=prompt,
            size=size,
            quality=quality,
            n=1,
            response_format="url" # Запрашиваем URL
        )

        # Извлекаем URL из ответа
        if response.data and len(response.data) > 0 and response.data[0].url:
            image_url = response.data[0].url
            logger.info("✅ Изображение DALL-E 3 успешно сгенерировано.")
            logger.debug(f"URL изображения DALL-E 3: {image_url}")
            return image_url
        else:
            logger.error(f"Ответ API DALL-E 3 не содержит ожидаемого URL. Ответ: {response}")
            return None

    except openai.AuthenticationError as e:
        logger.exception(f"Ошибка аутентификации OpenAI при вызове DALL-E 3: {e}")
    except openai.RateLimitError as e:
        logger.exception(f"Превышен лимит запросов OpenAI при вызове DALL-E 3: {e}")
    except openai.APIConnectionError as e:
         logger.exception(f"Ошибка соединения с API OpenAI при вызове DALL-E 3: {e}")
    except openai.APIStatusError as e:
         logger.exception(f"Ошибка статуса API OpenAI при вызове DALL-E 3: {e.status_code} - {e.response}")
    except openai.BadRequestError as e: # Обработка ошибок, связанных с промптом (например, модерация)
         logger.exception(f"Ошибка неверного запроса к DALL-E 3 (возможно, промпт отклонен модерацией): {e}")
    except openai.OpenAIError as e: # Общее исключение OpenAI
        logger.exception(f"Произошла ошибка API OpenAI при вызове DALL-E 3: {e}")
    except Exception as e:
        logger.exception(f"Произошла непредвиденная ошибка при вызове DALL-E 3: {e}")

    return None
# --- КОНЕЦ НОВОЙ ФУНКЦИИ ---


# --- Глобальная переменная для клиента OpenAI ---
openai_client = None

# --- Основная Логика Пайплайна ---

def run_iid_pipeline(input_json_path: Path, config_dir: Path):
    """Выполняет полный пайплайн ИИД + Генерация Медиа для одного входного файла."""
    global openai_client # Объявляем, что будем использовать глобальную переменную
    logger.info(f"===== Запуск полного пайплайна для файла: {input_json_path.name} =====")

    # --- 1. Загрузка конфигураций и входных данных ---
    creative_config_path = config_dir / "creative_config.json"
    main_config_path = config_dir / "config.json"

    creative_config = load_json_config(creative_config_path)
    # Передаем абсолютный путь в ConfigManager
    main_config_manager = ConfigManager(config_path=str(main_config_path.resolve()))
    input_data = load_json_config(input_json_path)

    if not creative_config or not input_data:
        logger.error("Не удалось загрузить creative_config или input_data. Прерывание.")
        return False

    # --- 2. Настройка OpenAI ---
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.error("Переменная окружения OPENAI_API_KEY не найдена. Прерывание.")
        return False
    # openai.api_key = api_key # Устарело для v1.x

    # Инициализация клиента OpenAI (>1.0) - делаем это один раз здесь
    if not openai_client: # Инициализируем только если еще не создан
        try:
            if hasattr(openai, 'OpenAI'):
                 openai_client = openai.OpenAI(api_key=api_key)
                 logger.info("Клиент OpenAI (>1.0) успешно инициализирован.")
            else:
                 logger.error("Используется старая версия библиотеки OpenAI (<1.0). Обновите: pip install --upgrade openai")
                 return False # Прерываем, если старая версия
        except Exception as e:
             logger.exception("Не удалось инициализировать клиент OpenAI.")
             return False # Прерываем, если ошибка инициализации

    openai_model = main_config_manager.get("OPENAI_SETTINGS.model", "gpt-4o")
    temp_step1 = 0.7; temp_step2 = 0.7; temp_step3 = 0.7; temp_step5 = 0.7; temp_step6 = 0.7

    # --- 3. Извлечение входных данных ---
    input_text = input_data.get("content")
    if not input_text:
        logger.error("Ключ 'content' не найден во входном JSON. Прерывание.")
        return False

    # --- 4. Форматирование списков для промптов ---
    moods_list_str = format_list_for_prompt(creative_config.get("moods", []), use_weights=True)
    arcs_list_str = format_list_for_prompt(creative_config.get("emotional_arcs", []))
    main_prompts_list = creative_config.get("creative_prompts", {}).get("main", [])
    prompts_list_str = format_list_for_prompt(main_prompts_list, use_weights=True)
    perspectives_list_str = format_list_for_prompt(creative_config.get("perspective_types", []))
    metaphors_list_str = format_list_for_prompt(creative_config.get("visual_metaphor_types", []))
    directors_list_str = format_list_for_prompt(creative_config.get("director_styles", []))
    artists_list_str = format_list_for_prompt(creative_config.get("artist_styles", []))

    # --- 5. Выполнение шагов ИИД (генерация промптов) ---
    final_mj_prompt = None
    final_runway_prompt = None
    frame_description_for_mj = None
    script = None # Инициализируем script здесь
    creative_brief = None # Инициализируем creative_brief

    # Проверяем наличие ВСЕХ необходимых ключей в creative_output
    creative_output_data = input_data.get('creative_output')
    has_existing_prompts = (
        isinstance(creative_output_data, dict) and
        all(k in creative_output_data for k in ['final_mj_prompt', 'final_runway_prompt', 'generated_frame_description', 'generated_script', 'creative_brief']) and
        all(creative_output_data.get(k) for k in ['final_mj_prompt', 'final_runway_prompt']) # Убедимся что промпты не пустые
    )

    if has_existing_prompts:
        logger.warning("Найдены существующие и полные данные в 'creative_output'. Пропускаем шаги генерации промптов (1-6).")
        creative_brief = creative_output_data['creative_brief']
        script = creative_output_data['generated_script']
        frame_description_for_mj = creative_output_data['generated_frame_description']
        final_mj_prompt = creative_output_data['final_mj_prompt']
        final_runway_prompt = creative_output_data['final_runway_prompt']
    else:
        # --- ЭКСПЕРИМЕНТ: Если промпты не найдены, прерываемся, т.к. генерация отключена ---
        logger.error("Данные 'creative_output' не найдены в JSON.")
        logger.error("Генерация промптов (Шаги 1-6) необходима, но генерация изображений отключена для этого теста.")
        logger.error("Пожалуйста, убедитесь, что 'creative_output' с 'final_runway_prompt' существует в JSON файле.")
        return False
        # --- КОНЕЦ ЭКСПЕРИМЕНТАЛЬНОЙ ПРОВЕРКИ ---

        # # --- Старая логика генерации промптов (закомментирована для теста) ---
        # if creative_output_data: # Если creative_output есть, но неполный
        #      logger.warning("Данные в 'creative_output' неполные или некорректные. Запускаем генерацию промптов заново.")

        # logger.info("--- Шаг 1: Выбор Эмоционального Ядра ---")
        # prompt1_text = PROMPT_STEP1.format(input_text=input_text, moods_list_str=moods_list_str, arcs_list_str=arcs_list_str)
        # core_brief = call_openai(prompt1_text, model=openai_model, temperature=temp_step1)
        # if not core_brief or not all(k in core_brief for k in ["chosen_type", "chosen_value", "justification"]):
        #     logger.error(f"Шаг 1 не удался: {core_brief}. Прерывание.")
        #     return False

        # logger.info("--- Шаг 2: Выбор Основного Креативного Драйвера ---")
        # prompt2_text = PROMPT_STEP2.format(input_text=input_text, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), prompts_list_str=prompts_list_str, perspectives_list_str=perspectives_list_str, metaphors_list_str=metaphors_list_str)
        # driver_brief = call_openai(prompt2_text, model=openai_model, temperature=temp_step2)
        # if not driver_brief or not all(k in driver_brief for k in ["chosen_driver_type", "chosen_driver_value", "justification"]):
        #     logger.error(f"Шаг 2 не удался: {driver_brief}. Прерывание.")
        #     return False

        # logger.info("--- Шаг 3: Выбор Эстетического Фильтра ---")
        # prompt3_text = PROMPT_STEP3.format(input_text=input_text, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), chosen_driver_json=json.dumps(driver_brief, ensure_ascii=False, indent=2), directors_list_str=directors_list_str, artists_list_str=artists_list_str)
        # aesthetic_brief = call_openai(prompt3_text, model=openai_model, temperature=temp_step3)
        # # Проверка на null значения, если style_needed=false
        # style_needed = aesthetic_brief.get("style_needed", False) if isinstance(aesthetic_brief, dict) else False
        # required_keys_step3 = ["style_needed"]
        # if style_needed:
        #     required_keys_step3.extend(["chosen_style_type", "chosen_style_value", "justification"])

        # # Проверяем наличие ключей и их значения, если стиль НЕ нужен
        # valid_step3 = True
        # if not aesthetic_brief or not all(k in aesthetic_brief for k in required_keys_step3):
        #     valid_step3 = False
        # elif not style_needed:
        #      # Если стиль не нужен, проверяем, что остальные ключи null или отсутствуют
        #      if aesthetic_brief.get("chosen_style_type") is not None or \
        #         aesthetic_brief.get("chosen_style_value") is not None or \
        #         aesthetic_brief.get("justification") is not None:
        #          logger.warning(f"Шаг 3: style_needed=false, но другие ключи не null: {aesthetic_brief}. Исправляем на null.")
        #          aesthetic_brief["chosen_style_type"] = None
        #          aesthetic_brief["chosen_style_value"] = None
        #          aesthetic_brief["justification"] = None
        # elif style_needed:
        #      # Если стиль нужен, проверяем, что значения не пустые строки
        #      if not aesthetic_brief.get("chosen_style_type") or \
        #         not aesthetic_brief.get("chosen_style_value") or \
        #         not aesthetic_brief.get("justification"):
        #          logger.error(f"Шаг 3: style_needed=true, но значения type/value/justification пустые. {aesthetic_brief}")
        #          valid_step3 = False


        # if not valid_step3:
        #     logger.error(f"Шаг 3 не удался или вернул некорректную структуру: {aesthetic_brief}. Прерывание.")
        #     return False

        # creative_brief = {"core": core_brief, "driver": driver_brief, "aesthetic": aesthetic_brief}
        # logger.info("--- Шаг 4: Креативный Бриф Собран ---")
        # logger.debug(f"Собранный Креативный Бриф: {json.dumps(creative_brief, ensure_ascii=False, indent=2)}")


        # logger.info("--- Шаг 5: Генерация Сценария и Описания Кадра ---")
        # prompt5_text = PROMPT_STEP5.format(input_text=input_text, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2))
        # script_frame_data = call_openai(prompt5_text, model=openai_model, temperature=temp_step5, max_tokens=1000)
        # if not script_frame_data or not all(k in script_frame_data for k in ["script", "first_frame_description"]):
        #     logger.error(f"Шаг 5 не удался: {script_frame_data}. Прерывание.")
        #     return False
        # script = script_frame_data["script"]
        # frame_description_for_mj = script_frame_data["first_frame_description"]
        # logger.info(f"Сгенерирован сценарий: {script[:100]}...")
        # logger.info(f"Сгенерировано описание кадра: {frame_description_for_mj[:100]}...")

        # logger.info("--- Шаг 6a: Адаптация Описания под Midjourney ---")
        # mj_params = main_config_manager.get("IMAGE_GENERATION", {})
        # # Используем правильный разделитель для aspect ratio
        # output_size_mj = mj_params.get("output_size", "16:9")
        # aspect_ratio = output_size_mj.replace('x', ':').replace('×', ':')
        # version = mj_params.get("midjourney_version", "6.0") # В конфиге 7.0, но в промпте 6.0 - используем из конфига
        # style = mj_params.get("midjourney_style", None) # В конфиге null
        # mj_parameters_json_for_prompt = json.dumps({"aspect_ratio": aspect_ratio, "version": version, "style": style}, ensure_ascii=False)
        # style_parameter_str = f" --style {style}" if style else ""
        # # Используем обновленный PROMPT_STEP6A
        # prompt6a_text = PROMPT_STEP6A.format(
        #     first_frame_description=frame_description_for_mj,
        #     creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2),
        #     input_text=input_text,
        #     mj_parameters_json=mj_parameters_json_for_prompt, # Передаем параметры как JSON строку
        #     aspect_ratio=aspect_ratio, # Отдельно для {aspect_ratio}
        #     version=version,           # Отдельно для {version}
        #     style_parameter_str=style_parameter_str # Отдельно для {style_parameter_str}
        # )
        # mj_prompt_data = call_openai(prompt6a_text, model=openai_model, temperature=temp_step6, max_tokens=1000) # Увеличим max_tokens на всякий случай для более длинного промпта
        # if not mj_prompt_data or "final_mj_prompt" not in mj_prompt_data:
        #     logger.error(f"Шаг 6a не удался: {mj_prompt_data}. Прерывание.")
        #     return False
        # final_mj_prompt = mj_prompt_data["final_mj_prompt"]
        # logger.info(f"Сгенерирован MJ промпт: {final_mj_prompt}")

        # logger.info("--- Шаг 6b: Адаптация Сценария под Runway ---")
        # prompt6b_text = PROMPT_STEP6B.format(script=script, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2), input_text=input_text)
        # runway_prompt_data = call_openai(prompt6b_text, model=openai_model, temperature=temp_step6, max_tokens=1000)
        # if not runway_prompt_data or "final_runway_prompt" not in runway_prompt_data:
        #     logger.error(f"Шаг 6b не удался: {runway_prompt_data}. Прерывание.")
        #     return False
        # final_runway_prompt = runway_prompt_data["final_runway_prompt"]
        # logger.info(f"Сгенерирован Runway промпт: {final_runway_prompt}")

        # logger.info(f"Обновление файла {input_json_path.name} сгенерированными данными...")
        # try:
        #     # Читаем файл заново, чтобы избежать конфликтов, если он изменился
        #     with open(input_json_path, 'r', encoding='utf-8') as f: current_input_data = json.load(f)
        #     # Обновляем или создаем 'creative_output'
        #     current_input_data['creative_output'] = {
        #         "creative_brief": creative_brief,
        #         "generated_script": script,
        #         "generated_frame_description": frame_description_for_mj,
        #         "final_mj_prompt": final_mj_prompt,
        #         "final_runway_prompt": final_runway_prompt,
        #         "prompts_processed_at_utc": datetime.now(timezone.utc).isoformat()
        #     }
        #     save_json_output(input_json_path, current_input_data)
        #     logger.info("✅ Промпты и бриф успешно добавлены/обновлены в исходном JSON.")
        # except Exception as e:
        #     logger.error(f"Не удалось обновить исходный JSON файл {input_json_path}: {e}")
        #     return False
        # # --- Конец старой логики генерации промптов ---


    # --- Проверка наличия Runway промпта (он нужен в любом случае) ---
    if not final_runway_prompt:
        logger.error("Отсутствует 'final_runway_prompt' в 'creative_output' JSON файла. Невозможно запустить Runway.")
        return False
    # final_mj_prompt больше не проверяем, т.к. он не используется в этом режиме
    # if not frame_description_for_mj:
    #      logger.warning("Отсутствует описание кадра ('generated_frame_description').")
    # if not script:
    #      logger.warning("Отсутствует сценарий ('generated_script').")


    logger.info("DEBUG: ПЕРЕХОД К БЛОКУ ГЕНЕРАЦИИ МЕДИА (РЕЖИМ ТЕСТА С ЛОКАЛЬНЫМ ФАЙЛОМ)...")

    # --- 7. НОВЫЙ БЛОК: Генерация Медиа (РЕЖИМ ТЕСТА) ---
    logger.info("===== Поиск локального изображения и запуск Runway =====")

    # Переменная для хранения пути к финальному изображению для Runway
    final_image_path_for_runway = None
    image_source = "None" # Источник изображения ('Local File', 'None')

    # Определяем базовое имя и директорию для выходных файлов
    output_base_name = input_json_path.stem
    output_dir = input_json_path.parent

    # --- 7.0 Поиск локального PNG файла ---
    # Ожидаемое имя файла: {имя_json_файла}_input.png
    local_image_filename = f"{output_base_name}_input.png"
    local_image_path = output_dir / local_image_filename
    logger.info(f"Поиск локального входного изображения: {local_image_path}")

    if local_image_path.is_file():
        logger.info(f"✅ Локальное изображение найдено: {local_image_path}")
        final_image_path_for_runway = local_image_path
        image_source = "Local File"
    else:
        logger.error(f"❌ Локальное изображение НЕ найдено по пути: {local_image_path}")
        logger.error("Пожалуйста, убедитесь, что PNG файл с правильным именем находится в той же папке, что и JSON.")
        # Не прерываем выполнение, Runway сам не запустится из-за отсутствия final_image_path_for_runway

    # --- 7.1 Midjourney (ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА) ---
    # logger.info("--- Шаг 7.1 Midjourney ОТКЛЮЧЕН для теста ---")
    # mj_api_key = os.getenv("MIDJOURNEY_API_KEY")
    # mj_init_endpoint = main_config_manager.get("API_KEYS.midjourney.endpoint")
    # mj_fetch_endpoint = main_config_manager.get("API_KEYS.midjourney.task_endpoint")
    # mj_image_path_temp = None
    # if mj_api_key and final_mj_prompt:
    #     # ... (весь код MJ) ...
    #     pass
    # else:
    #     logger.warning("Пропуск шага Midjourney (отключен или нет ключа/промпта).")

    # --- 7.1.1 Fallback на DALL-E 3 (ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА) ---
    # logger.info("--- Шаг 7.1.1 Fallback DALL-E 3 ОТКЛЮЧЕН для теста ---")
    # if final_image_path_for_runway is None and final_mj_prompt:
    #     # ... (весь код DALL-E fallback) ...
    #     pass
    # elif final_image_path_for_runway is not None:
    #     logger.info(f"Изображение для Runway было бы получено из {image_source} (если бы генерация была включена).")
    # else:
    #     logger.error("Не удалось бы получить изображение ни от Midjourney, ни от DALL-E 3 (генерация отключена).")


    # --- 7.2 Runway ---
    runway_video_path = None
    runway_api_key = os.getenv("RUNWAY_API_KEY") # Ключ все еще нужен

    # Проверяем наличие финального изображения (теперь из локального файла) и других условий
    if runway_api_key and final_runway_prompt and final_image_path_for_runway and final_image_path_for_runway.is_file():
        logger.info(f"--- Шаг 7.2: Запуск Runway (Источник изображения: {image_source}) ---")
        logger.info(f"Используется изображение: {final_image_path_for_runway}")
        logger.info(f"Используется промпт Runway: {final_runway_prompt}")

        if not RUNWAY_SDK_AVAILABLE:
             logger.error("SDK RunwayML недоступен. Невозможно запустить генерацию видео.")
        else:
            # Используем импортированную функцию generate_runway_video
            logger.info("Вызов generate_runway_video...")
            video_url = generate_runway_video(
                image_path=str(final_image_path_for_runway), # Передаем путь к локальному файлу
                script=final_runway_prompt,
                config=main_config_manager, # Передаем ConfigManager
                api_key=runway_api_key
            )

            if video_url:
                logger.info(f"Получен URL видео Runway: {video_url}")
                # Формируем путь к видео
                runway_video_path = output_dir / f"{output_base_name}_runway.mp4"
                # Используем download_video из utils
                logger.info(f"Попытка скачивания видео Runway в {runway_video_path}...")
                if download_video(video_url, str(runway_video_path), logger_instance=logger): # Передаем logger
                    logger.info(f"✅ Видео Runway сохранено: {runway_video_path}")
                else:
                    logger.error(f"Не удалось скачать видео Runway с {video_url}")
                    runway_video_path = None # Сбрасываем путь
            else:
                logger.error("Не удалось сгенерировать или получить URL видео Runway (ошибка или таймаут). Проверьте логи generate_runway_video.")

    elif not runway_api_key: logger.warning("Пропуск шага Runway из-за отсутствия API ключа.")
    elif not final_runway_prompt: logger.warning("Пропуск шага Runway из-за отсутствия финального промпта.")
    elif not final_image_path_for_runway or not final_image_path_for_runway.is_file():
        logger.warning(f"Пропуск шага Runway из-за отсутствия локального входного файла ({local_image_filename}).")


    # --- 8. Завершение ---
    # Проверяем наличие финального изображения (локального) и видео
    if final_image_path_for_runway and runway_video_path:
         logger.info(f"===== Пайплайн (Тест Runway) успешно завершен для {input_json_path.name} (Изображение: {image_source}, Видео: Runway) =====")
         return True
    elif final_image_path_for_runway:
         logger.warning(f"===== Пайплайн (Тест Runway) завершен для {input_json_path.name} (Изображение: {image_source}, Видео НЕ сгенерировано) =====")
         # Считаем это успехом для теста, если локальный файл был найден, но видео не сгенерировалось
         return True # Или False, если считать успехом только генерацию видео? Пока True, т.к. цель - тест Runway
    else:
         logger.error(f"===== Пайплайн (Тест Runway) завершен с ошибками для {input_json_path.name} (Локальное изображение не найдено) =====")
         return False

# --- Точка входа ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Запускает ИИД пайплайн и генерацию медиа (в режиме теста Runway с локальным файлом).") # Обновлено описание
    parser.add_argument("input_file", help="Путь к входному JSON файлу (например, C:\\Users\\boyar\\777\\555\\55\\20250220-1331.json)")

    # Определяем директорию скрипта и директорию конфига по умолчанию
    script_dir = Path(__file__).parent
    # Поднимаемся на уровень выше из 'scripts' в базовую директорию проекта
    project_base_dir = script_dir.parent
    default_config_dir = project_base_dir / "config"

    parser.add_argument("-c", "--config_dir", default=str(default_config_dir), help=f"Путь к папке с файлами конфигурации (по умолчанию: {default_config_dir})")
    args = parser.parse_args()

    # Преобразуем пути в объекты Path
    input_file_path = Path(args.input_file).resolve() # Получаем абсолютный путь
    config_dir_path = Path(args.config_dir).resolve() # Получаем абсолютный путь

    # --- Расширенные Проверки ---
    print(f"--- Проверка путей ---")
    print(f"Входной файл (абсолютный): {input_file_path}")
    print(f"Папка конфигурации (абсолютная): {config_dir_path}")
    print(f"Текущая рабочая директория: {Path.cwd()}")
    print(f"Содержимое sys.path: {sys.path}")

    if not input_file_path.is_file():
        print(f"Ошибка: Входной файл не найден: {input_file_path}")
        logger.critical(f"Входной файл не найден: {input_file_path}")
        sys.exit(1)
    if not config_dir_path.is_dir():
        print(f"Ошибка: Папка конфигурации не найдена: {config_dir_path}")
        logger.critical(f"Папка конфигурации не найдена: {config_dir_path}")
        sys.exit(1)
    if not (config_dir_path / "config.json").is_file():
        print(f"Ошибка: Основной config.json не найден в {config_dir_path}")
        logger.critical(f"Основной config.json не найден в {config_dir_path}")
        sys.exit(1)
    if not (config_dir_path / "creative_config.json").is_file():
        print(f"Ошибка: creative_config.json не найден в {config_dir_path}")
        logger.critical(f"creative_config.json не найден в {config_dir_path}")
        sys.exit(1)
    print(f"--- Проверка путей завершена успешно ---")


    # Установка ключа OpenAI (на всякий случай, если еще не установлен)
    # Перенесено в run_iid_pipeline, т.к. там он реально нужен для инициализации клиента

    # Запуск
    success = False
    try:
        success = run_iid_pipeline(input_file_path, config_dir_path)
    except Exception as e:
         logger.critical(f"Неперехваченная ошибка в run_iid_pipeline: {e}", exc_info=True)
         success = False # Убедимся, что success=False при любой ошибке

    # Выход
    exit_code = 0 if success else 1
    logger.info(f"--- Завершение iid_local_tester.py (Тест Runway) с кодом выхода: {exit_code} ---")
    sys.exit(exit_code)

