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
    CURRENT_SCRIPT_PATH = Path(__file__).resolve()
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
    from scripts.Workspace_media import fetch_piapi_status

    # Импортируем утилиты
    from modules.utils import ensure_directory_exists, download_image, download_video
    from modules.config_manager import ConfigManager

except ImportError as e:
     logging.exception(f"Критическая ошибка импорта модулей/функций проекта: {e}. Проверьте структуру папок и наличие файлов.")
     logging.error(f"Текущая рабочая директория: {os.getcwd()}")
     logging.error(f"sys.path: {sys.path}")
     try:
         CURRENT_SCRIPT_PATH_FALLBACK = Path(__file__).resolve()
         BASE_DIR_FALLBACK = CURRENT_SCRIPT_PATH_FALLBACK.parent.parent
         logging.error(f"Ожидаемый BASE_DIR: {BASE_DIR_FALLBACK}")
     except NameError:
         logging.error("Не удалось определить путь к текущему скрипту (__file__).")
     sys.exit(1)
except FileNotFoundError as e:
     logging.exception(f"Критическая ошибка: Не найден файл скрипта для импорта: {e}. Убедитесь, что структура папок верна.")
     sys.exit(1)


# --- Настройка Логгирования ---
# Установите logging.DEBUG для более подробных логов
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("iid_local_tester")

# --- Тексты Промптов ИИД ---

# --- ПРОМПТ STEP 1 (Определение Эмоционального Ядра) ---
PROMPT_STEP1 = """
# ЗАДАЧА: Определение Эмоционального Ядра Видео
## 1. ТВОЯ РОЛЬ:
Ты - AI Анализатор Текста. Твоя задача - проанализировать ИСХОДНЫЙ ТЕКСТ и выбрать ОДИН элемент (Настроение или Эмоциональную Дугу), который наилучшим образом определяет эмоциональную суть для 10-секундного видео.
## 2. ИСХОДНЫЙ ТЕКСТ:
'''
{input_text}
'''
## 3. ДОСТУПНЫЕ ЭЛЕМЕНТЫ ДЛЯ ВЫБОРА:
### Доступные Настроения (`moods`):
{moods_list_str}
*Примечание: Веса (если указаны) служат ориентиром (3 > 2 > 1).*
### Доступные Эмоциональные Дуги (`emotional_arcs`):
{arcs_list_str}
## 4. ИНСТРУКЦИИ ПО ВЫБОРУ:
1.  Прочти ИСХОДНЫЙ ТЕКСТ.
2.  Реши: лучше использовать статичное **Настроение** или динамическую **Эмоциональную Дугу**?
3.  Выбери **ОДИН** наиболее подходящий элемент из соответствующего списка выше.
4.  Сформулируй краткое (1 предложение) обоснование твоего выбора.
## 5. ФОРМАТ ВЫВОДА: **КРИТИЧЕСКИ ВАЖНО!**
Твой ответ ДОЛЖЕН быть **ТОЛЬКО валидным JSON объектом** и ничем иным. Не добавляй никакого текста до или после JSON объекта.
Структура JSON должна быть следующей:
* `chosen_type`: строка, значение "mood" или "arc".
* `chosen_value`: строка, точное название выбранного элемента из списка.
* `justification`: строка, краткое обоснование.
**Пример требуемого формата:** `{{{{ "chosen_type": "mood", "chosen_value": "Загадочное", "justification": "Текст содержит намеки и недосказанность." }}}}`
JSON:
"""
# --- КОНЕЦ ПРОМПТА STEP 1 ---

# --- ПРОМПТ STEP 2 (Выбор Креативного Драйвера) ---
PROMPT_STEP2 = """
# ЗАДАЧА: Выбор Основного Креативного Драйвера Видео
## 1. КОНТЕКСТ:
### 1.1. ИСХОДНЫЙ ТЕКСТ:
'''
{input_text}
'''
### 1.2. ВЫБРАННОЕ ЭМОЦИОНАЛЬНОЕ ЯДРО (из Шага 1):
{chosen_emotional_core_json}
## 2. ТВОЯ РОЛЬ:
Ты - ИИ Креативный Директор. Проанализировав текст и выбранное эмоциональное ядро, твоя задача - определить **ОДИН главный креативный инструмент (Драйвер)**, который наилучшим образом поможет раскрыть идею и эмоцию в 10-секундном видео.
## 3. ДОСТУПНЫЕ КАТЕГОРИИ И ИНСТРУМЕНТЫ:
### Категория 1: Креативные Подходы (`creative_prompts`)
*Инструкция: Выбери один из этих подходов, если считаешь, что для видео важна особая нарративная или концептуальная структура.*
{prompts_list_str}
### Категория 2: Типы Перспектив (`perspective_types`)
*Инструкция: Выбери один из этих ракурсов, если считаешь, что ключ к идее лежит в уникальной точке зрения.*
{perspectives_list_str}
### Категория 3: Типы Визуальных Метафор (`visual_metaphor_types`)
*Инструкция: Выбери один из этих типов метафор, если считаешь, что для первого кадра и общей концепции важен сильный символический образ.*
{metaphors_list_str}
## 4. ИНСТРУКЦИИ ПО ВЫБОРУ:
1.  Оцени, какой из трех типов инструментов (Креативный Подход, Перспектива или Визуальная Метафора) будет **наиболее эффективным** для передачи сути текста и выбранного эмоционального ядра.
2.  Выбери **ТОЛЬКО ОДИН** конкретный инструмент из списка соответствующей категории. Не выбирай инструменты из других категорий.
3.  При выборе Креативного Подхода (`creative_prompts`) старайся учитывать вес (3 > 2), но приоритет отдавай соответствию задаче.
4.  Кратко (1 предложение) обоснуй свой выбор категории и конкретного инструмента.
## 5. ФОРМАТ ВЫВОДА:
Предоставь результат СТРОГО в формате JSON со следующими ключами: `chosen_driver_type` (значение "prompt", "perspective" или "metaphor"), `chosen_driver_value` (точное название выбранного элемента) и `justification` (обоснование).
(Пример структуры JSON: `{{{{ "chosen_driver_type": "perspective", "chosen_driver_value": "Макро / Экстремальный крупный план", "justification": "Фокус на детали усилит зловещее настроение." }}}}`)
JSON:
"""
# --- КОНЕЦ ПРОМПТА STEP 2 ---

# --- ПРОМПТ STEP 3 (Выбор Эстетики и Ключевых Слов) ---
PROMPT_STEP3 = """
# ЗАДАЧА: Выбор Эстетического Фильтра (Стиля) и его Ключевых Слов - Опционально
## 1. КОНТЕКСТ:
### 1.1. ИСХОДНЫЙ ТЕКСТ:
'''
{input_text}
'''
### 1.2. ВЫБРАННОЕ ЭМОЦИОНАЛЬНОЕ ЯДРО (из Шага 1):
{chosen_emotional_core_json}
### 1.3. ВЫБРАННЫЙ ОСНОВНОЙ ДРАЙВЕР (из Шага 2):
{chosen_driver_json}
## 2. ТВОЯ РОЛЬ:
Ты - ИИ Креативный Директор и Аналитик Стилей. Оценив контекст, реши, **нужно ли добавлять специфический эстетический стиль** (режиссера или художника). Если да, выбери стиль и **сгенерируй ключевые слова**, описывающие его черты.
## 3. ДОСТУПНЫЕ СТИЛИ ДЛЯ ВЫБОРА (ЕСЛИ НУЖНО):
### Список Стилей Режиссеров (`director_styles`):
{directors_list_str}
### Список Стилей Художников (`artist_styles`):
{artists_list_str}
## 4. ИНСТРУКЦИИ ПО ВЫБОРУ И ГЕНЕРАЦИИ:
1.  Проанализируй контекст (текст, эмоц. ядро, драйвер).
2.  **Реши:** Усилит ли применение узнаваемого стиля общую идею и воздействие видео, или лучше оставить концепцию "чистой"?
3.  **Если стиль НЕ нужен:** Укажи `false` для `style_needed` и `null` для остальных ключей (`chosen_style_type`, `chosen_style_value`, `style_keywords`, `justification`).
4.  **Если стиль НУЖЕН:**
    а. Укажи `true` для `style_needed`.
    б. Определи, какой тип стиля (Режиссер или Художник) будет более уместен (`chosen_style_type`).
    в. Выбери **ОДИН** наиболее подходящий стиль из соответствующего списка выше (`chosen_style_value`).
    г. **Сгенерируй 2-4 ключевых слова или короткие фразы (на английском!) (`style_keywords`)**, описывающие **характерные визуальные или атмосферные черты** выбранного стиля, которые можно использовать в промптах для MJ/Runway (например, для стиля Хичкока: `["suspenseful atmosphere", "unusual camera angles", "slow burn tension"]`, для стиля Ван Гога: `["vibrant swirling brushstrokes", "emotional color use", "textured impasto"]`). **Избегай прямого упоминания имени в ключевых словах.**
    д. Кратко (1 предложение) обоснуй свой выбор стиля и почему он нужен (`justification`).
## 5. ФОРМАТ ВЫВОДА:
Предоставь результат СТРОГО в формате JSON со следующими ключами: `style_needed` (true или false), `chosen_style_type` ("director", "artist" или null), `chosen_style_value` (название стиля или null), `style_keywords` (список строк на английском или null) и `justification` (обоснование).
(Пример JSON если стиль нужен): `{{{{ "style_needed": true, "chosen_style_type": "director", "chosen_style_value": "Alfred Hitchcock", "style_keywords": ["suspenseful atmosphere", "unusual camera angles", "slow burn tension"], "justification": "Использование ключевых слов стиля усилит напряжение через ракурсы и атмосферу." }}}}`
(Пример JSON если стиль не нужен): `{{{{ "style_needed": false, "chosen_style_type": null, "chosen_style_value": null, "style_keywords": null, "justification": null }}}}`
JSON:
"""
# --- КОНЕЦ ПРОМПТА STEP 3 ---

# --- ПРОМПТ STEP 5 (Генерация Сценария и Описания Кадра) ---
PROMPT_STEP5 = """
# ЗАДАЧА: Генерация Сценария и Описания Первого Кадра
Ты — ИИД-сценарист и визионер. На основе Креативного Брифа создай:
1.  **Сценарий (Script):** Короткий (до 500 символов) сценарий для 10-секундного видео. Опиши ключевое действие, движение камеры и атмосферу, РЕАЛИЗУЯ выбранные Эмоциональное Ядро, Креативный Драйвер и (если есть) Эстетический Стиль (используя `style_keywords` из брифа для описания атмосферы/визуала, **избегая прямого упоминания имени** режиссера/художника). Фокусируйся на ВИЗУАЛЬНОМ повествовании.
2.  **Описание Первого Кадра (First Frame Description):** Детальное (до 500 символов) описание САМОГО ПЕРВОГО кадра видео. Опиши композицию, цвета, свет, ракурс камеры. Этот кадр должен быть квинтэссенцией всего ролика, задавать тон и передавать основную идею/настроение, учитывая Эстетический Стиль (через `style_keywords` из брифа, **избегая прямого упоминания имени**).
ИСХОДНЫЕ ДАННЫЕ:
Входной Текст (для контекста):
{input_text}
Креативный Бриф:
{creative_brief_json}
ТРЕБОВАНИЯ:
- Сценарий и Описание Кадра должны строго соответствовать Креативному Брифу.
- Общая длина ответа (сценарий + описание) не должна превышать ~1000 символов.
- **Текст сценария и описания должен быть на английском языке**, готов к использованию в Runway ML и Midjourney.
ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект с ДВУМЯ ключами:
- "script": строка, содержащая сгенерированный сценарий (на английском).
- "first_frame_description": строка, содержащая сгенерированное описание первого кадра (на английском).
ПРИМЕР JSON ОТВЕТА:
{{{{
"script": "Slow zoom out from a cracked pocket watch lying on dusty cobblestones. Rain begins to fall, reflecting neon signs. The watch hands spin backwards rapidly. Ends on a wide shot of a desolate, futuristic street. Atmosphere has a melancholy mystery, surreal vibe.",
"first_frame_description": "Extreme close-up on a cracked pocket watch face. Aged brass casing, intricate details. A single crack runs across the glass. Background is dark, out-of-focus cobblestones. Lighting is dim, focused on the watch, creating a chiaroscuro effect. Colors: Muted brass, dark greys, a hint of reflected blue neon. Visuals feature suspenseful atmosphere and unusual camera angles."
}}}}
JSON:
"""
# --- КОНЕЦ ПРОМПТА STEP 5 ---

# --- ПРОМПТ ШАГА 6A (Адаптация Описания Кадра для Midjourney V7 + Поддержка Анимации) ---
PROMPT_STEP6A = """
# ЗАДАЧА: Адаптация Описания Кадра для Промпта Midjourney V7

## 1. ТВОЯ РОЛЬ:
Ты - AI Промпт-Инженер, специализирующийся на Midjourney V7. Твоя задача - преобразовать концептуальное описание первого кадра и креативный бриф в максимально эффективный и детализированный промпт для Midjourney V7, который также будет хорошей основой для последующей анимации в Runway.

## 2. ВХОДНЫЕ ДАННЫЕ:

### 2.1. ОПИСАНИЕ ПЕРВОГО КАДРА (из Шага 5, на английском):
'''
{first_frame_description}
'''

### 2.2. КРЕАТИВНЫЙ БРИФ (Выбранные элементы с Шагов 1-3):
{creative_brief_json}

### 2.3. СЦЕНАРИЙ ВИДЕО (из Шага 5, для контекста планируемой анимации):
'''
{script}
'''

### 2.4. ИСХОДНЫЙ ТЕКСТ (для дополнительного контекста темы):
'''
{input_text}
'''

### 2.5. ТЕХНИЧЕСКИЕ ПАРАМЕТРЫ MJ (из конфига):
{mj_parameters_json}

## 3. ИНСТРУКЦИИ ПО ГЕНЕРАЦИИ ПРОМПТА ДЛЯ MIDJOURNEY V7:
1.  **Основа:** Используй `first_frame_description` как смысловую базу.
2.  **Детализация и Точность V7:** Добавь конкретные детали, текстуры, элементы окружения. Используй сильные, образные английские слова. Опиши композицию. Учитывай, что V7 лучше понимает промпты и генерирует более качественные текстуры и детали.
3.  **Эмоция/Дуга:** Убедись, что промпт передает `chosen_emotional_core`.
4.  **Драйвер:** Отрази `chosen_driver` (перспективу, метафору, подход) в описании.
5.  **Эстетика:** Если есть `style_keywords`, интегрируй их, **избегая имен**.
6.  **Тех. Параметры:** Добавь в конец `--ar {aspect_ratio} --v 7{style_parameter_str}`. (Версия изменена на 7!)
7.  **Запрещенные Слова:** Избегай чувствительных слов (насилие, интимность и т.п.).
8.  **Структура:** Единая строка на английском.
9.  **ПОДДЕРЖКА АНИМАЦИИ:** Проанализируй `script`. Если он предполагает появление или значительное изменение объекта (например, "reveal a city"), постарайся **очень тонко намекнуть** на его присутствие или начальное состояние в описании первого кадра для MJ (например, "faint silhouette of a city in the distant haze", "object glows faintly from within"). Это даст Runway визуальную основу. **Намек должен быть едва заметным**.

## 4. ФОРМАТ ВЫВОДА:
Предоставь результат СТРОГО в формате JSON с одним ключом: "final_mj_prompt".
(Пример: `{{{{ "final_mj_prompt": "Extreme close-up on a cracked pocket watch... distant haze subtly hints at towering structures... --ar 16:9 --v 7" }}}}`)
JSON:
"""
# --- КОНЕЦ ПРОМПТА ДЛЯ ШАГА 6A ---

# --- ПРОМПТ ШАГА 6B (Адаптация Сценария для Runway с фокусом на одном движении) ---
PROMPT_STEP6B = """
# ЗАДАЧА: Адаптация Сценария для Промпта Runway (Image-to-Video) - Тонкая Настройка

Ты — AI-промпт инженер, эксперт по Runway ML (модель Image-to-Video). Твоя задача — адаптировать Сценарий видео в **максимально выполнимый, конкретный и сфокусированный** текстовый промпт для Runway, который будет использоваться ВМЕСТЕ с изображением первого кадра. Цель - добиться предсказуемого движения камеры.

ИСХОДНЫЕ ДАННЫЕ:

Сценарий Видео (на английском):
{script}

Креативный Бриф (для контекста стиля и настроения):
{creative_brief_json}

Входной Текст (для контекста темы):
{input_text}

ТРЕБОВАНИЯ:
- Создай текстовый промпт для Runway (на английском языке).
- Промпт должен описывать ДЕЙСТВИЕ и ДВИЖЕНИЕ КАМЕРЫ, НАЧИНАЯ с первого кадра.
- **ФОКУС НА ОДНОМ ДВИЖЕНИИ КАМЕРЫ:** Выбери **ОДНО основное, простое движение камеры** (pan left/right, zoom in/out, tilt up/down, dolly forward/backward) из сценария или наиболее подходящее к сцене. Опиши его **четко, с указанием скорости** (slowly, rapidly). Это движение должно быть главным акцентом промпта. **Сделай акцент на движении камеры, повторив команду или усилив ее описание, если необходимо.**
- **УПРОЩЕНИЕ ДИНАМИКИ СЦЕНЫ:** Если камера активно движется, **минимизируй** описание сложных одновременных действий других объектов. Можно описать статичные элементы или очень простые изменения (например, "shadows slowly lengthen", "light source subtly brightens").
- **ДОСТИЖИМЫЕ ДЕЙСТВИЯ:** Описывай действия, которые модель может выполнить. **Избегай инструкций, нарушающих физику**. Вместо этого опиши *визуальный эффект* (например, "surreal shimmering effect").
- **ПОЯВЛЕНИЕ ОБЪЕКТОВ ОГРАНИЧЕНО:** Запрашивай появление/изменение объекта ("reveal...") только если можно предположить, что в исходном кадре есть его **визуальный намек**.
- Учти Эмоциональное Ядро, Драйвер и Эстетический Стиль (через `style_keywords`) для описания атмосферы.
- **ЗАПРЕТ ИМЕН:** **Категорически запрещено** включать имена собственные.
- **ЗАПРЕТ ЗВУКА:** **Категорически запрещено** упоминать звуки. **ТОЛЬКО ВИЗУАЛ**.
- Промпт должен быть единой строкой текста, лаконичным. Длина - до 1000 символов.

ФОРМАТ ОТВЕТА (СТРОГО JSON):
Верни ТОЛЬКО JSON объект с ОДНИМ ключом:
- "final_runway_prompt": строка, содержащая готовый текстовый промпт для Runway (Image-to-Video) на английском языке.

ПРИМЕР JSON ОТВЕТА (с фокусом на одном движении):
{{{{
"final_runway_prompt": "Slow pan left across the ancient, weathered ruins enveloped in mist, the camera continues panning left steadily. Shadows subtly shift on the stone walls. Maintain a mysterious vibe with unusual camera angles and a suspenseful atmosphere throughout the pan."
}}}}

JSON:
"""
# --- КОНЕЦ ПРОМПТА ДЛЯ ШАГА 6B ---

# --- ПРОМПТ ШАГА 6C (Перевод на русский) ---
PROMPT_STEP6C = """
# ЗАДАЧА: Перевод сгенерированных текстов на русский язык
## 1. ТВОЯ РОЛЬ:
Ты - AI Переводчик. Твоя задача - точно перевести предоставленные английские тексты на русский язык.
## 2. ВХОДНЫЕ ДАННЫЕ (АНГЛИЙСКИЙ ЯЗЫК):
### 2.1. Сценарий Видео (`script`):
'''
{script_en}
'''
### 2.2. Описание Первого Кадра (`first_frame_description`):
'''
{frame_description_en}
'''
### 2.3. Финальный Промпт для Midjourney (`final_mj_prompt`):
'''
{mj_prompt_en}
'''
### 2.4. Финальный Промпт для Runway (`final_runway_prompt`):
'''
{runway_prompt_en}
'''
## 3. ИНСТРУКЦИИ ПО ПЕРЕВОДУ:
1.  Переведи каждый из четырех текстов на русский язык, сохраняя смысл и стиль оригинала, насколько это возможно.
2.  Для промптов MJ и Runway постарайся сохранить структуру и ключевые термины, понятные для анализа человеком, даже если прямой перевод звучит немного технически.
## 4. ФОРМАТ ВЫВОДА: **КРИТИЧЕСКИ ВАЖНО!**
Твой ответ ДОЛЖЕН быть **ТОЛЬКО валидным JSON объектом** и ничем иным. Не добавляй никакого текста до или после JSON объекта.
Структура JSON должна быть следующей:
* `script_ru`: строка, перевод сценария.
* `first_frame_description_ru`: строка, перевод описания кадра.
* `final_mj_prompt_ru`: строка, перевод промпта MJ.
* `final_runway_prompt_ru`: строка, перевод промпта Runway.
JSON:
"""
# --- КОНЕЦ ПРОМПТА ДЛЯ ШАГА 6C ---

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
                 if not use_weights or category != 'main':
                     if lines: lines.append("")
                     lines.append(f"  Категория '{category}':")

                 formatted_sublist = format_list_for_prompt(cat_items, use_weights=(use_weights and category == 'main'))
                 if formatted_sublist != "- (Список пуст)":
                     indented_lines = [f"    {line}" if line.strip() else line for line in formatted_sublist.split('\n')]
                     lines.extend(indented_lines)
             else:
                 lines.append(f"* {category}: {cat_items}")
    else: return "- (Неверный формат данных)"
    return "\n".join(lines).strip()


def call_openai(prompt_text: str, model: str = "gpt-4o", temperature: float = 0.7, max_tokens: int = 1500) -> dict | None:
    """
    Выполняет вызов OpenAI API (версии >=1.0) и возвращает распарсенный JSON.
    """
    global openai_client
    if not openai_client:
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
        response = openai_client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Ты - AI ассистент, который строго следует инструкциям пользователя и всегда отвечает ТОЛЬКО в указанном формате (например, JSON), без какого-либо дополнительного текста."},
                {"role": "user", "content": prompt_text}
            ],
            max_tokens=max_tokens,
            temperature=temperature,
            response_format={"type": "json_object"}
        )
        raw_response_text = response.choices[0].message.content.strip() if response.choices and response.choices[0].message else ""
        logger.debug(f"Сырой ответ от OpenAI:\n{raw_response_text}")

        if not raw_response_text:
            logger.error("OpenAI вернул пустой ответ.")
            return None
        try:
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
    except openai.AuthenticationError as e:
        logger.exception(f"Ошибка аутентификации OpenAI: {e}")
    except openai.RateLimitError as e:
        logger.exception(f"Превышен лимит запросов OpenAI: {e}")
    except openai.APIConnectionError as e:
         logger.exception(f"Ошибка соединения с API OpenAI: {e}")
    except openai.APIStatusError as e:
         logger.exception(f"Ошибка статуса API OpenAI: {e.status_code} - {e.response}")
    except openai.OpenAIError as e:
        logger.exception(f"Произошла ошибка API OpenAI: {e}")
    except Exception as e:
        logger.exception(f"Произошла непредвиденная ошибка при вызове OpenAI: {e}")
    return None

def save_json_output(output_path: Path, data: dict):
    """Сохраняет словарь в JSON файл."""
    try:
        ensure_directory_exists(str(output_path.parent))
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logger.info(f"Результаты успешно сохранены в файл {output_path.name}.")
    except Exception as e:
        logger.error(f"Не удалось сохранить JSON файл {output_path}: {e}")


# --- Функция для запуска действия PiAPI (Upscale/Variation) ---
def trigger_piapi_action(original_task_id: str, action: str, api_key: str, endpoint: str) -> dict | None:
    """Запускает действие (например, upscale) для задачи Midjourney через PiAPI."""
    if not api_key or not endpoint or not original_task_id or not action:
        logger.error("Недостаточно данных для запуска действия PiAPI (trigger_piapi_action).")
        return None

    task_type = "upscale" if "upscale" in action else "variation" if "variation" in action else None
    if not task_type:
        logger.error(f"Не удалось определить тип задачи для действия '{action}'. Поддерживаются 'upscale' и 'variation'.")
        return None

    index_match = re.search(r'\d+$', action)
    if not index_match:
        logger.error(f"Не удалось извлечь индекс из действия '{action}'. Ожидался формат типа 'upscale1'.")
        return None
    index_str = index_match.group(0)

    payload = {
        "model": "midjourney",
        "task_type": task_type,
        "input": {
            "origin_task_id": original_task_id,
            "index": index_str
        }
    }

    headers = {'X-API-Key': api_key, 'Content-Type': 'application/json'}
    request_time = datetime.now(timezone.utc)

    logger.info(f"Отправка запроса на действие '{action}' (тип: {task_type}) для задачи {original_task_id} на {endpoint}...")
    logger.debug(f"Payload действия PiAPI (согласно документации): {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(endpoint, headers=headers, json=payload, timeout=60)
        logger.debug(f"Ответ от PiAPI Action Trigger: Status={response.status_code}, Body={response.text[:500]}")
        response.raise_for_status()
        result = response.json()

        new_task_id = result.get("result", result.get("task_id"))
        if not new_task_id and isinstance(result.get("data"), dict):
            new_task_id = result.get("data", {}).get("task_id")

        if new_task_id:
            timestamp_str = request_time.isoformat()
            logger.info(f"✅ Получен НОВЫЙ task_id для действия '{action}': {new_task_id} (запрошено в {timestamp_str})")
            return {"task_id": str(new_task_id), "requested_at_utc": timestamp_str}
        else:
            logger.warning(f"Ответ API на действие '{action}' не содержит нового task_id. Проверяем статус исходной задачи или детали ответа: {result}")
            return None
    except requests.exceptions.Timeout:
        logger.error(f"❌ Таймаут при запросе на действие '{action}' к PiAPI: {endpoint}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Ошибка сети/запроса на действие '{action}' к PiAPI: {e}")
        if e.response is not None:
            logger.error(f"    Статус: {e.response.status_code}, Тело: {e.response.text}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ Ошибка JSON ответа на действие '{action}' от PiAPI: {e}. Ответ: {response.text[:500]}")
        return None
    except Exception as e:
        logger.exception(f"❌ Неизвестная ошибка при запуске действия '{action}' PiAPI: {e}")
        return None
# --- КОНЕЦ ФУНКЦИИ ---

# --- Глобальная переменная для клиента OpenAI ---
openai_client = None

# --- Основная Логика Пайплайна ---

def run_iid_pipeline(input_json_path: Path, config_dir: Path):
    """Выполняет полный пайплайн ИИД + Генерация Медиа для одного входного файла."""
    global openai_client
    logger.info(f"===== Запуск полного пайплайна для файла: {input_json_path.name} =====")

    # --- 1. Загрузка конфигураций и входных данных ---
    creative_config_path = config_dir / "creative_config.json"
    main_config_path = config_dir / "config.json"

    creative_config = load_json_config(creative_config_path)
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

    if not openai_client:
        try:
            if hasattr(openai, 'OpenAI'):
                 openai_client = openai.OpenAI(api_key=api_key)
                 logger.info("Клиент OpenAI (>1.0) успешно инициализирован.")
            else:
                 logger.error("Используется старая версия библиотеки OpenAI (<1.0). Обновите: pip install --upgrade openai")
                 return False
        except Exception as e:
             logger.exception("Не удалось инициализировать клиент OpenAI.")
             return False

    openai_model = main_config_manager.get("OPENAI_SETTINGS.model", "gpt-4o")
    temp_step1 = 0.7; temp_step2 = 0.7; temp_step3 = 0.7; temp_step5 = 0.7; temp_step6 = 0.7
    temp_translate = 0.3

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
    script = None
    creative_brief = None

    # --- Переменные для перевода ---
    script_ru = None
    frame_description_ru = None
    final_mj_prompt_ru = None
    final_runway_prompt_ru = None
    translations = None

    # --- Проверка на существующие промпты ---
    creative_output_data = input_data.get('creative_output')
    has_existing_prompts = (
        isinstance(creative_output_data, dict) and
        all(k in creative_output_data for k in ['final_mj_prompt', 'final_runway_prompt', 'generated_frame_description', 'generated_script', 'creative_brief']) and
        all(creative_output_data.get(k) for k in ['final_mj_prompt', 'final_runway_prompt', 'generated_frame_description', 'generated_script', 'creative_brief'])
    )
    logger.debug(f"Проверка наличия существующих промптов (has_existing_prompts): {has_existing_prompts}")

    if has_existing_prompts:
        logger.warning("Найдены существующие и полные данные в 'creative_output'. Пропускаем шаги генерации промптов (1-6).")
        creative_brief = creative_output_data.get('creative_brief')
        script = creative_output_data.get('generated_script')
        frame_description_for_mj = creative_output_data.get('generated_frame_description')
        final_mj_prompt = creative_output_data.get('final_mj_prompt')
        final_runway_prompt = creative_output_data.get('final_runway_prompt')

        script_ru = creative_output_data.get('script_ru')
        frame_description_ru = creative_output_data.get('first_frame_description_ru')
        final_mj_prompt_ru = creative_output_data.get('final_mj_prompt_ru')
        final_runway_prompt_ru = creative_output_data.get('final_runway_prompt_ru')

        if not all([creative_brief, script, frame_description_for_mj, final_mj_prompt, final_runway_prompt]):
             logger.error("Ошибка чтения основных существующих данных из 'creative_output'. Запускаем регенерацию.")
             has_existing_prompts = False
             final_mj_prompt = None; final_runway_prompt = None
             frame_description_for_mj = None; script = None; creative_brief = None
             script_ru = None; frame_description_ru = None; final_mj_prompt_ru = None; final_runway_prompt_ru = None

    # --- Блок генерации промптов ---
    if not has_existing_prompts:
        enable_russian_translation = main_config_manager.get("WORKFLOW.enable_russian_translation", False)
        logger.info(f"Генерация русского перевода {'ВКЛЮЧЕНА' if enable_russian_translation else 'ОТКЛЮЧЕНА'} (WORKFLOW.enable_russian_translation)")

        logger.info("Запуск генерации промптов (Шаги 1-6)...")
        if creative_output_data:
             logger.warning("Данные в 'creative_output' были неполные или некорректные.")

        # --- Шаги 1-5 ---
        logger.info("--- Шаг 1: Выбор Эмоционального Ядра ---")
        prompt1_text = PROMPT_STEP1.format(input_text=input_text, moods_list_str=moods_list_str, arcs_list_str=arcs_list_str)
        core_brief = call_openai(prompt1_text, model=openai_model, temperature=temp_step1)
        if not core_brief or not all(k in core_brief for k in ["chosen_type", "chosen_value", "justification"]): logger.error(f"Шаг 1 не удался: {core_brief}."); return False

        logger.info("--- Шаг 2: Выбор Основного Креативного Драйвера ---")
        prompt2_text = PROMPT_STEP2.format(input_text=input_text, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), prompts_list_str=prompts_list_str, perspectives_list_str=perspectives_list_str, metaphors_list_str=metaphors_list_str)
        driver_brief = call_openai(prompt2_text, model=openai_model, temperature=temp_step2)
        if not driver_brief or not all(k in driver_brief for k in ["chosen_driver_type", "chosen_driver_value", "justification"]): logger.error(f"Шаг 2 не удался: {driver_brief}."); return False

        logger.info("--- Шаг 3: Выбор Эстетического Фильтра ---")
        prompt3_text = PROMPT_STEP3.format(input_text=input_text, chosen_emotional_core_json=json.dumps(core_brief, ensure_ascii=False, indent=2), chosen_driver_json=json.dumps(driver_brief, ensure_ascii=False, indent=2), directors_list_str=directors_list_str, artists_list_str=artists_list_str)
        aesthetic_brief = call_openai(prompt3_text, model=openai_model, temperature=temp_step3)

        valid_step3 = False
        if isinstance(aesthetic_brief, dict):
            style_needed = aesthetic_brief.get("style_needed", False)
            base_keys_exist = all(k in aesthetic_brief for k in ["style_needed", "chosen_style_type", "chosen_style_value", "style_keywords", "justification"])
            if base_keys_exist:
                if not style_needed:
                    if (aesthetic_brief.get("chosen_style_type") is None and aesthetic_brief.get("chosen_style_value") is None and aesthetic_brief.get("style_keywords") is None and aesthetic_brief.get("justification") is None): valid_step3 = True
                    else: logger.warning(f"Шаг 3: style_needed=false, но ключи не null: {aesthetic_brief}. Исправляем."); aesthetic_brief.update({"chosen_style_type":None, "chosen_style_value":None, "style_keywords":None, "justification":None}); valid_step3 = True
                else:
                    if (aesthetic_brief.get("chosen_style_type") and aesthetic_brief.get("chosen_style_value") and isinstance(aesthetic_brief.get("style_keywords"), list) and aesthetic_brief.get("justification")): valid_step3 = True
                    else: logger.error(f"Шаг 3: style_needed=true, но значения некорректны. {aesthetic_brief}")
            else: logger.error(f"Шаг 3: Отсутствуют ключи: {aesthetic_brief}")
        else: logger.error(f"Шаг 3: Ответ не словарь: {aesthetic_brief}")
        if not valid_step3: logger.error("Шаг 3 не удался."); return False

        # --- ПРИНУДИТЕЛЬНОЕ УСТАНОВЛЕНИЕ СТИЛЯ ---
        logger.warning("!!! ВНИМАНИЕ: Принудительное установление стиля 'Pablo Picasso (Кубизм)' для теста !!!")
        aesthetic_brief = {
            "style_needed": True,
            "chosen_style_type": "artist",
            "chosen_style_value": "Пабло Пикассо (Кубизм)",
            "style_keywords": ["fragmented objects", "multiple viewpoints", "geometric shapes", "abstract composition"],
            "justification": "Принудительный стиль для теста Пикассо."
        }
        # --- КОНЕЦ ПРИНУДИТЕЛЬНОГО УСТАНОВЛЕНИЯ СТИЛЯ ---


        creative_brief = {"core": core_brief, "driver": driver_brief, "aesthetic": aesthetic_brief}
        logger.info("--- Шаг 4: Креативный Бриф Собран (стиль может быть перезаписан) ---")
        logger.debug(f"Бриф: {json.dumps(creative_brief, ensure_ascii=False, indent=2)}")

        logger.info("--- Шаг 5: Генерация Сценария и Описания Кадра ---")
        prompt5_text = PROMPT_STEP5.format(input_text=input_text, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2))
        script_frame_data = call_openai(prompt5_text, model=openai_model, temperature=temp_step5, max_tokens=1000)
        if not script_frame_data or not all(k in script_frame_data for k in ["script", "first_frame_description"]): logger.error(f"Шаг 5 не удался: {script_frame_data}."); return False
        script = script_frame_data["script"]
        frame_description_for_mj = script_frame_data["first_frame_description"]
        logger.info(f"Сценарий: {script[:100]}...")
        logger.info(f"Описание кадра: {frame_description_for_mj[:100]}...")

        # --- Шаги 6a, 6b ---
        logger.info("--- Шаг 6a: Адаптация Описания под Midjourney V7 ---")
        mj_params = main_config_manager.get("IMAGE_GENERATION", {})
        aspect_ratio = "16:9"; version = "7" # Используем V7
        logger.info(f"Параметры MJ: --ar {aspect_ratio} --v {version}")
        style = mj_params.get("midjourney_style", None)
        mj_parameters_json_for_prompt = json.dumps({"aspect_ratio": aspect_ratio, "version": version, "style": style}, ensure_ascii=False)
        style_parameter_str = f" --style {style}" if style else ""
        prompt6a_text = PROMPT_STEP6A.format(
            first_frame_description=frame_description_for_mj,
            creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2),
            script=script,
            input_text=input_text,
            mj_parameters_json=mj_parameters_json_for_prompt,
            aspect_ratio=aspect_ratio,
            version=version,
            style_parameter_str=style_parameter_str
        )
        mj_prompt_data = call_openai(prompt6a_text, model=openai_model, temperature=temp_step6, max_tokens=1000)
        if not mj_prompt_data or "final_mj_prompt" not in mj_prompt_data: logger.error(f"Шаг 6a не удался: {mj_prompt_data}."); return False
        final_mj_prompt = mj_prompt_data["final_mj_prompt"]
        logger.info(f"MJ промпт (V7): {final_mj_prompt}")

        logger.info("--- Шаг 6b: Адаптация Сценария под Runway (Тонкая Настройка) ---")
        prompt6b_text = PROMPT_STEP6B.format(script=script, creative_brief_json=json.dumps(creative_brief, ensure_ascii=False, indent=2), input_text=input_text)
        runway_prompt_data = call_openai(prompt6b_text, model=openai_model, temperature=temp_step6, max_tokens=1000)
        if not runway_prompt_data or "final_runway_prompt" not in runway_prompt_data: logger.error(f"Шаг 6b не удался: {runway_prompt_data}."); return False
        final_runway_prompt = runway_prompt_data["final_runway_prompt"]
        logger.info(f"Runway промпт (финальный, настроенный): {final_runway_prompt}")

        # --- ШАГ 6c: Перевод на русский (если включено) ---
        if enable_russian_translation:
            logger.info("--- Шаг 6c: Перевод результатов на русский язык ---")
            translations = {}
            if all([script, frame_description_for_mj, final_mj_prompt, final_runway_prompt]):
                prompt6c_text = PROMPT_STEP6C.format(script_en=script, frame_description_en=frame_description_for_mj, mj_prompt_en=final_mj_prompt, runway_prompt_en=final_runway_prompt)
                translations = call_openai(prompt6c_text, model=openai_model, temperature=temp_translate, max_tokens=2000)
                if translations:
                    script_ru = translations.get("script_ru")
                    frame_description_ru = translations.get("first_frame_description_ru")
                    final_mj_prompt_ru = translations.get("final_mj_prompt_ru")
                    final_runway_prompt_ru = translations.get("final_runway_prompt_ru")
                    logger.info("✅ Перевод на русский выполнен.")
                    logger.debug(f"Перевод сценария: {script_ru[:100]}...")
                else:
                    logger.error("Шаг 6c (Перевод) не удался.")
            else:
                 logger.error("Недостаточно данных для запуска Шага 6c (Перевод).")
        else:
             logger.info("Шаг 6c (Перевод) пропущен согласно настройке.")
        # --- КОНЕЦ ШАГА 6c ---


        logger.info(f"Обновление файла {input_json_path.name} сгенерированными данными...")
        try:
            with open(input_json_path, 'r', encoding='utf-8') as f: current_input_data = json.load(f)
            # --- ОБНОВЛЕННАЯ СТРУКТУРА creative_output ---
            creative_output_payload = {
                "creative_brief": creative_brief, # Будет содержать принудительный стиль
                "generated_script": script,
                "generated_frame_description": frame_description_for_mj,
                "final_mj_prompt": final_mj_prompt,
                "final_runway_prompt": final_runway_prompt,
                "prompts_processed_at_utc": datetime.now(timezone.utc).isoformat()
            }
            if enable_russian_translation and translations:
                 creative_output_payload["script_ru"] = script_ru
                 creative_output_payload["first_frame_description_ru"] = frame_description_ru
                 creative_output_payload["final_mj_prompt_ru"] = final_mj_prompt_ru
                 creative_output_payload["final_runway_prompt_ru"] = final_runway_prompt_ru

            current_input_data['creative_output'] = creative_output_payload
            # --- КОНЕЦ ОБНОВЛЕННОЙ СТРУКТУРЫ ---
            save_json_output(input_json_path, current_input_data)
            logger.info("✅ Промпты, бриф (и переводы, если включены) успешно добавлены/обновлены в исходном JSON.")
        except Exception as e:
            logger.error(f"Не удалось обновить исходный JSON файл {input_json_path}: {e}")

    # --- Конец блока генерации промптов ---


    # --- Проверка наличия промптов перед генерацией медиа ---
    if not final_mj_prompt or not final_runway_prompt: logger.error("Отсутствуют финальные промпты MJ/Runway."); return False
    if not frame_description_for_mj: logger.warning("Отсутствует описание кадра.")
    if not script: logger.warning("Отсутствует сценарий.")


    logger.info("DEBUG: Промпты готовы (считаны из JSON или сгенерированы).")
    logger.info("DEBUG: ПЕРЕХОД К БЛОКУ ГЕНЕРАЦИИ МЕДИА...")

    # --- 7. БЛОК: Генерация Медиа ---
    logger.info("===== Начало генерации медиа =====")
    logger.info("DEBUG: Инициализация переменных для медиа...")
    mj_api_key = os.getenv("MIDJOURNEY_API_KEY")
    runway_api_key = os.getenv("RUNWAY_API_KEY")
    mj_init_endpoint = main_config_manager.get("API_KEYS.midjourney.endpoint")
    mj_fetch_endpoint = main_config_manager.get("API_KEYS.midjourney.task_endpoint")

    final_image_path_for_runway = None
    image_source = "None"

    output_base_name = input_json_path.stem
    output_dir = input_json_path.parent

    # --- Проверка на наличие готового PNG и промптов ---
    if has_existing_prompts:
        local_png_path = output_dir / f"{output_base_name}.png"
        logger.info(f"Проверка наличия локального файла изображения: {local_png_path}")
        png_exists = local_png_path.is_file()
        logger.debug(f"Результат проверки local_png_path.is_file(): {png_exists}")
        if png_exists:
            logger.warning(f"Найден локальный файл {local_png_path.name} и существующие промпты.")
            logger.warning("Пропускаем генерацию изображения (MJ), используем локальный файл для Runway.")
            final_image_path_for_runway = local_png_path
            image_source = "Local Pre-existing File"
        else:
            logger.info(f"Локальный файл {local_png_path.name} не найден. Продолжаем с генерацией изображения (MJ)...")

    # --- Запуск MJ ---
    if final_image_path_for_runway is None:
        logger.info("Запуск генерации изображения (MJ)...")

        if not mj_api_key: logger.warning("Ключ MJ не найден. Шаг MJ пропущен.")
        if not mj_init_endpoint or not mj_fetch_endpoint: logger.error("Эндпоинты MJ не найдены."); mj_api_key = None

        # --- 7.1 Midjourney ---
        mj_image_path_temp = None

        if mj_api_key and final_mj_prompt:
            logger.info("--- Шаг 7.1a: Запуск задачи Imagine Midjourney ---")
            imagine_task_id = None; upscale_task_id = None; final_image_url = None
            imagine_task_info = initiate_midjourney_task(prompt=final_mj_prompt, config=main_config_manager, api_key=mj_api_key, endpoint=mj_init_endpoint)

            if imagine_task_info and imagine_task_info.get("task_id"):
                imagine_task_id = imagine_task_info["task_id"]
                logger.info(f"Задача Imagine запущена, ID: {imagine_task_id}")
                logger.info(f"Начало опроса статуса Imagine задачи {imagine_task_id}...")
                imagine_result = None; start_time = time.time(); poll_interval = 15; poll_timeout = 300
                while time.time() - start_time < poll_timeout:
                    status_result = fetch_piapi_status(imagine_task_id, mj_api_key, mj_fetch_endpoint)
                    if status_result:
                        status = status_result.get("status"); progress = status_result.get("progress", "N/A")
                        logger.info(f"Статус Imagine {imagine_task_id}: {status} (Прогресс: {progress})")
                        if status in ["finished", "completed", "success"]: imagine_result = status_result; break
                        elif status in ["failed", "error"]: logger.error(f"❌ Imagine {imagine_task_id} failed: {status_result.get('error', 'Нет деталей')}"); imagine_result = status_result; break
                        elif status in ["pending", "running", "waiting", "queued", "processing"]: pass
                        else: logger.warning(f"Неизвестный статус Imagine {imagine_task_id}: {status}")
                    else: logger.warning(f"Не удалось получить статус Imagine {imagine_task_id}. Повтор через {poll_interval} сек.")
                    time.sleep(poll_interval)
                else: logger.warning(f"⏰ Таймаут ({poll_timeout} сек) ожидания Imagine {imagine_task_id}.")

                if imagine_result and imagine_result.get("status") in ["finished", "completed", "success"]:
                    available_actions = imagine_result.get("task_result", {}).get("actions", [])
                    action_to_trigger = None
                    for i in range(1, 5):
                        action = f"upscale{i}"
                        if action in available_actions:
                            action_to_trigger = action
                            break
                    if not action_to_trigger and available_actions:
                        action_to_trigger = available_actions[0]
                        logger.warning(f"Upscale не найден, используем первое доступное действие: {action_to_trigger}")
                    elif not available_actions:
                        logger.error(f"Нет доступных действий (upscale/variation) для Imagine {imagine_task_id}.")
                        action_to_trigger = None


                    if action_to_trigger:
                        logger.info(f"--- Шаг 7.1b: Запуск задачи '{action_to_trigger}' для {imagine_task_id} ---")
                        upscale_task_info = trigger_piapi_action(original_task_id=imagine_task_id, action=action_to_trigger, api_key=mj_api_key, endpoint=mj_init_endpoint)
                        if upscale_task_info and upscale_task_info.get("task_id"):
                            upscale_task_id = upscale_task_info["task_id"]
                            logger.info(f"Задача '{action_to_trigger}' запущена, ID: {upscale_task_id}")
                            logger.info(f"Начало опроса статуса задачи {upscale_task_id}...")
                            upscale_result = None; start_time_upscale = time.time(); poll_timeout_upscale = 300
                            while time.time() - start_time_upscale < poll_timeout_upscale:
                                status_result_upscale = fetch_piapi_status(upscale_task_id, mj_api_key, mj_fetch_endpoint)
                                if status_result_upscale:
                                    status_upscale = status_result_upscale.get("status"); progress_upscale = status_result_upscale.get("progress", "N/A")
                                    logger.info(f"Статус {action_to_trigger} {upscale_task_id}: {status_upscale} (Прогресс: {progress_upscale})")
                                    if status_upscale in ["finished", "completed", "success"]: upscale_result = status_result_upscale; break
                                    elif status_upscale in ["failed", "error"]: logger.error(f"❌ {action_to_trigger} {upscale_task_id} failed!"); upscale_result = status_result_upscale; break
                                    elif status_upscale in ["pending", "running", "waiting", "queued", "processing"]: pass
                                    else: logger.warning(f"Неизвестный статус {action_to_trigger} {upscale_task_id}: {status_upscale}")
                                else: logger.warning(f"Не удалось получить статус {action_to_trigger} {upscale_task_id}. Повтор через {poll_interval} сек.")
                                time.sleep(poll_interval)
                            else: logger.warning(f"⏰ Таймаут ({poll_timeout_upscale} сек) ожидания {action_to_trigger} {upscale_task_id}.")

                            if upscale_result and upscale_result.get("status") in ["finished", "completed", "success"]:
                                 task_result_data_upscale = upscale_result.get("task_result", upscale_result.get("data", upscale_result))
                                 if isinstance(task_result_data_upscale, dict):
                                     possible_keys_upscale = ["image_url", "imageUrl", "discord_image_url", "url"]
                                     for key in possible_keys_upscale:
                                         value = task_result_data_upscale.get(key)
                                         if isinstance(value, str) and value.startswith("http"): final_image_url = value; logger.info(f"Найден URL MJ в ключе '{key}'."); break
                                 if not final_image_url: logger.error(f"Не удалось извлечь URL MJ из результата {action_to_trigger}: {json.dumps(upscale_result, indent=2)}")
                            elif upscale_result: logger.error(f"{action_to_trigger} {upscale_task_id} не завершилась успешно: статус {upscale_result.get('status')}")
                            else: logger.error(f"Не получен результат {action_to_trigger} {upscale_task_id} (таймаут).")
                        else: logger.error(f"Не удалось запустить {action_to_trigger}. Ответ: {upscale_task_info}")
                elif imagine_result: logger.error(f"Imagine {imagine_task_id} не завершилась успешно: статус {imagine_result.get('status')}")
            else: logger.error(f"Не удалось запустить Imagine. Ответ: {imagine_task_info}")

            if final_image_url:
                logger.info(f"Попытка скачивания изображения Midjourney: {final_image_url}")
                img_extension = ".png"
                try:
                    from urllib.parse import urlparse
                    parsed_url_path = Path(urlparse(final_image_url).path)
                    if parsed_url_path.suffix and parsed_url_path.suffix.lower() in ['.png', '.jpg', '.jpeg', '.webp']: img_extension = parsed_url_path.suffix.lower()
                except Exception as url_parse_err: logger.warning(f"Не удалось определить расширение из URL MJ {final_image_url}: {url_parse_err}")
                mj_image_path_temp = output_dir / f"{output_base_name}_mj_final{img_extension}"
                if download_image(final_image_url, str(mj_image_path_temp), logger_instance=logger):
                    logger.info(f"✅ Финальное изображение Midjourney сохранено: {mj_image_path_temp}")
                    final_image_path_for_runway = mj_image_path_temp
                    image_source = "Midjourney"
                else: logger.error(f"Не удалось скачать MJ с {final_image_url}"); mj_image_path_temp = None
            else: logger.error("Финальный URL MJ не был получен или upscale/variation не удался.")

        elif not mj_api_key: logger.warning("Пропуск MJ: нет API ключа.")
        elif not final_mj_prompt: logger.warning("Пропуск MJ: нет промпта.")

    # --- Конец проверки if final_image_path_for_runway is None: ---

    if final_image_path_for_runway is not None:
        logger.info(f"Изображение для Runway успешно получено из источника: {image_source}.")
    else:
        logger.error("Не удалось получить изображение ни из локального файла, ни от Midjourney.")

    # --- 7.2 Runway ---
    runway_video_path = None
    if runway_api_key and final_runway_prompt and final_image_path_for_runway and final_image_path_for_runway.is_file():
        logger.info(f"--- Шаг 7.2: Запуск Runway (Источник изображения: {image_source}) ---")
        logger.info(f"Используется изображение: {final_image_path_for_runway}")
        logger.info(f"Используется промпт Runway (финальный): {final_runway_prompt}")

        if not RUNWAY_SDK_AVAILABLE: logger.error("SDK RunwayML недоступен.")
        else:
            logger.info("Вызов generate_runway_video (без доп. параметров)...")
            video_url = generate_runway_video(
                image_path=str(final_image_path_for_runway),
                script=final_runway_prompt,
                config=main_config_manager,
                api_key=runway_api_key
            )
            if video_url:
                logger.info(f"Получен URL видео Runway: {video_url}")
                runway_video_path = output_dir / f"{output_base_name}_runway.mp4"
                logger.info(f"Попытка скачивания видео Runway в {runway_video_path}...")
                if download_video(video_url, str(runway_video_path), logger_instance=logger):
                    logger.info(f"✅ Видео Runway сохранено: {runway_video_path}")
                else: logger.error(f"Не удалось скачать видео Runway с {video_url}"); runway_video_path = None
            else: logger.error("Не удалось сгенерировать или получить URL видео Runway.")

    elif not runway_api_key: logger.warning("Пропуск Runway: нет API ключа.")
    elif not final_runway_prompt: logger.warning("Пропуск Runway: нет промпта.")
    elif not final_image_path_for_runway or not final_image_path_for_runway.is_file(): logger.warning(f"Пропуск Runway: нет входного изображения ({image_source}).")


    # --- 8. Завершение ---
    if final_image_path_for_runway and runway_video_path:
         logger.info(f"===== Пайплайн успешно завершен для {input_json_path.name} (Изображение: {image_source}, Видео: Runway) ====="); return True
    elif final_image_path_for_runway:
         logger.warning(f"===== Пайплайн завершен для {input_json_path.name} (Только Изображение: {image_source}, Видео не сгенерировано) ====="); return True
    else:
         logger.error(f"===== Пайплайн завершен с ошибками для {input_json_path.name} (Изображение и Видео не сгенерированы) ====="); return False

# --- Точка входа ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Запускает ИИД пайплайн и генерацию медиа.")
    parser.add_argument("input_file", help="Путь к входному JSON файлу (например, C:\\Users\\boyar\\777\\555\\55\\20250220-1331.json)")
    script_dir = Path(__file__).parent
    project_base_dir = script_dir.parent
    default_config_dir = project_base_dir / "config"
    parser.add_argument("-c", "--config_dir", default=str(default_config_dir), help=f"Путь к папке с файлами конфигурации (по умолчанию: {default_config_dir})")
    args = parser.parse_args()
    input_file_path = Path(args.input_file).resolve()
    config_dir_path = Path(args.config_dir).resolve()

    print(f"--- Проверка путей ---")
    print(f"Входной файл (абсолютный): {input_file_path}")
    print(f"Папка конфигурации (абсолютная): {config_dir_path}")
    print(f"Текущая рабочая директория: {Path.cwd()}")
    print(f"Содержимое sys.path: {sys.path}")
    if not input_file_path.is_file(): print(f"Ошибка: Входной файл не найден: {input_file_path}"); logger.critical(f"Входной файл не найден: {input_file_path}"); sys.exit(1)
    if not config_dir_path.is_dir(): print(f"Ошибка: Папка конфигурации не найдена: {config_dir_path}"); logger.critical(f"Папка конфигурации не найдена: {config_dir_path}"); sys.exit(1)
    if not (config_dir_path / "config.json").is_file(): print(f"Ошибка: Основной config.json не найден в {config_dir_path}"); logger.critical(f"Основной config.json не найден в {config_dir_path}"); sys.exit(1)
    if not (config_dir_path / "creative_config.json").is_file(): print(f"Ошибка: creative_config.json не найден в {config_dir_path}"); logger.critical(f"creative_config.json не найден в {config_dir_path}"); sys.exit(1)
    print(f"--- Проверка путей завершена успешно ---")

    success = False
    try:
        success = run_iid_pipeline(input_file_path, config_dir_path)
    except Exception as e:
         logger.critical(f"Неперехваченная ошибка в run_iid_pipeline: {e}", exc_info=True)
         success = False

    exit_code = 0 if success else 1
    logger.info(f"--- Завершение iid_local_tester.py с кодом выхода: {exit_code} ---")
    sys.exit(exit_code)

