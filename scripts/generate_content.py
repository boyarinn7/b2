import json
import os
import sys
import requests
import openai
import textstat
import spacy
import re

# Добавляем путь к директории modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))

from datetime import datetime
from logger import get_logger
from error_handler import handle_error
from utils import ensure_directory_exists
from config_manager import ConfigManager

# === Инициализация ===
logger = get_logger("generate_content")
config = ConfigManager()

# === Настройка OpenAI API ===
try:
    openai.api_key = config.get('API_KEYS.openai.api_key')
    openai_model = config.get('API_KEYS.openai.model', 'gpt-4')
    if not openai.api_key:
        raise ValueError("❌ API ключ OpenAI не найден в конфигурации.")
    logger.info("✅ OpenAI API ключ успешно инициализирован.")
    logger.info(f"🔧 Используемая модель OpenAI: {openai_model}")
except Exception as e:
    handle_error("OpenAI Initialization Error", str(e))


class ContentGenerator:
    def __init__(self):
        self.topic_threshold = config.get('GENERATE.topic_threshold', 7)
        self.text_threshold = config.get('GENERATE.text_threshold', 8)
        self.max_attempts = config.get('GENERATE.max_attempts', 3)
        self.adaptation_enabled = config.get('GENERATE.adaptation_enabled', False)
        self.adaptation_params = config.get('GENERATE.adaptation_parameters', {})
        self.content_output_path = config.get('FILE_PATHS.content_output_path', 'core/generated_content.json')
        self.before_critique_path = config.get('FILE_PATHS.before_critique_path', 'core/before_critique.json')
        self.after_critique_path = config.get('FILE_PATHS.after_critique_path', 'core/after_critique.json')

        self.openai_model = config.get('API_KEYS.openai.model', 'gpt-4')
        self.temperature = config.get('API_KEYS.openai.temperature', 0.7)  # Инициализация температуры

    def analyze_seo(self, content):
        """
        SEO-анализ текста через Ubersuggest или SEMrush API.
        Возвращает SEO-метрики, такие как плотность ключевых слов и рекомендации.
        """
        try:
            logger.info("🔍 Выполняется SEO-анализ текста...")
            provider = config.get('EXTERNAL_TOOLS.seo_analysis.provider', 'ubersuggest')
            api_key = config.get('EXTERNAL_TOOLS.seo_analysis.api_key')

            if not api_key:
                raise ValueError("API-ключ для SEO-анализа не найден в конфиге.")

            if provider == 'ubersuggest':
                api_url = "https://app.neilpatel.com/ubersuggest/api"
                payload = {
                    "query": content,
                    "token": api_key,
                    "lang": "ru"
                }
                response = requests.post(api_url, json=payload)
            elif provider == 'semrush':
                api_url = "https://api.semrush.com/"
                payload = {
                    "type": "phrase_organic",
                    "key": api_key,
                    "phrase": content,
                    "database": "ru"
                }
                response = requests.get(api_url, params=payload)
            else:
                raise ValueError("Неподдерживаемый провайдер SEO-анализа.")

            response.raise_for_status()
            seo_results = response.json()
            logger.info(f"✅ Результат SEO-анализа ({provider}): {seo_results}")
            return seo_results

        except requests.exceptions.HTTPError as http_err:
            handle_error("SEO Analysis HTTP Error", f"HTTP error occurred: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            handle_error("SEO Analysis Connection Error", f"Connection error occurred: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            handle_error("SEO Analysis Timeout Error", f"Timeout error occurred: {timeout_err}")
        except ValueError as ve:
            handle_error("SEO Analysis Value Error", str(ve))
        except Exception as e:
            handle_error("SEO Analysis Error", str(e))
        return {}

    def analyze_grammar(self, content):
        """
        Проверка грамматики и стиля текста через LanguageTool API.
        Возвращает список ошибок и предложений.
        """
        try:
            logger.info("🔍 Выполняется проверка грамматики и стиля текста через LanguageTool API...")
            api_key = config.get('EXTERNAL_TOOLS.grammar_check.api_key')
            api_url = "https://api.languagetool.org/v2/check"

            payload = {
                "text": content,
                "language": "ru",
                "apiKey": api_key
            }
            response = requests.post(api_url, data=payload)
            response.raise_for_status()

            grammar_results = response.json()
            logger.info(f"✅ Результат проверки грамматики: {grammar_results}")
            return grammar_results

        except requests.exceptions.RequestException as e:
            handle_error("Grammar Analysis Error", str(e))
        except Exception as e:
            handle_error("Grammar Analysis Error", str(e))
        return {}

    def generate_sarcastic_comment(self, text):
        if not config.get('SARCASM.enabled', False):
            logger.info("🔕 Сарказм отключён в конфигурации.")
            return ""

        prompt = config.get('SARCASM.comment_prompt').format(text=text)
        try:
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=config.get('CONTENT.sarcasm.max_tokens_comment', 100),
                temperature=self.temperature
            )
            comment = response['choices'][0]['message']['content'].strip()
            logger.info("🎩 Саркастический комментарий успешно сгенерирован.")
            return comment
        except Exception as e:
            handle_error("Sarcasm Comment Generation Error", str(e))
            return ""

    def generate_interactive_poll(self, text):
        if not config.get('SARCASM.enabled', False):
            logger.info("🔕 Сарказм отключён в конфигурации.")
            return ""

        prompt = config.get('SARCASM.question_prompt').format(text=text)
        try:
            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=config.get('CONTENT.sarcasm.max_tokens_poll', 150),
                temperature=self.temperature
            )
            poll = response['choices'][0]['message']['content'].strip()
            logger.info("🎭 Интерактивный вопрос успешно сгенерирован.")
            return poll
        except Exception as e:
            handle_error("Sarcasm Poll Generation Error", str(e))
            return ""

    def append_sarcasm_to_post(self, text):
        """
        Добавляет саркастический комментарий и опрос к основному тексту, с четким разделением блоков.
        """
        if not config.get('SARCASM.enabled', False):
            logger.info("🔕 Сарказм отключён в конфигурации.")
            return text

        # Генерация саркастического комментария
        comment = self.generate_sarcastic_comment(text)
        # Генерация интерактивного вопроса
        poll = self.generate_interactive_poll(text)

        # Очистка текста от ненужных разделителей и вставок
        cleaned_text = text.replace("**Текст:**", "").replace("✨ Интересный факт:", "").replace("🎯 Важно:", "")
        cleaned_text = re.sub(r"\*\*.*?\*\*", "", cleaned_text).strip()

        # Формирование финального текста с четкими метками
        final_text = (
            "🔷🔷🔷 **ОСНОВНОЙ ТЕКСТ** 🔷🔷🔷\n"
            f"{cleaned_text.strip()}"
        )

        if comment:
            final_text += (
                "\n\n🔶🔶🔶 **САРКАСТИЧЕСКИЙ КОММЕНТАРИЙ** 🔶🔶🔶\n"
                f"{comment.strip()}"
            )
        if poll:
            final_text += (
                "\n\n🔸🔸🔸 **ИНТЕРАКТИВНЫЙ ВОПРОС** 🔸🔸🔸\n"
                f"{poll.strip()}"
            )

        logger.info("✅ Барон Сарказм добавил свой комментарий и интерактивный опрос.")
        return final_text

    def analyze_topic_generation(self):
        """
        Анализ генерации тем на основе обратной связи, архива и фокусов.
        """
        try:
            logger.info("🔍 Анализ генерации тем: обратная связь, архив и последние фокусы...")

            # === 1. Загрузка обратной связи ===
            feedback_path = config.get('FILE_PATHS.feedback_file', 'core/data/feedback.json')
            if os.path.exists(feedback_path):
                with open(feedback_path, 'r', encoding='utf-8') as file:
                    feedback_data = json.load(file)
                positive_feedback_topics = [entry['topic'] for entry in feedback_data if entry.get('rating', 0) >= 8]
                logger.info(f"✅ Загружена обратная связь: {len(positive_feedback_topics)} успешных тем.")
            else:
                positive_feedback_topics = []
                logger.warning("⚠️ Файл обратной связи не найден.")

            # === 2. Анализ архива успешных публикаций ===
            archive_folder = config.get('FILE_PATHS.archive_folder', 'core/archive/')
            successful_topics = []
            if os.path.exists(archive_folder):
                for filename in os.listdir(archive_folder):
                    if filename.endswith('.json'):
                        with open(os.path.join(archive_folder, filename), 'r', encoding='utf-8') as file:
                            archive_data = json.load(file)
                        if archive_data.get('success', False):
                            successful_topics.append(archive_data.get('topic', ''))
                logger.info(f"✅ Загружено из архива: {len(successful_topics)} успешных тем.")
            else:
                logger.warning("⚠️ Папка архива не найдена.")

            # === 3. Исключение последних 10 фокусов ===
            last_focus_areas = config.get('CONTENT.topic.focus_areas', [])[-10:]
            logger.info(f"🔄 Последние 10 фокусов: {last_focus_areas}")

            # === 4. Компиляция данных для генерации ===
            combined_focus_areas = list(set(positive_feedback_topics + successful_topics + last_focus_areas))
            if not combined_focus_areas:
                combined_focus_areas = config.get('CONTENT.topic.focus_areas', [])

            logger.info(f"📊 Финальные фокусы для генерации: {combined_focus_areas}")

            return combined_focus_areas

        except Exception as e:
            handle_error("Topic Analysis Error", str(e))
            return config.get('CONTENT.topic.focus_areas', [])

    def adapt_prompts(self):
        """Адаптация промптов на основе параметров из обратной связи."""
        if not self.adaptation_enabled:
            logger.info("🔄 Адаптация промптов отключена.")
            return

        logger.info("🔄 Применяю адаптацию промптов на основе обратной связи...")
        for key, value in self.adaptation_params.items():
            logger.info(f"🔧 Параметр '{key}' обновлён до {value}")

    def generate_topic(self):
        """
        Генерация темы с учётом исключений, обратной связи и архива.
        """
        try:
            # Получаем допустимые фокусы
            valid_focus_areas = self.get_valid_focus_areas()

            # Приоритет обратной связи и архива
            chosen_focus = self.prioritize_focus_from_feedback_and_archive(valid_focus_areas)

            if not chosen_focus:
                raise ValueError("Не удалось выбрать фокус для генерации темы.")

            # Генерация темы
            prompt_template = config.get('CONTENT.topic.prompt_template')
            prompt = prompt_template.format(focus_areas=chosen_focus)

            logger.info("🔄 Запрос к OpenAI для генерации темы...")
            topic = self.request_openai(prompt)

            self.update_focus_tracker(chosen_focus)
            self.save_to_generated_content("topic", {"topic": topic})

            logger.info(f"✅ Тема успешно сгенерирована: {topic}")
            return topic

        except Exception as e:
            handle_error("Topic Generation Error", str(e))

    def clear_generated_content(self):
        """
        Полная очистка файла с результатами перед записью новой темы.
        """
        try:
            logger.info("🧹 Полная очистка файла с результатами перед записью новой темы.")

            # Убедимся, что папка для файла существует
            folder = os.path.dirname(self.content_output_path)
            if not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"📁 Папка для сохранения данных создана: {folder}")

            # Полная очистка файла
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump({}, file, ensure_ascii=False, indent=4)

            logger.info("✅ Файл успешно очищен.")
        except PermissionError:
            handle_error("Clear Content Error", f"Нет прав на запись в файл: {self.content_output_path}")
        except Exception as e:
            handle_error("Clear Content Error", str(e))

    def request_openai(self, prompt):
        """Запрос к OpenAI для генерации контента."""
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

    def critique_content(self, content):
        """Критика текста через OpenAI API."""
        try:
            logger.info("🔄 Запрос к OpenAI для критики контента...")
            critique = self.request_openai(f"Проведи анализ текста:\n{content}")
            logger.info("✅ Критика успешно проведена.")
            self.save_to_generated_content("critique", {"critique": critique})
            return critique
        except Exception as e:
            handle_error("Critique Error", e)

    def improve_content(self, content, critique, readability_results=None, semantic_results=None):
        """
        Улучшение текста с учётом критики и дополнительных данных анализа.
        """


        try:
            logger.info("🔄 Запрос к OpenAI для улучшения текста...")

            # Безопасная инициализация переменных, если они не переданы
            readability_results = readability_results or {}
            semantic_results = semantic_results or {}

            prompt_template = config.get('CONTENT.improve.prompt_template')
            prompt = prompt_template.format(
                critique=critique,
                readability_results=readability_results,
                semantic_results=semantic_results,
                content=content
            )

            response = openai.ChatCompletion.create(
                model=self.openai_model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=config.get('CONTENT.improve.max_tokens', 600),
                temperature=self.temperature
            )

            text_after_critique = response['choices'][0]['message']['content'].strip()

            # Убираем служебные фразы из улучшенного текста
            text_after_critique = text_after_critique.replace(
                "🎯 Важно: Постоянно держите в уме основную тему. Не отклоняйтесь от главной идеи текста.",
                "")

            improved_content = response['choices'][0]['message']['content'].strip()
            logger.info("✅ Текст успешно улучшен.")
            return improved_content

        except Exception as e:
            handle_error("Improvement Error", e)
            return content

    def save_to_generated_content(self, stage, data):
        """
        Сохраняет данные на каждом этапе, обновляя существующие данные в файле.

        Аргументы:
            stage (str): Этап сохранения (например, 'topic', 'critique', 'text_initial').
            data (dict): Данные, которые нужно сохранить.
        """
        try:
            logger.info(f"🔄 Обновление данных и сохранение в файл: {self.content_output_path}")

            # Проверка существования папки и её создание, если она отсутствует
            folder = os.path.dirname(self.content_output_path)
            if not os.path.exists(folder):
                os.makedirs(folder)
                logger.info(f"📁 Папка для сохранения данных создана: {folder}")

            # Чтение существующих данных
            if os.path.exists(self.content_output_path):
                with open(self.content_output_path, 'r', encoding='utf-8') as file:
                    try:
                        result_data = json.load(file)
                    except json.JSONDecodeError:
                        result_data = {}
            else:
                result_data = {}

            # Обновление данных для текущего этапа
            result_data["timestamp"] = datetime.utcnow().isoformat()
            result_data[stage] = data

            # Запись обновлённых данных обратно в файл
            with open(self.content_output_path, 'w', encoding='utf-8') as file:
                json.dump(result_data, file, ensure_ascii=False, indent=4)

            logger.info(f"✅ Данные успешно обновлены и сохранены на этапе: {stage}")

        except FileNotFoundError:
            handle_error("Save to Generated Content Error", f"Файл не найден: {self.content_output_path}")
        except PermissionError:
            handle_error("Save to Generated Content Error", f"Нет прав на запись в файл: {self.content_output_path}")
        except Exception as e:
            handle_error("Save to Generated Content Error", str(e))


    def analyze_readability(self, content):
            """Анализ читабельности текста с помощью TextStat."""
            try:
                logger.info("🔍 Выполняется анализ читабельности текста...")
                readability_score = textstat.flesch_reading_ease(content)
                word_count = textstat.lexicon_count(content)
                sentence_count = textstat.sentence_count(content)

                readability_results = {
                    "readability_score": readability_score,
                    "word_count": word_count,
                    "sentence_count": sentence_count
                }

                logger.info(f"✅ Результат анализа читабельности: {readability_results}")
                return readability_results
            except Exception as e:
                handle_error("Readability Analysis Error", e)

    def update_focus_tracker(self, new_focus):
        """
        Добавляет новый фокус в начало списка и удаляет старые, если меток больше 200.
        """
        try:
            tracker_file = config.get('FILE_PATHS.focus_tracker', 'core/data/focus_tracker.json')

            # Создание файла, если он отсутствует
            if not os.path.exists(tracker_file):
                with open(tracker_file, 'w', encoding='utf-8') as file:
                    json.dump([], file)

            # Чтение текущих меток
            with open(tracker_file, 'r', encoding='utf-8') as file:
                focus_tracker = json.load(file)

            # Добавляем новую метку в начало
            focus_tracker.insert(0, new_focus)

            # Ограничиваем список 200 метками
            if len(focus_tracker) > 200:
                focus_tracker = focus_tracker[:200]

            # Запись обновлённых меток
            with open(tracker_file, 'w', encoding='utf-8') as file:
                json.dump(focus_tracker, file, ensure_ascii=False, indent=4)

            logger.info(f"✅ Фокус '{new_focus}' добавлен в tracker. Всего меток: {len(focus_tracker)}.")

        except Exception as e:
            handle_error("Focus Tracker Update Error", str(e))

    def get_valid_focus_areas(self):
        """
        Исключает первые 10 фокусов из focus_tracker.json из списка focus_areas.
        """
        try:
            tracker_file = config.get('FILE_PATHS.focus_tracker', 'core/data/focus_tracker.json')
            focus_areas = config.get('CONTENT.topic.focus_areas', [])

            # Чтение последних фокусов
            if os.path.exists(tracker_file):
                with open(tracker_file, 'r', encoding='utf-8') as file:
                    focus_tracker = json.load(file)
                excluded_foci = focus_tracker[:10]
            else:
                excluded_foci = []

            # Исключение фокусов
            valid_focus_areas = [focus for focus in focus_areas if focus not in excluded_foci]

            logger.info(f"🔄 Исключены 10 фокусов: {excluded_foci}")
            logger.info(f"✅ Доступные фокусы для генерации: {valid_focus_areas}")

            return valid_focus_areas

        except Exception as e:
            handle_error("Focus Area Filtering Error", str(e))
            return config.get('CONTENT.topic.focus_areas', [])

    def prioritize_focus_from_feedback_and_archive(self, valid_focus_areas):
        """
        Выбирает первый подходящий фокус из обратной связи или архива.
        """
        try:
            # Обратная связь
            feedback_path = config.get('FILE_PATHS.feedback_file', 'core/data/feedback.json')
            if os.path.exists(feedback_path):
                with open(feedback_path, 'r', encoding='utf-8') as file:
                    feedback_data = json.load(file)
                feedback_foci = [entry['topic'] for entry in feedback_data if entry.get('rating', 0) >= 8]
            else:
                feedback_foci = []

            # Архив
            archive_folder = config.get('FILE_PATHS.archive_folder', 'core/archive/')
            archive_foci = []
            if os.path.exists(archive_folder):
                for filename in os.listdir(archive_folder):
                    if filename.endswith('.json'):
                        with open(os.path.join(archive_folder, filename), 'r', encoding='utf-8') as file:
                            archive_data = json.load(file)
                        if archive_data.get('success', False):
                            archive_foci.append(archive_data.get('topic'))

            # Приоритет выбора
            for focus in feedback_foci + archive_foci:
                if focus in valid_focus_areas:
                    logger.info(f"✅ Приоритетный фокус выбран из обратной связи или архива: {focus}")
                    return focus

            # Если ни один не подошёл, выбираем первый доступный из valid_focus_areas
            if valid_focus_areas:
                logger.info(f"🔄 Используется первый доступный фокус: {valid_focus_areas[0]}")
                return valid_focus_areas[0]

            logger.warning("⚠️ Нет доступных фокусов для генерации.")
            return None

        except Exception as e:
            handle_error("Focus Prioritization Error", str(e))
            return None



    def analyze_keywords(self, content):
            """Семантический анализ и выделение ключевых слов с помощью SpaCy."""
            try:
                logger.info("🔍 Выполняется семантический анализ текста с помощью SpaCy...")
                nlp = spacy.load("en_core_web_sm")
                doc = nlp(content)

                keywords = [token.text for token in doc if token.is_alpha and not token.is_stop]
                named_entities = [(ent.text, ent.label_) for ent in doc.ents]

                semantic_results = {
                    "keywords": keywords[:10],  # Берём топ-10 ключевых слов
                    "named_entities": named_entities
                }

                logger.info(f"✅ Результат семантического анализа: {semantic_results}")
                return semantic_results
            except Exception as e:
                handle_error("Semantic Analysis Error", e)

    def run(self):
        self.adapt_prompts()

        # Очистка данных перед генерацией новой темы
        self.clear_generated_content()

        # Генерация темы
        topic = self.generate_topic()
        self.save_to_generated_content("topic", {"topic": topic})

        # Генерация текста
        text_initial = self.request_openai(config.get('CONTENT.text.prompt_template').format(topic=topic))
        self.save_to_generated_content("text_initial", {"content": text_initial})

        # Критика текста
        critique = self.request_openai(config.get('CONTENT.critique.prompt_template').format(content=text_initial))

        # Анализ читабельности
        readability_results = self.analyze_readability(text_initial)
        self.save_to_generated_content("readability", readability_results)

        # Семантический анализ
        semantic_results = self.analyze_keywords(text_initial)
        self.save_to_generated_content("semantic_analysis", semantic_results)

        # Улучшение текста
        text_after_critique = self.improve_content(
            text_initial, critique, readability_results, semantic_results
        )
        self.save_to_generated_content("text_improved", {"content": text_after_critique})

        # Добавление сарказма
        final_text = self.append_sarcasm_to_post(text_after_critique)
        self.save_to_generated_content("final_text", {"content": final_text})
        logger.info("🚀 Генерация контента завершена.")

if __name__ == "__main__":
    generator = ContentGenerator()
    generator.run()
