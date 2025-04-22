import os
import openai
import json

# --- Загрузка ключа API ---
# Убедитесь, что переменная окружения OPENAI_API_KEY установлена
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    print("Ошибка: Переменная окружения OPENAI_API_KEY не найдена.")
    exit()

# В старых версиях (<1.0.0) ключ API устанавливается так
openai.api_key = api_key

# --- Формирование Промпта ---
# ВАЖНО: Промпт НЕ содержит слово "серобуромалиновый"
# и НЕ содержит инструкций по чтению файла.
# Он просто просит назвать значение для ключа 'color'.
prompt_text = "Назови значение для конфигурационного ключа 'color'."

print(f"Отправка промпта в OpenAI: '{prompt_text}'")
print("-" * 20)

# --- Вызов OpenAI API (Синтаксис для старых версий < 1.0.0) ---
try:
    # Используем старый синтаксис openai.ChatCompletion.create
    response = openai.ChatCompletion.create(
        model="gpt-4o", # Или gpt-3.5-turbo
        messages=[
            {"role": "system", "content": "Ты - помощник, отвечающий на вопросы."},
            {"role": "user", "content": prompt_text}
        ],
        max_tokens=50,
        temperature=0.5 # Низкая температура для более предсказуемого ответа
    )

    # --- Вывод результата ---
    # В старых версиях доступ к ответу немного отличается
    ai_response = response['choices'][0]['message']['content'].strip()
    print(f"Ответ от OpenAI:")
    print(ai_response)
    print("-" * 20)

    # --- Проверка (для наглядности) ---
    if "серобуромалиновый" in ai_response.lower():
        print("!!! Неожиданно! AI каким-то образом упомянул слово 'серобуромалиновый'.")
    else:
        print("Как и ожидалось, AI не смог получить доступ к файлу и назвать конкретное значение.")

# --- Обработка ошибок (Синтаксис для старых версий < 1.0.0) ---
# Используем openai.error.AuthenticationError и другие ошибки из openai.error
except openai.error.AuthenticationError:
     print("Ошибка аутентификации OpenAI. Проверьте ваш API ключ.")
except openai.error.OpenAIError as e: # Общая ошибка OpenAI для старых версий
    print(f"Произошла ошибка API OpenAI: {e}")
except Exception as e:
    print(f"Произошла непредвиденная ошибка: {e}")
