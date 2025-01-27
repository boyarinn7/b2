import openai
import os

# Получаем API-ключ из переменной окружения
openai.api_key = os.getenv("OPENAI_API_KEY")

# Проверка, что ключ загружен правильно
if openai.api_key:
    print("API ключ успешно получен.")
else:
    print("Ошибка: API ключ не найден.")

# Запрос к API для получения информации о балансе
def get_usage():
    try:
        # Получаем данные о текущем использовании
        usage_data = openai.Usage.retrieve()
        print("Информация о балансе:", usage_data)
    except Exception as e:
        print("Ошибка при получении информации:", e)

# Вызов функции
get_usage()
