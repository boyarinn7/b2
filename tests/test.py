import os
import sys

# Укажите путь к корневой папке со шрифтами
# Используем r"" для корректной обработки обратных слешей в Windows
root_folder_path = r"C:\Users\boyar\b2\fonts"

# Расширения файлов шрифтов, которые ищем (в нижнем регистре)
font_extensions = ('.ttf', '.otf')

print(f"--- Попытка рекурсивного чтения папки: {root_folder_path} ---")

font_files_found = []

try:
    # Проверяем, существует ли путь и является ли он папкой
    if os.path.isdir(root_folder_path):
        print(f"Поиск файлов шрифтов ({', '.join(font_extensions)}) в '{root_folder_path}' и подпапках...")

        # Рекурсивно обходим все папки и файлы
        for subdir, dirs, files in os.walk(root_folder_path):
            for filename in files:
                # Проверяем расширение файла (в нижнем регистре)
                if filename.lower().endswith(font_extensions):
                    # Добавляем имя файла (можно добавить и относительный путь, если нужно)
                    # full_path = os.path.join(subdir, filename) # Полный путь
                    # relative_path = os.path.relpath(full_path, root_folder_path) # Относительный путь
                    font_files_found.append(filename) # Собираем только имена файлов

        if font_files_found:
             # Сортируем для удобства
            font_files_found.sort()
            print(f"\nНайденные файлы шрифтов ({len(font_files_found)} шт.):")
            # Выводим имена файлов
            for filename in font_files_found:
                print(f"- {filename}")
        else:
            print(f"  (Файлы шрифтов с расширениями {font_extensions} не найдены)")

    elif os.path.exists(root_folder_path):
        print(f"Ошибка: Указанный путь '{root_folder_path}' не является папкой.")
        sys.exit(1) # Выход с кодом ошибки
    else:
        print(f"Ошибка: Папка не найдена по пути '{root_folder_path}'.")
        sys.exit(1) # Выход с кодом ошибки

except PermissionError:
    print(f"Ошибка: Нет прав доступа для чтения папки '{root_folder_path}' или ее подпапок.")
    sys.exit(1) # Выход с кодом ошибки
except Exception as e:
    print(f"Произошла непредвиденная ошибка: {e}")
    sys.exit(1) # Выход с кодом ошибки

print("--- Чтение папки завершено ---")
