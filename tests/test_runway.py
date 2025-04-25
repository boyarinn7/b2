import os
import sys
import json
from pathlib import Path
import re

# --- Конфигурация ---

# Путь к корневой папке со шрифтами (ВАЖНО: проверьте правильность)
ROOT_FONT_DIR = r"C:\Users\boyar\b2\fonts"

# Расширения файлов шрифтов (в нижнем регистре)
FONT_EXTENSIONS = ('.ttf', '.otf')

# --- ИЗМЕНЕНИЕ ЗДЕСЬ: Определяем путь к трекеру относительно скрипта ---
# Получаем директорию, где находится сам скрипт
script_dir = Path(__file__).resolve().parent
# Формируем полный путь к файлу трекера в той же директории
TRACKER_FILE_PATH = script_dir / "topics_tracker.json"
# --- КОНЕЦ ИЗМЕНЕНИЯ ---

# Словарь: Фокус -> Базовое имя файла шрифта (из предложений Google Fonts)
# ВАЖНО: Проверьте и при необходимости скорректируйте базовые имена,
# чтобы они соответствовали началу имен ваших файлов!
focus_to_font_base = {
    "Исторические факты": "PTSerif",
    "Исторические личности": "CormorantGaramond",
    "Знаковые битвы (т)": "Oswald",
    "Трагедии человечества (т)": "Merriweather",
    "Доисторические события": "Arvo",
    "Древние цивилизации": "Forum",
    "Необычные открытия": "Montserrat",
    "Артефакты и реликвии": "OldStandardTT",
    "Конспирологические теории": "PTSansNarrow",
    "Загадки истории": "EBGaramond",
    "Исторические курьезы": "Comfortaa",
    "Великие изобретения": "Roboto",
    "Культурные феномены": "PlayfairDisplay",
    "Исторические карты": "PTSans",
    "История через цитаты": "Merriweather",
    "Династии и монархи": "CormorantGaramond",
    "Важные документы истории": "PTSerif",
    "Мифы и легенды": "YesevaOne",
    "Исторические праздники": "YesevaOne",
    "Великие путешественники": "PTSans",
    "История медицины": "PTSerif",
    "Религии и верования": "OldStandardTT",
    "Архитектурные чудеса": "Forum",
    "История в искусстве": "PlayfairDisplay",
    "Экономические кризисы и взлеты (т)": "RobotoSlab",
    "История технологий": "Roboto",
    "Исторические конфликты и их разрешение (т)": "Oswald",
    "Женщины в истории": "CormorantGaramond",
    "История науки": "Montserrat",
    "Исторические кулинарные традиции": "Alice",
    "История моды": "PlayfairDisplay",
    "Исторические спортивные события": "Oswald",
    "История образования": "PTSerif",
    "Исторические эпидемии и пандемии (т)": "Merriweather",
    "История окружающей среды (т)": "PTSans",
    "__default__": "Roboto" # Шрифт по умолчанию
}

# --- Функции ---

def load_tracker_focuses(tracker_path):
    """Загружает список фокусов из файла topics_tracker.json."""
    # Используем tracker_path как объект Path
    tracker_path_obj = Path(tracker_path)
    print(f"Попытка загрузки трекера из: {tracker_path_obj.resolve()}") # Логируем полный путь
    try:
        # Проверяем существование файла перед открытием
        if not tracker_path_obj.is_file():
            print(f"Ошибка: Файл трекера не найден по абсолютному пути: {tracker_path_obj.resolve()}")
            return None

        with open(tracker_path_obj, 'r', encoding='utf-8') as f:
            tracker_data = json.load(f)
        focuses = tracker_data.get("all_focuses")
        if not focuses or not isinstance(focuses, list):
            print(f"Ошибка: Ключ 'all_focuses' не найден или не является списком в {tracker_path_obj.name}")
            return None
        print(f"Успешно загружено {len(focuses)} фокусов из {tracker_path_obj.name}.")
        return focuses
    # FileNotFoundError теперь обрабатывается проверкой is_file() выше
    except json.JSONDecodeError:
        print(f"Ошибка: Некорректный JSON в файле трекера: {tracker_path_obj.name}")
        return None
    except PermissionError:
        print(f"Ошибка: Нет прав доступа к файлу трекера: {tracker_path_obj.resolve()}")
        return None
    except Exception as e:
        print(f"Ошибка при чтении файла трекера {tracker_path_obj.name}: {e}")
        return None

# --- Остальные функции (find_font_files, get_font_base_name) без изменений ---
def find_font_files(root_dir, extensions):
    """Рекурсивно находит все файлы шрифтов и возвращает словарь {имя_файла: полный_путь}."""
    found_files = {}
    print(f"\nПоиск файлов ({', '.join(extensions)}) в '{root_dir}' и подпапках...")
    try:
        if not os.path.isdir(root_dir):
            print(f"Ошибка: Папка не найдена: {root_dir}")
            return None

        for subdir, dirs, files in os.walk(root_dir):
            for filename in files:
                if filename.lower().endswith(extensions):
                    full_path = os.path.join(subdir, filename)
                    # Используем Path для нормализации пути
                    normalized_path = str(Path(full_path).resolve())
                    if filename in found_files:
                         print(f"Предупреждение: Найден дубликат имени файла '{filename}'. Используется путь: {normalized_path}")
                    found_files[filename] = normalized_path
        print(f"Найдено всего файлов шрифтов: {len(found_files)}")
        return found_files
    except PermissionError:
        print(f"Ошибка: Нет прав доступа для чтения '{root_dir}' или подпапок.")
        return None
    except Exception as e:
        print(f"Ошибка при поиске файлов: {e}")
        return None

def get_font_base_name(filename):
    """Извлекает базовое имя шрифта до первого тире или цифры (упрощенно)."""
    base = Path(filename).stem # Имя файла без расширения
    # Удаляем общие суффиксы Regular, Bold и т.д. для лучшего совпадения
    base = re.sub(r'-(Regular|Italic|Bold|Medium|Light|Thin|Black|ExtraLight|ExtraBold|SemiBold|VariableFont_wght|Condensed|SemiCondensed)$', '', base, flags=re.IGNORECASE)
    # Можно добавить более сложные правила, если нужно
    return base

# --- Основной скрипт ---
if __name__ == "__main__":
    print("--- Запуск скрипта очистки шрифтов ---")

    # 1. Загрузка фокусов
    # Теперь TRACKER_FILE_PATH - это объект Path с абсолютным путем
    all_focuses = load_tracker_focuses(TRACKER_FILE_PATH)
    if all_focuses is None:
        sys.exit(1)

    # 2. Поиск всех файлов шрифтов
    all_found_fonts = find_font_files(ROOT_FONT_DIR, FONT_EXTENSIONS)
    if all_found_fonts is None:
        sys.exit(1)
    if not all_found_fonts:
        print("В указанной папке и подпапках шрифты не найдены.")
        sys.exit(0)

    # 3. Определение необходимых шрифтов
    required_font_files = {} # {базовое_имя_шрифта: имя_файла_для_использования}
    focus_to_final_path = {} # {фокус: полный_путь_к_файлу}
    needed_base_names = set(focus_to_font_base.values()) # Набор нужных базовых имен

    print("\nОпределение необходимых шрифтов для фокусов...")
    # Сначала ищем файлы для конкретных фокусов
    for focus in all_focuses:
        target_base_name = focus_to_font_base.get(focus)
        if not target_base_name:
            print(f"Предупреждение: Для фокуса '{focus}' не найдено сопоставление шрифта. Будет использован шрифт по умолчанию.")
            target_base_name = focus_to_font_base.get("__default__")
            if not target_base_name:
                 print("Ошибка: Шрифт по умолчанию '__default__' не задан в focus_to_font_base!")
                 continue # Пропускаем этот фокус

        # Если для этого базового имени шрифт уже найден, используем его
        if target_base_name in required_font_files:
             filename_to_use = required_font_files[target_base_name]
             # Убедимся, что файл все еще существует в словаре найденных
             if filename_to_use in all_found_fonts:
                 focus_to_final_path[focus] = all_found_fonts[filename_to_use]
             else:
                 print(f"Ошибка: Ранее найденный файл '{filename_to_use}' для '{target_base_name}' больше не доступен.")
                 # Попробуем найти заново или использовать дефолт
                 target_base_name = focus_to_font_base.get("__default__") # Переключаемся на дефолт
                 if target_base_name in required_font_files: # Если дефолт уже найден
                      filename_to_use = required_font_files[target_base_name]
                      if filename_to_use in all_found_fonts:
                          focus_to_final_path[focus] = all_found_fonts[filename_to_use]
                      else:
                          print(f"Ошибка: Дефолтный файл '{filename_to_use}' тоже недоступен.")
                          continue # Пропускаем фокус
                 else: # Ищем дефолт заново (код поиска ниже)
                     pass # Поиск произойдет ниже
             continue # Переходим к следующему фокусу

        # Ищем подходящий файл (логика поиска остается)
        found_match = None
        preferred_file = None # Для хранения Regular версии, если найдется

        for filename, full_path in all_found_fonts.items():
             # Сравниваем начало имени файла (без расширения, регистронезависимо)
             file_stem_lower = Path(filename).stem.lower()
             target_base_lower = target_base_name.lower()
             # Ищем точное совпадение или совпадение с тире/пробелом/подчеркиванием
             # Добавим ^ для поиска с начала строки
             if re.match(rf"^{re.escape(target_base_lower)}(?:[-_\s]|$)", file_stem_lower):
                # Отдаем предпочтение файлу 'Regular', если он еще не найден
                if preferred_file is None and 'regular' in file_stem_lower and 'italic' not in file_stem_lower:
                     preferred_file = filename
                     # Не прерываем сразу, вдруг есть более точное совпадение позже
                # Запоминаем первое найденное совпадение на случай, если Regular нет
                if found_match is None:
                    found_match = filename

        # Выбираем файл для использования: сначала предпочтительный, потом любой найденный
        filename_to_use = preferred_file if preferred_file else found_match

        if filename_to_use:
            required_font_files[target_base_name] = filename_to_use
            focus_to_final_path[focus] = all_found_fonts[filename_to_use]
            print(f"- Для фокуса '{focus}' выбран файл: {filename_to_use}")
        else:
            print(f"Предупреждение: Не найден файл для базового имени '{target_base_name}' (фокус: '{focus}'). Попытка использовать шрифт по умолчанию.")
            # Пытаемся использовать шрифт по умолчанию
            default_base_name = focus_to_font_base.get("__default__")
            if default_base_name:
                 # Повторяем поиск для шрифта по умолчанию
                 if default_base_name in required_font_files:
                     filename_to_use = required_font_files[default_base_name]
                     if filename_to_use in all_found_fonts:
                         focus_to_final_path[focus] = all_found_fonts[filename_to_use]
                         print(f"- Для фокуса '{focus}' используется шрифт по умолчанию: {filename_to_use}")
                     else:
                          print(f"Ошибка: Дефолтный файл '{filename_to_use}' недоступен для фокуса '{focus}'.")
                 else:
                     # Ищем файл для шрифта по умолчанию (аналогично основному поиску)
                     found_default_match = None
                     preferred_default_file = None
                     for fname, fpath in all_found_fonts.items():
                         file_stem_lower = Path(fname).stem.lower()
                         target_base_lower = default_base_name.lower()
                         if re.match(rf"^{re.escape(target_base_lower)}(?:[-_\s]|$)", file_stem_lower):
                             if preferred_default_file is None and 'regular' in file_stem_lower and 'italic' not in file_stem_lower:
                                 preferred_default_file = fname
                             if found_default_match is None: found_default_match = fname
                             # Можно добавить break, если нашли Regular
                             if preferred_default_file: break

                     filename_to_use = preferred_default_file if preferred_default_file else found_default_match
                     if filename_to_use:
                         required_font_files[default_base_name] = filename_to_use
                         focus_to_final_path[focus] = all_found_fonts[filename_to_use]
                         print(f"- Для фокуса '{focus}' используется шрифт по умолчанию: {filename_to_use}")
                     else:
                          print(f"Ошибка: Не найден файл и для шрифта по умолчанию '{default_base_name}'!")
            else:
                 print("Ошибка: Шрифт по умолчанию '__default__' не задан!")


    # 4. Определение лишних файлов
    print("\nОпределение лишних файлов...")
    required_filenames_set = set(required_font_files.values())
    extra_files_to_delete = {} # {имя_файла: полный_путь}

    for filename, full_path in all_found_fonts.items():
        if filename not in required_filenames_set:
            extra_files_to_delete[filename] = full_path

    # 5. Вывод списка на удаление и запрос подтверждения
    if extra_files_to_delete:
        print(f"\nНайдены следующие ЛИШНИЕ файлы шрифтов ({len(extra_files_to_delete)} шт.), которые НЕ НУЖНЫ для фокусов:")
        sorted_extra_files = sorted(extra_files_to_delete.keys())
        for i, filename in enumerate(sorted_extra_files):
            print(f"  {i+1}. {filename} (Путь: {extra_files_to_delete[filename]})")

        print("\n!!! ВНИМАНИЕ !!!")
        print("Эти файлы будут УДАЛЕНЫ с вашего диска.")
        confirm = ""
        try:
            confirm = input("Вы уверены, что хотите удалить эти файлы? (введите 'yes' для подтверждения): ").strip().lower()
        except EOFError:
             print("\nВвод не обнаружен (EOF). Удаление отменено.")


        # 6. Удаление (если подтверждено)
        if confirm == 'yes':
            print("\nУдаление лишних файлов...")
            deleted_count = 0
            failed_count = 0
            for filename, full_path in extra_files_to_delete.items():
                try:
                    os.remove(full_path)
                    print(f" - Удален: {filename}")
                    deleted_count += 1
                except PermissionError:
                    print(f" ! Ошибка прав доступа при удалении: {filename}")
                    failed_count += 1
                except Exception as e:
                    print(f" ! Ошибка при удалении {filename}: {e}")
                    failed_count += 1
            print(f"\nУдалено файлов: {deleted_count}")
            if failed_count > 0:
                print(f"Не удалось удалить файлов: {failed_count}")
        else:
            print("\nУдаление отменено пользователем.")
            # Очищаем список, чтобы не показывать его в финальном выводе
            extra_files_to_delete.clear()

    else:
        print("\nЛишних файлов шрифтов не найдено.")

    # 7. Вывод финального словаря путей
    print("\n--- Финальное сопоставление 'Фокус -> Путь к шрифту' ---")
    # Создаем JSON-совместимый вывод
    # Используем Path(p).as_posix() для путей в формате Linux/веб
    output_dict_posix = {f: Path(p).as_posix() for f, p in focus_to_final_path.items()}
    output_json = json.dumps(output_dict_posix, indent=4, ensure_ascii=False)
    print(output_json)

    # Сохраняем JSON в файл для удобства
    output_filename = "focus_font_paths.json"
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write(output_json)
        print(f"\nСопоставление также сохранено в файл: {output_filename}")
    except Exception as e:
        print(f"\nНе удалось сохранить сопоставление в файл: {e}")


    print("\n--- Скрипт завершен ---")
