# -*- coding: utf-8 -*-
"""
Главный файл запуска для выполнения полного цикла обработки транзакций:
1. Скачивание CSV-вложений из Outlook
2. Обработка и отправка транзакций
"""

import os
import subprocess
import sys
from pathlib import Path


def run_script(script_name: str, description: str) -> bool:
    """
    Запускает указанный скрипт и возвращает результат выполнения.
    
    Args:
        script_name: Имя скрипта для запуска
        description: Описание действия для вывода в консоль
    
    Returns:
        bool: True, если выполнение прошло успешно, иначе False
    """
    print(f"\n{'='*60}")
    print(f"ЗАПУСК: {description}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        print(f"{description} завершено успешно")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении {description}: {e}")
        return False
    except FileNotFoundError:
        print(f"Файл {script_name} не найден")
        return False


def main():
    """
    Основная функция для выполнения полного цикла обработки транзакций
    """
    print("НАЧАЛО ВЫПОЛНЕНИЯ ПОЛНОГО ЦИКЛА ОБРАБОТКИ ТРАНЗАКЦИЙ")
    print("Этапы:")
    print("1. Скачивание CSV-вложений из Outlook (main.py)")
    print("2. Обработка и отправка транзакций (transaction_parser.py)")
    
    # Запуск скачивания вложений
    download_success = run_script('main.py', 'Скачивание CSV-вложений из Outlook')
    
    if not download_success:
        print("\nЗавершение выполнения из-за ошибки на этапе скачивания")
        return
    
    # Запуск обработки транзакций
    process_success = run_script('transaction_parser.py', 'Обработка и отправка транзакций')
    
    if not process_success:
        print("\nЗавершение выполнения из-за ошибки на этапе обработки")
        return
    
    print(f"\n{'='*60}")
    print("ПОЛНЫЙ ЦИКЛ ОБРАБОТКИ ТРАНЗАКЦИЙ ЗАВЕРШЕН УСПЕШНО")
    print("1. CSV-вложения скачаны")
    print("2. Транзакции обработаны и отправлены")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()