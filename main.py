# -*- coding: utf-8 -*-
"""
Скачивание CSV-вложений из Inbox заданного ящика Outlook за период.
Требования: Windows, Outlook Desktop запущен, pip install pywin32
"""

import os
import re
from datetime import datetime, time, timedelta
import sys
from io import StringIO
import logging
import os
from datetime import datetime
import console_logger

try:
    import win32com.client as win32
    from win32com.client import constants
except ImportError:  # pragma: no cover - для сред без Outlook/pywin32
    win32 = None
    constants = None

# Загрузка параметров из .env файла
from dotenv import load_dotenv
load_dotenv()

# === ПАРАМЕТРЫ ДЛЯ ВАС ===
ACCOUNT_SMTP = os.getenv("ACCOUNT_SMTP", "scs@sakhalin.gov.ru")
DATE_START = os.getenv("DATE_START")  # включительно, локальное время ПК
DATE_END   = os.getenv("DATE_END")  # включительно, локальное время ПК
OUTPUT_DIR = os.getenv("OUTPUT_DIR")  # можно изменить


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def ensure_dir(path: str):
    """
    Создает директорию, если она не существует.
    
    :param path: Путь к директории
    :return: Путь к директории
    """
    os.makedirs(path, exist_ok=True)
    return path

def sanitize_filename(name: str, max_len: int = 120) -> str:
    """
    Очищает имя файла от недопустимых символов для Windows-путей и ограничивает длину.
    
    :param name: Исходное имя файла
    :param max_len: Максимальная длина имени файла
    :return: Очищенное имя файла
    """
    # Уберём недопустимые символы для Windows-пути
    name = re.sub(r'[\\/:*?"<>|\r\n]+', "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len].rstrip()
    return name or "untitled"

def dt_range_str(d1: datetime, d2: datetime) -> str:
    """
    Формирует строку диапазона дат в формате YYYY-MM-DD_YYYY-MM-DD.
    
    :param d1: Начальная дата
    :param d2: Конечная дата
    :return: Строка диапазона дат
    """
    return f"{d1.date().isoformat()}_{d2.date().isoformat()}"

def outlook_us_datetime_str(dt: datetime) -> str:
    """
    Преобразует дату в формат США, требуемый для фильтрации в Outlook.
    Формат строится вручную, чтобы избежать зависимостей от локали.
    
    :param dt: Объект даты и времени
    :return: Строка даты и времени в формате США
    """
    # Outlook Restrict ожидает формат США: MM/DD/YYYY HH:MM AM/PM
    # Формируем вручную, чтобы избежать локализованных обозначений AM/PM
    hour12 = dt.hour % 12 or 12
    am_pm = "AM" if dt.hour < 12 else "PM"
    return f"{dt.month:02d}/{dt.day:02d}/{dt.year:04d} {hour12:02d}:{dt.minute:02d} {am_pm}"

def build_received_time_filter(start_inclusive: datetime, end_inclusive: datetime):
    """
    Формирует строку фильтра для Outlook.Items.Restrict и возвращает эксклюзивную верхнюю границу.
    
    :param start_inclusive: Начальная дата и время (включительно)
    :param end_inclusive: Конечная дата и время (включительно)
    :return: Кортеж (filter_str, end_exclusive)
    """
    if start_inclusive > end_inclusive:
        raise ValueError("start_inclusive must be earlier than or equal to end_inclusive")
    next_day = (end_inclusive + timedelta(days=1)).date()
    end_exclusive = datetime.combine(next_day, time(0, 0, 0))
    filter_str = (
        f"[ReceivedTime] >= '{outlook_us_datetime_str(start_inclusive)}' AND "
        f"[ReceivedTime] < '{outlook_us_datetime_str(end_exclusive)}'"
    )
    return filter_str, end_exclusive

def get_account_by_smtp(session, smtp_lower: str):
    """
    Ищет аккаунт по SMTP-адресу.
    
    :param session: Сессия Outlook
    :param smtp_lower: SMTP-адрес в нижнем регистре
    :return: Объект аккаунта или None
    """
    # Ищем точный аккаунт по SMTP
    for i in range(1, session.Accounts.Count + 1):
        acc = session.Accounts.Item(i)
        if str(acc.SmtpAddress).lower() == smtp_lower:
            return acc
    return None

def get_smtp_from_recipient(recipient):
    """
    Пытаемся извлечь SMTP адрес из recipient.
    Если не получилось — вернём нормализованное DisplayName.
    
    :param recipient: Объект получателя
    :return: SMTP-адрес или нормализованное имя
    """
    try:
        addr_entry = recipient.AddressEntry
        if addr_entry is not None:
            # Для EX типичного адресата:
            PR_SMTP = "http://schemas.microsoft.com/mapi/proptag/0x39FE001E"
            smtp = addr_entry.PropertyAccessor.GetProperty(PR_SMTP)
            if smtp:
                return smtp
    except Exception:
        pass
    # fallback
    disp = str(recipient.Address or recipient.Name or recipient)
    # Возможны строки вида "Имя <user@domain>"
    m = re.search(r'<?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})>?', disp)
    if m:
        return m.group(1)
    return sanitize_filename(disp) or "unknown"

def get_primary_to_smtp(mail):
    """
    Берём первого адресата из To (если есть). Если нет — 'unknown'.
    
    :param mail: Объект письма
    :return: SMTP-адрес первого получателя или 'unknown'
    """
    try:
        rcpts = mail.Recipients
        if rcpts and rcpts.Count > 0:
            return get_smtp_from_recipient(rcpts.Item(1)) or "unknown"
    except Exception:
        pass
    # Доп. попытка парсинга text To:
    to_text = str(getattr(mail, "To", "") or "")
    m = re.search(r'([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})', to_text)
    return m.group(1) if m else "unknown"

# === ОСНОВНАЯ ЛОГИКА ===

def main():
    """
    Основная функция для скачивания CSV-вложений из Inbox заданного ящика Outlook за период.
    """
    if win32 is None:
        print("Библиотека win32com.client не найдена. Установите pywin32 и запустите на Windows.")
        return
    # 1) Подключаемся к запущенному Outlook
    outlook = win32.Dispatch("Outlook.Application")
    session = outlook.GetNamespace("MAPI")

    # 2) Находим нужный аккаунт/хранилище по SMTP
    acc = get_account_by_smtp(session, ACCOUNT_SMTP.lower())
    if not acc:
        logging.error(f"Аккаунт с SMTP '{ACCOUNT_SMTP}' не найден среди session.Accounts.")
        logging.info("Доступные аккаунты:")
        for i in range(1, session.Accounts.Count + 1):
            acc_temp = session.Accounts.Item(i)
            logging.info(f"  - {acc_temp.SmtpAddress}")
        return

    store = acc.DeliveryStore  # хранилище данного аккаунта
    logging.info(f"Найден аккаунт: {acc.SmtpAddress}")
    
    # Получаем системные папки для определения, какие исключить
    sent_folder = store.GetDefaultFolder(5)  # Отправленные (5)
    deleted_folder = store.GetDefaultFolder(3) # Удаленные (3)
    drafts_folder = store.GetDefaultFolder(16)  # Черновики (16)
    
    # Получаем корневую папку хранилища
    root_folder = store.GetRootFolder()
    
    # Рекурсивная функция для получения всех подпапок
    def get_all_folders(parent_folder, exclude_folder_ids):
        folders = []
        # Проверяем текущую папку
        if parent_folder.DefaultItemType == 0 and parent_folder.EntryID not in exclude_folder_ids:
            folders.append(parent_folder)
        # Рекурсивно обрабатываем подпапки
        for i in range(1, parent_folder.Folders.Count + 1):
            subfolder = parent_folder.Folders.Item(i)
            folders.extend(get_all_folders(subfolder, exclude_folder_ids))
        return folders
    
    # Получаем все папки аккаунта, кроме "Отправленные", "Удаленные" и "Черновики"
    exclude_folder_ids = [sent_folder.EntryID, deleted_folder.EntryID, drafts_folder.EntryID]
    all_folders = get_all_folders(root_folder, exclude_folder_ids)

    # 3) Границы времени
    start_dt = datetime.combine(datetime.fromisoformat(DATE_START).date(), time(0, 0, 0))
    end_dt_inclusive = datetime.combine(datetime.fromisoformat(DATE_END).date(), time(23, 59, 59))

    if start_dt > end_dt_inclusive:
        print(f"Ошибка: DATE_START ({DATE_START}) позже DATE_END ({DATE_END}).")
        return

    # Формируем фильтр для Restrict и получаем время верхней границы (эксклюзивно)
    time_filter_str, _ = build_received_time_filter(start_dt, end_dt_inclusive)
    
    # Собираем элементы из всех подходящих папок с применением фильтра по дате каждой папке
    
    all_filtered_items = []
    for folder in all_folders:
        items = folder.Items
        # Сортируем по дате получения, новые сверху — так можно рано остановиться
        items.Sort("[ReceivedTime]", True)

        count = items.Count

        for i in range(1, count + 1):
            itm = items.Item(i)

            recv_dt = itm.ReceivedTime

            recv_py = datetime(recv_dt.year, recv_dt.month, recv_dt.day,
                            recv_dt.hour, recv_dt.minute, recv_dt.second)

            # Пропускаем слишком новые (после верхней границы)
            if recv_py > end_dt_inclusive:
                continue

            # Если отсортировано по убыванию и мы ушли ниже нижней границы —
            # дальше только старее, можно прервать цикл по этой папке.
            if recv_py < start_dt:
                break

            # В интервале — забираем
            all_filtered_items.append(itm)
            #else:
            #    print(f" - Письмо '{item.Subject}' от {item.SenderEmailAddress} имеет дату {recv_py}, которая вне диапазона {start_dt} - {end_dt_inclusive} и будет пропущено")
    
    if not all_filtered_items:
        print("Не найдено писем в указанном диапазоне дат")
        return
    
    # Сортируем все собранные элементы по времени получения
    all_filtered_items.sort(key=lambda x: x.ReceivedTime, reverse=True)
    items = all_filtered_items
    # Заменяем переменную items на filtered для дальнейшей работы
    filtered = items

    # 5) Подготовка папки назначения
    root_dir = ensure_dir(os.path.join(OUTPUT_DIR, dt_range_str(start_dt, end_dt_inclusive)))

    saved_files = 0
    scanned_mails = 0
    emails_with_saved_attachments = 0 # Количество писем, в которых были найдены вложения
    emails_with_valid_attachments = set() # Множество для отслеживания писем, в которых есть подходящие вложения
    emails_without_valid_attachments = [] # Список для отслеживания писем, в которых не было подходящих вложений
    emails_with_multiple_attachments = [] # Список для отслеживания писем с несколькими вложениями

    # Некоторые элементы в Inbox могут быть не MailItem, фильтруем по .Class
    print(f"Начинаем перебор {len(filtered)} писем...")
    downloaded_attachments = []  # Список для хранения информации о скачанных вложениях
    for i, itm in enumerate(filtered, 1):
        try:
            item_class = itm.Class
            if item_class != 43:  # 43 - это константа olMail
                print(f"  - Пропускаем, это не MailItem (Class = {item_class})")
                continue
        except Exception as e:
            print(f"  - Ошибка при проверке класса элемента: {e}")
            continue

        print(f"Обрабатываем письмо {i}: '{itm.Subject}' от {itm.SenderEmailAddress}")
        scanned_mails += 1
        # Основные поля письма
        recv_dt = itm.ReceivedTime  # COM datetime
        recv_py = datetime(
            recv_dt.year, recv_dt.month, recv_dt.day, recv_dt.hour, recv_dt.minute, recv_dt.second
        )
        # Отправитель (Sender) — используем в структуре папок
        # Сначала пробуем использовать имя отправителя, если оно доступно
        sender_name = itm.SenderName or itm.SenderEmailAddress or "unknown"
        # Убираем email из имени, если оно в формате "Имя <email@domain.com>"
        match = re.search(r'^(.+?)\s*<.*>$', sender_name)
        if match:
            sender_name = match.group(1).strip()
        # Папка по отправителю будет создана позже, если найдутся подходящие вложения
        
        sender_folder_name = sanitize_filename(sender_name)
        mail_dir = os.path.join(
            root_dir,
            sender_folder_name
        )

        atts = getattr(itm, "Attachments", None)
        if not atts or atts.Count == 0:
            print(f" - Письмо '{itm.Subject}' (от {itm.SenderEmailAddress}) не имеет вложений")
            # Добавляем информацию о письме без вложений в список
            attachment_info = f"{recv_py.strftime('%Y-%m-%d %H:%M:%S')} - {sender_name} - {itm.Subject}"
            emails_without_valid_attachments.append(attachment_info)
            continue
        
        # Проверяем, есть ли в письме несколько вложений
        if atts.Count > 1:
            # Подсчитываем количество вложений с нужными расширениями
            valid_attachment_count = 0
            for a in list(range(1, atts.Count + 1)):
                att = atts.Item(a)
                fname = str(att.FileName or "")
                if fname.lower().endswith((".csv", ".xlsx", ".xls")):
                    valid_attachment_count += 1
            
            if valid_attachment_count > 1:
                # Добавляем информацию о письме с несколькими подходящими вложениями
                email_info = {
                    'date': recv_py.strftime('%Y-%m-%d %H:%M:%S'),
                    'sender': sender_name,
                    'subject': itm.Subject,
                    'attachment_count': valid_attachment_count,
                    'attachments': []
                }
                emails_with_multiple_attachments.append(email_info)
        
        # Проверяем, есть ли вложения с нужными расширениями (.csv, .xlsx, .xls)
        has_valid_attachments = False
        for a in list(range(1, atts.Count + 1)):
            att = atts.Item(a)
            fname = str(att.FileName or "")
            if fname.lower().endswith((".csv", ".xlsx", ".xls")):
                has_valid_attachments = True
                break
        
        # Если нет подходящих вложений, пропускаем письмо
        if not has_valid_attachments:
            print(f" - Письмо '{itm.Subject}' (от {itm.SenderEmailAddress}) не имеет подходящих вложений (CSV/Excel)")
            # Добавляем информацию о письме без подходящих вложений в список
            attachment_info = f"{recv_py.strftime('%Y-%m-%d %H:%M:%S')} - {sender_name} - {itm.Subject}"
            emails_without_valid_attachments.append(attachment_info)
            continue
            

        # Создаем папку для письма только если есть подходящие вложения
        mail_dir = ensure_dir(mail_dir)

        print(f"  - Письмо '{itm.Subject}' (от {itm.SenderEmailAddress}) имеет подходящие вложения")
        
        # Сохраняем только .csv и .xlsx/.xls (без учета регистра)
        for a in list(range(1, atts.Count + 1)):
            att = atts.Item(a)
            fname = str(att.FileName or "")
            print(f"    - Вложение: {fname}")
            
            # Проверяем расширение файла и сохраняем только нужные типы
            if fname.lower().endswith((".csv", ".xlsx", ".xls")):
                print(f"      - Найден файл: {fname}")
                # Добавляем дату к имени файла
                date_prefix = recv_py.strftime('%Y-%m-%d')
                safe_name = sanitize_filename(fname)
                name_part, ext = os.path.splitext(safe_name)
                new_name = f"{date_prefix}_{name_part}{ext}"
                target_path = os.path.join(mail_dir, new_name)

                # Если имени хватает для уникальности — ок; иначе добавим индекс
                base, ext = os.path.splitext(target_path)
                k = 1
                while os.path.exists(target_path):
                    target_path = f"{base}__{k}{ext}"
                    k += 1

                att.SaveAsFile(target_path)
                saved_files += 1
                # Добавляем информацию о скачанном вложении в лог
                attachment_info = f"{recv_py.strftime('%Y-%m-%d %H:%M:%S')} - {sender_name} - {fname}"
                downloaded_attachments.append(attachment_info)
                print(f"      - Сохранен как: {target_path}")
                
                # Если это письмо с несколькими вложениями, добавляем информацию о вложении
                for email_info in emails_with_multiple_attachments:
                    if email_info['date'] == recv_py.strftime('%Y-%m-%d %H:%M:%S') and email_info['subject'] == itm.Subject:
                        email_info['attachments'].append(fname)
            else:
                print(f"      - Пропущен (не CSV/Excel): {fname}")

        # Реализация логики сохранения информации о письмах, которые не подошли по критериям вложений, уже частично выполнена выше

        # Отслеживаем письма, в которых были найдены вложения
        emails_with_valid_attachments.add((recv_py.strftime('%Y-%m-%d %H:%M:%S'), sender_name, itm.Subject))

    print(f"\n=== СТАТИСТИКА ===")
    print(f"Всего писем с подходящими вложениями: {len(emails_with_valid_attachments)}")
    print(f"Просмотрено писем: {scanned_mails}")
    print(f"Сохранено CSV-вложений: {saved_files}")
    print(f"Папка: {root_dir}")
    
    # Выводим информацию о письмах без подходящих вложений
    if emails_without_valid_attachments:
        print(f"\n=== ПИСЬМА БЕЗ ПОДХОДЯЩИХ ВЛОЖЕНИЙ ===")
        print(f"Количество писем без подходящих вложений: {len(emails_without_valid_attachments)}")
        for email_info in emails_without_valid_attachments:
            print(f"  - {email_info}")
    
    # Выводим информацию о письмах с несколькими вложениями
    if emails_with_multiple_attachments:
        print(f"\n=== ПИСЬМА С НЕСКОЛЬКИМИ ПОДХОДЯЩИМИ ВЛОЖЕНИЯМИ ===")
        print(f"Количество писем с несколькими подходящими вложениями: {len(emails_with_multiple_attachments)}")
        for email_info in emails_with_multiple_attachments:
            print(f"  - Дата: {email_info['date']}, Отправитель: {email_info['sender']}, Тема: {email_info['subject']}")
            print(f"    Количество вложений: {email_info['attachment_count']}")
            print(f"    Вложения:")
            for attachment in email_info['attachments']:
                print(f"      - {attachment}")

if __name__ == "__main__":
    # Запускаем основную функцию с перехватом вывода в лог-файл
    console_logger.capture_console_output(
        output_dir=OUTPUT_DIR,
        date_start=DATE_START,
        date_end=DATE_END,
        script_name="main",
        func=main
    )
