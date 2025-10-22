import os
import pandas as pd
import requests
import re
from datetime import datetime, time, timedelta
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
import json

# Загрузка параметров из .env файла
from dotenv import load_dotenv
load_dotenv()

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def ensure_dir(path: str):
    """
    Создает директорию, если она не существует.
    
    :param path: Путь к директории
    :return: Путь к директории
    """
    os.makedirs(path, exist_ok=True)
    return path

def dt_range_str(d1: datetime, d2: datetime) -> str:
    """
    Формирует строку диапазона дат в формате YYYY-MM-DD_YYYY-MM-DD.
    
    :param d1: Начальная дата
    :param d2: Конечная дата
    :return: Строка диапазона дат
    """
    return f"{d1.date().isoformat()}_{d2.date().isoformat()}"

# Настройка логирования
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === ФУНКЦИИ ДЛЯ КРАСИВОГО ВЫВОДА ===
def print_header(text: str, char: str = "="):
    """
    Печатает заголовок с обрамлением
    """
    print(f"\n{char * len(text)}")
    print(text)
    print(f"{char * len(text)}\n")

def print_section(text: str, char: str = "-"):
    """
    Печатает раздел с обрамлением
    """
    print(f"\n{char * len(text)}")
    print(text)
    print(f"{char * len(text)}")

def print_info(text: str):
    """
    Печатает информационное сообщение
    """
    print(f"[INFO]      {text}")

def print_success(text: str):
    """
    Печатает сообщение об успешной операции
    """
    print(f"[SUCCESS]   {text}")

def print_warning(text: str):
    """
    Печатает предупреждение
    """
    print(f"[WARNING]   {text}")

def print_error(text: str):
    """
    Печатает сообщение об ошибке
    """
    print(f"[ERROR] {text}")

def print_fail(text: str):
    """
    Печатает сообщение об ошибке
    """
    print(f"[FAIL]      {text}")

def print_processing(text: str):
    """
    Печатает сообщение о процессе обработки
    """
    print(f"[PROCESSING]    {text}")

class TransactionParser:
    """
    Класс для автоматического обнаружения и парсинга CSV и Excel файлов,
    извлечения данных транзакций и отправки их на API endpoint.
    Поддерживает сопоставление транзакций с организациями по ext_id на основе
    названия папки, в которой находится файл транзакции.
    """
    
    
    def __init__(self, api_endpoint: str, bearer_token: str, directory_path: str, org_mapping_path: Optional[str] = None):
        """
        Инициализация парсера
        
        Args:
            api_endpoint: URL API endpoint для отправки транзакций
            bearer_token: Bearer токен для авторизации
            directory_path: Директория для поиска файлов
            org_mapping_path: Путь к файлу сопоставления организаций (опционально)
        """
        self.api_endpoint = api_endpoint
        self.bearer_token = bearer_token
        self.directory_path = Path(directory_path)
        self.org_mapping = self.load_org_mapping(org_mapping_path)
        # logger.info(f"Инициализирован TransactionParser с параметрами:")
        # logger.info(f"  - API Endpoint: {api_endpoint}")
        # logger.info(f"  - Bearer Token: {'*' * len(bearer_token) if bearer_token else 'None'}")
        # logger.info(f"  - Directory Path: {self.directory_path}")
        # logger.info(f"  - Directory Path exists: {self.directory_path.exists()}")
        # logger.info(f"  - Organization mapping loaded: {len(self.org_mapping) if self.org_mapping else 0} entries")
        
        # Возможные имена колонок для каждой сущности
        self.possible_column_names = {
            'datetime': ['date-time_transaction'],
            'id_transaction': ['id_transaction'],
            'card_number': ['id_card'],  # Номер карты теперь берется из id_card
            'total_price': ['total_price'],
            'total_discount': ['total_discount']
        }

    def load_org_mapping(self, org_mapping_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Загрузка сопоставления названий организаций с их идентификаторами
        
        Args:
            org_mapping_path: Путь к файлу сопоставления (опционально)
            
        Returns:
            Словарь сопоставления названий организаций и их идентификаторов
        """
        if org_mapping_path is None:
            org_mapping_path = "org_mapping.json"  # путь по умолчанию
            
        # Сохраняем путь к файлу сопоставления как атрибут экземпляра
        self._org_mapping_path = org_mapping_path
        
        mapping_file = Path(org_mapping_path)
        if mapping_file.exists():
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get("organization_mappings", {})
            except Exception as e:
                logger.error(f"Ошибка при загрузке файла сопоставления организаций {org_mapping_path}: {e}")
                return {}
        else:
            logger.warning(f"Файл сопоставления организаций не найден: {org_mapping_path}. Используется пустое сопоставление.")
            return {}

    def get_org_id_by_folder_name(self, folder_name: str) -> Optional[int]:
        """
        Получение идентификатора организации по названию папки
        
        Args:
            folder_name: Название папки (организации)
            
        Returns:
            Идентификатор организации или None, если не найден
        """
        # Проверяем точное совпадение в сопоставлениях (регистронезависимо)
        folder_name_lower = folder_name.lower()
        for org_name, org_data in self.org_mapping.items():
            if org_name.lower() == folder_name_lower:
                if isinstance(org_data, dict):
                    if "ext_id" in org_data and org_data["ext_id"] is not None:
                        ext_id = org_data["ext_id"]
                        return int(ext_id) if isinstance(ext_id, str) else ext_id
                    else:
                        return None
                elif isinstance(org_data, int):
                    return org_data
                else:
                    # Если формат данных некорректный, возвращаем None
                    print_warning(f"Некорректный формат данных для организации '{org_name}', пропускаем сопоставление")
                    return None
        
        # Если ничего не найдено, возвращаем None
        return None
     
    def ensure_org_in_mapping(self, folder_name: str) -> None:
        """
        Обеспечивает наличие записи для папки в сопоставлениях,
        создавая пустую запись при необходимости
        """
        if folder_name not in self.org_mapping:
            # Создаем пустую запись для новой папки
            self.org_mapping[folder_name] = {"ext_id": None}
            # logger.info(f"Добавлена новая запись в сопоставления для папки '{folder_name}' с пустым ext_id")
            # Сохраняем обновленное сопоставление в файл
            self.save_org_mapping()
    
    def save_org_mapping(self) -> None:
        """
        Сохранение сопоставления организаций в файл
        """
        org_mapping_path = getattr(self, '_org_mapping_path', 'org_mapping.json')
        mapping_file = Path(org_mapping_path)
        try:
            # Создаем директорию, если она не существует
            mapping_file.parent.mkdir(parents=True, exist_ok=True)
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump({"organization_mappings": self.org_mapping}, f, ensure_ascii=False, indent=2)
            # logger.info(f"Сопоставление организаций сохранено в {mapping_file}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении файла сопоставления организаций {mapping_file}: {e}")
    
    def find_transaction_files(self) -> List[Path]:
        """
        Поиск CSV и Excel файлов в указанной директории и её поддиректориях
        
        Returns:
            Список путей к найденным файлам
        """
        # print_info(f"Открываем папку: {self.directory_path}")
        # logger.info(f"Поиск файлов транзакций в директории: {self.directory_path}")
        # logger.info(f"Существует ли директория: {self.directory_path.exists()}")
        
        if not self.directory_path.exists():
            logger.error(f"Директория не существует: {self.directory_path}")
            print_error(f"Директория не существует: {self.directory_path}")
            return []
        
        files = []
        folder_stats = {}  # Словарь для отслеживания файлов по папкам
        
        for ext in ['*.csv', '*.xlsx', '*.xls']:
            # Сначала ищем в текущей директории
            ext_files = list(self.directory_path.glob(ext))
            # logger.info(f"Найдено {len(ext_files)} файлов с расширением {ext} в основной директории: {[f.name for f in ext_files]}")
            
            # Собираем статистику по папкам
            for file in ext_files:
                folder_name = file.parent.name
                if folder_name not in folder_stats:
                    folder_stats[folder_name] = []
                folder_stats[folder_name].append(file.name)
            
            files.extend(ext_files)
            
            # Затем ищем рекурсивно в поддиректориях
            recursive_ext_files = list(self.directory_path.rglob(ext))
            recursive_ext_files = [f for f in recursive_ext_files if f.parent != self.directory_path]  # Исключаем уже найденные
            
            for file in recursive_ext_files:
                folder_name = file.parent.name
                if folder_name not in folder_stats:
                    folder_stats[folder_name] = []
                folder_stats[folder_name].append(file.name)
            
            # logger.info(f"Найдено {len(recursive_ext_files)} файлов с расширением {ext} в поддиректориях: {[f.name for f in recursive_ext_files]}")
            files.extend(recursive_ext_files)
        
        # logger.info(f"Всего найдено {len(files)} файлов для обработки")
        
        # Выводим информацию о найденных папках и количестве файлов в каждой
        print(f"Найдено {len(folder_stats)} папок с файлами транзакций:")
        for folder, file_list in folder_stats.items():
            print(f"{folder}: {len(file_list)} файлов")
            for file_name in file_list:
                print(f"    {file_name}")
        
        print(f"Всего файлов для обработки: {len(files)}")
        
        return files
    
    
    def normalize_date_format(self, date_value: Any) -> Optional[str]:
        """
        Нормализация различных форматов даты в ISO формат
        
        Args:
            date_value: Значение даты в любом формате
            
        Returns:
            Дата в формате ISO или None если не удалось распознать
        """
        if pd.isna(date_value) or date_value is None:
            return None
        
        # Пробуем различные форматы дат
        possible_formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d.%m.%Y",
            "%m/%d/%Y",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M",
            "%d.%m.%Y %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
        ]
        
        date_str = str(date_value).strip()
        
        for fmt in possible_formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                return parsed_date.isoformat()
            except ValueError:
                continue
        
        # Если не удалось распознать формат, пробуем использовать pandas
        try:
            parsed_date = pd.to_datetime(date_str)
            if pd.isna(parsed_date):
                print_warning(f"Не удалось распознать формат даты: {date_str}")
                return None
            return parsed_date.isoformat()
        except:
            print_warning(f"Не удалось распознать формат даты: {date_str}")
            return None
    
    
    def find_column_by_names(self, df: pd.DataFrame, possible_names: List[str]) -> Optional[str]:
        """
        Поиск колонки по возможным именам (без учета регистра)
        
        Args:
            df: DataFrame
            possible_names: Список возможных имен колонки
            
        Returns:
            Название найденной колонки или None
        """
        #logger.debug(f"Поиск колонки среди возможных имен: {possible_names}")
        #logger.debug(f"Доступные колонки в DataFrame: {list(df.columns)}")
        
        for name in possible_names:
            for col in df.columns:
                if col.lower().replace('_', '').replace(' ', '') == name.lower().replace('_', '').replace(' ', ''):
                    #logger.debug(f"Найдена колонка: {col} для имени {name}")
                    return col
        
        logger.debug(f"Не найдено ни одной из возможных колонок: {possible_names}")
        return None
    
    def validate_transaction_data(self, transaction: Dict[str, Any]) -> bool:
        """
        Валидация данных транзакции
        
        Args:
            transaction: Словарь с данными транзакции
            
        Returns:
            True если данные валидны, иначе False
        """
        required_fields = ['id_transaction', 'card_number', 'total_price', 'ext_id']
        
        # Проверяем наличие обязательных полей
        for field in required_fields:
            if field not in transaction or transaction[field] is None:
                logger.error(f"Отсутствует обязательное поле: {field}")
                return False
        
        # Проверяем формат даты
        if 'datetime' in transaction and transaction['datetime']:
            try:
                datetime.fromisoformat(transaction['datetime'].replace('Z', '+00:00'))
            except ValueError:
                logger.error(f"Неверный формат даты: {transaction['datetime']}")
                return False
        
        # Проверяем, что номер карты не пустой
        card_number = transaction['card_number']
        if not card_number or str(card_number).strip() == '':
            logger.error(f"Номер карты пустой: {card_number}")
            return False
        
        # Проверяем, что ext_id - это число
        try:
            int(transaction['ext_id'])
        except (ValueError, TypeError):
            logger.error(f"Неверный формат ext_id: {transaction.get('ext_id')}")
            return False
        
        # Проверяем, что цены - числа
        try:
            float(transaction['total_price'])
            if 'total_discount' in transaction and transaction['total_discount'] is not None:
                float(transaction['total_discount'])
        except (ValueError, TypeError):
            logger.error(f"Неверный формат цены: {transaction.get('total_price')} или скидки: {transaction.get('total_discount')}")
            return False
        
        return True
    
    def parse_file_detailed(self, file_path: Path, folder_name: str = None) -> tuple:
        """
        Парсинг CSV или Excel файла и извлечение транзакций с детализированным выводом
        
        Args:
            file_path: Путь к файлу
            folder_name: Название папки (если не указано, определяется из пути)
            
        Returns:
            tuple: (список транзакций, количество извлеченных, количество не извлеченных)
        """
        # Определяем папку (организацию) по родительскому каталогу файла, если не указана
        if folder_name is None:
            folder_name = file_path.parent.name
        
        # Обеспечиваем наличие записи для папки в сопоставлениях
        self.ensure_org_in_mapping(folder_name)
        org_id = self.get_org_id_by_folder_name(folder_name)
        
        # Проверяем, есть ли организация в справочнике
        if org_id is None:
            print_warning(f"{file_path.name:<50}    Не найден идентификатор организации для папки '{folder_name}' или ext_id пустой. Файл будет пропущен.")
            return None, 0, 0  # None означает, что файл не был обработан
        
        # Для совместимости с API используем org_id как ext_id
        ext_id = org_id
        
        try:
            # Загружаем файл в зависимости от расширения
            if file_path.suffix.lower() == '.csv':
                # Пробуем определить разделитель
                with open(file_path, 'r', encoding='utf-8') as f:
                    first_line = f.readline()
                
                if ';' in first_line:
                    df = pd.read_csv(file_path, delimiter=';')
                elif ',' in first_line:
                    df = pd.read_csv(file_path, delimiter=',')
                else:
                    df = pd.read_csv(file_path)
            elif file_path.suffix.lower() in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                logger.error(f"Неподдерживаемый формат файла: {file_path}")
                return None, 0, 0
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {file_path}: {e}")
            return None, 0, 0
        
        # Находим колонки по возможным именам
        datetime_col = self.find_column_by_names(df, self.possible_column_names['datetime'])
        id_transaction_col = self.find_column_by_names(df, self.possible_column_names['id_transaction'])
        card_number_col = self.find_column_by_names(df, self.possible_column_names['card_number'])  # Теперь ищем колонку id_card
        total_price_col = self.find_column_by_names(df, self.possible_column_names['total_price'])
        total_discount_col = self.find_column_by_names(df, self.possible_column_names['total_discount'])
        
        if not all([id_transaction_col, card_number_col, total_price_col]):
            logger.error(f"Не найдены все обязательные колонки в файле {file_path}")
            logger.error(f"datetime: {datetime_col}, id_transaction: {id_transaction_col}, card_number: {card_number_col}, total_price: {total_price_col}")
            return None, 0, 0
        
        transactions = []
        extracted_count = 0
        failed_count = 0
        
        for index, row in df.iterrows():
            try:
                transaction = {}
                
                # Парсим дату
                if datetime_col:
                    date_value = row[datetime_col]
                    transaction['datetime'] = self.normalize_date_format(date_value)
                
                # Парсим ID транзакции
                transaction['id_transaction'] = str(row[id_transaction_col]).strip() if pd.notna(row[id_transaction_col]) else None
                
                # Добавляем ext_id из названия папки
                transaction['ext_id'] = ext_id
                
                # Берем номер карты из id_card без парсинга
                card_number = row[card_number_col]
                if pd.notna(card_number) and str(card_number).strip():
                    transaction['card_number'] = str(card_number).strip()
                else:
                    print_warning(f"{file_path.name:<50}    Пропускаем транзакцию с пустым номером карты: {card_number}")
                    failed_count += 1
                    continue
                
                # Парсим общую цену
                try:
                    transaction['total_price'] = float(row[total_price_col])
                except (ValueError, TypeError):
                    print_warning(f"{file_path.name:<50}    Пропускаем транзакцию с неверной ценой: {row[total_price_col]}")
                    failed_count += 1
                    continue
                
                # Парсим скидку
                if total_discount_col and pd.notna(row[total_discount_col]):
                    try:
                        transaction['total_discount'] = float(row[total_discount_col])
                    except (ValueError, TypeError):
                        print_warning(f"{file_path.name:<50}    Пропускаем скидку с неверным форматом: {row[total_discount_col]}")
                        transaction['total_discount'] = 0.0
                else:
                    transaction['total_discount'] = 0.0
                
                # Валидируем транзакцию
                if self.validate_transaction_data(transaction):
                    transactions.append(transaction)
                    extracted_count += 1
                else:
                    print_warning(f"{file_path.name:<50}    Транзакция не прошла валидацию: {transaction}")
                    failed_count += 1
                
            except Exception as e:
                logger.error(f"Ошибка при обработке строки {index} в файле {file_path}: {e}")
                failed_count += 1
                continue
        
        return transactions, extracted_count, failed_count
    
    def parse_file(self, file_path: Path) -> List[Dict[str, Any]]:
        """
        Парсинг CSV или Excel файла и извлечение транзакций (для совместимости)
        
        Args:
            file_path: Путь к файлу
            
        Returns:
            Список транзакций
        """
        transactions, _, _ = self.parse_file_detailed(file_path)
        return transactions if transactions is not None else []
    
    def send_transactions(self, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Отправка транзакций на API endpoint
        
        Args:
            transactions: Список транзакций для отправки
            
        Returns:
            Ответ от API
        """
        # headers = {
        #     'Content-Type': 'application/json',
        #     'Authorization': f'Bearer {self.bearer_token}'
        # }
        #
        # payload = {
        #     'transactions': transactions
        # }
        #
        # try:
        #     response = requests.post(self.api_endpoint, json=payload, headers=headers)
        #     response.raise_for_status()
        #     logger.info(f"Успешно отправлено {len(transactions)} транзакций")
        #     return {
        #         'success': True,
        #         'status_code': response.status_code,
        #         'response': response.json()
        #     }
        # except requests.exceptions.HTTPError as e:
        #     logger.error(f"HTTP ошибка при отправке транзакций: {e}")
        #     return {
        #         'success': False,
        #         'status_code': response.status_code if 'response' in locals() else None,
        #         'error': str(e),
        #         'response': response.text if 'response' in locals() else None
        #     }
        # except requests.exceptions.RequestException as e:
        #     logger.error(f"Ошибка при отправке транзакций: {e}")
        #     return {
        #         'success': False,
        #         'error': str(e)
        #     }
        # except Exception as e:
        #     logger.error(f"Неизвестная ошибка при отправке транзакций: {e}")
        #     return {
        #         'success': False,
        #         'error': str(e)
        #     }
        
        # Заглушка вместо отправки HTTP - просто возвращаем успешный результат без отправки
        print_info(f"Отправка {len(transactions)} транзакций пропущена (HTTP-запрос закомментирован)")
        return {
            'success': True,
            'status_code': 200,
            'response': {'message': 'Transactions processed (mock response)', 'count': len(transactions)}
        }
    
    def process_directory(self) -> Dict[str, Any]:
        """
        Обработка всей директории: поиск файлов, парсинг и отправка транзакций
        
        Returns:
            Статистика обработки
        """
        print_header("НАЧАЛО ОБРАБОТКИ ФАЙЛОВ ТРАНЗАКЦИЙ")
        
        files = self.find_transaction_files()
        all_transactions = []
        
        # Группируем файлы по папкам
        files_by_folder = {}
        for file_path in files:
            folder_name = file_path.parent.name
            if folder_name not in files_by_folder:
                files_by_folder[folder_name] = []
            files_by_folder[folder_name].append(file_path)
        
        # Статистика по папкам
        folder_stats = {}
        
        for folder_name, folder_files in files_by_folder.items():
            print(f"\n{'='*79}")
            print(f"Папка: {folder_name}")
            print(f"{'-'*79}")
            
            folder_processed = 0
            folder_success = 0
            folder_errors = 0
            folder_extracted = 0
            folder_failed = 0
            folder_transactions = []
            
            for file_path in folder_files:
                try:
                    # Обработка файла с детализированным выводом
                    transactions, extracted_count, failed_count = self.parse_file_detailed(file_path, folder_name)
                    
                    if transactions is not None:
                        folder_processed += 1
                        if extracted_count > 0 or (extracted_count == 0 and failed_count == 0):  # файл обработан без ошибок
                            folder_success += 1
                            print_success(f"{file_path.name:<50}    Извлечено: {extracted_count:<3}   Не извлечено: {failed_count:<3}")
                        else:
                            folder_errors += 1
                            print_fail(f"{file_path.name:<50}    Извлечено: {extracted_count:<3}   Не извлечено: {failed_count:<3}")
                            
                        folder_extracted += extracted_count
                        folder_failed += failed_count
                        all_transactions.extend(transactions)
                        
                        # Выводим результат обработки файла
                    else:
                        # файл не обработан (ошибка или отсутствие org_id)
                        folder_processed += 1
                        folder_errors += 1
                        print_fail(f"{file_path.name:<50}    Извлечено: 0   Не извлечено: 0")
                        
                except Exception as e:
                    print_error(f"{file_path.name:<50}    Ошибка при парсинге файла: {e}")
                    folder_processed += 1
                    folder_errors += 1
                    print_fail(f"{file_path.name:<50}    Извлечено: 0   Не извлечено: 0")
                    continue
            
            # Выводим итоги по папке
            print(f"{'-'*79}")
            print("Итого по папке:")
            print(f"  Файлов обработано:       {folder_processed}")
            print(f"  Успешно:                 {folder_success}")
            print(f"  Ошибок:                  {folder_errors}")
            print(f"  Извлечено транзакций:    {folder_extracted}")
            print(f"  Не извлечено транзакций: {folder_failed}")
            
            folder_stats[folder_name] = {
                'processed': folder_processed,
                'success': folder_success,
                'errors': folder_errors,
                'extracted': folder_extracted,
                'failed': folder_failed
            }
        
        # Выводим общий итог
        print(f"\n{'='*79}")
        print("ОБЩИЙ ИТОГ:")
        print(f"{'-'*79}")
        
        total_folders = len(folder_stats)
        total_files = sum(stat['processed'] for stat in folder_stats.values())
        total_success = sum(stat['success'] for stat in folder_stats.values())
        total_errors = sum(stat['errors'] for stat in folder_stats.values())
        total_extracted = sum(stat['extracted'] for stat in folder_stats.values())
        total_failed = sum(stat['failed'] for stat in folder_stats.values())
        
        print(f"Всего папок:              {total_folders}")
        print(f"Всего файлов:             {total_files}")
        print(f"Успешно:                  {total_success}")
        print(f"Ошибок:                   {total_errors}")
        print(f"Извлечено транзакций:     {total_extracted}")
        print(f"Не извлечено транзакций:  {total_failed}")
        print(f"{'='*79}")
        
        if all_transactions:
            print_success("Транзакции успешно извлечены, готовим к отправке...")
            result = self.send_transactions(all_transactions)
            result['total_processed_files'] = total_success + total_errors
            result['total_skipped_files'] = total_errors
            result['total_transactions_extracted'] = total_extracted
            return result
        else:
            print_warning("Не найдено транзакций для отправки")
            return {
                'success': True,
                'total_processed_files': total_success + total_errors,
                'total_skipped_files': total_errors,
                'total_transactions_extracted': 0,
                'message': 'No transactions found to send'
            }


def main():
    """
    Пример использования
    """
    # Параметры для подключения к API
    API_ENDPOINT = os.getenv('API_ENDPOINT', 'http://localhost:8000/api/v2/uploadTransactions')
    BEARER_TOKEN = os.getenv('BEARER_TOKEN', 'your_token_here')
    
    # Загружаем параметры даты и OUTPUT_DIR для формирования DIRECTORY_PATH
    DATE_START = os.getenv("DATE_START", "2025-10-01")  # включительно, локальное время ПК
    DATE_END = os.getenv("DATE_END", "2025-10-21") # включительно, локальное время ПК
    OUTPUT_DIR = os.getenv("OUTPUT_DIR", r"C:\Outlook_CSV_Downloads")  # можно изменить
    
    # Проверяем, задан ли DIRECTORY_PATH в .env файле
    DIRECTORY_PATH = os.getenv("DIRECTORY_PATH")
    # logger.info(f"Значение DIRECTORY_PATH из .env: {DIRECTORY_PATH}")
    # logger.info(f"Значения DATE_START: {DATE_START}, DATE_END: {DATE_END}, OUTPUT_DIR: {OUTPUT_DIR}")
    
    if DIRECTORY_PATH is None:
        # Если DIRECTORY_PATH не задан, вычисляем путь на основе дат
        start_dt = datetime.combine(datetime.fromisoformat(DATE_START).date(), time(0, 0, 0))
        end_dt_inclusive = datetime.combine(datetime.fromisoformat(DATE_END).date(), time(23, 59, 59))
        
        # Формируем DIRECTORY_PATH как OUTPUT_DIR + диапазон дат
        calculated_path = os.path.join(OUTPUT_DIR, dt_range_str(start_dt, end_dt_inclusive))
        # logger.info(f"Вычисленный путь к директории: {calculated_path}")
        DIRECTORY_PATH = ensure_dir(calculated_path)
    else:
        # Если DIRECTORY_PATH задан, используем его как есть
        # logger.info(f"Используем DIRECTORY_PATH из .env: {DIRECTORY_PATH}")
        DIRECTORY_PATH = ensure_dir(DIRECTORY_PATH)
    
    # logger.info(f"Финальный путь к директории: {DIRECTORY_PATH}")
    
    # Загружаем путь к файлу сопоставления организаций из .env
    ORG_MAPPING_PATH = os.getenv("ORG_MAPPING_PATH")
    
    print_header("ЗАПУСК ПАРСЕРА ТРАНЗАКЦИЙ")
    print_info(f"API Endpoint: {API_ENDPOINT}")
    print_info(f"Диапазон дат: {DATE_START} - {DATE_END}")
    print_info(f"Директория для обработки: {DIRECTORY_PATH}")
    print_info(f"Файл сопоставления организаций: {ORG_MAPPING_PATH or 'org_mapping.json (по умолчанию)'}")
    print_processing("Инициализация парсера...")
    
    parser = TransactionParser(
        api_endpoint=API_ENDPOINT,
        bearer_token=BEARER_TOKEN,
        directory_path=DIRECTORY_PATH,
        org_mapping_path=ORG_MAPPING_PATH
    )
    
    result = parser.process_directory()

if __name__ == "__main__":
    main()