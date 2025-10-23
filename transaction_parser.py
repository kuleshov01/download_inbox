import json
import logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
import os
from datetime import datetime, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv
import console_logger

load_dotenv()

DEFAULT_ORG_MAPPING_PATH = "org_mapping.json"
SUPPORTED_FILE_PATTERNS: Tuple[str, ...] = ("*.csv", "*.xlsx", "*.xls")
CSV_DELIMITERS: Tuple[str, ...] = (";", ",")
DATE_FORMATS: Tuple[str, ...] = (
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
)
FOLDER_SUMMARY_WIDTH = 79
Transaction = Dict[str, Any]
COLUMN_NAME_CANDIDATES: Dict[str, List[str]] = {
    "datetime": ["date-time_transaction", "datetime_transaction"],
    "id_transaction": ["id_transaction"],
    "card_number": ["id_card"],
    "total_price": ["total_price"],
    "total_discount": ["total_discount"],
}


class TransactionSkip(Exception):
    """Внутреннее исключение для пропуска некорректных строк транзакций."""


# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def ensure_dir(path: str) -> str:
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
    print(f"[ERROR]     {text}")

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
    Поддерживает сопоставление транзакций с организациями по token на основе
    названия папки, в которой находится файл транзакции.
    
    
    """
    
    
    def __init__(self, api_endpoint: str, directory_path: str, org_mapping_path: Optional[str] = None) -> None:
        """
        Инициализация парсера
        
        Args:
            api_endpoint: URL API endpoint для отправки транзакций
            directory_path: Директория для поиска файлов
            org_mapping_path: Путь к файлу сопоставления организаций (опционально)
        """
        self.api_endpoint: str = api_endpoint
        self.directory_path: Path = Path(directory_path)
        self.org_mapping: Dict[str, Any] = self.load_org_mapping(org_mapping_path)
        self.possible_column_names: Dict[str, List[str]] = {
            key: value[:] for key, value in COLUMN_NAME_CANDIDATES.items()
        }
        # logger.info(f"Инициализирован TransactionParser с параметрами:")
        # logger.info(f"  - API Endpoint: {api_endpoint}")
        # logger.info(f"  - Directory Path: {self.directory_path}")
        # logger.info(f" - Directory Path exists: {self.directory_path.exists()}")
        # logger.info(f" - Organization mapping loaded: {len(self.org_mapping) if self.org_mapping else 0} entries")

    def load_org_mapping(self, org_mapping_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Загрузка сопоставления названий организаций с их идентификаторами
        
        Args:
            org_mapping_path: Путь к файлу сопоставления (опционально)
            
        Returns:
            Словарь сопоставления названий организаций их идентификаторов
        """
        path = org_mapping_path or DEFAULT_ORG_MAPPING_PATH
        self._org_mapping_path: str = path
        mapping_file = Path(path)

        if not mapping_file.exists():
            logger.warning(f"Файл сопоставления организаций не найден: {path}. Используется пустое сопоставление.")
            return {}

        try:
            with open(mapping_file, "r", encoding="utf-8") as mapping:
                data = json.load(mapping)
        except json.JSONDecodeError as exc:
            logger.error(f"Ошибка при разборе файла сопоставления организаций {path}: {exc}")
            return {}
        except OSError as exc:
            logger.error(f"Ошибка при загрузке файла сопоставления организаций {path}: {exc}")
            return {}

        mappings = data.get("organization_mappings", {})
        if isinstance(mappings, dict):
            return mappings

        logger.error(f"Некорректный формат файла сопоставления организаций: {path}")
        return {}

    def _extract_token(self, org_name: str, org_data: Any) -> Optional[str]:
        """Возвращает token для записи справочника организаций."""
        if isinstance(org_data, dict):
            token = org_data.get("token")
            if token is None:
                return None
            return token
        if isinstance(org_data, str):
            return org_data

        print_warning(f"Некорректный формат данных для организации '{org_name}', пропускаем сопоставление")
        return None

    def get_org_id_by_folder_name(self, folder_name: str) -> Optional[str]:
        """
        Получение идентификатора организации по названию папки
        
        Args:
            folder_name: Название папки (организации)
            
        Returns:
            Токен организации или None, если не найден
        """
        # Проверяем точное совпадение в сопоставлениях (регистронезависимо)
        folder_name_lower = folder_name.lower()
        for org_name, org_data in self.org_mapping.items():
            if org_name.lower() == folder_name_lower:
                return self._extract_token(org_name, org_data)

        return None
     
    def ensure_org_in_mapping(self, folder_name: str) -> None:
        """
        Обеспечивает наличие записи для папки в сопоставлениях,
        создавая пустую запись при необходимости
        """
        if folder_name not in self.org_mapping:
            # Создаем пустую запись для новой папки
            self.org_mapping[folder_name] = {"token": "", "name": ""}
            # logger.info(f"Добавлена новая запись в сопоставления для папки '{folder_name}' с пустым token")
            # Сохраняем обновленное сопоставление в файл
            self.save_org_mapping()
    
    def save_org_mapping(self) -> None:
        """
        Сохранение сопоставления организаций в файл
        """
        org_mapping_path = getattr(self, '_org_mapping_path', DEFAULT_ORG_MAPPING_PATH)
        mapping_file = Path(org_mapping_path)
        try:
            # Создаем директорию, если она не существует
            mapping_file.parent.mkdir(parents=True, exist_ok=True)
            with open(mapping_file, 'w', encoding='utf-8') as f:
                json.dump({"organization_mappings": self.org_mapping}, f, ensure_ascii=False, indent=2)
            # logger.info(f"Сопоставление организаций сохранено в {mapping_file}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении файла сопоставления организаций {mapping_file}: {e}")
    
    def _collect_files_for_pattern(self, pattern: str) -> List[Path]:
        """Возвращает список файлов, подходящих под шаблон в директории и поддиректориях."""
        files = list(self.directory_path.glob(pattern))
        recursive_files = [
            file_path
            for file_path in self.directory_path.rglob(pattern)
            if file_path.parent != self.directory_path
        ]
        files.extend(recursive_files)
        return files

    @staticmethod
    def _update_folder_stats(folder_stats: Dict[str, List[str]], files: Iterable[Path]) -> None:
        """Обновляет статистику файлов по папкам."""
        for file_path in files:
            folder_stats.setdefault(file_path.parent.name, []).append(file_path.name)
    
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
        
        files: List[Path] = []
        folder_stats: Dict[str, List[str]] = {}
        
        for pattern in SUPPORTED_FILE_PATTERNS:
            pattern_files = self._collect_files_for_pattern(pattern)
            files.extend(pattern_files)
            self._update_folder_stats(folder_stats, pattern_files)
        
        print(f"Найдено {len(folder_stats)} папок с файлами транзакций:")
        for folder, file_list in folder_stats.items():
            print(f"{folder}: {len(file_list)} файлов")
            for file_name in file_list:
                print(f"    {file_name}")
        
        print(f"Всего файлов для обработки: {len(files)}")
        
        return files

    @staticmethod
    def _detect_csv_delimiter(file_path: Path) -> Optional[str]:
        """Определяет разделитель CSV файла по первой строке."""
        with open(file_path, "r", encoding="utf-8") as csv_file:
            first_line = csv_file.readline()
        for delimiter in CSV_DELIMITERS:
            if delimiter in first_line:
                return delimiter
        return None

    def _read_transaction_file(self, file_path: Path) -> pd.DataFrame:
        """Возвращает DataFrame с данными транзакций из CSV или Excel файла."""
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            delimiter = self._detect_csv_delimiter(file_path)
            # Указываем, что столбцы с идентификаторами должны быть строковыми
            return pd.read_csv(file_path, delimiter=delimiter, dtype={'id_card': str, 'id_transaction': str}) if delimiter else pd.read_csv(file_path, dtype={'id_card': str, 'id_transaction': str})
        if suffix in (".xlsx", ".xls"):
            # Для Excel файлов также укажем типы данных
            return pd.read_excel(file_path, dtype={'id_card': str, 'id_transaction': str})
        raise ValueError(f"Неподдерживаемый формат файла: {file_path}")

    @staticmethod
    def _parse_card_number(card_number: Any, file_path: Path) -> Optional[str]:
        """Проверяет номер карты и возвращает его строковое представление."""
        if pd.notna(card_number) and str(card_number).strip():
            card_number_str = str(card_number).strip()
            # Проверяем, что номер карты состоит из 19 символов и начинается с "9643"
            if len(card_number_str) == 19 and card_number_str.startswith("9643"):
                return card_number_str
            else:
                print_warning(f"{file_path.name:<50}    Пропускаем транзакцию с некорректным номером карты: {card_number_str}")
                return None
        print_warning(f"{file_path.name:<50}    Пропускаем транзакцию с пустым номером карты: {card_number}")
        return None

    @staticmethod
    def _parse_total_price(total_price: Any, file_path: Path) -> Optional[float]:
        """Преобразует цену к float, печатая предупреждение при ошибке."""
        try:
            return float(total_price)
        except (ValueError, TypeError):
            print_warning(f"{file_path.name:<50}    Пропускаем транзакцию с неверной ценой: {total_price}")
            return None

    @staticmethod
    def _parse_total_discount(total_discount: Any, file_path: Path) -> Optional[float]:
        """Преобразует цену к float, печатая предупреждение при ошибке."""
        try:
            return float(total_discount)
        except (ValueError, TypeError):
            print_warning(f"{file_path.name:<50}    Пропускаем транзакцию с неверной скидкой: {total_discount}")
            return None

    def _build_transaction(
        self,
        row: pd.Series,
        file_path: Path,
        datetime_col: Optional[str],
        id_transaction_col: str,
        card_number_col: str,
        total_price_col: str,
        total_discount_col: Optional[str],
    ) -> Transaction:
        """Формирует словарь транзакции на основе строки DataFrame."""
        transaction: Transaction = {}

        if datetime_col:
            transaction["datetime"] = self.normalize_date_format(row[datetime_col])

        transaction["id_transaction"] = str(row[id_transaction_col]).strip() if pd.notna(row[id_transaction_col]) else None

        card_number = self._parse_card_number(row[card_number_col], file_path)
        if card_number is None:
            raise TransactionSkip
        transaction["card_number"] = card_number

        total_price = self._parse_total_price(row[total_price_col], file_path)
        if total_price is None:
            raise TransactionSkip
        transaction["total_price"] = total_price

        total_discount = self._parse_total_discount(row[total_discount_col], file_path)
        if total_discount is None:
            raise TransactionSkip
        transaction["total_discount"] = total_discount

        return transaction

    def _extract_transactions(
        self,
        df: pd.DataFrame,
        file_path: Path,
        datetime_col: Optional[str],
        id_transaction_col: str,
        card_number_col: str,
        total_price_col: str,
        total_discount_col: Optional[str],
    ) -> Tuple[List[Transaction], int, int]:
        """Извлекает транзакции из DataFrame с подсчетом статистики."""
        transactions: List[Transaction] = []
        extracted_count = 0
        failed_count = 0

        for index, row in df.iterrows():
            try:
                transaction = self._build_transaction(
                    row,
                    file_path,
                    datetime_col,
                    id_transaction_col,
                    card_number_col,
                    total_price_col,
                    total_discount_col,
                )
                extracted_count += 1
                transactions.append(transaction)  # Добавляем транзакцию в список
            except TransactionSkip:
                failed_count += 1
                continue
            except Exception as exc:
                logger.error(f"Ошибка при обработке строки {index} в файле {file_path}: {exc}")
                failed_count += 1
                continue

        # print(f"DEBUG: _extract_transactions - вернули {len(transactions)} транзакций, extracted_count={extracted_count}, failed_count={failed_count}")
        return transactions, extracted_count, failed_count

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
        date_str = str(date_value).strip()
        
        for fmt in DATE_FORMATS:
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
        except Exception:
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
        
        return None
    
    
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
        
        self.ensure_org_in_mapping(folder_name)
        token = self.get_org_id_by_folder_name(folder_name)

        if token == "":
            print_warning(f"{file_path.name:<50}    Не найден токен для папки '{folder_name}' или token пустой. Файл будет пропущен.")
            return None, 0, 0

        try:
            df = self._read_transaction_file(file_path)
            # print(f"DEBUG: parse_file_detailed - DataFrame shape: {df.shape}")
        except ValueError as exc:
            logger.error(str(exc))
            return None, 0, 0
        except Exception as exc:
            logger.error(f"Ошибка при чтении файла {file_path}: {exc}")
            return None, 0, 0

        datetime_col = self.find_column_by_names(df, self.possible_column_names["datetime"])
        id_transaction_col = self.find_column_by_names(df, self.possible_column_names["id_transaction"])
        card_number_col = self.find_column_by_names(df, self.possible_column_names["card_number"])
        total_price_col = self.find_column_by_names(df, self.possible_column_names["total_price"])
        total_discount_col = self.find_column_by_names(df, self.possible_column_names["total_discount"])

        # print(f"DEBUG: parse_file_detailed - datetime_col: {datetime_col}, id_transaction_col: {id_transaction_col}, card_number_col: {card_number_col}, total_price_col: {total_price_col}, total_discount_col: {total_discount_col}")

        required_cols = {
            "datetime": datetime_col,
            "id_transaction": id_transaction_col,
            "card_number": card_number_col,
            "total_price": total_price_col,
            "total_discount": total_discount_col,
        }

        # Определяем, какие колонки не найдены (значение None или пустое)
        missing = [name for name, value in required_cols.items() if not value]

        if missing:
            print_error(f"{file_path.name:<50}    Отсутствуют обязательные колонки: {', '.join(missing)}. см COLUMN_NAME_CANDIDATES")
            return None, 0, 0

        transactions, extracted_count, failed_count = self._extract_transactions(
            df,
            file_path,
            datetime_col,
            id_transaction_col,
            card_number_col,
            total_price_col,
            total_discount_col,
        )

        return transactions, extracted_count, failed_count
    

    def _process_folder(self, folder_name: str, folder_files: List[Path]) -> Tuple[List[Transaction], Dict[str, int], int]:
        """Обрабатывает файлы внутри папки и возвращает транзакции, статистику и количество успешно отправленных транзакций."""
        print(f"\n{'=' * FOLDER_SUMMARY_WIDTH}")
        print(f"Папка: {folder_name}")
        print(f"{'-' * FOLDER_SUMMARY_WIDTH}")
        
        stats = {
            "processed": 0,
            "success": 0,
            "errors": 0,
            "extracted": 0,
            "failed": 0,
        }
        folder_transactions: List[Transaction] = []
        successful_transactions_count = 0  # Добавляем подсчет успешно отправленных транзакций
        
        for file_path in folder_files:
            try:
                transactions, extracted_count, failed_count = self.parse_file_detailed(file_path, folder_name)
                # print(f"DEBUG: parse_file_detailed вернул {len(transactions) if transactions else 'None'} транзакций, extracted_count={extracted_count}, failed_count={failed_count}")
            except Exception as exc: # noqa: BLE001
                print_error(f"{file_path.name:<50}    Ошибка при парсинге файла: {exc}")
                stats["processed"] += 1
                stats["errors"] += 1
                print_fail(f"{file_path.name:<50}    Извлечено: 0   Не извлечено: 0")
                continue
        
            if transactions is not None:
                stats["processed"] += 1
                if extracted_count > 0 or (extracted_count == 0 and failed_count == 0):
                    stats["success"] += 1
                    print_success(f"{file_path.name:<50}    Извлечено: {extracted_count:<3}   Не извлечено: {failed_count:<3}")
                else:
                    stats["errors"] += 1
                    print_fail(f"{file_path.name:<50}    Извлечено: {extracted_count:<3}   Не извлечено: {failed_count:<3}")
        
                stats["extracted"] += extracted_count
                stats["failed"] += failed_count
                folder_transactions.extend(transactions)
            else:
                stats["processed"] += 1
                stats["errors"] += 1
                print_fail(f"{file_path.name:<50}    Извлечено: 0   Не извлечено: 0")
        
        # Отправляем транзакции для текущей папки
        if not folder_transactions:
            print_warning(f"{file_path.name:<50}    Нет транзакций для отправки в папке {folder_name}")
        else:
            print_info(f"{file_path.name:<50}    Отправка {len(folder_transactions)} транзакций для папки {folder_name}...")
            result = self.send_transactions(folder_transactions, folder_name)

            if not result.get('success'):
                print_error(
                    f"{file_path.name:<50}    Ошибка при отправке транзакций для папки {folder_name}: "
                    f"{result.get('error', 'Неизвестная ошибка')} (HTTP {result.get('status_code')})"
                )
            else:
                response = result.get('response')
                if not isinstance(response, dict) or 'status' not in response:
                    error_msg = response.get('message') if isinstance(response, dict) else str(response)
                    print_error(
                        f"{file_path.name:<50}    Некорректный формат ответа от сервера. "
                        f"Сообщение: {error_msg}"
                    )
                else:
                    status = response['status']
                    res = response.get('result') if isinstance(response.get('result'), dict) else {}

                    def _list(x):
                        return x if isinstance(x, (list, tuple)) else []

                    if status == 0:
                        print_error(f"{file_path.name:<50}    Ошибка валидации транзакций для папки {folder_name}: статус 0")
                        for msg in _list((response.get('errors') or {}).get('transactions')):
                            print_error(f"{file_path.name:<50}    Ошибка: {msg}")

                    elif status in (1, 2):
                        # Нормализация ключа успехов: для status=1 — 'success', для status=2 — 'successes'
                        success_ids = _list(res.get('success')) or _list(res.get('successes'))
                        fail_ids = _list(res.get('fail'))
                        already_exist_ids = _list(res.get('already_exist'))

                        # Обновляем количество успешно отправленных транзакций
                        successful_transactions_count = len(success_ids)

                        for item in fail_ids:
                            print_error(f"{file_path.name:<50}    Ошибка транзакции: {item}")
                        for item in already_exist_ids:
                            print_warning(f"{file_path.name:<50}    Транзакция уже существует: {item}")

                        print_info(
                            f"{file_path.name:<50}    Успешно: {len(success_ids)}, "
                            f"Ошибок: {len(fail_ids)}, Уже существуют: {len(already_exist_ids)}"
                        )

                    else:
                        print_error(
                            f"{file_path.name:<50}    Неизвестный статус при отправке транзакций для папки {folder_name}: {status}"
                        )

        print(f"{'-' * FOLDER_SUMMARY_WIDTH}")
        print("Итого по папке:")
        print(f" Файлов обработано:       {stats['processed']}")
        print(f" Успешно:                 {stats['success']}")
        print(f" Ошибок:                  {stats['errors']}")
        print(f" Извлечено транзакций:    {stats['extracted']}")
        print(f" Не извлечено транзакций: {stats['failed']}")
        print(f" Успешно отправлено транзакций: {successful_transactions_count}") 

        return folder_transactions, stats, successful_transactions_count
    
    def send_transactions(self, transactions: List[Transaction], folder_name: str = None) -> Dict[str, Any]:
        """
        Отправка транзакций на API endpoint
        
        Args:
            transactions: Список транзакций для отправки
            
        Returns:
            Ответ от API
        """
        
        token = self.get_org_id_by_folder_name(folder_name)

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {token}',
            'User-Agent': 'PostmanRuntime/7.48.0'
        }
    
        payload = {
            'transactions': transactions
        }
    
        try:
            response = requests.post(self.api_endpoint, json=payload, headers=headers)
            response.raise_for_status()
            # logger.info(f"Успешно отправлено {len(transactions)} транзакций")
            return {
                'success': True,
                'status_code': response.status_code,
                'response': response.json()
            }
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP ошибка при отправке транзакций: {e}")
            return {
                'success': False,
                'status_code': response.status_code if 'response' in locals() else None,
                'error': str(e),
                'response': response.text if 'response' in locals() else None
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при отправке транзакций: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        except Exception as e:
            logger.error(f"Неизвестная ошибка при отправке транзакций: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def process_directory(self) -> Dict[str, Any]:
        """
        Обработка всей директории: поиск файлов, парсинг и отправка транзакций
        
        Returns:
            Статистика обработки
        """
        print_header("НАЧАЛО ОБРАБОТКИ ФАЙЛОВ ТРАНЗАКЦИЙ")
        
        files = self.find_transaction_files()
        all_transactions: List[Transaction] = []
        
        files_by_folder: Dict[str, List[Path]] = {}
        for file_path in files:
            files_by_folder.setdefault(file_path.parent.name, []).append(file_path)
        
        folder_stats: Dict[str, Dict[str, int]] = {}
        
        total_successful_transactions = 0  # Добавляем подсчет успешно отправленных транзакций
        
        for folder_name, folder_files in files_by_folder.items():
            folder_transactions, stats, successful_count = self._process_folder(folder_name, folder_files)
            all_transactions.extend(folder_transactions)
            folder_stats[folder_name] = stats
            total_successful_transactions += successful_count # Суммируем успешно отправленные транзакции
        
        print(f"\n{'=' * FOLDER_SUMMARY_WIDTH}")
        print("ОБЩИЙ ИТОГ:")
        print(f"{'-' * FOLDER_SUMMARY_WIDTH}")
        
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
        print(f"Успешно отправлено транзакций: {total_successful_transactions}")  # Добавляем вывод успешно отправленных транзакций
        print(f"{'=' * FOLDER_SUMMARY_WIDTH}")
         
        # Убрана повторная отправка транзакций, так как они уже отправлены в _process_folder
        print_success("Обработка завершена. Транзакции были отправлены по папкам.")
        return {
            'success': True,
            'total_processed_files': total_success + total_errors,
            'total_skipped_files': total_errors,
            'total_transactions_extracted': total_extracted,
            'total_successful_transactions': total_successful_transactions,  # Добавляем общее количество успешно отправленных транзакций возвращаемый результат
            'message': 'Transactions processed and sent by folder'
        }


def main():
    """
    Пример использования
    """
    # Параметры для подключения к API
    API_ENDPOINT = os.getenv('API_ENDPOINT')
    
    # Загружаем параметры даты и OUTPUT_DIR для формирования DIRECTORY_PATH
    DATE_START = os.getenv("DATE_START", "2025-10-01")  # включительно, локальное время ПК
    DATE_END = os.getenv("DATE_END", "2025-10-21") # включительно, локальное время ПК
    OUTPUT_DIR = os.getenv("OUTPUT_DIR", r"C:\Outlook_CSV_Downloads")  # можно изменить
    
    # Проверяем, задан ли DIRECTORY_PATH в .env файле
    DIRECTORY_PATH = os.getenv("DIRECTORY_PATH")
    
    if DIRECTORY_PATH is None:
        # Если DIRECTORY_PATH не задан, вычисляем путь на основе дат
        start_dt = datetime.combine(datetime.fromisoformat(DATE_START).date(), time(0, 0, 0))
        end_dt_inclusive = datetime.combine(datetime.fromisoformat(DATE_END).date(), time(23, 59, 59))
        
        # Формируем DIRECTORY_PATH как OUTPUT_DIR + диапазон дат
        calculated_path = os.path.join(OUTPUT_DIR, dt_range_str(start_dt, end_dt_inclusive))
        DIRECTORY_PATH = ensure_dir(calculated_path)
    else:
        # Если DIRECTORY_PATH задан, используем его как есть
        DIRECTORY_PATH = ensure_dir(DIRECTORY_PATH)
    
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
        directory_path=DIRECTORY_PATH,
        org_mapping_path=ORG_MAPPING_PATH
    )
    
    result = parser.process_directory()

if __name__ == "__main__":
    # Получаем параметры даты из переменных окружения
    DATE_START = os.getenv("DATE_START", "2025-10-01")
    DATE_END = os.getenv("DATE_END", "2025-10-21")
    OUTPUT_DIR = os.getenv("OUTPUT_DIR", r"C:\Outlook_CSV_Downloads")
    
    # Запускаем основную функцию с перехватом вывода в лог-файл
    console_logger.capture_console_output(
        output_dir=OUTPUT_DIR,
        date_start=DATE_START,
        date_end=DATE_END,
        script_name="transaction_parser",
        func=main
    )
