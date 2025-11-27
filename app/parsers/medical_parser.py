import pandas as pd
import json
import os
from datetime import datetime
from collections import Counter
from config.logging import get_logger

logger = get_logger(__name__)


class MedicalParser:
    def __init__(self):
        self.processed_files = []

    def analyze_substances_and_consumers(self, input_file_path):
        """Анализирует Excel файл и возвращает структурированные данные"""
        try:
            logger.info(f"Начинаем анализ файла: {input_file_path}")

            # Загружаем файл, пропускаем первые 6 строк (шапка)
            df = pd.read_excel(input_file_path, sheet_name='Действующий', skiprows=6, header=None)

            logger.info(f"Загружено строк: {len(df)}")

            # Индексы колонок
            REG_NUMBER_COL = 2  # C - Номер регистрации
            DATE_COL = 3  # D - Дата регистрации
            MANUFACTURER_COL = 6  # G - Производитель
            COUNTRY_COL = 7  # H - Страна
            TRADE_NAME_COL = 8  # I - Торговое название
            INN_NAME_COL = 9  # J - МНН название
            FORMS_COL = 10  # K - Формы выпуска

            # 1. Отделяем субстанции от препаратов
            substances_mask = df[FORMS_COL].astype(str).str.contains('субстанция', case=False, na=False)
            substances_df = df[substances_mask].copy()
            preparations_df = df[~substances_mask].copy()

            logger.info(f"Найдено субстанций: {len(substances_df)}")
            logger.info(f"Найдено препаратов: {len(preparations_df)}")

            # 2. Создаем список уникальных МНН субстанций
            unique_substances = set()
            substance_manufacturers = {}

            for idx, row in substances_df.iterrows():
                inn_name = str(row[INN_NAME_COL]).strip()
                trade_name = str(row[TRADE_NAME_COL]).strip()
                manufacturer = str(row[MANUFACTURER_COL]).strip()

                if inn_name and inn_name not in ['', 'nan', '~']:
                    unique_substances.add(inn_name)
                    if inn_name not in substance_manufacturers:
                        substance_manufacturers[inn_name] = []
                    substance_manufacturers[inn_name].append(manufacturer)

                if trade_name and trade_name not in ['', 'nan', '~']:
                    unique_substances.add(trade_name)
                    if trade_name not in substance_manufacturers:
                        substance_manufacturers[trade_name] = []
                    substance_manufacturers[trade_name].append(manufacturer)

            logger.info(f"Уникальных субстанций для поиска - {len(unique_substances)}")

            # 3. Ищем препараты, которые используют эти субстанции
            consumers_data = []

            for substance in unique_substances:
                if len(substance) < 2:  # Пропускаем слишком короткие названия
                    continue

                substance_lower = substance.lower()

                for idx, row in preparations_df.iterrows():
                    inn_name = str(row[INN_NAME_COL]).lower()
                    trade_name = str(row[TRADE_NAME_COL]).lower()

                    # Ищем субстанцию в МНН или торговом названии препарата
                    if (substance_lower in inn_name or
                            substance_lower in trade_name):
                        consumer_info = {
                            'substance_name': substance,
                            'preparation_trade_name': str(row[TRADE_NAME_COL]),
                            'preparation_inn_name': str(row[INN_NAME_COL]),
                            'preparation_manufacturer': str(row[MANUFACTURER_COL]),
                            'preparation_country': str(row[COUNTRY_COL]),
                            'registration_number': str(row[REG_NUMBER_COL]),
                            'registration_date': str(row[DATE_COL]),
                            'release_forms': str(row[FORMS_COL])
                        }
                        consumers_data.append(consumer_info)

            logger.info(f"Найдено связей препарат-субстанция - {len(consumers_data)}")

            # Статистика
            manufacturer_stats = Counter()
            for manufacturers in substance_manufacturers.values():
                for manufacturer in manufacturers:
                    if manufacturer and manufacturer not in ['', 'nan', '~']:
                        manufacturer_stats[manufacturer] += 1

            substance_usage = Counter()
            for consumer in consumers_data:
                substance_usage[consumer['substance_name']] += 1

            country_stats = Counter()
            for idx, row in substances_df.iterrows():
                country = str(row[COUNTRY_COL]).strip()
                if country and country not in ['', 'nan', '~']:
                    country_stats[country] += 1

            # Формируем итоговый результат
            result = {
                'timestamp': datetime.now().isoformat(),
                'source_file': input_file_path,
                'statistics': {
                    'total_records': len(df),
                    'substances_found': len(substances_df),
                    'preparations_found': len(preparations_df),
                    'substance_consumers_found': len(consumers_data),
                    'unique_substances': len(unique_substances),
                    'top_manufacturers': dict(manufacturer_stats.most_common(20)),
                    'top_substances': dict(substance_usage.most_common(20)),
                    'countries_distribution': dict(country_stats.most_common(10))
                },
                'substances_manufacturers': [
                    {
                        'substance_name': substance,
                        'manufacturers': manufacturers
                    }
                    for substance, manufacturers in substance_manufacturers.items()
                ],
                'substance_consumers': consumers_data
            }

            logger.info("Анализ файла завершен успешно")
            return result

        except Exception as e:
            logger.error(f"Ошибка при анализе файла - {e}")
            raise

    def save_analysis_results(self, analysis_result, output_dir="./app/parsers/data/results"):
        """Сохраняет результаты анализа в файлы"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M")

            # JSON с полным анализом
            json_file = os.path.join(output_dir, f"grls_analysis_{timestamp}.json")
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, ensure_ascii=False, indent=2, default=str)

            logger.info(f"Результаты сохранены в - {json_file}")
            return json_file

        except Exception as e:
            logger.error(f"Ошибка при сохранении результатов - {e}")
            raise


# def test_medical_parser():
#     """Тестовая функция"""
#     parser = MedicalParser()
#
#     # Путь к файлу который скачал archive_parser
#     test_file = "./app/parsers/data/extracted/grls2025-11-21-1-Действующий.xlsx"
#
#     if os.path.exists(test_file):
#         result = parser.analyze_substances_and_consumers(test_file)
#         print("Парсинг завершен")
#         print(f"Статистика: {result['statistics']}")
#         return result
#     else:
#         print("Ошибка не найден файл")
#         return None
#
#
# if __name__ == "__main__":
#     test_medical_parser()