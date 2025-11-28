import os
from config.logging import get_logger
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json

import psycopg2
from psycopg2.extras import RealDictCursor

logger = get_logger(__name__)


class PostgresHandler:
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL не установлен в .env")

    def _get_connection(self):
        """Возвращает соединение с PostgreSQL"""
        return psycopg2.connect(self.database_url)

    def test_connection(self):
        """Проверяет соединение с базой данных"""
        try:
            conn = self._get_connection()
            conn.close()
            logger.info("Соединение с PostgreSQL установлено")
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения к PostgreSQL - {e}")
            return False

    def save_analysis_result(self, analysis_result: Dict) -> int:
        """Сохраняет результат анализа в PostgreSQL с версионированием"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Сохраняем сессию анализа
            cursor.execute('''
                INSERT INTO analysis_sessions 
                (timestamp, source_file, total_records, substances_found, preparations_found, consumers_found)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (
                analysis_result['timestamp'],
                analysis_result['source_file'],
                analysis_result['statistics']['total_records'],
                analysis_result['statistics']['substances_found'],
                analysis_result['statistics']['preparations_found'],
                analysis_result['statistics']['substance_consumers_found']
            ))

            session_id = cursor.fetchone()[0]

            # Обрабатываем производителей субстанций с версионированием
            manufacturer_changes = self._process_substance_manufacturers(
                cursor, session_id, analysis_result['substances_manufacturers']
            )

            # Обрабатываем препараты с версионированием
            consumer_changes = self._process_substance_consumers(
                cursor, session_id, analysis_result['substance_consumers']
            )

            # Обновляем статистику изменений
            total_changes = manufacturer_changes + consumer_changes
            analysis_result['statistics']['changes_detected'] = total_changes

            conn.commit()
            logger.info(f"Результаты анализа сохранены в БД (сессия - {session_id}, изменений - {total_changes})")
            return session_id

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка сохранения в БД - {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _process_substance_manufacturers(self, cursor, session_id: int, current_manufacturers: List[Dict]) -> int:
        """Обрабатывает производителей субстанций с версионированием"""
        changes_count = 0
        current_timestamp = datetime.now().isoformat()

        for substance_data in current_manufacturers:
            substance_name = substance_data['substance_name']
            current_manufacturers_list = substance_data['manufacturers']

            # Ищем существующую запись
            cursor.execute('''
                SELECT id, manufacturers, version 
                FROM substance_manufacturers 
                WHERE substance_name = %s AND is_current = TRUE
            ''', (substance_name,))

            existing_record = cursor.fetchone()

            if existing_record:
                # Сравниваем производителей
                existing_manufacturers = existing_record[1]
                if set(existing_manufacturers) != set(current_manufacturers_list):
                    # Производители изменились - создаем новую версию
                    changes_count += 1

                    # Помечаем старую версию как неактуальную
                    cursor.execute('''
                        UPDATE substance_manufacturers 
                        SET is_current = FALSE 
                        WHERE id = %s
                    ''', (existing_record[0],))

                    # Создаем новую версию
                    cursor.execute('''
                        INSERT INTO substance_manufacturers 
                        (substance_name, manufacturers, first_seen_date, last_seen_date, version)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        substance_name,
                        json.dumps(current_manufacturers_list, ensure_ascii=False),
                        existing_record[3] if len(existing_record) > 3 else current_timestamp,
                        current_timestamp,
                        existing_record[2] + 1
                    ))

                    # Записываем изменение в журнал
                    cursor.execute('''
                        INSERT INTO substance_manufacturer_changes 
                        (substance_name, old_manufacturers, new_manufacturers, change_type, session_id)
                        VALUES (%s, %s, %s, %s, %s)
                    ''', (
                        substance_name,
                        json.dumps(existing_manufacturers, ensure_ascii=False),
                        json.dumps(current_manufacturers_list, ensure_ascii=False),
                        'modified',
                        session_id
                    ))
                else:
                    # Производители не изменились - обновляем last_seen_date
                    cursor.execute('''
                        UPDATE substance_manufacturers 
                        SET last_seen_date = %s 
                        WHERE id = %s
                    ''', (current_timestamp, existing_record[0]))
            else:
                # Новая субстанция
                changes_count += 1
                cursor.execute('''
                    INSERT INTO substance_manufacturers 
                    (substance_name, manufacturers, first_seen_date, last_seen_date)
                    VALUES (%s, %s, %s, %s)
                ''', (
                    substance_name,
                    json.dumps(current_manufacturers_list, ensure_ascii=False),
                    current_timestamp,
                    current_timestamp
                ))

                # Записываем в журнал
                cursor.execute('''
                    INSERT INTO substance_manufacturer_changes 
                    (substance_name, new_manufacturers, change_type, session_id)
                    VALUES (%s, %s, %s, %s)
                ''', (
                    substance_name,
                    json.dumps(current_manufacturers_list, ensure_ascii=False),
                    'added',
                    session_id
                ))

        return changes_count

    def _process_substance_consumers(self, cursor, session_id: int, current_consumers: List[Dict]) -> int:
        """Обрабатывает препараты с версионированием"""
        changes_count = 0
        current_timestamp = datetime.now().isoformat()

        for consumer in current_consumers:
            # Ищем существующую запись
            cursor.execute('''
                SELECT id, preparation_trade_name, preparation_inn_name, preparation_manufacturer,
                       preparation_country, registration_number, registration_date, release_forms, version
                FROM substance_consumers 
                WHERE substance_name = %s AND preparation_trade_name = %s 
                AND preparation_manufacturer = %s AND registration_number = %s
                AND is_current = TRUE
            ''', (
                consumer['substance_name'],
                consumer['preparation_trade_name'],
                consumer['preparation_manufacturer'],
                consumer['registration_number']
            ))

            existing_record = cursor.fetchone()

            if existing_record:
                # Проверяем изменения
                changed_fields = self._get_changed_fields(existing_record, consumer)
                if changed_fields:
                    # Есть изменения - создаем новую версию
                    changes_count += 1

                    # Помечаем старую версию как неактуальную
                    cursor.execute('''
                        UPDATE substance_consumers 
                        SET is_current = FALSE 
                        WHERE id = %s
                    ''', (existing_record[0],))

                    # Создаем новую версию
                    cursor.execute('''
                        INSERT INTO substance_consumers 
                        (substance_name, preparation_trade_name, preparation_inn_name,
                         preparation_manufacturer, preparation_country, registration_number,
                         registration_date, release_forms, first_seen_date, last_seen_date, version)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        consumer['substance_name'],
                        consumer['preparation_trade_name'],
                        consumer['preparation_inn_name'],
                        consumer['preparation_manufacturer'],
                        consumer['preparation_country'],
                        consumer['registration_number'],
                        consumer['registration_date'],
                        consumer['release_forms'],
                        existing_record[8] if len(existing_record) > 8 else current_timestamp,  # first_seen_date
                        current_timestamp,
                        existing_record[8] + 1 if len(existing_record) > 8 else 2  # version
                    ))

                    # Записываем изменение в журнал
                    cursor.execute('''
                        INSERT INTO substance_consumer_changes 
                        (substance_name, preparation_trade_name, preparation_inn_name,
                         preparation_manufacturer, preparation_country, registration_number,
                         change_type, changed_fields, session_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        consumer['substance_name'],
                        consumer['preparation_trade_name'],
                        consumer['preparation_inn_name'],
                        consumer['preparation_manufacturer'],
                        consumer['preparation_country'],
                        consumer['registration_number'],
                        'modified',
                        json.dumps(changed_fields, ensure_ascii=False),
                        session_id
                    ))
                else:
                    # Нет изменений - обновляем last_seen_date
                    cursor.execute('''
                        UPDATE substance_consumers 
                        SET last_seen_date = %s 
                        WHERE id = %s
                    ''', (current_timestamp, existing_record[0]))
            else:
                # Новый препарат
                changes_count += 1
                cursor.execute('''
                    INSERT INTO substance_consumers 
                    (substance_name, preparation_trade_name, preparation_inn_name,
                     preparation_manufacturer, preparation_country, registration_number,
                     registration_date, release_forms, first_seen_date, last_seen_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    consumer['substance_name'],
                    consumer['preparation_trade_name'],
                    consumer['preparation_inn_name'],
                    consumer['preparation_manufacturer'],
                    consumer['preparation_country'],
                    consumer['registration_number'],
                    consumer['registration_date'],
                    consumer['release_forms'],
                    current_timestamp,
                    current_timestamp
                ))

                # Записываем в журнал
                cursor.execute('''
                    INSERT INTO substance_consumer_changes 
                    (substance_name, preparation_trade_name, preparation_inn_name,
                     preparation_manufacturer, preparation_country, registration_number,
                     change_type, session_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    consumer['substance_name'],
                    consumer['preparation_trade_name'],
                    consumer['preparation_inn_name'],
                    consumer['preparation_manufacturer'],
                    consumer['preparation_country'],
                    consumer['registration_number'],
                    'added',
                    session_id
                ))

        return changes_count

    def _get_changed_fields(self, existing_record: Tuple, new_data: Dict) -> List[str]:
        """Определяет какие поля изменились"""
        changed_fields = []

        field_mapping = [
            (1, 'preparation_trade_name'),
            (2, 'preparation_inn_name'),
            (3, 'preparation_manufacturer'),
            (4, 'preparation_country'),
            (5, 'registration_number'),
            (6, 'registration_date'),
            (7, 'release_forms')
        ]

        for idx, field_name in field_mapping:
            if idx < len(existing_record) and str(existing_record[idx]) != str(new_data[field_name]):
                changed_fields.append(field_name)

        return changed_fields

    def cleanup_old_files(self, days_to_keep: int = 30):
        """Очищает старые файлы и соответствующие записи в БД"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Находим файлы старше days_to_keep дней
            cutoff_date = datetime.now().replace(
                day=datetime.now().day - days_to_keep
            ).isoformat()

            cursor.execute('''
                SELECT id, source_file FROM analysis_sessions 
                WHERE timestamp < %s
            ''', (cutoff_date,))

            old_sessions = cursor.fetchall()

            for session_id, source_file in old_sessions:
                # Удаляем связанные записи
                cursor.execute('DELETE FROM substance_manufacturer_changes WHERE session_id = %s', (session_id,))
                cursor.execute('DELETE FROM substance_consumer_changes WHERE session_id = %s', (session_id,))
                cursor.execute('DELETE FROM analysis_sessions WHERE id = %s', (session_id,))

                # Удаляем файл если он существует
                if os.path.exists(source_file):
                    os.remove(source_file)
                    logger.info(f"Удален старый файл: {source_file}")

            conn.commit()
            logger.info(f"Очистка завершена: удалено {len(old_sessions)} старых сессий")

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка при очистке старых файлов: {e}")
        finally:
            if conn:
                conn.close()

    def get_change_history(self, substance_name: str = None, limit: int = 50) -> List[Dict]:
        """Возвращает историю изменений"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            query = '''
                SELECT * FROM substance_manufacturer_changes
            '''
            params = []

            if substance_name:
                query += ' WHERE substance_name = %s'
                params.append(substance_name)

            query += ' ORDER BY changed_at DESC LIMIT %s'
            params.append(limit)

            cursor.execute(query, params)
            changes = cursor.fetchall()

            return [dict(change) for change in changes]

        except Exception as e:
            logger.error(f"Ошибка получения истории изменений: {e}")
            return []
        finally:
            if conn:
                conn.close()


def test_postgres():
    """Тест PostgreSQL соединения"""
    handler = PostgresHandler()

    if handler.test_connection():
        print("PostgreSQL connection successful")

        # Тестовые данные
        test_data = {
            'timestamp': datetime.now().isoformat(),
            'source_file': 'test.xlsx',
            'statistics': {
                'total_records': 100,
                'substances_found': 10,
                'preparations_found': 90,
                'substance_consumers_found': 50
            },
            'substances_manufacturers': [
                {
                    'substance_name': 'Тестовая субстанция',
                    'manufacturers': ['Производитель 1', 'Производитель 2']
                }
            ],
            'substance_consumers': [
                {
                    'substance_name': 'Тестовая субстанция',
                    'preparation_trade_name': 'Тестовый препарат',
                    'preparation_inn_name': 'Тест МНН',
                    'preparation_manufacturer': 'Производитель',
                    'preparation_country': 'Россия',
                    'registration_number': '123',
                    'registration_date': '2024-01-01',
                    'release_forms': 'таблетки'
                }
            ]
        }

        session_id = handler.save_analysis_result(test_data)
        print(f"Test data saved with session: {session_id}")


if __name__ == "__main__":
    test_postgres()