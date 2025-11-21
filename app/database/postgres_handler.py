import os
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class PostgresHandler:
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:password@localhost:5432/medical_parser'
        )

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
            logger.error(f"Ошибка подключения к PostgreSQL: {e}")
            return False

    def save_analysis_result(self, analysis_result: Dict) -> int:
        """Сохраняет результат анализа в PostgreSQL"""
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

            # Сохраняем производителей субстанций
            for substance_data in analysis_result['substances_manufacturers']:
                cursor.execute('''
                    INSERT INTO substance_manufacturers 
                    (session_id, substance_name, manufacturers)
                    VALUES (%s, %s, %s)
                ''', (
                    session_id,
                    substance_data['substance_name'],
                    json.dumps(substance_data['manufacturers'], ensure_ascii=False)
                ))

            # Сохраняем связи препарат-субстанция
            for consumer in analysis_result['substance_consumers']:
                cursor.execute('''
                    INSERT INTO substance_consumers 
                    (session_id, substance_name, preparation_trade_name, preparation_inn_name,
                     preparation_manufacturer, preparation_country, registration_number,
                     registration_date, release_forms)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    session_id,
                    consumer['substance_name'],
                    consumer['preparation_trade_name'],
                    consumer['preparation_inn_name'],
                    consumer['preparation_manufacturer'],
                    consumer['preparation_country'],
                    consumer['registration_number'],
                    consumer['registration_date'],
                    consumer['release_forms']
                ))

            conn.commit()
            logger.info(f"Результаты анализа сохранены в БД (сессия: {session_id})")
            return session_id

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка сохранения в БД: {e}")
            raise
        finally:
            if conn:
                conn.close()

    def get_latest_analysis(self) -> Optional[Dict]:
        """Возвращает последний результат анализа"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute('''
                SELECT * FROM analysis_sessions 
                ORDER BY created_at DESC 
                LIMIT 1
            ''')

            latest_session = cursor.fetchone()
            return dict(latest_session) if latest_session else None

        except Exception as e:
            logger.error(f"Ошибка получения данных - {e}")
            return None
        finally:
            if conn:
                conn.close()


def test_postgres():
    """Тест PostgreSQL соединения"""
    handler = PostgresHandler()

    if handler.test_connection():
        print("✅ PostgreSQL connection successful")

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
        print(f"✅ Test data saved with session: {session_id}")


if __name__ == "__main__":
    test_postgres()