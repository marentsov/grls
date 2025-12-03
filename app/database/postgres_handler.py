import os
from config.logging import get_logger
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import json

import psycopg2
from psycopg2 import IntegrityError
from psycopg2.extras import RealDictCursor

logger = get_logger(__name__)


class PostgresHandler:
    def __init__(self, database_url: Optional[str] = None):
        self.database_url = database_url or os.getenv('DATABASE_URL')
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

            # Сохраняем сессию анализа и СРАЗУ КОММИТИМ
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

            # КОММИТИМ сессию сразу, чтобы она была доступна в других транзакциях
            conn.commit()
            logger.info(f"Сессия анализа создана: {session_id}")

            # Теперь обрабатываем данные в отдельных транзакциях
            manufacturer_changes = self._process_substance_manufacturers(
                session_id, analysis_result['substances_manufacturers']
            )

            consumer_changes = self._process_substance_consumers(
                session_id, analysis_result['substance_consumers']
            )

            logger.info(
                f"Результаты анализа сохранены в БД (сессия - {session_id}, изменений - {manufacturer_changes + consumer_changes})")
            return session_id

        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Ошибка сохранения в БД - {e}")
            raise
        finally:
            if conn:
                conn.close()

    def _process_substance_manufacturers(self, session_id: int, current_manufacturers: List[Dict]) -> int:
        """Обрабатывает производителей субстанций с версионированием - КАЖДЫЙ в отдельной транзакции"""
        changes_count = 0

        for substance_data in current_manufacturers:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                changes_count += self._process_single_manufacturer(cursor, session_id, substance_data)
                conn.commit()

            except Exception as e:
                logger.error(
                    f"Ошибка при обработке производителя {substance_data.get('substance_name', 'unknown')}: {e}")
                if conn:
                    conn.rollback()
                continue
            finally:
                if conn:
                    conn.close()

        return changes_count

    def _process_substance_consumers(self, session_id: int, current_consumers: List[Dict]) -> int:
        """Обрабатывает препараты с версионированием - КАЖДЫЙ препарат в ОТДЕЛЬНОЙ транзакции"""
        changes_count = 0

        for consumer in current_consumers:
            conn = None
            try:
                conn = self._get_connection()
                cursor = conn.cursor()

                changes_count += self._process_single_consumer(cursor, session_id, consumer)
                conn.commit()

            except Exception as e:
                logger.error(f"Ошибка при обработке препарата {consumer.get('preparation_trade_name', 'unknown')}: {e}")
                if conn:
                    conn.rollback()
                continue
            finally:
                if conn:
                    conn.close()

        return changes_count

    def _process_single_manufacturer(self, cursor, session_id: int, substance_data: Dict) -> int:
        """Обрабатывает ОДНОГО производителя"""
        current_timestamp = datetime.now()
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
            existing_id, existing_manufacturers, existing_version = existing_record

            # Сравниваем производителей
            if set(existing_manufacturers) != set(current_manufacturers_list):
                # Производители изменились - создаем новую версию

                # Помечаем старую версию как неактуальную
                cursor.execute('''
                    UPDATE substance_manufacturers 
                    SET is_current = FALSE 
                    WHERE id = %s
                ''', (existing_id,))

                # Создаем новую версию
                cursor.execute('''
                    INSERT INTO substance_manufacturers 
                    (substance_name, manufacturers, first_seen_date, last_seen_date, version)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (
                    substance_name,
                    json.dumps(current_manufacturers_list, ensure_ascii=False),
                    current_timestamp,
                    current_timestamp,
                    existing_version + 1
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
                return 1
            else:
                # Производители не изменились - обновляем last_seen_date
                cursor.execute('''
                    UPDATE substance_manufacturers 
                    SET last_seen_date = %s 
                    WHERE id = %s
                ''', (current_timestamp, existing_id))
                return 0
        else:
            # Новая субстанция
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
            return 1

    def _process_single_consumer(self, cursor, session_id: int, consumer: Dict) -> int:
        """Обрабатывает ОДИН препарат"""
        current_timestamp = datetime.now()

        # Формируем уникальный ключ для препарата
        unique_key = (
            consumer['substance_name'],
            consumer['preparation_trade_name'],
            consumer['preparation_manufacturer'],
            consumer['registration_number']
        )

        # Ищем существующую запись
        cursor.execute('''
            SELECT id, preparation_inn_name, preparation_country, 
                   registration_date, release_forms, version, first_seen_date
            FROM substance_consumers 
            WHERE substance_name = %s AND preparation_trade_name = %s 
            AND preparation_manufacturer = %s AND registration_number = %s
            AND is_current = TRUE
        ''', unique_key)

        existing_record = cursor.fetchone()

        if existing_record:
            existing_id, existing_inn, existing_country, existing_date, existing_forms, existing_version, existing_first_seen = existing_record

            # Проверяем изменения
            changed_fields = []
            if existing_inn != consumer['preparation_inn_name']:
                changed_fields.append('preparation_inn_name')
            if existing_country != consumer['preparation_country']:
                changed_fields.append('preparation_country')
            if existing_date != consumer['registration_date']:
                changed_fields.append('registration_date')
            if existing_forms != consumer['release_forms']:
                changed_fields.append('release_forms')

            if changed_fields:
                # Есть изменения - создаем новую версию

                # Помечаем старую версию как неактуальную
                cursor.execute('''
                    UPDATE substance_consumers 
                    SET is_current = FALSE 
                    WHERE id = %s
                ''', (existing_id,))

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
                    existing_first_seen,
                    current_timestamp,
                    existing_version + 1
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

                return 1
            else:
                # Нет изменений - просто обновляем last_seen_date
                cursor.execute('''
                    UPDATE substance_consumers 
                    SET last_seen_date = %s 
                    WHERE id = %s
                ''', (current_timestamp, existing_id))
                return 0

        else:
            # Новый препарат - пробуем вставить
            try:
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

                return 1

            except IntegrityError:
                # Если возникла ошибка уникальности - значит запись уже существует
                # Обновляем last_seen_date у существующей записи
                cursor.execute('''
                    UPDATE substance_consumers 
                    SET last_seen_date = %s, is_current = TRUE
                    WHERE substance_name = %s AND preparation_trade_name = %s 
                    AND preparation_manufacturer = %s AND registration_number = %s
                    AND is_current = TRUE
                ''', (
                    current_timestamp,
                    consumer['substance_name'],
                    consumer['preparation_trade_name'],
                    consumer['preparation_manufacturer'],
                    consumer['registration_number']
                ))
                logger.debug(
                    f"Обновлена last_seen_date для существующего препарата: {consumer['preparation_trade_name']}")
                return 0

    def cleanup_old_files(self, days_to_keep: int = 30):
        """Очищает только старые файлы, по умолчанию 30"""
        try:
            # Очищаем файлы в директории extracted
            extracted_dir = "./app/parsers/data/extracted"

            if os.path.exists(extracted_dir):
                cutoff_date = datetime.now() - timedelta(days=days_to_keep)
                deleted_count = 0

                for filename in os.listdir(extracted_dir):
                    if filename.lower().endswith(('.xlsx', '.xls')):
                        filepath = os.path.join(extracted_dir, filename)
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))

                        if file_mtime < cutoff_date:
                            os.remove(filepath)
                            logger.info(f"Удален старый файл: {filename}")
                            deleted_count += 1

                logger.info(f"Очистка завершена: удалено {deleted_count} старых файлов")

            # Также очищаем старые ZIP архивы
            self._cleanup_old_archives(days_to_keep)

        except Exception as e:
            logger.error(f"Ошибка при очистке старых файлов: {e}")


    def _cleanup_old_archives(self, days_to_keep: int = 30):
        """Очищает старые ZIP архивы в директории загрузок"""
        try:
            download_dir = "./app/parsers/data"

            if not os.path.exists(download_dir):
                return

            cutoff_date = datetime.now() - timedelta(days=days_to_keep)

            for filename in os.listdir(download_dir):
                if filename.startswith('grls_archive_') and filename.endswith('.zip'):
                    filepath = os.path.join(download_dir, filename)
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(filepath))

                    if file_mtime < cutoff_date:
                        os.remove(filepath)
                        logger.info(f"Удален старый архив: {filename}")

        except Exception as e:
            logger.error(f"Ошибка при очистке архивов: {e}")


def test_postgres():
    """Тест PostgreSQL соединения"""
    handler = PostgresHandler()

    if handler.test_connection():
        print("PostgreSQL connection successful")
        print("Test passed!")


if __name__ == "__main__":
    test_postgres()