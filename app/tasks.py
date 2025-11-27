from celery import Celery
import logging
from datetime import datetime
import os

logger = logging.getLogger(__name__)

# Создаем Celery app
celery_app = Celery('medical_parser')
celery_app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0'),
)

@celery_app.task
def full_medical_pipeline_task():
    """Полный пайплайн"""
    logger.info("🚀 Starting pipeline task")
    try:
        from app.parsers.archive_parser import ArchiveParser
        from app.parsers.medical_parser import MedicalParser
        from app.database.postgres_handler import PostgresHandler

        # Тут должен быть реальный код:
        # 1. Скачать архив
        archive_parser = ArchiveParser()
        download_result = archive_parser.download_archive()

        # 2. Проанализировать файл
        if download_result['status'] == 'success' and download_result['operating_file']:
            medical_parser = MedicalParser()
            analysis_result = medical_parser.analyze_substances_and_consumers(
                download_result['operating_file']
            )

            # 3. Сохранить в БД
            db_handler = PostgresHandler()
            session_id = db_handler.save_analysis_result(analysis_result)

            return {'status': 'success', 'session_id': session_id}

        return {'status': 'error', 'message': 'File not found'}

    except Exception as e:
        return {'status': 'error', 'error': str(e)}

@celery_app.task
def simple_test_task():
    """Простая задача"""
    return {'status': 'success', 'message': 'Hello from Celery!'}