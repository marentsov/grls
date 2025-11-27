import logging
from config.celery import celery_app


logger = logging.getLogger(__name__)


@celery_app.task
def full_medical_pipeline_task():
    """Полный пайплайн, включает в себя скачивание архива, анализ файлов, сохранение результатов в БД"""
    logger.info("🚀 Starting pipeline task")
    try:
        from app.parsers.archive_parser import ArchiveParser
        from app.parsers.medical_parser import MedicalParser
        from app.database.postgres_handler import PostgresHandler

        # 1. Скачиваем архив
        archive_parser = ArchiveParser()
        download_result = archive_parser.download_archive()

        # 2. Анализируем файл
        if download_result['status'] == 'success' and download_result['operating_file']:
            medical_parser = MedicalParser()
            analysis_result = medical_parser.analyze_substances_and_consumers(
                download_result['operating_file']
            )

            # 3. Сохраняем в БД
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