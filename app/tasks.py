from config.celery import celery_app
from config.logging import get_logger


logger = get_logger(__name__)


@celery_app.task
def full_medical_pipeline_task():
    """Полный пайплайн, включает в себя скачивание архива, анализ файлов, сохранение результатов в БД"""
    logger.info("Начинаем основной пайплайн")
    try:
        from app.parsers.archive_parser import ArchiveParser
        from app.parsers.medical_parser import MedicalParser
        from app.database.postgres_handler import PostgresHandler

        # 1. Скачиваем архив
        archive_parser = ArchiveParser()
        download_result = archive_parser.download_archive()
        logger.info('Скачиваем архив')

        # 2. Анализируем файл
        if download_result['status'] == 'success' and download_result['operating_file']:
            logger.info('Архив скачан')
            medical_parser = MedicalParser()
            analysis_result = medical_parser.analyze_substances_and_consumers(
                download_result['operating_file']
            )
            logger.info('Анализируем файл')

            # 3. Сохраняем в БД
            db_handler = PostgresHandler()
            session_id = db_handler.save_analysis_result(analysis_result)
            logger.info('Сохраняем в БД')

            return {'status': 'success', 'session_id': session_id}

        logger.warning('Файл не найден')
        return {'status': 'error', 'message': 'File not found'}


    except Exception as e:
        logger.warning(f'Ошибка - {e}')
        return {'status': 'error', 'error': str(e)}


@celery_app.task
def simple_test_task():
    """Простая задача"""
    return {'status': 'success', 'message': 'Hello from Celery!'}