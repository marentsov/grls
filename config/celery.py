import os
from celery import Celery
from celery.schedules import crontab

celery_app = Celery('medical_parser')
celery_app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND'),
    imports=['app.tasks'],
    timezone='Europe/Moscow',
    beat_schedule={
        'test-medical-pipeline-1555': {
            'task': 'app.tasks.full_medical_pipeline_task',
            'schedule': crontab(hour=9, minute=40),  # каждый день в 9:00
        },
    },
)