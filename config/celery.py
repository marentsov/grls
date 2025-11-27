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
        'daily-medical-pipeline-9am': {
            'task': 'app.tasks.full_medical_pipeline_task',
            'schedule': crontab(hour=9, minute=0),  # Каждый день в 9:00
        },
        'daily-medical-pipeline-6pm': {
            'task': 'app.tasks.full_medical_pipeline_task',
            'schedule': crontab(hour=18, minute=0),  # Каждый день в 18:00
        },
    },
)