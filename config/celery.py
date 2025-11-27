import os
from celery import Celery
from celery.schedules import crontab

celery_app = Celery('medical_parser')
celery_app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0'),
    imports=['app.tasks'],
    timezone = 'Europe/Moscow',
    beat_schedule={
        'daily-medical-pipeline': {
            'task': 'app.tasks.full_medical_pipeline_task',
            'schedule': crontab(minute='*/40'),
        },
    },
)