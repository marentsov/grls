from celery.schedules import crontab

# КОНФИГУРАЦИЯ РАСПИСАНИЯ ЗАДАЧ

BEAT_SCHEDULE = {
    # Запуск sample_task.py каждые 5 минут (через subprocess)
    'run-sample-script-every-5-min': {
        'task': 'app.tasks.run_sample_script_task',
        'schedule': crontab(minute='*/5'),
        'args': (),
    },}

#     # Задача каждый день в 9:00 утра
#     'daily-morning-task': {
#         'task': 'app.tasks.sample_daily_task',
#         'schedule': crontab(hour=9, minute=0),
#         'args': ('Morning task executed!',),
#         'options': {'queue': 'default'}
#     },
#
#     # Задача каждый понедельник в 10:00
#     'weekly-report-task': {
#         'task': 'app.tasks.sample_weekly_task',
#         'schedule': crontab(day_of_week=1, hour=10, minute=0),
#         'args': ('Weekly report time!',),
#         'options': {'queue': 'reports'}
#     },
#
#     # Задача каждый день в 18:30 вечера
#     'evening-cleanup-task': {
#         'task': 'app.tasks.sample_daily_task',
#         'schedule': crontab(hour=18, minute=30),
#         'args': ('Evening cleanup!',),
#         'options': {'queue': 'default'}
#     },
#
#     # Задача каждые 30 секунд (для тестирования)
#     'test-frequent-task': {
#         'task': 'app.tasks.test_frequent_task',
#         'schedule': 30.0,  # секунды
#         'args': (),
#         'options': {'queue': 'default'}
#     },
# }

# варианты расписания
"""
# Каждые 10 минут
crontab(minute='*/10')

# Каждый час в 0 минут
crontab(minute=0)

# Каждый день в 6:30 утра
crontab(hour=6, minute=30)

# По рабочим дням (пн-пт) в 8:00
crontab(day_of_week='1-5', hour=8, minute=0)

# По выходным в 11:00
crontab(day_of_week='6,7', hour=11, minute=0)

# 1-го числа каждого месяца в 9:00
crontab(day_of_month=1, hour=9, minute=0)

# Каждые 30 секунд (через интервал)
30.0
"""