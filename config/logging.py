import logging
import sys


def setup_logging():
    """Базовая настройка логирования"""

    # форматтер
    formatter = logging.Formatter(
        '%(asctime)s | %(name)-25s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # хендлер в консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # корневой логгер
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    root_logger.addHandler(console_handler)

    # убираем лишнее
    logging.getLogger('celery').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)


def get_logger(name):
    """
    Получить логгер с указанным именем
    Использовать: logger = get_logger(__name__)
    """
    return logging.getLogger(name)


# инициализация
setup_logging()