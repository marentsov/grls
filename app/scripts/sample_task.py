"""
Пример скрипта, который можно запускать как задачу, для тестирования конфига селери и редиса
"""
import time
from datetime import datetime


def run_sample_script():
    print(f"запускаем простой скрипт в {datetime.now()}")

    # Имитация работы
    for i in range(5):
        print(f"шаг{i + 1}/5...")
        time.sleep(1)

    result = {
        'status': 'success',
        'executed_at': datetime.now().isoformat(),
        'message': 'Скрипт успешно завершен!'
    }

    print(f"скрипт завершен {result}")
    return result


if __name__ == "__main__":
    run_sample_script()