import os
import requests
import zipfile
from datetime import datetime
from config.logging import get_logger
from urllib.parse import urljoin
import re
import shutil

logger = get_logger(__name__)


class ArchiveParser:
    def __init__(self, base_url="https://grls.minzdrav.gov.ru", download_dir="./app/parsers/data"):
        self.base_url = base_url
        self.download_dir = download_dir
        self.session = requests.Session()

        # настройка сессии
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        })

        # создаем директорию для загрузок если ее нет
        os.makedirs(download_dir, exist_ok=True)

    def download_archive(self):
        """Скачивает архив с сайта ГРЛС и находит файл с 'Действующий' в названии"""
        try:
            logger.info("Начинаем парсинг архива")

            archive_url = self._construct_download_url()
            logger.info(f"URL: {archive_url}")

            zip_path = self._download_file(archive_url)
            extracted_files = self._extract_archive(zip_path)
            excel_files = self._find_excel_files(extracted_files)
            operating_file = self._find_operating_file(excel_files)

            # удаляем все ненужные файлы
            if operating_file:
                operating_file = self._cleanup_files(operating_file, extracted_files)

            result = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'archive_url': archive_url,
                'zip_path': zip_path,
                'operating_file': operating_file,
                'message': 'Арихив скачан и очищен'
            }

            if operating_file:
                logger.info(f"'Действующий' файл найден: {os.path.basename(operating_file)}")
            else:
                logger.warning("'Действующий' файл НЕ найден")

            return result

        except Exception as e:
            logger.error(f"Не удалось скачать архив {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

    def _construct_download_url(self):
        """Создает URL для скачивания архива"""
        file_guid = "0fa74bc9-c435-4405-9146-eebd3b0b300c"
        user_req = "8590283"
        download_path = f"GetGRLS.ashx?FileGUID={file_guid}&UserReq={user_req}"
        return urljoin(self.base_url, download_path)

    def _download_file(self, url, chunk_size=8192):
        """Скачивает файл по URL"""
        try:
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"grls_archive_{timestamp}.zip"
            filepath = os.path.join(self.download_dir, filename)

            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            logger.info(f"Файл скачан {filepath} ({file_size_mb:.2f} MB)")

            return filepath

        except requests.exceptions.RequestException as e:
            logger.error(f"Загрузка файла не удалась: {e}")
            raise

    def _extract_archive(self, zip_path, extract_dir=None):
        """Распаковывает ZIP архив"""
        if extract_dir is None:
            extract_dir = os.path.join(self.download_dir, "extracted")

        os.makedirs(extract_dir, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                zip_ref.extractall(extract_dir)
                logger.info(f"Архив распакован: {len(file_list)} файлы помещены в {extract_dir}")

                extracted_files = [os.path.join(extract_dir, f) for f in file_list]
                return extracted_files

        except zipfile.BadZipFile as e:
            logger.error(f"Ошибка при распаковке архива - {e}")
            raise

    def _find_excel_files(self, file_list):
        """Находит Excel файлы в списке файлов"""
        excel_extensions = ('.xlsx', '.xls')
        excel_files = []

        for file_path in file_list:
            if os.path.isfile(file_path) and file_path.lower().endswith(excel_extensions):
                excel_files.append(file_path)
                logger.info(f"Ищем ексель файлы {os.path.basename(file_path)}")

        return excel_files

    def _find_operating_file(self, file_list):
        """Находит файл с 'Действующий' в названии"""
        operating_patterns = [
            'действующий',
        ]

        for file_path in file_list:
            filename = os.path.basename(file_path).lower()

            for pattern in operating_patterns:
                if pattern in filename:
                    logger.info(f"Нашли 'Действующий' файл {os.path.basename(file_path)}")
                    return file_path

            if re.search(r'действ', filename, re.IGNORECASE):
                logger.info(f"Нашли файл с паттерном 'действ' - {os.path.basename(file_path)}")
                return file_path

        if file_list:
            logger.warning(f"Не удалось найти нужный файл -  {os.path.basename(file_list[0])}")
            return file_list[0]

        return None

    def _cleanup_files(self, operating_file, all_files):
        """Удаляет все файлы кроме operating файла"""
        try:
            operating_filename = os.path.basename(operating_file)
            operating_dir = os.path.dirname(operating_file)

            logger.info(f"Очищаем файлы, оставляем только: {operating_filename}")

            # удаляем все файлы кроме нужного
            for file_path in all_files:
                if os.path.isfile(file_path) and file_path != operating_file:
                    os.remove(file_path)
                    logger.info(f"Удаляем файл: {os.path.basename(file_path)}")

            # удаляем пустые директории
            for root, dirs, files in os.walk(operating_dir, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        if not os.listdir(dir_path):  # Если директория пустая
                            os.rmdir(dir_path)
                            logger.info(f"Удаляем пустую директорию: {dir_path}")
                    except OSError:
                        pass

            # перемещаем operating файл в корень extracted
            if operating_dir != os.path.join(self.download_dir, "extracted"):
                new_operating_path = os.path.join(self.download_dir, "extracted", operating_filename)
                shutil.move(operating_file, new_operating_path)
                operating_file = new_operating_path
                logger.info(f"Перемещаем файл в {new_operating_path}")

            logger.info("Очистка завершена - оставлен только файл 'Действующий'")
            return operating_file

        except Exception as e:
            logger.error(f"Очистка не удалась - {e}")
            return operating_file

    def get_latest_operating_file(self):
        """Возвращает путь к последнему operating файлу"""
        extracted_dir = os.path.join(self.download_dir, "extracted")

        if not os.path.exists(extracted_dir):
            return None

        excel_files = []
        for root, dirs, files in os.walk(extracted_dir):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')):
                    excel_files.append(os.path.join(root, file))

        excel_files.sort(key=os.path.getmtime, reverse=True)

        for excel_file in excel_files:
            if self._is_operating_file(excel_file):
                return excel_file

        return excel_files[0] if excel_files else None

    def _is_operating_file(self, file_path):
        """Проверяет, является ли файл operating файлом"""
        filename = os.path.basename(file_path).lower()
        patterns = ['действующий']
        return any(pattern in filename for pattern in patterns) or \
            re.search(r'действ', filename, re.IGNORECASE) is not None

    def cleanup_old_files(self, keep_last=3):
        """Очищает старые файлы, оставляя только последние"""
        try:
            zip_files = [f for f in os.listdir(self.download_dir)
                         if f.startswith('grls_archive_') and f.endswith('.zip')]

            zip_files.sort(key=lambda x: os.path.getctime(os.path.join(self.download_dir, x)),
                           reverse=True)

            for old_file in zip_files[keep_last:]:
                file_path = os.path.join(self.download_dir, old_file)
                os.remove(file_path)
                logger.info(f"Удаляем старый файл - {old_file}")

        except Exception as e:
            logger.error(f"Удаление не удалось -  {e}")


# def test():
#     """Простая функция для тестирования"""
#     print("Тестируем ArchiveParser")
#     parser = ArchiveParser()
#     result = parser.download_archive()
#     print(f"Статус - {result['status']}")
#     print(f"Файл 'Действующий' - {result['operating_file']}")
#     return result


# if __name__ == "__main__":
#     test()