#!/usr/bin/env python3
"""
Тестовый скрипт для проверки ArchiveParser
"""
import sys
import os
import logging

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)

# Простой импорт из текущей директории
from archive_parser import ArchiveParser


def main():
    print("🚀 Testing ArchiveParser...")

    try:
        parser = ArchiveParser()
        print("📥 Downloading archive...")
        result = parser.download_archive()

        print("\n" + "=" * 50)
        print("📊 RESULTS:")
        print("=" * 50)
        print(f"Status: {result['status']}")
        print(f"Archive URL: {result['archive_url']}")
        print(f"ZIP path: {result['zip_path']}")
        print(f"Operating file: {result['operating_file']}")

        if result['operating_file']:
            print(f"✅ Operating file found: {os.path.basename(result['operating_file'])}")

            # Проверяем что файл действительно существует
            if os.path.exists(result['operating_file']):
                file_size_mb = os.path.getsize(result['operating_file']) / (1024 * 1024)
                print(f"📏 File size: {file_size_mb:.2f} MB")
                print(f"🎯 SUCCESS! Only operating file remains in extracted folder")
            else:
                print("❌ File not found on disk")
        else:
            print("❌ No operating file found")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()