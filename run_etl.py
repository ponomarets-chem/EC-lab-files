#!/usr/bin/env python3
"""
🎯 СКРИПТ ДЛЯ АКТИВАЦИИ ETL ПАЙПЛАЙНА
Автоматически запускает весь процесс ETL
"""
import sys
import os

# Добавляем src в путь для импортов
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))


import time
from datetime import datetime

class ETLRunner:
    def __init__(self):
        self.start_time = None
        self.source_url = "https://drive.google.com/uc?id=1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
        
    def print_header(self):
        """Красивый заголовок"""
        print("\n" + "="*60)
        print("🎯 АКТИВАЦИЯ ETL ПАЙПЛАЙНА")
        print("="*60)
        print(f"🕒 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📁 Рабочая директория: {os.getcwd()}")
        print("="*60)
    
    def check_dependencies(self):
        """Проверяет наличие всех зависимостей"""
        print("\n🔍 ПРОВЕРКА ЗАВИСИМОСТЕЙ...")
        
        dependencies = {
            'pandas': 'pandas',
            'gdown': 'gdown', 
            'psycopg2': 'psycopg2-binary',
            'dotenv': 'python-dotenv',
            'pyarrow': 'pyarrow'
        }
        
        missing_deps = []
        for package, install_name in dependencies.items():
            try:
                __import__(package)
                print(f"   ✅ {package}")
            except ImportError:
                print(f"   ❌ {package}")
                missing_deps.append(install_name)
        
        if missing_deps:
            print(f"\n⚠️  Отсутствуют зависимости: {', '.join(missing_deps)}")
            print("   Установите командой: pip install " + " ".join(missing_deps))
            return False
        return True
    
    def check_etl_modules(self):
        """Проверяет наличие модулей ETL"""
        print("\n🔍 ПРОВЕРКА МОДУЛЕЙ ETL...")
        
        modules = ['extract', 'transform', 'load', 'validate']
        all_ok = True
        
        for module in modules:
            try:
                __import__(f'etl.{module}')
                print(f"   ✅ etl.{module}")
            except Exception as e:
                print(f"   ❌ etl.{module}: {e}")
                all_ok = False
        
        return all_ok
    
    def run_etl(self):
        """Запускает основной ETL процесс"""
        print("\n🚀 ЗАПУСК ETL ПРОЦЕССА...")
        
        try:
            # Импортируем модули ETL
            from etl.extract import extract_data
            from etl.transform import transform_data
            from etl.load import load_to_database
            
            # ШАГ 1: EXTRACT
            print("\n📥 ЭТАП 1: ИЗВЛЕЧЕНИЕ ДАННЫХ")
            print("   📥 Загрузка из Google Drive...")
            raw_data_path = extract_data(self.source_url)
            
            if not raw_data_path:
                print("   ❌ Ошибка при извлечении данных!")
                return False
            print(f"   ✅ Данные сохранены: {raw_data_path}")
            
            # ШАГ 2: TRANSFORM
            print("\n🔄 ЭТАП 2: ТРАНСФОРМАЦИЯ ДАННЫХ")
            print("   🛠️  Приведение типов и очистка...")
            csv_path, parquet_path = transform_data(raw_data_path)
            print(f"   ✅ CSV сохранен: {csv_path}")
            print(f"   ✅ Parquet сохранен: {parquet_path}")
            
            # ШАГ 3: LOAD
            print("\n📤 ЭТАП 3: ЗАГРУЗКА В БАЗУ ДАННЫХ")
            print("   🗄️  Подключение к PostgreSQL...")
            success = load_to_database(
                csv_path, 
                table_name="ponomarets",
                max_rows=100
            )
            
            if success:
                print("   ✅ Данные успешно загружены в БД!")
                return True
            else:
                print("   ❌ Ошибка при загрузке в БД!")
                return False
                
        except Exception as e:
            print(f"   💥 Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def check_results(self):
        """Проверяет результаты выполнения"""
        print("\n🔍 ПРОВЕРКА РЕЗУЛЬТАТОВ...")
        
        results = {
            'data/raw/': 'Сырые данные',
            'data/processed/processed_data.csv': 'Обработанные CSV',
            'data/processed/processed_data.parquet': 'Обработанные Parquet'
        }
        
        all_exists = True
        for path, description in results.items():
            if os.path.exists(path):
                if os.path.isdir(path):
                    files = os.listdir(path)
                    print(f"   ✅ {description}: {len(files)} файлов")
                else:
                    size = os.path.getsize(path)
                    print(f"   ✅ {description}: {size} байт")
            else:
                print(f"   ❌ {description}: не найден")
                all_exists = False
        
        return all_exists
    
    def print_summary(self, success, elapsed_time):
        """Выводит итоговую статистику"""
        print("\n" + "="*60)
        if success:
            print("🎉 ETL ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЕН!")
        else:
            print("💥 ETL ПАЙПЛАЙН ЗАВЕРШЕН С ОШИБКАМИ")
        
        print(f"⏱️  Общее время выполнения: {elapsed_time:.2f} секунд")
        print("="*60)
    
    def run(self):
        """Основной метод запуска"""
        self.start_time = time.time()
        self.print_header()
        
        # Проверяем зависимости
        if not self.check_dependencies():
            return False
        
        # Проверяем модули ETL
        if not self.check_etl_modules():
            print("\n⚠️  Не все модули ETL доступны!")
            return False
        
        # Запускаем ETL
        success = self.run_etl()
        
        # Проверяем результаты
        if success:
            self.check_results()
        
        # Выводим итоги
        elapsed_time = time.time() - self.start_time
        self.print_summary(success, elapsed_time)
        
        return success

def main():
    """Точка входа"""
    runner = ETLRunner()
    
    try:
        success = runner.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Неожиданная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()