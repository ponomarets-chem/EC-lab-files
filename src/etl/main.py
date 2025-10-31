import argparse
import sys
import os

def main():
    """Главная функция ETL пайплайна"""
    print("🚀 ETL пайплайн запущен!")
    
    parser = argparse.ArgumentParser(description='ETL пайплайн для обработки электрохимических данных')
    
    # Обязательный аргумент - источник данных
    parser.add_argument(
        'source', 
        type=str, 
        help='Путь к исходным данным (файл или URL Google Drive)'
    )
    
    # Опциональные аргументы
    parser.add_argument(
        '--table-name', 
        type=str, 
        default='ponomarets',
        help='Название таблицы в БД (по умолчанию: ponomarets)'
    )
    
    parser.add_argument(
        '--max-rows', 
        type=int, 
        default=100,
        help='Максимальное количество строк для загрузки в БД (по умолчанию: 100)'
    )
    
    args = parser.parse_args()
    
    print(f"📁 Источник: {args.source}")
    print(f"🗄️ Таблица: {args.table_name}") 
    print(f"🔢 Макс строк: {args.max_rows}")
    
    try:
        # Пробуем импортировать модули
        print("\n📥 Импортируем модули...")
        from etl.extract import extract_data
        from etl.transform import transform_data
        from etl.load import load_to_database
        
        print("✅ Модули загружены успешно!")
        
        # Шаг 1: Extract
        print("\n📥 Шаг 1: Извлечение данных...")
        raw_data_path = extract_data(args.source)
        
        if not raw_data_path:
            print("❌ Ошибка на этапе извлечения данных")
            return 1
        
        # Шаг 2: Transform  
        print("\n🔄 Шаг 2: Трансформация данных...")
        csv_path, parquet_path = transform_data(raw_data_path)
        
        # Шаг 3: Load
        print("\n📤 Шаг 3: Загрузка данных в PostgreSQL...")
        success = load_to_database(
            csv_path, 
            table_name=args.table_name,
            max_rows=args.max_rows
        )
        
        if success:
            print("\n🎉 ETL пайплайн успешно завершен!")
            return 0
        else:
            print("\n💥 ETL пайплайн завершен с ошибками на этапе загрузки")
            return 1
            
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
    
