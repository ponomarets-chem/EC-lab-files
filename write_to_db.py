import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

print("Начало работы скрипта")

# Загружаем настройки подключения
env_path = '.env'
print(f"Ищем файл с настройками: {env_path}")

try:
    load_dotenv(env_path)
    print("Настройки подключения загружены")
except Exception as e:
    print(f"Ошибка загрузки настроек: {e}")

def load_from_parquet():
    """Загружает подготовленные данные из Parquet файла"""
    parquet_file = "инжиниринг.parquet"
    
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"Parquet файл {parquet_file} не найден")
    
    print("Загружаем подготовленные данные из Parquet...")
    df = pd.read_parquet(parquet_file)
    print(f"Данные загружены: {len(df)} строк, {len(df.columns)} колонок")
    print(f"Колонки: {list(df.columns)}")
    return df

def normalize_column_name(col_name):
    """Нормализует название колонки для PostgreSQL"""
    # Заменяем проблемные символы на подчеркивания
    normalized = (col_name.replace("/", "_per_")
                  .replace("(", "")
                  .replace(")", "")
                  .replace("<", "")
                  .replace(">", "")
                  .replace(" ", "_")
                  .replace("-", "_")
                  .replace(".", "_")
                  .replace("µ", "u")
                  .replace("%", "percent")
                  .lower())
    return normalized

def main():
    try:
        print("Получаем данные для подключения...")
        
        # Читаем настройки из переменных окружения
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        database = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        
        print("Проверяем настройки:")
        print(f"  Хост: {host}")
        print(f"  Порт: {port}")
        print(f"  База данных: {database}")
        print(f"  Пользователь: {user}")
        print(f"  Пароль: {'*' * len(password) if password else 'не указан'}")
        
        if not all([host, port, database, user, password]):
            raise Exception("Не все настройки подключения указаны! Проверьте файл .env")
        
        credentials = {
            'host': host,
            'port': int(port),
            'database': database,
            'user': user,
            'password': password
        }
        
        print(f"Подключаемся к базе данных: {credentials['host']}:{credentials['port']}")
        conn = psycopg2.connect(**credentials)
        cursor = conn.cursor()
        print("Подключение установлено")
        
        # Загружаем данные из ПОДГОТОВЛЕННОГО Parquet файла
        df = load_from_parquet()
        
        # Берем первые 100 строк
        df_100 = df.head(100)
        print(f"Готовим к записи: {len(df_100)} строк, {len(df_100.columns)} колонок")
        
        # Создаем таблицу с названием фамилии
        table_name = "ponomarets"
        print(f"Создаем таблицу: {table_name}")
        
        # Удаляем старую таблицу если она есть
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print("Старая таблица удалена")
        
        # Создаем нормализованные названия колонок
        normalized_columns = [normalize_column_name(col) for col in df_100.columns]
        
        # Выводим таблицу соответствия ДО создания таблицы
        print("\n📋 СООТВЕТСТВИЕ КОЛОНОК (оригинал -> нормализованное):")
        print("=" * 80)
        for i, original_col in enumerate(df_100.columns):
            print(f"  {original_col:40} -> {normalized_columns[i]}")
        print("=" * 80)
        
        # Создаем таблицу на основе структуры данных
        columns_sql = []
        for i, col in enumerate(df_100.columns):
            dtype = str(df_100[col].dtype)
            normalized_col = normalized_columns[i]
            
            if 'category' in dtype or 'object' in dtype:
                sql_type = 'TEXT'
            elif 'int' in dtype:
                sql_type = 'INTEGER'
            elif 'float' in dtype:
                sql_type = 'REAL'
            else:
                sql_type = 'TEXT'
                
            columns_sql.append(f'"{normalized_col}" {sql_type}')
        
        create_sql = f'CREATE TABLE {table_name} ({", ".join(columns_sql)})'
        cursor.execute(create_sql)
        print("Новая таблица создана")
        
        # Записываем данные
        print(f"Записываем данные в таблицу {table_name}...")
        
        # Подготавливаем SQL запрос с нормализованными названиями колонок
        columns_str = ', '.join([f'"{col}"' for col in normalized_columns])
        placeholders = ', '.join(['%s'] * len(normalized_columns))
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        total_inserted = 0
        
        for index, row in df_100.iterrows():
            try:
                # Преобразуем строку в список значений
                values = []
                for col in df_100.columns:
                    value = row[col]
                    # Обрабатываем NaN значения
                    if pd.isna(value):
                        values.append(None)
                    else:
                        # Преобразуем в базовые типы Python
                        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
                            values.append(str(value))
                        else:
                            values.append(value)
                
                # Вставляем одну строку
                cursor.execute(insert_sql, values)
                total_inserted += 1
                
                if total_inserted % 20 == 0:
                    print(f"   Записано строк: {total_inserted}/{len(df_100)}")
                
            except Exception as e:
                print(f"Ошибка в строке {index}: {e}")
                continue
        
        conn.commit()
        
        print(f"Успешно записано строк: {total_inserted}")
        
        # Проверяем результат
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Проверка: в таблице {count} строк")
        
        # Выводим структуру таблицы из PostgreSQL
        print(f"\n🏗️  СТРУКТУРА ТАБЛИЦЫ В POSTGRESQL:")
        print("=" * 80)
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        
        columns_info = cursor.fetchall()
        print(f"{'Колонка в БД':30} {'Тип':15} {'NULLable':10}")
        print("-" * 80)
        for col_info in columns_info:
            print(f"{col_info[0]:30} {col_info[1]:15} {col_info[2]:10}")
        print("=" * 80)
        
        # Выводим пример данных из таблицы
        print(f"\n📊 ПРИМЕР ДАННЫХ ИЗ ТАБЛИЦЫ (первые 3 строки):")
        print("=" * 80)
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
        sample_data = cursor.fetchall()
        
        # Получаем названия колонок для заголовка
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        col_names = [desc[0] for desc in cursor.description]
        
        # Выводим заголовок с названиями колонок
        header = " | ".join([f"{name:15}" for name in col_names])
        print(header)
        print("-" * len(header))
        
        # Выводим данные
        for row in sample_data:
            row_str = " | ".join([f"{str(val):15}" for val in row])
            print(row_str)
        print("=" * 80)
        
        cursor.close()
        conn.close()
        
        print("\n✅ Задание выполнено!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
    print("Скрипт завершен")
