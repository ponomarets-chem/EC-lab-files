import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import re

print("Начало работы скрипта")

# Загружаем настройки подключения
env_path = r'C:\Users\ponom\Desktop\инжиниринг упраления данными\.env'
print(f"Ищем файл с настройками: {env_path}")

try:
    load_dotenv(env_path)
    print("Настройки подключения загружены")
except Exception as e:
    print(f"Ошибка загрузки настроек: {e}")

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
        
        # Загружаем данные из CSV файла
        print("Читаем данные из файла...")
        df = pd.read_csv("инжиниринг.csv", sep=";", header=61, encoding="cp1251", low_memory=False)
        
        # Обрабатываем названия колонок
        df.columns = [col.replace("�", "µ").strip() for col in df.columns]
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        
        # Берем первые 100 строк
        df_100 = df.head(100)
        print(f"Готовим к записи: {len(df_100)} строк, {len(df_100.columns)} колонок")
        
        # Создаем таблицу с названием фамилии
        table_name = "ponomarets"
        print(f"Создаем таблицу: {table_name}")
        
        # Удаляем старую таблицу если она есть
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        print("Старая таблица удалена")
        
        # Создаем новую таблицу
        create_sql = f"""
        CREATE TABLE {table_name} (
            id TEXT,
            mode TEXT,
            "time/s" TEXT,
            "control/V" TEXT,
            "Ewe/V" TEXT,
            "<I>/mA" TEXT
        )
        """
        cursor.execute(create_sql)
        print("Новая таблица создана")
        
        # Записываем данные
        print(f"Записываем данные в таблицу {table_name}...")
        
        total_inserted = 0
        for index, row in df_100.iterrows():
            try:
                insert_sql = f"""
                INSERT INTO {table_name} (id, mode, "time/s", "control/V", "Ewe/V", "<I>/mA") 
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                # Берем значения как строки
                id_val = str(row['id']) if 'id' in row and pd.notna(row['id']) else ''
                mode_val = str(row['mode']) if 'mode' in row and pd.notna(row['mode']) else ''
                time_val = str(row['time/s']) if 'time/s' in row and pd.notna(row['time/s']) else ''
                control_val = str(row['control/V']) if 'control/V' in row and pd.notna(row['control/V']) else ''
                ewe_val = str(row['Ewe/V']) if 'Ewe/V' in row and pd.notna(row['Ewe/V']) else ''
                current_val = str(row['<I>/mA']) if '<I>/mA' in row and pd.notna(row['<I>/mA']) else ''
                
                cursor.execute(insert_sql, (id_val, mode_val, time_val, control_val, ewe_val, current_val))
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
        
        # Показываем пример данных
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_data = cursor.fetchall()
        print("Пример записанных данных:")
        for i, row in enumerate(sample_data):
            print(f"   {i+1}: {row}")
        
        cursor.close()
        conn.close()
        
        print("Задание выполнено!")
        
    except Exception as e:
        print(f"Ошибка: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
    print("Скрипт завершен")
