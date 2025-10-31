import pandas as pd

def validate_raw_data(df):
    """
    Валидирует сырые данные
    """
    if df.empty:
        print("❌ Данные пустые")
        return False
    
    if df.shape[0] == 0:
        print("❌ Нет строк в данных")
        return False
    
    if df.shape[1] == 0:
        print("❌ Нет колонок в данных")
        return False
    
    print(f"✅ Сырые данные: {df.shape[0]} строк, {df.shape[1]} колонок")
    return True

def validate_processed_data(df):
    """
    Валидирует обработанные данные
    """
    if df.empty:
        print("❌ Обработанные данные пустые")
        return False
    
    # Проверяем типы данных
    numeric_cols = df.select_dtypes(include=['number']).columns
    datetime_cols = df.select_dtypes(include=['datetime']).columns
    
    print(f"✅ Числовые колонки: {len(numeric_cols)}")
    print(f"✅ Дата-время колонки: {len(datetime_cols)}")
    
    return True
