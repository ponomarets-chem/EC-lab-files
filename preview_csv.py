import pandas as pd

# Путь к CSV
local_csv = "инжиниринг.csv"

# Попробуем автоматически определить разделитель
with open(local_csv, 'r', encoding='cp1251') as f:
    first_line = f.readline()
    if ';' in first_line:
        sep = ';'
    elif ',' in first_line:
        sep = ','
    else:
        sep = None  # pandas сам попытается угадать

print(f"Используемый разделитель: {sep}")

# Читаем только заголовок и первые 10 строк после него
df_preview = pd.read_csv(
    local_csv,
    sep=sep,
    header=61,      # 62-я строка как имена колонок
    decimal=',',
    encoding='cp1251',
    nrows=10
)

# Показываем первые 10 строк
print("Первые 10 строк CSV:")
print(df_preview.head(10))

# Показываем типы колонок, которые pandas определил сам
print("\nТипы колонок:")
print(df_preview.dtypes)

# Опционально: выводим, сколько пропущенных значений в каждой колонке
print("\nКоличество пропусков (NaN) в каждой колонке:")
print(df_preview.isna().sum())
