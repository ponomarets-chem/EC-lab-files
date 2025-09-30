import pandas as pd

# Путь к CSV
local_csv = "инжиниринг.csv"

# Читаем только заголовок и первые 10 строк после него
# header=61 => 62-я строка используется как имена колонок
df_preview = pd.read_csv(
    local_csv,
    sep=';',        
    header=61,      # 62-я строка
    decimal=',',
    encoding='cp1251',
    nrows=10        # только первые 10 строк
)

# Показываем первые 10 строк
print("Первые 10 строк CSV:")
print(df_preview.head(10))

# Показываем типы колонок, которые pandas определил сам
print("\nТипы колонок:")
print(df_preview.dtypes)
