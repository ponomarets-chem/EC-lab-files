import pandas as pd
import re
import os

# Словарь типов колонок
TYPE_MAP = {
    "id": "category",
    "mode": "Int64",
    "ox/red": "category",
    "error": "Int64",
    "control changes": "Int64",
    "Ns changes": "Int64",
    "counter inc.": "Int64",
    "Ns": "Int64",
    "time/s": "float64",
    "control/V": "float64",
    "Ewe/V": "float64",
    "<I>/mA": "float64",
    "dQ/C": "float64",
    "(Q-Qo)/C": "float64",
    "I Range": "Int64",
    "Q charge/discharge/mA.h": "float64",
    "half cycle": "Int64",
    "Energy charge/W.h": "float64",
    "Energy discharge/W.h": "float64",
    "Capacitance charge/µF": "float64",
    "Capacitance discharge/µF": "float64",
    "Q discharge/mA.h": "float64",
    "Q charge/mA.h": "float64",
    "Capacity/mA.h": "float64",
    "Efficiency/%": "float64",
    "cycle number": "float64",
    "P/W": "float64"
}

def normalize_cell(x):
    """Функция очистки числовых ячеек"""
    if isinstance(x, str):
        s = x.strip().replace(",", ".").replace("−", "-").replace(" ", "")
        if re.fullmatch(r"[-+]?\d*\.?\d*(e[-+]?\d+)?", s, flags=re.IGNORECASE):
            try:
                return float(s)
            except ValueError:
                return pd.NA
        return x
    return x

def transform_data(input_path, output_dir="data/processed"):
    """Трансформирует данные: приводит типы, очищает"""
    os.makedirs(output_dir, exist_ok=True)

    print("Читаем CSV с 62-й строки как заголовок...")
    df = pd.read_csv(
        input_path,
        sep=";",
        header=61,
        encoding="cp1251",
        low_memory=False
    )

    # Заменяем некорректные символы в названиях колонок
    df.columns = [col.replace("�", "µ").strip() for col in df.columns]

    # Убираем полностью пустые или Unnamed колонки
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    print("Нормализуем числовые данные...")

    # Приведение типов по TYPE_MAP
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            print(f"Обрабатываем колонку: {col} -> {dtype}")
            
            if dtype == "category":
                df[col] = df[col].astype("category")
                
            elif dtype == "Int64":
                df[col] = df[col].map(normalize_cell)
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
                temp_series = df[col].dropna()
                if len(temp_series) > 0:
                    if (temp_series == temp_series.astype(int)).all():
                        df[col] = df[col].astype("Int64")
                    else:
                        print(f"  Колонка {col} содержит дробные значения, оставляем как float")
                        df[col] = df[col].astype("float64")
                else:
                    df[col] = df[col].astype("Int64")
                    
            elif dtype == "float64":
                df[col] = df[col].map(normalize_cell)
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    print("Первые 10 ID:")
    if "id" in df.columns:
        print(df["id"].head(10))
    else:
        print("Колонка id не найдена!")

    print("Типы колонок после обработки:")
    print(df.dtypes)

    # Сохраняем в разных форматах
    csv_path = os.path.join(output_dir, "processed_data.csv")
    parquet_path = os.path.join(output_dir, "processed_data.parquet")
    
    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy", index=False)
    
    print(f"Данные сохранены в: {csv_path}")
    print(f"Данные сохранены в: {parquet_path}")
    
    return csv_path, parquet_path
