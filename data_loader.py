import os
import pandas as pd
import gdown

# Ссылка на файл в Google Drive
FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"

# Локальные имена файлов для CSV и Parquet
local_csv = "инжиниринг.csv"
out_parquet = "инжиниринг.parquet"

# Словарь типов колонок, чтобы привести их к нужным форматам
TYPE_MAP = {
    "-0,3V CA with magnet_C01.mpt": "category",
    "mode": "Int64",
    "0": "Int64",
    "error": "Int64",
    "control changes": "Int64",
    "Ns changes": "Int64",
    "counter inc.": "Int64",
    "Ns": "Int64",
    "time/s": "float",
    "control/V": "float",
    "Ewe/V": "float",
    "<I>/mA": "float",
    "dQ/C": "float",
    "(Q-Qo)/C": "float",
    "I Range": "Int64",
    "Q charge/discharge/mA.h": "float",
    "half cycle": "Int64",
    "Energy charge/W.h": "float",
    "Energy discharge/W.h": "float",
    "Capacitance charge/µF": "float",
    "Capacitance discharge/µF": "float",
    "Q discharge/mA.h": "float",
    "Q charge/mA.h": "float",
    "Capacity/mA.h": "float",
    "Efficiency/%": "float",
    "cycle number": "float",
    "P/W": "float"
}

# Функция скачивания файла, если его нет локально
def download_if_needed():
    if not os.path.exists(local_csv):
        print("Файл не найден локально. Скачиваем...")
        gdown.download(url, local_csv, quiet=False)
    else:
        print("Файл уже существует. Используем локальный файл.")

# Функция чтения CSV и приведения колонок к нужным типам
def load_and_cast():
    print("Читаем CSV с 62-й строки как заголовок...")
    df = pd.read_csv(
        local_csv,
        sep=';',        # Используем разделитель ;
        header=61,      # 62-я строка как заголовок
        decimal=',',    # Десятичные числа с запятой
        encoding='cp1251', 
        low_memory=False
    )

    print("Приводим типы колонок согласно TYPE_MAP...")
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            # Если колонка datetime
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                # Если числовая, конвертируем с coerce (ошибки станут NaN)
                df[col] = pd.to_numeric(df[col], errors="coerce") if "Int" in dtype or "float" in dtype else df[col].astype(dtype)
        else:
            # Сообщение, если колонки нет в файле
            print(f"⚠️ ВНИМАНИЕ: колонки {col} нет в файле!")

    return df

# Функция сохранения в Parquet
def save_parquet(df):
    print("Сохраняем в Parquet:", out_parquet)
    df.to_parquet(out_parquet, engine="pyarrow", compression="snappy", index=False)
    print("Файл сохранён:", out_parquet)

# Главная функция
def main():
    download_if_needed()
    df = load_and_cast()

    # Здесь я проверяю и вывожу типы колонок
    print("\nТипы колонок в DataFrame:")
    print(df.dtypes)

    # Показываем первые 10 строк для наглядности
    print("\nПервые 10 строк:")
    print(df.head(10))

    save_parquet(df)

if __name__ == "__main__":
    main()
