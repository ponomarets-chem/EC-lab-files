import os
import pandas as pd
import gdown
import sys

FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"
local_filename = "инжиниринг.csv"
out_parquet = "инжиниринг.parquet"
Список типов: mode	0	error	control changes	Ns changes	counter inc.	Ns	time/s	
control/V	Ewe/V	<I>/mA	dQ/C	(Q-Qo)/C	I Range	Q charge/discharge/mA.h	
half cycle	Energy charge/W.h	Energy discharge/W.h	Capacitance charge/µF	
Capacitance discharge/µF	Q discharge/mA.h	Q charge/mA.h	Capacity/mA.h	
Efficiency/%	cycle number	P/W

TYPE_MAP = {
    "Source.Name": "category",
    "mode": "Int64",
    "0": "Int64",
    "error": "Int64"
    "control changes": "Int64",
    "Ns changes": "Int64",
    "counter inc.": "Int64",
    "Ns": "Int64",
    "time/s": "float",
    "control/V": "float",
    "Ewe/V": "float"
    "<I>/mA": "float",
    "dQ/C": "float",
    "(Q-Qo)/C": "float",
    "I Range": "Int64",
    "Q charge/discharge/mA.h": "float"
    "half cycle": "Int64",
    "Energy charge/W.h": "float",
    "Energy discharge/W.h": "float"
    "Capacitance charge/µF": "float",
    "Capacitance discharge/µF": "float",
    "Q discharge/mA.h": "float"
    "Q charge/mA.h": "float",
    "Capacity/mA.h": "float",
    "Efficiency/%": "float",
    "cycle number": "float",
    "P/W": "float"
}
# Скачиваем файл, если его нет
if not os.path.exists(local_filename):
    print("Файл не найден локально. Скачиваем...")
    gdown.download(url, local_filename, quiet=False)
else:
    print("Файл уже существует. Используем локальный файл.")
# Чтение и приведение типов
def load_and_cast():
    print("Читаем CSV...")
    df = pd.read_csv(local_csv, low_memory=False)

    print("Приводим типы согласно TYPE_MAP...")
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        else:
            print(f"⚠️ ВНИМАНИЕ: колонки {col} нет в файле!")
    return df
# Сохранение в parquet

def save_parquet(df):
    print("Сохраняем в Parquet:", out_parquet)
    df.to_parquet(out_parquet, engine="pyarrow", compression="snappy", index=False)
    print("Файл сохранён:", out_parquet)

def main():
    download_if_needed()
    df = load_and_cast()
    print("Первые 10 строк:")
    print(df.head(10))
    save_parquet(df)

if __name__ == "__main__":
    main()

