import os
import pandas as pd
import gdown

FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"
local_csv = "инжиниринг.csv"
out_parquet = "инжиниринг.parquet"


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

def download_if_needed():
    if not os.path.exists(local_csv):  # <-- было local_filename
        print("Файл не найден локально. Скачиваем...")
        gdown.download(url, local_csv, quiet=False)  # <-- тоже local_csv
    else:
        print("Файл уже существует. Используем локальный файл.")


def load_and_cast():
    print("Читаем CSV с 62-й строки как заголовок...")
    df = pd.read_csv(
        local_csv,
        sep=';',
        header=61,      # 62-я строка как заголовок
        decimal=',',
        encoding='cp1251',
        low_memory=False
    )

    print("Приводим типы колонок согласно TYPE_MAP...")
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            else:
                df[col] = pd.to_numeric(df[col], errors="coerce") if "Int" in dtype or "float" in dtype else df[col].astype(dtype)
        else:
            print(f"⚠️ ВНИМАНИЕ: колонки {col} нет в файле!")

    return df

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
