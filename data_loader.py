import os
import pandas as pd
import gdown

# Ссылка на файл в Google Drive
FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"

# Локальные имена файлов для CSV и Parquet
local_csv = "инжиниринг.csv"
out_parquet = "инжиниринг.parquet"

# Словарь типов колонок
TYPE_MAP = {
    "-0,3V CA with magnet_C01.mpt": "category",
    "mode": "Int64",
    "ox/red": "category",
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
    print("Читаем CSV, пропуская метаданные и используя правильный заголовок...")
    df = pd.read_csv(
        local_csv,
        sep=';',        # Разделитель ;
        header=61,      # 62-я строка — заголовок
        decimal=',',    # Десятичный разделитель — запятая
        encoding='cp1251',
        low_memory=False
    )

    # Убираем "Unnamed" хвостовые колонки
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # Убираем строки, где кроме первой колонки всё пусто
    df = df.dropna(how="all", subset=df.columns[1:])

    print("Приводим типы колонок согласно TYPE_MAP...")
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype in ("Int64", "float"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        else:
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

    # Вывод первых 10 строк для проверки
    print("\nПервые 10 строк:")
    print(df.head(10))

    print("\nТипы колонок в DataFrame:")
    print(df.dtypes)

    save_parquet(df)

if __name__ == "__main__":
    main()
