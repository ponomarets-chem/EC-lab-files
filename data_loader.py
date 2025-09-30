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
    "id": "category",
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

def download_if_needed():
    if not os.path.exists(local_csv):
        print("Файл не найден локально. Скачиваем...")
        gdown.download(url, local_csv, quiet=False)
    else:
        print("Файл уже существует. Используем локальный файл.")

def load_and_cast():
    print("Читаем CSV, пропуская метаданные и используя правильный заголовок...")
    df = pd.read_csv(
        local_csv,
        sep=";",        # разделитель ;
        header=61,      # 62-я строка — заголовок
        encoding="cp1251",
        dtype=str,      # читаем всё как строки
        low_memory=False
    )

    # убираем "Unnamed" хвостовые колонки
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # заменяем запятые на точки и конвертим в правильные типы
    for col in df.columns:
        df[col] = (
            df[col]
            .str.replace(",", ".", regex=False)   # десятичная точка
            .str.replace(r"\s+", "", regex=True)  # убираем пробелы
        )

        # применяем TYPE_MAP
        if col in TYPE_MAP:
            t = TYPE_MAP[col]
            if t in ("float", "Int64"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif t == "category":
                df[col] = df[col].astype("category")

    # находим первую валидную строку по time/s
    if "time/s" in df.columns:
        first_valid = df["time/s"].first_valid_index()
        if first_valid is not None:
            df = df.loc[first_valid:].reset_index(drop=True)
            print(f"Отрезали все строки до {first_valid}, теперь данные начинаются с чисел.")
        else:
            print("⚠️ ВНИМАНИЕ: не найдено валидных числовых данных в 'time/s'!")
    else:
        print("⚠️ Колонки 'time/s' нет в файле!")

    return df


def save_parquet(df):
    print("Сохраняем в Parquet:", out_parquet)
    df.to_parquet(out_parquet, engine="pyarrow", compression="snappy", index=False)
    print("Файл сохранён:", out_parquet)

def main():
    download_if_needed()
    df = load_and_cast()

    print("\nПервые 10 строк:")
    print(df.head(10))

    print("\nТипы колонок в DataFrame:")
    print(df.dtypes)

    save_parquet(df)

if __name__ == "__main__":
    main()
