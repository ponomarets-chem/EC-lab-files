import os
import pandas as pd
import gdown

# ID файла на Google Drive
FILE_ID = "1oC-liv_Ni5OMboPo2p257Gohk3vMO4IH"
URL = f"https://drive.google.com/uc?id={FILE_ID}"

# Локальные имена файлов
LOCAL_CSV = "инжиниринг.csv"
OUT_PARQUET = "инжиниринг.parquet"


def download_if_needed():
    if not os.path.exists(LOCAL_CSV):
        print("Файл не найден локально. Скачиваем...")
        gdown.download(URL, LOCAL_CSV, quiet=False)
    else:
        print("Файл уже существует. Используем локальный файл.")


def load_and_cast():
    """Читаем CSV и приводим данные к числовому виду"""
    print("Определяем строку с заголовками...")

    header_line = None
    with open(LOCAL_CSV, "r", encoding="latin1") as f:
        for i, line in enumerate(f):
            if "time/s" in line and ";" in line:
                header_line = i
                break

    if header_line is None:
        raise ValueError("❌ Не удалось найти строку с заголовками (нет 'time/s').")

    print(f"Нашли заголовки на строке {header_line}")

    df = pd.read_csv(
        LOCAL_CSV,
        sep=";",
        header=header_line,
        encoding="latin1",
        dtype=str,
        engine="python",
        quoting=3,
        on_bad_lines="skip"
    )

    print(f"Загружено {df.shape[0]} строк и {df.shape[1]} столбцов")

    # чистим числа
    for col in df.columns:
        df[col] = df[col].str.replace(",", ".", regex=False).str.strip()
        df[col] = pd.to_numeric(df[col], errors="ignore")

    # убираем NaN → 0
    df = df.fillna(0)

    return df


def save_parquet(df):
    print("Сохраняем в Parquet:", OUT_PARQUET)
    df.to_parquet(OUT_PARQUET, engine="pyarrow", compression="snappy", index=False)
    print("Файл сохранён:", OUT_PARQUET)


def main():
    download_if_needed()
    df = load_and_cast()

    print("\nПример данных:")
    print(df.head(10))

    print("\nТипы колонок:")
    print(df.dtypes)

    save_parquet(df)


if __name__ == "__main__":
    main()
