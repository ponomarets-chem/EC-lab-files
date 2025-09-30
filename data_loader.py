import os
import gdown
import pandas as pd

CSV_URL = "https://drive.google.com/uc?id=1oC-liv_Ni5OMboPo2p257Gohk3vMO4IH"
LOCAL_CSV = "инжиниринг.csv"


def download_csv():
    """Скачиваем CSV, если его нет локально"""
    if not os.path.exists(LOCAL_CSV):
        print("Файл не найден локально. Скачиваем...")
        gdown.download(CSV_URL, LOCAL_CSV, quiet=False)
    else:
        print("Файл найден локально.")


def load_and_cast():
    """Читаем CSV и приводим данные к числовому виду"""
    print("Читаем CSV, игнорируя кривые кавычки и битые строки...")

    df = pd.read_csv(
        LOCAL_CSV,
        sep=";",
        header=61,          # 62-я строка = заголовок
        encoding="latin1",  # широкая поддержка символов
        dtype=str,          # всё сначала как строки
        engine="python",    # более гибкий парсер
        quoting=3,          # игнорируем кавычки
        on_bad_lines="skip" # пропускаем битые строки
    )

    print(f"Загружено {df.shape[0]} строк и {df.shape[1]} столбцов")

    # Чистим и конвертируем всё в числа
    df = df.applymap(lambda x: x.replace(",", ".") if isinstance(x, str) else x)
    df = df.apply(pd.to_numeric, errors="coerce")

    # Убираем NaN → 0
    df = df.fillna(0)

    return df


def main():
    download_csv()
    df = load_and_cast()
    print("\nПример данных:")
    print(df.head())
    print("\nТипы данных:")
    print(df.dtypes)


if __name__ == "__main__":
    main()
