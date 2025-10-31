import os
import pandas as pd
import gdown

# Настройки файла и ссылки
FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"
local_csv = "инжиниринг.csv"

# Функция скачивания файла, если его нет
def download_if_needed():
    if not os.path.exists(local_csv):
        print("Файл не найден локально. Скачиваем...")
        gdown.download(url, local_csv, quiet=False)
    else:
        print("Файл уже существует. Используем локальный файл.")

# Функция просмотра первых строк CSV и типов колонок
def preview_csv():
    print("Читаем CSV с 62-й строки как заголовок...")
    df_preview = pd.read_csv(
        local_csv,
        sep=';',       # поменяй на ',' если разделитель другой
        header=61,     # 62-я строка как заголовок
        decimal=',',
        encoding='cp1251',
        nrows=10       # только первые 10 строк
    )

    print("\nПервые 10 строк CSV:")
    print(df_preview.head(10))

    print("\nТипы колонок:")
    print(df_preview.dtypes)

def main():
    download_if_needed()
    preview_csv()

if __name__ == "__main__":
    main()
