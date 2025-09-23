import os
import pandas as pd
import gdown

FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"
local_filename = "инжиниринг.csv"

# Скачиваем файл, если его нет
if not os.path.exists(local_filename):
    print("Файл не найден локально. Скачиваем...")
    gdown.download(url, local_filename, quiet=False)
else:
    print("Файл уже существует. Используем локальный файл.")

# Чтение CSV с локального файла (вне if/else!)
raw_data = pd.read_csv(local_filename)

# Показываем первые 10 строк
print(raw_data.head(10))
