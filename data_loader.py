import os
import pandas as pd
import gdown

#строчка выше нужна потому что файл не открывается в браузере он слишком большой
# сюда вставила последний кусок ссылки - это айди. Без хвостика в конце
FILE_ID = "1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR"  

# ссылку для скачивания
url = f"https://drive.google.com/uc?id={FILE_ID}"
local_filename = "data.csv"
# Скачиваем файл, если его нет
# -----------------------------
if not os.path.exists(local_filename):
    print("Файл не найден локально. Скачиваем...")
    gdown.download(url, local_filename, quiet=False)
else:
    print("Файл уже существует. Используем локальный файл.")
    
    # Пробуем открыть с стандартными настройками
    raw_data = pd.read_csv(local_filename)

# показываем первые 10 строк
print(raw_data.head(10))
