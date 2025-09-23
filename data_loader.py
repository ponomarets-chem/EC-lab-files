import pandas as pd

# сюда вставила последний кусок ссылки - это айди. Без хвостика в конце
FILE_ID = "1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR"  

# ссылку для скачивания
url = f"https://drive.google.com/uc?id={FILE_ID}"

# читаем CSV по ссылке
raw_data = pd.read_csv(url)

# показываем первые 10 строк
print(raw_data.head(10))
