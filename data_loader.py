import pandas as pd

# сюда вставила последний кусок ссылки - это айди
FILE_ID = "1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR?ths=true"  

# ссылку для скачивания
url = f"https://drive.google.com/drive/u/1/folders/={FILE_ID}"

# читаем CSV по ссылке
raw_data = pd.read_csv(url)

# показываем первые 10 строк
print(raw_data.head(10))
