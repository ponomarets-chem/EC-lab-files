import pandas as pd
import gdown

#строчка выше нужна потому что файл не открывается в браузере он слишком большой
# сюда вставила последний кусок ссылки - это айди. Без хвостика в конце
FILE_ID = "1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR"  

# ссылку для скачивания
url = f"https://drive.google.com/uc?id={FILE_ID}"

# сначала скачиваем файл локально, он слишком большой для прямого чтения
gdown.download(url, "data.csv", quiet=False)

# читаем CSV по ссылке
raw_data = pd.read_csv("data.csv")

# показываем первые 10 строк
print(raw_data.head(10))
