import pandas as pd

FILE_ID = "https://drive.google.com/drive/u/1/folders/1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR?ths=true" 

raw_data = pd.read_csv(url)

print(raw_data.head(10))
