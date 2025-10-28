# EC-lab-files
# Домашка №1
Collection of EC-lab files exported to txt format
Data set -https://drive.google.com/drive/u/1/folders/1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR?ths=true 
# Домашка №2
## Как запустить
1. Установить зависимости:
pip install -r requirements.txt
2. Запустить:
python data_loader.py
Заметки: пришлось скачивать локально файл, так как он огромный, поэтому в коде немного больше строк чем хотелось бы
## Результат
<img width="620" height="69" alt="image" src="https://github.com/user-attachments/assets/9dc4b465-6650-42d7-88a4-98fbdde628b9" />


# Типизация и перевод в паркет

Все категории были прописаны, также пришлось добавить n-ное количество строчек кода для того чтоб данные отображались хоть как-то кроме Nan. Для этого сместила когда начинает типизировать все пайтон, поменяла разделитель десятичный
что было в коде
## Первые 10 строк:
  -0,3V CA with magnet_C01.mpt  ...       P/W
0    -0.3VCAwithmagnet_C01.mpt  ...  0.005965
1    -0.3VCAwithmagnet_C01.mpt  ...  0.003460
2    -0.3VCAwithmagnet_C01.mpt  ...  0.002029
3    -0.3VCAwithmagnet_C01.mpt  ...  0.001556
4    -0.3VCAwithmagnet_C01.mpt  ...  0.001308
5    -0.3VCAwithmagnet_C01.mpt  ...  0.001153
6    -0.3VCAwithmagnet_C01.mpt  ...  0.001046
7    -0.3VCAwithmagnet_C01.mpt  ...  0.000967
8    -0.3VCAwithmagnet_C01.mpt  ...  0.000908
9    -0.3VCAwithmagnet_C01.mpt  ...  0.000860
<img width="756" height="512" alt="image" src="https://github.com/user-attachments/assets/d1a405a2-c95a-45b7-9d0d-f3883ff04818" />


[10 rows x 27 columns]

Типы колонок в DataFrame:
-0,3V CA with magnet_C01.mpt    category
mode                             float64
ox/red                          category
error                            float64
control changes                  float64
Ns changes                       float64
counter inc.                     float64
Ns                               float64
time/s                           float64
control/V                        float64
Ewe/V                            float64
<I>/mA                           float64
dQ/C                             float64
(Q-Qo)/C                         float64
I Range                          float64
Q charge/discharge/mA.h          float64
half cycle                       float64
Energy charge/W.h                float64
Energy discharge/W.h             float64
Capacitance charge/µF            float64
Capacitance discharge/µF         float64
Q discharge/mA.h                 float64
Q charge/mA.h                    float64
Capacity/mA.h                    float64
Efficiency/%                     float64
cycle number                     float64
P/W                              float64
dtype: object


# Электрохимический анализ данных

## 📊 EDA Ноутбук

[![Open In Nbviewer](https://raw.githubusercontent.com/jupyter/design/master/logos/Badges/nbviewer_badge.svg)](https://nbviewer.org/github/username/repository/blob/main/notebooks/EDA.ipynb)

### Особенности:
- Интерактивная 3D визуализация CV экспериментов
- Анимированные вольтамперные характеристики  
- Сравнительный анализ категорий экспериментов
- Единый стиль оформления
