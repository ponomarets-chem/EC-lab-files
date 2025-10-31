# EC-lab-files 🌸

Проект для анализа данных электрохимических исследований с красивой визуализацией! 🎨✨

## 📊 О проекте

Collection of EC-lab files exported to txt format 📈  
Data set -https://drive.google.com/drive/u/1/folders/1CcFVavXoBoMwNZRdlXCKA-LY8l8kU_kR?ths=true 

## 📊 Цель проекта
Визуализировать файлы для полноценного сравнения. 

# Структура
```
├── etl/ # ETL пакет
│ ├── extract.py # Извлечение данных
│ ├── transform.py # Трансформация данных
│ ├── load.py # Загрузка в БД
│ ├── validate.py # Валидация
│ └── main.py # CLI интерфейс
├── notebooks/
│ └── eda_analysis.ipynb # EDA и визуализация
├── requirements.txt # Зависимости
└── run_etl.py # Автоматический запуск
```

## 🚀 Быстрый старт

### Установка
```bash
# Клонирование репозитория
git clone <your-repo-url>
cd "инжиниринг упраления данными"

# Установка зависимостей
pip install -r requirements.txt

## 🚀 Запуск ETL pipeline
# Автоматический запуск
python run_etl.py
```

## 📊 Описание данных

Данные содержат электрохимические измерения с следующими характеристиками:

Объем: 1,048,514 строк × 27 колонок

Размер: 222 МБ (сырые), 188 МБ (обработанные)

Ключевые параметры: напряжение, ток, емкость, энергия, время

Важно отметить что все измерения ВРЕМЕННЫЕ (идут от времени), что создает интересные зависимости, которые хочется визуализировать. Интересная и полезная визуализация есть в ноутбуке

Форматы: CSV, Parquet, PostgreSQL

## 🔊 Основные колонки:
id - идентификатор эксперимента

Ewe/V - напряжение электрода

<I>/mA - сила тока

time/s - время измерения

Capacity/mA.h - емкость

Efficiency/% - эффективность

#👾 Описание ETL процесса

## 📥 Extract (Извлечение)

Загрузка данных из Google Drive

Проверка целостности через SHA256

Сохранение в data/raw/

## 🔄 Transform (Трансформация)
Приведение типов данных (27 колонок)

Нормализация числовых значений

Очистка и обработка аномалий

Экспорт в CSV и Parquet

## 📤 Load (Загрузка)
Подключение к PostgreSQL

Создание оптимизированной схемы данных

Загрузка данных в таблицу

Валидация результатов


# 🎨 Визуализация

## Детальный визуальный анализ прописан в ноутбуке
DA Analysis](https://img.shields.io/badge/📊_View_EDA_Notebook-NbViewer-blue)](https://nbviewer.org/github/ponomarets-chem/EC-lab-files/blo
## ⚠️ Примечание

3D графики и анимации в ноутбуке видны только в интерактивных средах:
- Локальный Jupyter Notebook
- Google Colab
- VS Code с расширением Jupyter
## 🔍 Exploratory Data Analysis (EDA)

[![View Eb/main/notebooks/EDA.ipynb)


## 🚀 Быстрый старт

```bash
# Клонировать репозиторий
git clone https://github.com/ponomarets-chem/EC-lab-files.git
cd EC-lab-files

# Установить зависимости
pip install pandas matplotlib plotly jupyter

# Запустить ноутбук
jupyter notebook notebooks/EDA.ipynb
```bash

## 👤 Автор
pink_chemist - ponomarets - Инжиниринг управления данными






