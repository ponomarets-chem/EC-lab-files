import os
import hashlib
import pandas as pd
import gdown

# === Настройки ===

# Ссылка на файл в Google Drive
FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
url = f"https://drive.google.com/uc?id={FILE_ID}"

# Имена файлов
local_csv = "инжиниринг.csv"
out_parquet = "инжиниринг.parquet"

# Эталонный SHA256-хэш
EXPECTED_HASH = "d380426c075b294b3a5808b987a352c53e8b3ff3ae99e6bec50423a710166c1f"

# Словарь типов колонок - ИСПРАВЛЕННЫЙ
TYPE_MAP = {
    "id": "category",
    "mode": "Int64",
    "ox/red": "category",
    "error": "Int64",
    "control changes": "Int64",
    "Ns changes": "Int64",
    "counter inc.": "Int64",
    "Ns": "Int64",
    "time/s": "float64",
    "control/V": "float64",
    "Ewe/V": "float64",
    "<I>/mA": "float64",
    "dQ/C": "float64",
    "(Q-Qo)/C": "float64",
    "I Range": "Int64",
    "Q charge/discharge/mA.h": "float64",
    "half cycle": "Int64",
    "Energy charge/W.h": "float64",
    "Energy discharge/W.h": "float64",
    "Capacitance charge/µF": "float64",
    "Capacitance discharge/µF": "float64",
    "Q discharge/mA.h": "float64",
    "Q charge/mA.h": "float64",
    "Capacity/mA.h": "float64",
    "Efficiency/%": "float64",
    "cycle number": "float64",
    "P/W": "float64"
}

# === Вспомогательные функции ===

def compute_file_hash(path: str, algorithm: str = "sha256") -> str:
    """Вычисляет хэш файла (по умолчанию SHA-256)."""
    hash_func = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def download_if_needed():
    """Скачивает файл, если его нет или хэш не совпадает."""
    need_download = False

    if os.path.exists(local_csv):
        print("Файл уже существует. Проверяем хэш…")
        local_hash = compute_file_hash(local_csv)
        print(f"Текущий SHA256: {local_hash}")

        if EXPECTED_HASH != "d380426c075b294b3a5808b987a352c53e8b3ff3ae99e6bec50423a710166c1f" and local_hash.lower() != EXPECTED_HASH.lower():
            print("⚠️ Хэш не совпадает с ожидаемым! Перекачиваем файл.")
            need_download = True
        else:
            print("✅ Хэш совпадает — используем локальный файл.")
    else:
        print("Файл не найден — начинаем скачивание.")
        need_download = True

    if need_download:
        if os.path.exists(local_csv):
            try:
                os.remove(local_csv)
            except Exception as e:
                print("Не удалось удалить старый файл:", e)

        gdown.download(url, local_csv, quiet=False)

        if not os.path.exists(local_csv):
            raise RuntimeError("❌ Не удалось скачать файл.")

        new_hash = compute_file_hash(local_csv)
        print(f"Хэш скачанного файла: {new_hash}")

        if EXPECTED_HASH != "REPLACE_WITH_REAL_HASH" and new_hash.lower() != EXPECTED_HASH.lower():
            raise RuntimeError("❌ Скачанный файл не совпадает по хэшу! Возможно, источник изменился.")

import re

def load_and_cast():
    """Загружает CSV и приводит типы колонок, очищает числовые данные и показывает первые строки."""
    print("Читаем CSV с 62-й строки как заголовок...")
    df = pd.read_csv(
        local_csv,
        sep=";",
        header=61,
        encoding="cp1251",
        low_memory=False
    )

    # Заменяем некорректные символы в названиях колонок
    df.columns = [col.replace("�", "µ").strip() for col in df.columns]

    # Убираем полностью пустые или 'Unnamed' колонки
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    print("🧮 Нормализуем числовые данные (запятые → точки, e-формат)...")

    # Функция очистки числовых ячеек
    def normalize_cell(x):
        if isinstance(x, str):
            s = x.strip().replace(",", ".").replace("−", "-").replace(" ", "")
            if re.fullmatch(r"[-+]?\d*\.?\d*(e[-+]?\d+)?", s, flags=re.IGNORECASE):
                try:
                    return float(s)
                except ValueError:
                    return pd.NA
            return x  # оставляем строки нетронутыми
        return x

    # Приведение типов по TYPE_MAP - БЕЗОПАСНЫЙ СПОСОБ
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            print(f"Обрабатываем колонку: {col} -> {dtype}")
            
            if dtype == "category":
                df[col] = df[col].astype("category")
                
            elif dtype == "Int64":
                # Сначала нормализуем, затем пытаемся преобразовать в целые
                df[col] = df[col].map(normalize_cell)
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
                # Проверяем, можно ли безопасно преобразовать в целые
                temp_series = df[col].dropna()
                if len(temp_series) > 0:
                    # Проверяем, все ли значения целые
                    if (temp_series == temp_series.astype(int)).all():
                        df[col] = df[col].astype("Int64")
                    else:
                        print(f"  ⚠️ Колонка {col} содержит дробные значения, оставляем как float")
                        df[col] = df[col].astype("float64")
                else:
                    df[col] = df[col].astype("Int64")
                    
            elif dtype == "float64":
                df[col] = df[col].map(normalize_cell)
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Печать первых строк для проверки
    print("\nПервые 10 ID:")
    if "id" in df.columns:
        print(df["id"].head(10))
    else:
        print("⚠️ Колонка 'id' не найдена!")

    print("\nТипы колонок после обработки:")
    print(df.dtypes)

    return df


def save_parquet(df):
    """Сохраняет DataFrame в Parquet."""
    print("Сохраняем в Parquet:", out_parquet)
    df.to_parquet(out_parquet, engine="pyarrow", compression="snappy", index=False)
    print("✅ Файл сохранён:", out_parquet)


# === Основная функция ===

def main():
    download_if_needed()
    df = load_and_cast()

    print("\nПервые 5 строк:")
    print(df.head(5))

    print("\nНазвания колонок:")
    print(list(df.columns))

    print("\nИнформация о данных:")
    print(df.info())

    save_parquet(df)


if __name__ == "__main__":
    main()
