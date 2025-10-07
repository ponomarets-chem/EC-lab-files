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

# Словарь типов колонок
TYPE_MAP = {
    "id": "category",
    "mode": "Int64",
    "0": "Int64",
    "error": "Int64",
    "control changes": "Int64",
    "Ns changes": "Int64",
    "counter inc.": "Int64",
    "Ns": "Int64",
    "time/s": "float",
    "control/V": "float",
    "Ewe/V": "float",
    "<I>/mA": "float",
    "dQ/C": "float",
    "(Q-Qo)/C": "float",
    "I Range": "Int64",
    "Q charge/discharge/mA.h": "float",
    "half cycle": "Int64",
    "Energy charge/W.h": "float",
    "Energy discharge/W.h": "float",
    "Capacitance charge/µF": "float",
    "Capacitance discharge/µF": "float",
    "Q discharge/mA.h": "float",
    "Q charge/mA.h": "float",
    "Capacity/mA.h": "float",
    "Efficiency/%": "float",
    "cycle number": "float",
    "P/W": "float"
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

def load_and_cast():
    """Загружает CSV и приводит типы колонок."""
    print("Читаем CSV с 62-й строки как заголовок...")
    df = pd.read_csv(
        local_csv,
        sep=";",
        header=61,
        decimal=",",
        encoding="cp1251",
        low_memory=False
    )

    print("Приводим типы колонок согласно TYPE_MAP…")
    missing = []
    for col, dtype in TYPE_MAP.items():
        if col in df.columns:
            if "Int" in dtype or "float" in dtype:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(dtype)
        else:
            missing.append(col)

    if missing:
        print(f"⚠️ ВНИМАНИЕ: отсутствуют следующие колонки: {missing}")

    print("\nПервые 10 строк таблицы:")
    print(df.head(10))  

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

    print("\nПервые 10 строк:")
    print(df.head(10))

    print("\nСтроки 62–72:")
    print(df.iloc[61:72])

    print("\nТипы колонок:")
    print(df.dtypes)

    save_parquet(df)


if __name__ == "__main__":
    main()
