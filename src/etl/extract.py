import os
import hashlib
import pandas as pd
import gdown

def compute_file_hash(path: str, algorithm: str = "sha256") -> str:
    """Вычисляет хэш файла (по умолчанию SHA-256)."""
    hash_func = hashlib.new(algorithm)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def extract_data(source_path, output_dir="data/raw"):
    """
    Загружает данные из источника и сохраняет в raw формате
    """
    # === Настройки ===
    FILE_ID = "1YF8duBM5HERkyCPAUPlzrs9mirZInNkT"
    url = f"https://drive.google.com/uc?id={FILE_ID}"
    local_csv = os.path.join(output_dir, "инжиниринг.csv")
    EXPECTED_HASH = "d380426c075b294b3a5808b987a352c53e8b3ff3ae99e6bec50423a710166c1f"

    # Создаем директорию, если её нет
    os.makedirs(output_dir, exist_ok=True)

    # === Функция скачивания ===
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

    # === Основная логика ===
    if "drive.google.com" in source_path or "uc?id=" in source_path:
        print("📥 Обнаружена ссылка на Google Drive")
        download_if_needed()
    else:
        print(f"📥 Копируем локальный файл: {source_path}")
        # Для локальных файлов просто копируем
        import shutil
        shutil.copy2(source_path, local_csv)

    print(f"✅ Сырые данные сохранены в: {local_csv}")
    return local_csv
