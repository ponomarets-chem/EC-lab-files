import requests
import pandas as pd

def fetch_spells():
    """Загружает список заклинаний из 5e-bits API"""
    url = "https://api.open5e.com/spells/"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    spells = data["results"]

    # создаём DataFrame с нужными полями
    df = pd.DataFrame(spells, columns=["name", "level", "school", "casting_time", "duration"])
    return df

if __name__ == "__main__":
    df = fetch_spells()
    print("✨ Заклинания из D&D 5e API ✨\n")
    print(df.head(10))
    print(f"\nВсего загружено: {len(df)} записей.")
