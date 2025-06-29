import os
from pathlib import Path
from datetime import datetime
import psycopg2
import json

# === Настройки подключения к PostgreSQL ===
conn_params = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'postgres',         
    'user': 'Marina',           
    'password': 'Snezhinka.2023'        
}

# === Функция загрузки одного JSON в staging ===
def load_json_to_staging(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    for acc in data:
        cur.execute("""
            INSERT INTO staging.accounts_raw (account_id, client_id, balance, opened_at, load_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            acc["account_id"],
            acc["client_id"],
            acc["balance"],
            acc["opened_at"],
            extract_date_from_filename(file_path.name)
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Загружено: {file_path.name} ({len(data)} записей)")

# === Вставка/обновление в reporting.accounts ===
def upsert_to_reporting():
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO reporting.accounts (account_id, client_id, balance, opened_at, load_date)
        SELECT DISTINCT ON (account_id) account_id, client_id, balance, opened_at, load_date
        FROM staging.accounts_raw
        ORDER BY account_id, load_date DESC
        ON CONFLICT (account_id) DO UPDATE
        SET 
            client_id = EXCLUDED.client_id,
            balance = EXCLUDED.balance,
            opened_at = EXCLUDED.opened_at,
            load_date = EXCLUDED.load_date
    """)

    conn.commit()
    cur.close()
    conn.close()
    print(f"🔄 Обновление reporting.accounts завершено")

# === Вспомогательная: дата из имени файла
def extract_date_from_filename(filename: str) -> datetime.date:
    base = filename.replace("accounts_", "").replace(".json", "")
    return datetime.strptime(base, "%Y-%m-%d").date()

# === Основной цикл по всем JSON в директории ===
def process_all_jsons(json_dir="json_data"):
    files = sorted(Path(json_dir).glob("accounts_2024-*.json"))
    for file_path in files:
        load_json_to_staging(file_path)
        upsert_to_reporting()

# === Запуск ===
if __name__ == "__main__":
    process_all_jsons("json_data")

