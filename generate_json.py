import json
import random
import datetime
from pathlib import Path

# Генерация одного JSON-файла на 1 дату
def generate_accounts_for_date(file_date: datetime.date, num_accounts: int = 50, dup_ratio: float = 0.1, out_dir="json_data"):
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(out_dir) / f"accounts_{file_date.isoformat()}.json"

    start_date = file_date - datetime.timedelta(days=365 * 5)
    accounts = []

    for _ in range(num_accounts):
        acc_id = str(random.randint(10**8, 10**12 - 1))
        client_id = str(random.randint(1000, 9999))
        balance = round(random.uniform(1000.00, 1_500_000.00), 2)
        opened_at = start_date + datetime.timedelta(days=random.randint(0, 5 * 365))
        accounts.append({
            "account_id": acc_id,
            "client_id": client_id,
            "balance": balance,
            "opened_at": opened_at.isoformat()
        })

    # Дубликаты (10% по умолчанию)
    num_dups = int(num_accounts * dup_ratio)
    for _ in range(num_dups):
        dup = random.choice(accounts)
        accounts.append(dup.copy())

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(accounts, f, indent=2, ensure_ascii=False)

    print(f"✅ {file_path.name} сохранён: {len(accounts)} записей")

# Основная генерация по дням 2024 года за исключением выходных дней
def generate_for_all_workdays(year: int = 2024):
    start = datetime.date(year, 1, 1)
    end = datetime.date(year, 12, 31)
    current = start
    while current <= end:
        if current.weekday() < 5:  # Пн–Пт
            generate_accounts_for_date(current)
        current += datetime.timedelta(days=1)

# Запуск
if __name__ == "__main__":
    generate_for_all_workdays(2024)
