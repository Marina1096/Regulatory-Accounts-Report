-- Создание схем
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS reporting;

-- Таблица для загрузки JSON-данных
DROP TABLE IF EXISTS staging.accounts_raw CASCADE;
CREATE TABLE staging.accounts_raw (
    account_id   TEXT,
    client_id    TEXT,
    balance      NUMERIC(18, 2),
    opened_at    DATE,
    load_date    DATE DEFAULT CURRENT_DATE
);

-- Основная таблица для отчетности (актуальные данные без дублей)
DROP TABLE IF EXISTS reporting.accounts CASCADE;
CREATE TABLE reporting.accounts (
    account_id   TEXT PRIMARY KEY,
    client_id    TEXT NOT NULL,
    balance      NUMERIC(18, 2) NOT NULL,
    opened_at    DATE NOT NULL,
    load_date    DATE NOT null
);

-- BI-витрина: только счета с балансом > 500000
DROP VIEW IF EXISTS reporting.accounts_view;
CREATE VIEW reporting.accounts_view AS
SELECT
    account_id,
    client_id,
    balance,
    opened_at,
    load_date
FROM reporting.accounts
WHERE balance > 500000;

SELECT load_date, COUNT(*) 
FROM reporting.accounts_view
GROUP BY load_date
ORDER BY load_date;

select * from reporting.accounts_view;