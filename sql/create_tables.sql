-- DDL для загрузки платежей
-- Схема берется из search_path:
-- PGOPTIONS='-c search_path=payments_etl' psql -v ON_ERROR_STOP=1 -f sql/create_tables.sql

BEGIN;

-- допустимые валюты
CREATE TABLE IF NOT EXISTS ref_currency (
    currency text PRIMARY KEY CHECK (currency ~ '^[A-Z]{3}$')
);

INSERT INTO ref_currency (currency)
VALUES ('KGS'), ('USD'), ('EUR'), ('RUB')
ON CONFLICT DO NOTHING;

-- договоры, грузятся из contracts.csv до платежей
CREATE TABLE IF NOT EXISTS contracts (
    contract_id text PRIMARY KEY,
    customer_id text NOT NULL,
    open_dt     date NOT NULL
);

-- таблица загруженных файлов, одна строка на содержимое файла
CREATE TABLE IF NOT EXISTS load_files (
    file_id        int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name      text NOT NULL,
    file_seq       int NOT NULL, -- из имени: payments_002.csv -> 2
    file_hash      text NOT NULL UNIQUE, -- sha256
    status         text NOT NULL CHECK (status IN ('processing', 'success', 'failed')),
    error_code     text,
    error_message  text,
    rows_read      int,
    rows_rejected  int,
    rows_inserted  int,
    rows_updated   int,
    rows_unchanged int,
    started_at     timestamptz NOT NULL DEFAULT now(),
    finished_at    timestamptz
);
-- имя уникально только среди success файлов
-- упавший файл можно перезалить под тем же именем, а успешный перезаписать нельзя
CREATE UNIQUE INDEX IF NOT EXISTS load_files_success_name_uq
    ON load_files (file_name)
    WHERE status = 'success';

-- grain-зерно: одна строка на payment_id, в ней последняя корректная версия
CREATE TABLE IF NOT EXISTS payments (
    payment_id     text PRIMARY KEY CHECK (payment_id ~ '^[A-Z0-9_-]{1,30}$'),
    contract_id    text NOT NULL REFERENCES contracts,
    prov_dt        date NOT NULL,
    amount         numeric(18,2) NOT NULL CHECK (amount > 0),
    currency       text NOT NULL REFERENCES ref_currency,
    source_file_id int NOT NULL REFERENCES load_files,
    source_seq     int NOT NULL,  -- дублирую file_seq, чтобы проверять порядок версий в update без join
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS payments_prov_dt_idx     ON payments (prov_dt);
CREATE INDEX IF NOT EXISTS payments_contract_id_idx ON payments (contract_id);

-- отклоненные строки: все причины сразу и сырые значения как есть
CREATE TABLE IF NOT EXISTS payments_rejected (
    reject_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id     int NOT NULL REFERENCES load_files,
    row_num     int NOT NULL,   -- как в редакторе, заголовок = 1
    payment_id  text,           -- чтобы быстро найти, почему не загрузился конкретный платеж
    raw_values  jsonb NOT NULL, -- массивом: так влезает и строка с лишними колонками
    reasons     text[] NOT NULL CHECK (cardinality(reasons) > 0),
    rejected_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (file_id, row_num)
);
CREATE INDEX IF NOT EXISTS payments_rejected_payment_id_idx ON payments_rejected (payment_id);

COMMIT;