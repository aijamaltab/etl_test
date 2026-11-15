-- запись результата проверки в таблицы одним запросом:
-- новые платежи, исправления и отклоненные строки.
-- все три CTE выполняются в любом случае и работают с разными строками
-- на выходе - сколько строк вставили, обновили и отклонили.
-- параметры: file_id, file_seq

WITH inserted AS (
    INSERT INTO payments (payment_id, contract_id, prov_dt, amount, currency, source_file_id, source_seq)
    SELECT payment_id, contract_id, prov_dt, amount, currency, %(file_id)s, %(file_seq)s
    FROM stg_checked
    WHERE action = 'insert'
    RETURNING 1
),
updated AS (
    UPDATE payments p
    SET
        contract_id = s.contract_id,
        prov_dt = s.prov_dt,
        amount = s.amount,
        currency = s.currency,
        source_file_id = %(file_id)s,
        source_seq = %(file_seq)s,
        updated_at = now()
    FROM stg_checked s
    WHERE s.action = 'update'
      AND p.payment_id = s.payment_id
      AND p.source_seq <= %(file_seq)s  -- старый файл не должен откатить версию из нового
    RETURNING 1
),
rejected AS (
    INSERT INTO payments_rejected (file_id, row_num, payment_id, raw_values, reasons)
    SELECT %(file_id)s, row_num, payment_id, raw_values, reasons
    FROM stg_checked
    WHERE action = 'reject'
    RETURNING 1
)
SELECT
    (SELECT count(*) FROM inserted) AS n_inserted,
    (SELECT count(*) FROM updated) AS n_updated,
    (SELECT count(*) FROM rejected) AS n_rejected;