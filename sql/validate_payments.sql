-- проверка строк одного файла
-- берет сырые строки из stg_payments, результат кладет в stg_checked:
-- почищенные значения, все причины отказа и что делать со строкой (insert / update / unchanged / stale / reject)
-- file_seq - номер файла, который сейчас грузим
-- обе temp tables живут до конца транзакции файла

CREATE TEMP TABLE stg_checked ON COMMIT DROP AS
WITH norm AS (
    -- убираю пробелы, пустое значение = NULL, валюту в верхний регистр
    -- id оставляю как есть: p1 и P1 у источника могут быть разными платежами
    SELECT
        row_num,
        n_fields,
        raw_values,
        nullif(btrim(payment_id), '') AS payment_id,
        nullif(btrim(contract_id), '') AS contract_id,
        nullif(btrim(prov_dt_raw), '') AS prov_dt_raw,
        nullif(btrim(amount_raw), '') AS amount_raw,
        upper(nullif(btrim(currency), '')) AS currency
    FROM stg_payments
),
parsed AS (
    -- превращаю текст в дату так, чтобы не уронить файл:
    -- '2026-09-31'::date дал бы ошибку на весь файл, поэтому сначала проверяю сама:
    -- 1) формат ГГГГ-ММ-ДД, 2) такой день есть в этом месяце (31 сентября нет).
    -- не прошло -> NULL -> строка уйдет в reject с INVALID_DATE.
    -- CASE внутри CASE, а не AND: так день точно не считается на мусорной строке
    SELECT
        n.*,
        CASE WHEN n.prov_dt_raw ~ '^[12][0-9]{3}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$' THEN
            CASE WHEN substr(n.prov_dt_raw, 9, 2)::int <= extract(day FROM
                    make_date(substr(n.prov_dt_raw, 1, 4)::int, substr(n.prov_dt_raw, 6, 2)::int, 1)
                    + interval '1 month - 1 day')
                THEN n.prov_dt_raw::date
            END
        END AS prov_dt,
        -- регулярка отсекает NaN, Infinity, 12,5 и +5
        CASE WHEN n.amount_raw ~ '^-?[0-9]+(\.[0-9]+)?$' THEN n.amount_raw::numeric END AS amount_num
    FROM norm n
),
checked AS (
    SELECT
        p.*,
        CASE
            WHEN p.n_fields <> 5 THEN ARRAY['COLUMN_COUNT_MISMATCH']
            ELSE array_remove(ARRAY[
                CASE WHEN p.payment_id IS NULL THEN 'MISSING_PAYMENT_ID' END,
                CASE WHEN p.payment_id !~ '^[A-Z0-9_-]{1,30}$' THEN 'INVALID_PAYMENT_ID' END,
                CASE WHEN p.contract_id IS NULL THEN 'MISSING_CONTRACT_ID' END,
                CASE WHEN p.contract_id IS NOT NULL AND c.contract_id IS NULL THEN 'CONTRACT_NOT_FOUND' END,
                CASE WHEN p.prov_dt_raw IS NULL THEN 'MISSING_PROV_DT' END,
                CASE WHEN p.prov_dt_raw IS NOT NULL AND p.prov_dt IS NULL THEN 'INVALID_DATE' END,
                -- нет даты или договора -> сравнение дает NULL, причина не добавится
                -- и не надо: такая строка уже отклонена по своей причине
                CASE WHEN p.prov_dt < c.open_dt THEN 'DATE_BEFORE_CONTRACT_OPEN' END,
                CASE WHEN p.prov_dt > current_date THEN 'DATE_IN_FUTURE' END,
                CASE WHEN p.amount_raw IS NULL THEN 'MISSING_AMOUNT' END,
                CASE WHEN p.amount_raw IS NOT NULL AND p.amount_num IS NULL THEN 'INVALID_AMOUNT' END,
                CASE WHEN p.amount_num <= 0 THEN 'NON_POSITIVE_AMOUNT' END,
                CASE WHEN scale(p.amount_num) > 2 THEN 'AMOUNT_SCALE' END,
                -- больше не влезет в numeric(18,2), INSERT упадет на весь файл
                CASE WHEN p.amount_num > 9999999999999999.99 THEN 'AMOUNT_TOO_LARGE' END,
                CASE WHEN p.currency IS NULL THEN 'MISSING_CURRENCY' END,
                CASE WHEN p.currency IS NOT NULL AND r.currency IS NULL THEN 'INVALID_CURRENCY' END
            ], NULL)
        END AS reasons
    FROM parsed p
    LEFT JOIN contracts c ON c.contract_id = p.contract_id
    LEFT JOIN ref_currency r ON r.currency = p.currency
),
dup_groups AS (
    -- дубли ищу по всем строкам, не только по валидным:
    -- две разные версии от источника - проблема данных, а не моей проверки
    -- строки с неверным числом колонок пропускаю, там поля съехали
    -- n_versions - сколько разных вариантов строки у одного payment_id;
    -- сумму сравниваю как число (12.5 = 12.50), если не распарсилась - как текст
    SELECT
        payment_id,
        count(DISTINCT ROW(
            contract_id,
            prov_dt_raw,
            amount_num,
            CASE WHEN amount_num IS NULL THEN amount_raw END,
            currency
        )) AS n_versions
    FROM checked
    WHERE n_fields = 5
      AND payment_id IS NOT NULL
    GROUP BY payment_id
    HAVING count(*) > 1
),
with_dups AS (
    -- rn это номер строки среди строк с тем же payment_id (по порядку в файле)
    SELECT
        ch.*,
        g.n_versions,
        row_number() OVER (PARTITION BY ch.payment_id ORDER BY ch.row_num) AS rn
    FROM checked ch
    LEFT JOIN dup_groups g ON g.payment_id = ch.payment_id
),
final AS (
    SELECT
        row_num,
        raw_values,
        payment_id,
        contract_id,
        prov_dt,
        amount_num AS amount,  -- в numeric(18,2) приведется при вставке, тут еще бывает мусор
        currency,
        CASE
            -- если у платежа в файле разные версии: отклоняю все (почему - в README)
            WHEN n_versions > 1 THEN array_append(reasons, 'DUPLICATE_KEY_CONFLICT')
            -- если одинаковые строки: первая идет дальше, остальные в дубли
            WHEN n_versions = 1 AND rn > 1 THEN array_append(reasons, 'DUPLICATE_ROW')
            ELSE reasons
        END AS reasons
    FROM with_dups
)
SELECT
    f.*,
    CASE
        WHEN cardinality(f.reasons) > 0 THEN 'reject'
        WHEN p.payment_id IS NULL THEN 'insert'
        -- в payments уже версия из более нового файла
        -- при проверке порядка файлов такого не бывает, поэтому загрузчик падает с version_conflict
        WHEN p.source_seq > %(file_seq)s THEN 'stale'
        WHEN (p.contract_id, p.prov_dt, p.amount, p.currency)
            IS DISTINCT FROM (f.contract_id, f.prov_dt, f.amount, f.currency) THEN 'update'
        ELSE 'unchanged'
    END AS action
FROM final f
LEFT JOIN payments p ON p.payment_id = f.payment_id;
