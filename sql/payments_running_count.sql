-- Кол-во платежей по датам нарастающим итогом

-- что такое "период" в тз не сказано, поэтому период задается параметрами,
-- а итог считаю двумя способами:
--   cnt_from_period_start - с начала заданного периода
--   cnt_mtd - с начала месяца (если под периодом имелся в виду месяц)
-- если даты не передать, период = от первого до последнего платежа в таблице.
-- выводятся все даты периода, в дни без платежей 0.
-- считаются текущие версии платежей: если исправление перенесло дату, платеж считается под новой датой
-- отклоненные строки не считаются!

-- создать (схема через search_path, как в create_tables.sql):
--   PGOPTIONS='-c search_path=payments_etl' psql -v ON_ERROR_STOP=1 -f sql/payments_running_count.sql
-- вызвать:
--   SELECT * FROM payments_etl.payments_running_count();                            -- весь период данных
--   SELECT * FROM payments_etl.payments_running_count('2026-09-01', '2026-09-30');  -- свой период

CREATE OR REPLACE FUNCTION payments_running_count(p_date_from date DEFAULT NULL, p_date_to date DEFAULT NULL)
RETURNS TABLE (dt date, cnt_day bigint, cnt_from_period_start bigint, cnt_mtd bigint)
LANGUAGE sql STABLE
-- запоминаем схему при создании, иначе payments будет искаться в search_path того, кто вызывает
SET search_path FROM CURRENT
AS $$
    WITH bounds AS (
        -- даты не передали - берем от первого до последнего платежа
        SELECT
            coalesce(p_date_from, min(prov_dt)) AS date_from,
            coalesce(p_date_to, max(prov_dt)) AS date_to
        FROM payments
    ),
    days AS (
        -- календарь через date + int вместо generate_series, так как генерация по датам вернула бы timestamptz
        -- и результат зависел бы от часового пояса
        SELECT b.date_from + i AS dt
        FROM bounds b, generate_series(0, b.date_to - b.date_from) AS i
    ),
    daily AS (
        SELECT p.prov_dt, count(*) AS cnt
        FROM payments p, bounds b
        WHERE p.prov_dt BETWEEN b.date_from AND b.date_to
        GROUP BY p.prov_dt
    )
    SELECT
        d.dt,
        coalesce(daily.cnt, 0),
        sum(coalesce(daily.cnt, 0)) OVER (ORDER BY d.dt)::bigint, -- sum(bigint) дает numeric, поэтому ::bigint
        sum(coalesce(daily.cnt, 0)) OVER (PARTITION BY date_trunc('month', d.dt) ORDER BY d.dt)::bigint
    FROM days d
    LEFT JOIN daily ON daily.prov_dt = d.dt
    ORDER BY d.dt;
$$;