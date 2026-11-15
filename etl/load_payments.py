"""
Загрузка платежей из CSV в PostgreSQL

Запуск: python3 etl/load_payments.py data
Подключение и настройки - через переменные окружения (PG*, ETL_SCHEMA, ETL_TIMEZONE), см. README.

Проверки качества данных на двух уровнях:
- файл целиком - здесь, в python: кодировка, заголовок, пустой файл, порядок и повтор файлов,
  справочник договоров. Если не прошло - файл не грузим.
- каждая строка платежа - в sql/validate_payments.sql. Плохие строки уходят в payments_rejected
  с причинами, остальные грузит sql/apply_payments.sql.
В конце по каждому файлу сверка: прочитано = добавлено + обновлено + без изменений + отклонено.
"""
import csv
import hashlib
import io
import json
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
import psycopg2

SQL_DIR = Path(__file__).parent.parent / "sql"
PAYMENT_COLUMNS = ["payment_id", "contract_id", "prov_dt", "amount", "currency"]
CONTRACT_COLUMNS = ["contract_id", "customer_id", "open_dt"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("payments_etl")

class FileError(Exception):
    
    # ошибка на уровне всего файла: файл не грузим и дальше по очереди не идем
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code

def read_csv(path, expected_header):
    
    #возвращает список (номер строки, поля) и сколько пустых строк пропустили
    #номер строки считается как в редакторе, заголовок = 1 строка
    try:
        text = path.read_bytes().decode("utf-8-sig")
    except UnicodeDecodeError:
        raise FileError("FILE_ENCODING", "файл не в UTF-8")

    reader = csv.reader(io.StringIO(text, newline=""))
    header = next(reader, None)
    if header is None or [h.strip() for h in header] != expected_header:
        raise FileError("FILE_HEADER_MISMATCH", f"ожидался заголовок {expected_header}, пришел {header}")

    rows = []
    blank = 0
    for fields in reader:
        # пустую строку пропускаю, это лишний перенос в выгрузке
        # а ',,,,' - запись с пустыми полями, ее проверяем как обычно
        if not fields or (len(fields) == 1 and fields[0].strip() == ""):
            blank += 1
            continue
        rows.append((reader.line_num, fields))

    if not rows:
        raise FileError("FILE_EMPTY", "в файле нет строк с данными")
    return rows, blank


def find_payment_files(data_dir):
    # берем только payments_NNN.csv и сортируем по номеру
    files = []
    for path in data_dir.glob("*.csv"):
        if path.name == "contracts.csv":
            continue
        m = re.fullmatch(r"payments_(\d{3})\.csv", path.name)
        if m:
            files.append((int(m.group(1)), path))
        else:
            log.warning("%s: имя не подходит под payments_NNN.csv, пропускаю", path.name)
    files.sort()
    return files


def load_contracts(conn, path):
    # справочник грузим целиком. если в нем ошибка - останавливаемся,
    #без нормального справочника проверять платежи смысла нет
    rows, _ = read_csv(path, CONTRACT_COLUMNS)
    errors = []
    contracts = {}

    for line, fields in rows:
        if len(fields) != 3:
            errors.append(f"строка {line}: нужно 3 поля, а их {len(fields)}")
            continue
        contract_id, customer_id, open_dt = [f.strip() for f in fields]
        if contract_id == "" or customer_id == "" or open_dt == "":
            errors.append(f"строка {line}: пустое поле")
            continue
        if contract_id in contracts:
            errors.append(f"строка {line}: contract_id {contract_id} повторяется")
            continue
        try:
            datetime.strptime(open_dt, "%Y-%m-%d")
        except ValueError:
            errors.append(f"строка {line}: плохая дата {open_dt}")
            continue
        contracts[contract_id] = (contract_id, customer_id, open_dt)

    if errors:
        raise FileError("CONTRACTS_INVALID", "; ".join(errors))

    with conn, conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO contracts (contract_id, customer_id, open_dt)
            VALUES (%s, %s, %s)
            ON CONFLICT (contract_id) DO UPDATE
            SET customer_id = EXCLUDED.customer_id, open_dt = EXCLUDED.open_dt
        """, list(contracts.values()))
    log.info("contracts.csv: договоров %d", len(contracts))


def should_load(conn, name, seq, file_hash):
    # True - файл грузим, False - пропускаем, FileError - нарушен порядок файлов
    with conn, conn.cursor() as cur:
        # 1. этот же файл (по содержимому) уже загружен -> пропускаем
        cur.execute("SELECT file_name FROM load_files WHERE file_hash = %s AND status = 'success'",
            (file_hash,))
        row = cur.fetchone()
        if row:
            if row[0] != name:
                log.warning("%s: такой же файл уже загружен под именем %s", name, row[0])
            log.info("%s: skipped, файл уже загружен (sha256 совпадает)", name)
            return False

        # 2. имя то же, а содержимое другое -> ошибка, исправления должны идти новым файлом
        cur.execute("SELECT 1 FROM load_files WHERE file_name = %s AND status = 'success'", (name,))
        if cur.fetchone():
            raise FileError("FILE_CONTENT_CHANGED",
                "файл с таким именем уже загружен, но содержимое другое. "
                "исправления должны приходить новым файлом со следующим номером")

        # 3. номер меньше уже загруженного -> ошибка, иначе старые данные затрут новые
        cur.execute("SELECT max(file_seq) FROM load_files WHERE status = 'success'")
        max_seq = cur.fetchone()[0]
        if max_seq is not None and seq < max_seq:
            raise FileError("FILE_OUT_OF_ORDER", f"номер файла {seq} меньше уже загруженного {max_seq}")
    return True


def register_file(conn, name, seq, file_hash):
    # пишем файл в реестр со статусом processing и возвращаем file_id.
    # если этот файл уже был (упал в прошлый раз) - берем его старую строку
    with conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO load_files (file_name, file_seq, file_hash, status)
            VALUES (%s, %s, %s, 'processing')
            ON CONFLICT (file_hash) DO UPDATE
            SET file_name = EXCLUDED.file_name,
                file_seq = EXCLUDED.file_seq,
                status = 'processing',
                error_code = NULL,
                error_message = NULL,
                started_at = now(),
                finished_at = NULL
            RETURNING file_id
        """, (name, seq, file_hash))
        return cur.fetchone()[0]


def mark_failed(conn, file_id, code, message):
    conn.rollback()
    with conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE load_files
            SET status = 'failed', error_code = %s, error_message = %s, finished_at = now()
            WHERE file_id = %s
        """, (code, message[:2000], file_id))


def copy_to_staging(cur, rows):
    # кладу строки файла во временную таблицу через COPY.
    # если полей не 5, по колонкам раскладывать нельзя (поля съехали),
    # такая строка лежит только в raw_values
    cur.execute("""
        CREATE TEMP TABLE stg_payments (
            row_num int PRIMARY KEY,
            n_fields int NOT NULL,
            raw_values jsonb NOT NULL,
            payment_id text,
            contract_id text,
            prov_dt_raw text,
            amount_raw text,
            currency text
        ) ON COMMIT DROP
    """)
    buf = io.StringIO()
    writer = csv.writer(buf)
    for line, fields in rows:
        if len(fields) == 5:
            values = fields
        else:
            values = [None, None, None, None, None]
        writer.writerow([line, len(fields), json.dumps(fields, ensure_ascii=False)] + values)
    buf.seek(0)
    cur.copy_expert("COPY stg_payments FROM STDIN WITH (FORMAT csv)", buf)
    # temp-таблицы autovacuum не анализирует, без этого на большом файле план может быть плохой
    cur.execute("ANALYZE stg_payments")


def process_file(conn, path, seq):
    # грузит один файл платежей и возвращает статистику (или None, если файл пропущен)
    file_hash = hashlib.sha256(path.read_bytes()).hexdigest()

    try:
        if not should_load(conn, path.name, seq, file_hash):
            return None
    except FileError as e:
        file_id = register_file(conn, path.name, seq, file_hash)
        mark_failed(conn, file_id, e.code, str(e))
        raise

    file_id = register_file(conn, path.name, seq, file_hash)
    try:
        rows, blank = read_csv(path, PAYMENT_COLUMNS)

        # весь файл - одна транзакция: либо записалось все, либо ничего
        with conn, conn.cursor() as cur:
            copy_to_staging(cur, rows)

            sql_text = (SQL_DIR / "validate_payments.sql").read_text(encoding="utf-8")
            cur.execute(sql_text, {"file_seq": seq})

            # сколько строк куда решили отправить: insert / update / unchanged / reject / stale
            cur.execute("SELECT action, count(*) FROM stg_checked GROUP BY action")
            plan = dict(cur.fetchall())
            if plan.get("stale", 0) > 0:
                raise FileError("VERSION_CONFLICT",
                    f"платежей с версией из более нового файла: {plan['stale']}")

            sql_text = (SQL_DIR / "apply_payments.sql").read_text(encoding="utf-8")
            cur.execute(sql_text, {"file_id": file_id, "file_seq": seq})
            inserted, updated, rejected = cur.fetchone()

            stats = {
                "read": len(rows),
                "rejected": rejected,
                "inserted": inserted,
                "updated": updated,
                "unchanged": plan.get("unchanged", 0),
            }

            # сверка 1: записали ровно столько, сколько решили
            if (inserted, updated, rejected) != (plan.get("insert", 0), plan.get("update", 0), plan.get("reject", 0)):
                raise FileError("RECONCILIATION_FAILED", f"записано не то, что планировали: {plan}")
            # сверка 2: каждая прочитанная строка куда-то попала
            total = inserted + updated + stats["unchanged"] + rejected
            if total != stats["read"]:
                raise FileError("RECONCILIATION_FAILED", f"прочитано {stats['read']}, а разложено {total}")

            cur.execute("""
                UPDATE load_files
                SET status = 'success', finished_at = now(),
                    rows_read = %s, rows_rejected = %s, rows_inserted = %s,
                    rows_updated = %s, rows_unchanged = %s
                WHERE file_id = %s
            """, (stats["read"], rejected, inserted, updated, stats["unchanged"], file_id))

    except FileError as e:
        mark_failed(conn, file_id, e.code, str(e))
        raise
    except Exception as e:
        # что-то непредвиденное (например, упала БД) - транзакция уже откатилась
        mark_failed(conn, file_id, "UNEXPECTED_ERROR", f"{type(e).__name__}: {e}")
        raise FileError("UNEXPECTED_ERROR", str(e))

    if blank:
        log.info("%s: пропущено пустых строк: %d", path.name, blank)
    return stats


def print_summary(results):
    line = "{:<18} {:<9} | прочитано {:>4} | отклонено {:>4} | добавлено {:>4} | обновлено {:>4} | без изменений {:>4}"
    keys = ["read", "rejected", "inserted", "updated", "unchanged"]
    total = {k: 0 for k in keys}
    print()
    for name, status, stats in results:
        if stats is None:
            print(f"{name:<18} {status}")
            continue
        print(line.format(name, status, *[stats[k] for k in keys]))
        for k in keys:
            total[k] += stats[k]
    print(line.format("ИТОГО", "", *[total[k] for k in keys]))


def main():
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    schema = os.environ.get("ETL_SCHEMA", "payments_etl")
    timezone = os.environ.get("ETL_TIMEZONE", "Asia/Bishkek")

    # хост, пользователь и пароль берутся из PG* переменных, здесь только схема и часовой пояс.
    # от часового пояса зависит current_date в проверке "дата в будущем"
    conn = psycopg2.connect(options=f"-c search_path={schema} -c timezone={timezone}")
    try:
        # лок на всю сессию, чтобы два запуска не грузили одновременно.
        # снимется сам, если процесс упадет. поэтому processing на старте = прошлый запуск упал
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (f"payments_etl:{schema}",))
            got_lock = cur.fetchone()[0]
        conn.commit()
        if not got_lock:
            log.error("уже идет другой запуск загрузки, выхожу")
            return 2

        load_contracts(conn, data_dir / "contracts.csv")

        results = []
        for seq, path in find_payment_files(data_dir):
            try:
                stats = process_file(conn, path, seq)
            except FileError as e:
                log.error("%s: %s - %s. Дальше файлы не грузим: они могут исправлять платежи из этого",
                    path.name, e.code, e)
                results.append((path.name, "failed", None))
                print_summary(results)
                return 1
            if stats is None:
                results.append((path.name, "skipped", None))
            else:
                results.append((path.name, "processed", stats))

        print_summary(results)
        return 0
    except FileError as e:
        log.error("%s - %s", e.code, e)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())