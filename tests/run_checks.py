"""
Проверки загрузчика на тестовых данных

Запуск из корня проекта: python3 tests/run_checks.py

Все делается в отдельной схеме payments_etl_test, основная схема не трогается
Схема создается заново в начале и удаляется в конце
Подключение - через те же PG* переменные, что и у загрузчика
У каждого блока подписано, какое требование ТЗ он проверяет
"""
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
import psycopg2

SCHEMA = "payments_etl_test"
ok_count = 0
fail_count = 0

def query(sql_text):
    # выполнить запрос в тестовой схеме и вернуть первое значение
    conn = psycopg2.connect(options=f"-c search_path={SCHEMA}")
    with conn, conn.cursor() as cur:
        cur.execute(sql_text)
        value = cur.fetchone()[0]
    conn.close()
    return value

def payments_snapshot():
    # все платежи списком - чтобы сравнить "до" и "после"
    conn = psycopg2.connect(options=f"-c search_path={SCHEMA}")
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT payment_id, contract_id, prov_dt, amount, currency, source_seq
            FROM payments
            ORDER BY payment_id
        """)
        rows = cur.fetchall()
    conn.close()
    return rows

def run_loader(data_dir):
    # запустить загрузчик на папке, вернуть код выхода и весь вывод
    env = dict(os.environ, ETL_SCHEMA=SCHEMA)
    result = subprocess.run(
        [sys.executable, "etl/load_payments.py", str(data_dir)],
        env=env, capture_output=True, encoding="utf-8",  # загрузчик пишет в utf-8 и на Windows
    )
    return result.returncode, result.stdout + result.stderr

def check(name, got, expected):
    global ok_count, fail_count
    if got == expected:
        print(f"  [OK]   {name}")
        ok_count += 1
    else:
        print(f"  [FAIL] {name}: ожидали {expected!r}, получили {got!r}")
        fail_count += 1

def file_stats(file_name):
    # статистика файла из реестра в виде "прочитано/отклонено/добавлено/обновлено/без изменений"
    return query(f"""
        SELECT rows_read || '/' || rows_rejected || '/' || rows_inserted || '/' || rows_updated || '/' || rows_unchanged
        FROM load_files
        WHERE file_name = '{file_name}' AND status = 'success'
    """)

def create_schema():
    # схема с нуля: те же sql-файлы, что и для основной схемы
    conn = psycopg2.connect(options=f"-c search_path={SCHEMA}")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        cur.execute(f"CREATE SCHEMA {SCHEMA}")
        cur.execute(Path("sql/create_tables.sql").read_text(encoding="utf-8"))
        cur.execute(Path("sql/payments_running_count.sql").read_text(encoding="utf-8"))
    conn.close()

def drop_schema():
    conn = psycopg2.connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.close()

def main():
    os.chdir(Path(__file__).parent.parent)  # корень проекта
    tmp = Path(tempfile.mkdtemp())  # сюда кладу испорченные копии файлов
    create_schema()
    print(f"Тестовая схема: {SCHEMA}")

    # ТЗ: загрузка CSV в PostgreSQL с проверками, статистика, ошибка строки не останавливает файл
    print("\n== 1. Первая загрузка трех файлов")
    code, output = run_loader("data")
    check("загрузчик завершился без ошибки", code, 0)
    check("payments_001: прочитано 29 / отклонено 21 / добавлено 8", file_stats("payments_001.csv"), "29/21/8/0/0")
    check("payments_002: 8 / 1 / добавлено 4 / обновлено 2 / без изменений 1", file_stats("payments_002.csv"), "8/1/4/2/1")
    check("payments_003 (BOM, CRLF как из Excel): 4 / 0 / 3 / 1 / 0", file_stats("payments_003.csv"), "4/0/3/1/0")
    check("в payments 15 платежей", query("SELECT count(*) FROM payments"), 15)
    check("в payments_rejected 22 строки", query("SELECT count(*) FROM payments_rejected"), 22)
    check("в 001 плохие строки отклонены, а хорошие загружены - файл не остановился",
        query("SELECT rows_rejected > 0 AND rows_inserted > 0 FROM load_files WHERE file_name = 'payments_001.csv'"), True)

    # ТЗ: отклоненные записи сохраняются с указанием причин
    print("\n== 2. Причины отклонения")
    check("у каждой отклоненной строки есть причина",
        query("SELECT count(*) FROM payments_rejected WHERE cardinality(reasons) = 0"), 0)
    check("P014: две ошибки - две причины",
        query("SELECT reasons FROM payments_rejected WHERE payment_id = 'P014'"), ["INVALID_AMOUNT", "INVALID_CURRENCY"])
    check("P010: 31 сентября - такой даты нет",
        query("SELECT reasons FROM payments_rejected WHERE payment_id = 'P010'"), ["INVALID_DATE"])
    check("P023: две разные версии в одном файле - отклонены обе",
        query("SELECT count(*) FROM payments_rejected WHERE payment_id = 'P023'"), 2)

    # ТЗ: корректная запись из последующего файла обновляет ранее загруженный платеж
    print("\n== 3. Исправления из следующих файлов")
    check("P005: сумма исправлена файлом 002 (1200 -> 1250)",
        str(query("SELECT amount FROM payments WHERE payment_id = 'P005'")), "1250.00")
    check("P007: ошибочная правка отклонена, осталось 500",
        str(query("SELECT amount FROM payments WHERE payment_id = 'P007'")), "500.00")
    check("P022: после конфликта верная версия пришла в 003",
        str(query("SELECT amount FROM payments WHERE payment_id = 'P022'")), "110.00")

    # ТЗ: повторная обработка файла не создает дублей и не меняет итоговые данные
    print("\n== 4. Повторный запуск (идемпотентность)")
    before = payments_snapshot()
    code, output = run_loader("data")
    check("загрузчик завершился без ошибки", code, 0)
    check("все три файла пропущены", output.count("skipped, файл уже загружен"), 3)
    check("платежи не изменились", payments_snapshot(), before)
    check("отклоненные строки не задублировались", query("SELECT count(*) FROM payments_rejected"), 22)

    # ТЗ: SQL, который для каждой даты выводит количество платежей с начала периода
    print("\n== 5. Running count")
    check("итог на последнюю дату = 15 платежей",
        query("SELECT max(cnt_from_period_start) FROM payments_running_count()"), 15)
    check("в день без платежей 0, а не пропуск",
        query("SELECT cnt_day FROM payments_running_count() WHERE dt = '2026-09-05'"), 0)

    # сверх ТЗ: принятый файл нельзя подменить, исправления только новым файлом
    print("\n== 6. Уже загруженный файл пришел с другим содержимым")
    changed = tmp / "changed"
    shutil.copytree("data", changed)
    text = (changed / "payments_001.csv").read_text(encoding="utf-8")
    (changed / "payments_001.csv").write_text(text.replace("200.50", "999.00"), encoding="utf-8")
    before = payments_snapshot()
    code, output = run_loader(changed)
    check("загрузчик остановился с ошибкой", code, 1)
    check("причина FILE_CONTENT_CHANGED", "FILE_CONTENT_CHANGED" in output, True)
    check("платежи не изменились", payments_snapshot(), before)

    # сверх ТЗ: файлы идут строго по порядку, следующий ждет, пока не исправят упавший
    print("\n== 7. Битый файл останавливает очередь, исправленный грузится")
    broken = tmp / "broken"
    shutil.copytree("data", broken)
    (broken / "payments_004.csv").write_text(
        "payment_id;contract_id;prov_dt;amount;currency\nP040;C001;2026-09-26;10.00;KGS\n")
    (broken / "payments_005.csv").write_text(
        "payment_id,contract_id,prov_dt,amount,currency\nP041,C001,2026-09-27,20.00,KGS\n")
    code, output = run_loader(broken)
    check("загрузчик остановился с ошибкой", code, 1)
    check("004 упал с FILE_HEADER_MISMATCH",
        query("SELECT error_code FROM load_files WHERE file_name = 'payments_004.csv'"), "FILE_HEADER_MISMATCH")
    check("005 не грузился, потому что идет после упавшего",
        query("SELECT count(*) FROM load_files WHERE file_name = 'payments_005.csv'"), 0)
    (broken / "payments_004.csv").write_text(
        "payment_id,contract_id,prov_dt,amount,currency\nP040,C001,2026-09-26,10.00,KGS\n")
    code, output = run_loader(broken)
    check("после исправления 004 все загрузилось", code, 0)
    check("P040 и P041 в платежах",
        query("SELECT count(*) FROM payments WHERE payment_id IN ('P040', 'P041')"), 2)

    drop_schema()
    shutil.rmtree(tmp)

    print("\n========================================")
    if fail_count == 0:
        print(f"Все проверки пройдены: {ok_count}")
        return 0
    print(f"Не прошло: {fail_count} из {ok_count + fail_count}")
    return 1

if __name__ == "__main__":
    sys.exit(main())