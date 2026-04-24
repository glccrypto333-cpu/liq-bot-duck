from __future__ import annotations
import psycopg
from psycopg.rows import dict_row
from config import DATABASE_URL
from logger import log

def _conn():
    return psycopg.connect(DATABASE_URL, autocommit=True, row_factory=dict_row)

def init_db() -> None:
    if not DATABASE_URL:
        log("DATABASE_URL не задан, пропускаю init_db")
        return

    ddl = [
        """
        CREATE TABLE IF NOT EXISTS oi_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            oi_open DOUBLE PRECISION NOT NULL,
            oi_high DOUBLE PRECISION NOT NULL,
            oi_low DOUBLE PRECISION NOT NULL,
            oi_close DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price_open DOUBLE PRECISION NOT NULL,
            price_high DOUBLE PRECISION NOT NULL,
            price_low DOUBLE PRECISION NOT NULL,
            price_close DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS volume_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            volume DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS oi_агрегаты(
            окно TEXT NOT NULL,
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            oi_open DOUBLE PRECISION NOT NULL,
            oi_high DOUBLE PRECISION NOT NULL,
            oi_low DOUBLE PRECISION NOT NULL,
            oi_close DOUBLE PRECISION NOT NULL,
            oi_изменение_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS price_агрегаты(
            окно TEXT NOT NULL,
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price_open DOUBLE PRECISION NOT NULL,
            price_high DOUBLE PRECISION NOT NULL,
            price_low DOUBLE PRECISION NOT NULL,
            price_close DOUBLE PRECISION NOT NULL,
            price_изменение_pct DOUBLE PRECISION
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS volume_агрегаты(
            окно TEXT NOT NULL,
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            volume_sum DOUBLE PRECISION NOT NULL,
            volume_avg DOUBLE PRECISION NOT NULL,
            volume_max DOUBLE PRECISION NOT NULL,
            volume_min DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS oi_сверка(
            calculated_at TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            источник_основной TEXT,
            источник_подтверждения TEXT,
            тип_состояния TEXT,
            наклон_15м DOUBLE PRECISION,
            наклон_30м DOUBLE PRECISION,
            наклон_1ч DOUBLE PRECISION,
            наклон_4ч DOUBLE PRECISION,
            согласованность_15м_к_4ч DOUBLE PRECISION,
            согласованность_30м_к_4ч DOUBLE PRECISION,
            согласованность_1ч_к_4ч DOUBLE PRECISION,
            расхождение_bybit_binance_15м DOUBLE PRECISION,
            расхождение_bybit_binance_30м DOUBLE PRECISION,
            расхождение_bybit_binance_1ч DOUBLE PRECISION,
            расхождение_bybit_binance_4ч DOUBLE PRECISION,
            шум_api DOUBLE PRECISION,
            потери_точек DOUBLE PRECISION,
            оценка_качества DOUBLE PRECISION,
            класс_надёжности TEXT,
            причина_оценки TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS raw_integrity_report(
            calculated_at TIMESTAMPTZ NOT NULL,
            metric TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            duplicates_found INTEGER NOT NULL,
            missing_candles INTEGER NOT NULL,
            invalid_timestamps INTEGER NOT NULL,
            empty_rows INTEGER NOT NULL,
            integrity_score DOUBLE PRECISION NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS аудит_ои(
            calculated_at TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bot_oi_open DOUBLE PRECISION,
            audit_oi_open DOUBLE PRECISION,
            bot_oi_close DOUBLE PRECISION,
            audit_oi_close DOUBLE PRECISION,
            bot_oi_delta_pct DOUBLE PRECISION,
            audit_oi_delta_pct DOUBLE PRECISION,
            drift_oi_delta_pct DOUBLE PRECISION,
            unique_candles INTEGER NOT NULL,
            validation_status TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS аудит_цены(
            calculated_at TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bot_price_open DOUBLE PRECISION,
            audit_price_open DOUBLE PRECISION,
            bot_price_close DOUBLE PRECISION,
            audit_price_close DOUBLE PRECISION,
            bot_price_delta_pct DOUBLE PRECISION,
            audit_price_delta_pct DOUBLE PRECISION,
            drift_price_delta_pct DOUBLE PRECISION,
            unique_candles INTEGER NOT NULL,
            validation_status TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS аудит_объёма(
            calculated_at TIMESTAMPTZ NOT NULL,
            symbol TEXT NOT NULL,
            exchange TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bot_volume_sum DOUBLE PRECISION,
            audit_volume_sum DOUBLE PRECISION,
            bot_volume_avg DOUBLE PRECISION,
            audit_volume_avg DOUBLE PRECISION,
            drift_volume_pct DOUBLE PRECISION,
            unique_candles INTEGER NOT NULL,
            validation_status TEXT NOT NULL
        )
        """
    ]

    with _conn() as conn, conn.cursor() as cur:
        for sql in ddl:
            cur.execute(sql)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_oi5m ON oi_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_price5m ON price_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volume5m ON volume_5m_сырые(exchange, symbol, ts_open)")
    log("Postgres: подключение успешно, таблицы готовы")

def execute_sql(sql: str, params: tuple = ()) -> None:
    if not DATABASE_URL:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)

def bulk_insert(table: str, rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return

    mapping = {
        "oi_5m_сырые": ("(ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close)", "(%s,%s,%s,%s,%s,%s,%s,%s)"),
        "price_5m_сырые": ("(ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close)", "(%s,%s,%s,%s,%s,%s,%s,%s)"),
        "volume_5m_сырые": ("(ts_open, ts_close, exchange, symbol, volume)", "(%s,%s,%s,%s,%s)"),
        "oi_агрегаты": ("(окно, ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close, oi_изменение_pct)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "price_агрегаты": ("(окно, ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close, price_изменение_pct)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "volume_агрегаты": ("(окно, ts_open, ts_close, exchange, symbol, volume_sum, volume_avg, volume_max, volume_min)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "oi_сверка": ("(calculated_at, exchange, symbol, источник_основной, источник_подтверждения, тип_состояния, наклон_15м, наклон_30м, наклон_1ч, наклон_4ч, согласованность_15м_к_4ч, согласованность_30м_к_4ч, согласованность_1ч_к_4ч, расхождение_bybit_binance_15м, расхождение_bybit_binance_30м, расхождение_bybit_binance_1ч, расхождение_bybit_binance_4ч, шум_api, потери_точек, оценка_качества, класс_надёжности, причина_оценки)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "raw_integrity_report": ("(calculated_at, metric, exchange, symbol, duplicates_found, missing_candles, invalid_timestamps, empty_rows, integrity_score)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "аудит_ои": ("(calculated_at, symbol, exchange, timeframe, bot_oi_open, audit_oi_open, bot_oi_close, audit_oi_close, bot_oi_delta_pct, audit_oi_delta_pct, drift_oi_delta_pct, unique_candles, validation_status)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "аудит_цены": ("(calculated_at, symbol, exchange, timeframe, bot_price_open, audit_price_open, bot_price_close, audit_price_close, bot_price_delta_pct, audit_price_delta_pct, drift_price_delta_pct, unique_candles, validation_status)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
        "аудит_объёма": ("(calculated_at, symbol, exchange, timeframe, bot_volume_sum, audit_volume_sum, bot_volume_avg, audit_volume_avg, drift_volume_pct, unique_candles, validation_status)", "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"),
    }
    cols, vals = mapping[table]
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany(f"INSERT INTO {table} {cols} VALUES {vals}", rows)

def replace_table(table: str, rows: list[tuple]) -> None:
    if not DATABASE_URL:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table}")
    if rows:
        bulk_insert(table, rows)

def fetch_rows(sql: str, params: tuple = ()) -> list[dict]:
    if not DATABASE_URL:
        return []
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())

def cleanup_old_data(days: int) -> None:
    if not DATABASE_URL:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM oi_5m_сырые WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))
        cur.execute("DELETE FROM price_5m_сырые WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))
        cur.execute("DELETE FROM volume_5m_сырые WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))
        cur.execute("DELETE FROM oi_агрегаты WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))
        cur.execute("DELETE FROM price_агрегаты WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))
        cur.execute("DELETE FROM volume_агрегаты WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))
