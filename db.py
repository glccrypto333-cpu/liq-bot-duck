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

    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            oi_open DOUBLE PRECISION NOT NULL,
            oi_high DOUBLE PRECISION NOT NULL,
            oi_low DOUBLE PRECISION NOT NULL,
            oi_close DOUBLE PRECISION NOT NULL,
            collected_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY(exchange, symbol, ts_open)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS price_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price_open DOUBLE PRECISION NOT NULL,
            price_high DOUBLE PRECISION NOT NULL,
            price_low DOUBLE PRECISION NOT NULL,
            price_close DOUBLE PRECISION NOT NULL,
            collected_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY(exchange, symbol, ts_open)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volume_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            collected_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY(exchange, symbol, ts_open)
        )
        """)

        # Миграция старых таблиц без primary key: удалить дубли и добавить уникальность.
        for table in ["oi_5m_сырые", "price_5m_сырые", "volume_5m_сырые"]:
            cur.execute(f"""
            DELETE FROM {table} a
            USING {table} b
            WHERE a.ctid < b.ctid
              AND a.exchange = b.exchange
              AND a.symbol = b.symbol
              AND a.ts_open = b.ts_open
            """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_oi5m_candle ON oi_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_price5m_candle ON price_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_volume5m_candle ON volume_5m_сырые(exchange, symbol, ts_open)")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_aggregates(
            metric TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            open_value DOUBLE PRECISION,
            high_value DOUBLE PRECISION,
            low_value DOUBLE PRECISION,
            close_value DOUBLE PRECISION,
            sum_value DOUBLE PRECISION,
            avg_value DOUBLE PRECISION,
            delta_pct DOUBLE PRECISION,
            unique_candles INTEGER NOT NULL,
            PRIMARY KEY(metric, timeframe, exchange, symbol, ts_close)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS validation_audit(
            calculated_at TIMESTAMPTZ NOT NULL,
            metric TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            bot_open DOUBLE PRECISION,
            audit_open DOUBLE PRECISION,
            bot_close DOUBLE PRECISION,
            audit_close DOUBLE PRECISION,
            bot_delta_pct DOUBLE PRECISION,
            audit_delta_pct DOUBLE PRECISION,
            bot_sum DOUBLE PRECISION,
            audit_sum DOUBLE PRECISION,
            bot_avg DOUBLE PRECISION,
            audit_avg DOUBLE PRECISION,
            drift DOUBLE PRECISION,
            unique_candles INTEGER NOT NULL,
            validation_status TEXT NOT NULL,
            PRIMARY KEY(metric, timeframe, exchange, symbol, ts_close)
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_integrity_report(
            calculated_at TIMESTAMPTZ NOT NULL,
            metric TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            unique_candles INTEGER NOT NULL,
            missing_candles INTEGER NOT NULL,
            invalid_timestamps INTEGER NOT NULL,
            integrity_score DOUBLE PRECISION NOT NULL,
            PRIMARY KEY(metric, exchange, symbol)
        )
        """)
    log("Postgres: подключение успешно, canonical tables готовы")

def execute(sql: str, params: tuple = ()) -> None:
    if not DATABASE_URL:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)

def fetch(sql: str, params: tuple = ()) -> list[dict]:
    if not DATABASE_URL:
        return []
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return list(cur.fetchall())

def upsert_oi(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO oi_5m_сырые(ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close, collected_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close = EXCLUDED.ts_close,
            oi_open = EXCLUDED.oi_open,
            oi_high = EXCLUDED.oi_high,
            oi_low = EXCLUDED.oi_low,
            oi_close = EXCLUDED.oi_close,
            collected_at = NOW()
        """, rows)

def upsert_price(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO price_5m_сырые(ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close, collected_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close = EXCLUDED.ts_close,
            price_open = EXCLUDED.price_open,
            price_high = EXCLUDED.price_high,
            price_low = EXCLUDED.price_low,
            price_close = EXCLUDED.price_close,
            collected_at = NOW()
        """, rows)

def upsert_volume(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO volume_5m_сырые(ts_open, ts_close, exchange, symbol, volume, collected_at)
        VALUES (%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close = EXCLUDED.ts_close,
            volume = EXCLUDED.volume,
            collected_at = NOW()
        """, rows)

def upsert_bot_aggregates(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO bot_aggregates(metric, timeframe, ts_open, ts_close, exchange, symbol, open_value, high_value, low_value, close_value, sum_value, avg_value, delta_pct, unique_candles)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (metric, timeframe, exchange, symbol, ts_close)
        DO UPDATE SET
            ts_open=EXCLUDED.ts_open,
            open_value=EXCLUDED.open_value,
            high_value=EXCLUDED.high_value,
            low_value=EXCLUDED.low_value,
            close_value=EXCLUDED.close_value,
            sum_value=EXCLUDED.sum_value,
            avg_value=EXCLUDED.avg_value,
            delta_pct=EXCLUDED.delta_pct,
            unique_candles=EXCLUDED.unique_candles
        """, rows)

def replace_validation(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE validation_audit")
    if rows:
        with _conn() as conn, conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO validation_audit(
                calculated_at, metric, timeframe, ts_close, exchange, symbol,
                bot_open, audit_open, bot_close, audit_close,
                bot_delta_pct, audit_delta_pct, bot_sum, audit_sum, bot_avg, audit_avg,
                drift, unique_candles, validation_status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)

def replace_integrity(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE raw_integrity_report")
    if rows:
        with _conn() as conn, conn.cursor() as cur:
            cur.executemany("""
            INSERT INTO raw_integrity_report(calculated_at, metric, exchange, symbol, unique_candles, missing_candles, invalid_timestamps, integrity_score)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)

def cleanup_old(days: int) -> None:
    for table in ["oi_5m_сырые", "price_5m_сырые", "volume_5m_сырые", "bot_aggregates", "validation_audit"]:
        col = "ts_open" if table != "validation_audit" else "ts_close"
        execute(f"DELETE FROM {table} WHERE {col} < NOW() - (%s || ' days')::interval", (days,))
