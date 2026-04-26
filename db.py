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
        # RAW canonical tables
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
            collected_at TIMESTAMPTZ DEFAULT NOW()
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
            collected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volume_5m_сырые(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            collected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)

        # Add collected_at if old tables exist without it
        for table in ["oi_5m_сырые", "price_5m_сырые", "volume_5m_сырые"]:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS collected_at TIMESTAMPTZ DEFAULT NOW()")

        # Deduplicate old raw rows before unique index
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

        # IMPORTANT: rebuild derived tables to guarantee schema correctness
        cur.execute("DROP TABLE IF EXISTS bot_aggregates")
        cur.execute("DROP TABLE IF EXISTS validation_audit")
        cur.execute("DROP TABLE IF EXISTS raw_integrity_report")

        cur.execute("""
        CREATE TABLE bot_aggregates(
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
            unique_candles INTEGER NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE validation_audit(
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
            validation_status TEXT NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE raw_integrity_report(
            calculated_at TIMESTAMPTZ NOT NULL,
            metric TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            unique_candles INTEGER NOT NULL,
            missing_candles INTEGER NOT NULL,
            invalid_timestamps INTEGER NOT NULL,
            integrity_score DOUBLE PRECISION NOT NULL
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS coverage_report(
            calculated_at TIMESTAMPTZ NOT NULL,
            metric TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            first_ts_open TIMESTAMPTZ,
            last_ts_open TIMESTAMPTZ,
            expected_candles INTEGER NOT NULL,
            actual_candles INTEGER NOT NULL,
            missing_candles INTEGER NOT NULL,
            coverage_pct DOUBLE PRECISION NOT NULL,
            missing_pct DOUBLE PRECISION NOT NULL,
            invalid_timestamps INTEGER NOT NULL,
            quality_status TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS gap_report(
            calculated_at TIMESTAMPTZ NOT NULL,
            metric TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            gap_start TIMESTAMPTZ NOT NULL,
            gap_end TIMESTAMPTZ NOT NULL,
            missing_candles INTEGER NOT NULL,
            gap_minutes DOUBLE PRECISION NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS active_symbol_universe(
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            source TEXT NOT NULL DEFAULT 'runtime_limit',
            PRIMARY KEY(exchange, symbol)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_silence(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            stage INTEGER NOT NULL,
            stage_name TEXT NOT NULL,
            score DOUBLE PRECISION NOT NULL,
            reason TEXT NOT NULL,
            oi_delta_pct DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,
            market_state TEXT,
            invalid_reason TEXT
        )
        """)




        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_volume_state(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            volume_state INTEGER NOT NULL,
            volume_state_name TEXT NOT NULL,
            reason TEXT NOT NULL,
            volume_delta_pct DOUBLE PRECISION,
            normalized_volume DOUBLE PRECISION,
            volume_percentile INTEGER,
            noise_state TEXT,
            market_state TEXT,
            invalid_reason TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_price_state(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            price_state INTEGER NOT NULL,
            price_state_name TEXT NOT NULL,
            reason TEXT NOT NULL,
            price_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,
            market_state TEXT,
            invalid_reason TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_oi_slope(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            stage INTEGER NOT NULL,
            stage_name TEXT NOT NULL,
            strength DOUBLE PRECISION NOT NULL,
            raw_strength DOUBLE PRECISION,
            oi_quality TEXT,
            reason TEXT NOT NULL,
            oi_delta_pct DOUBLE PRECISION,
            oi_acceleration DOUBLE PRECISION,
            oi_prev_avg DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,
            silence_stage INTEGER,
            silence_stage_name TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_regime(
            calculated_at TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            market_state TEXT NOT NULL,
            scenario TEXT NOT NULL,
            confidence TEXT NOT NULL,
            reason TEXT NOT NULL,
            oi_delta_pct DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,
            continuation_score DOUBLE PRECISION,
            exhaustion_score DOUBLE PRECISION,
            compression_score DOUBLE PRECISION,
            invalid_reason TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS request_failure_report(
            calculated_at TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            data_type TEXT NOT NULL,
            error_type TEXT NOT NULL,
            error_message TEXT NOT NULL
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_bot_agg_main ON bot_aggregates(metric, timeframe, exchange, symbol, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_validation_main ON validation_audit(metric, timeframe, exchange, symbol, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_oi_main ON oi_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_price_main ON price_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_volume_main ON volume_5m_сырые(exchange, symbol, ts_open)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coverage_report_main ON coverage_report(metric, exchange, symbol, coverage_pct)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gap_report_main ON gap_report(metric, exchange, symbol, gap_start)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_active_symbol_universe_main ON active_symbol_universe(exchange, symbol)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_silence_main ON market_silence(exchange, symbol, timeframe, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_silence_stage ON market_silence(stage, timeframe)")
        cur.execute("ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS normalized_volume DOUBLE PRECISION")
        cur.execute("ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_percentile INTEGER")
        cur.execute("ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS noise_state TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_volume_state_main ON market_volume_state(exchange, symbol, timeframe, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_volume_state_name ON market_volume_state(volume_state_name, timeframe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_price_state_main ON market_price_state(exchange, symbol, timeframe, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_price_state_name ON market_price_state(price_state_name, timeframe)")
        cur.execute("ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS raw_strength DOUBLE PRECISION")
        cur.execute("ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_quality TEXT")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_oi_slope_main ON market_oi_slope(exchange, symbol, timeframe, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_oi_slope_stage ON market_oi_slope(stage, timeframe, strength)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_regime_main ON market_regime(exchange, symbol, timeframe, ts_close)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_regime_scenario ON market_regime(scenario, confidence, timeframe)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_request_failure_report_main ON request_failure_report(exchange, symbol, data_type)")


    log("Postgres: canonical schema + derived tables готовы")

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
            ts_close=EXCLUDED.ts_close,
            oi_open=EXCLUDED.oi_open,
            oi_high=EXCLUDED.oi_high,
            oi_low=EXCLUDED.oi_low,
            oi_close=EXCLUDED.oi_close,
            collected_at=NOW()
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
            ts_close=EXCLUDED.ts_close,
            price_open=EXCLUDED.price_open,
            price_high=EXCLUDED.price_high,
            price_low=EXCLUDED.price_low,
            price_close=EXCLUDED.price_close,
            collected_at=NOW()
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
            ts_close=EXCLUDED.ts_close,
            volume=EXCLUDED.volume,
            collected_at=NOW()
        """, rows)

def replace_bot_aggregates(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE bot_aggregates")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO bot_aggregates(
            metric, timeframe, ts_open, ts_close, exchange, symbol,
            open_value, high_value, low_value, close_value,
            sum_value, avg_value, delta_pct, unique_candles
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)

def replace_validation(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE validation_audit")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO validation_audit(
            calculated_at, metric, timeframe, ts_close, exchange, symbol,
            bot_open, audit_open, bot_close, audit_close,
            bot_delta_pct, audit_delta_pct,
            bot_sum, audit_sum, bot_avg, audit_avg,
            drift, unique_candles, validation_status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)

def replace_integrity(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE raw_integrity_report")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO raw_integrity_report(
            calculated_at, metric, exchange, symbol,
            unique_candles, missing_candles, invalid_timestamps, integrity_score
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)

def replace_coverage(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE coverage_report")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO coverage_report(
            calculated_at,
            metric,
            exchange,
            symbol,
            first_ts_open,
            last_ts_open,
            expected_candles,
            actual_candles,
            missing_candles,
            coverage_pct,
            missing_pct,
            invalid_timestamps,
            quality_status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_gaps(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE gap_report")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO gap_report(
            calculated_at,
            metric,
            exchange,
            symbol,
            gap_start,
            gap_end,
            missing_candles,
            gap_minutes
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_active_universe(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE active_symbol_universe")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO active_symbol_universe(exchange, symbol, source, activated_at)
        VALUES (%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol)
        DO UPDATE SET source=EXCLUDED.source, activated_at=NOW()
        """, rows)


def active_universe_sql(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        "EXISTS ("
        "SELECT 1 FROM active_symbol_universe au "
        f"WHERE au.exchange = {prefix}exchange "
        f"AND au.symbol = {prefix}symbol"
        ")"
    )




def replace_market_silence(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE market_silence")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_silence(
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            stage,
            stage_name,
            score,
            reason,
            oi_delta_pct,
            price_delta_pct,
            volume_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_market_regime(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE market_regime")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_regime(
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            market_state,
            scenario,
            confidence,
            reason,
            oi_delta_pct,
            price_delta_pct,
            volume_delta_pct,
            range_width_pct,
            continuation_score,
            exhaustion_score,
            compression_score,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)




def replace_volume_state(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE market_volume_state")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_volume_state(
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            volume_state,
            volume_state_name,
            reason,
            volume_delta_pct,
            normalized_volume,
            volume_percentile,
            noise_state,
            market_state,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_price_state(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE market_price_state")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_price_state(
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            price_state,
            price_state_name,
            reason,
            price_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_oi_slope(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE market_oi_slope")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_oi_slope(
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            stage,
            stage_name,
            strength,
            raw_strength,
            oi_quality,
            reason,
            oi_delta_pct,
            oi_acceleration,
            oi_prev_avg,
            price_delta_pct,
            volume_delta_pct,
            range_width_pct,
            silence_stage,
            silence_stage_name
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_request_failures(rows: list[tuple]) -> None:
    execute("TRUNCATE TABLE request_failure_report")
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO request_failure_report(
            calculated_at,
            exchange,
            symbol,
            data_type,
            error_type,
            error_message
        ) VALUES (%s,%s,%s,%s,%s,%s)
        """, rows)


def load_quarantine_symbols(min_coverage_pct: float = 95.0) -> set[tuple[str, str]]:
    if not DATABASE_URL:
        return set()

    rows = fetch("""
        SELECT exchange, symbol
        FROM coverage_report
        WHERE coverage_pct < %s
           OR invalid_timestamps > 0
        GROUP BY exchange, symbol
    """, (min_coverage_pct,))

    return {(r["exchange"], r["symbol"]) for r in rows}

def cleanup_old(days: int) -> None:
    for table in ["oi_5m_сырые", "price_5m_сырые", "volume_5m_сырые"]:
        execute(f"DELETE FROM {table} WHERE ts_open < NOW() - (%s || ' days')::interval", (days,))

def migrate_canonical_ts_close() -> None:
    """
    v3.5.1 migration:
    Приводит старые raw-свечи к canonical close:
    ts_close = ts_open + interval '5 minutes'

    Это убирает старые Binance close time вида xx:04:59.999.
    """
    if not DATABASE_URL:
        return

    with _conn() as conn, conn.cursor() as cur:
        for table in ["oi_5m_сырые", "price_5m_сырые", "volume_5m_сырые"]:
            cur.execute(
                f"""
                UPDATE {table}
                SET ts_close = ts_open + interval '5 minutes'
                WHERE ts_close IS DISTINCT FROM ts_open + interval '5 minutes'
                """
            )

    log("Postgres: canonical ts_close migration completed")
