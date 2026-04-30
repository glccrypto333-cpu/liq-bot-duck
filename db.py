from __future__ import annotations
import psycopg
from psycopg.rows import dict_row
from config import DATABASE_URL, RAW_RETENTION_DAYS
import os
import time

DB_STATEMENT_TIMEOUT_MS = int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "60000"))
from logger import log


def _runtime_ddl_enabled() -> bool:
    return os.getenv("RUN_DDL_MIGRATIONS") == "1"

def _executemany_with_lock_retry(cur, sql: str, rows: list[tuple], batch_size: int = 100) -> None:
    if not rows:
        return

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]

        for attempt in range(3):
            try:
                cur.executemany(sql, batch)
                break
            except Exception as exc:
                if "LockNotAvailable" not in type(exc).__name__ and "lock timeout" not in str(exc).lower():
                    raise
                if attempt == 2:
                    raise
                log(f"lock retry: batch={len(batch)} attempt={attempt + 1}")
                time.sleep(5 * (attempt + 1))


def safe_ddl(cur, sql: str) -> None:
    try:
        cur.execute(sql)
    except psycopg.errors.LockNotAvailable as exc:
        log(f"DDL skipped due lock timeout: {sql[:120]} | {exc}")

_DB_CONN = None

def _apply_session_settings(conn):
    try:
        conn.execute("SET statement_timeout = 0")
        conn.execute("SET idle_in_transaction_session_timeout = 0")
        conn.execute("SET lock_timeout = '30s'")
    except Exception:
        pass


def _conn():
    global _DB_CONN

    if _DB_CONN is not None and not _DB_CONN.closed:
        return _DB_CONN

    _DB_CONN = psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=5
        )
    _apply_session_settings(_DB_CONN)
    return _DB_CONN

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
            log(f"DDL deferred: collected_at migration skipped for {table}")

        run_runtime_ddl = _runtime_ddl_enabled()

        if run_runtime_ddl:
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
        else:
            log("DDL deferred: raw dedupe + unique indexes skipped")

        # IMPORTANT: rebuild derived tables to guarantee schema correctness
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
            unique_candles INTEGER NOT NULL
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
            validation_status TEXT NOT NULL
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
            volume_structure TEXT,
            volume_quality TEXT,
            volume_baseline_24h DOUBLE PRECISION,
            volume_hold_state TEXT,
            volume_reason TEXT,
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
            price_structure TEXT,
            price_quality TEXT,
            price_slope_state TEXT,
            price_trend_24h TEXT,
            price_range_from_median_pct DOUBLE PRECISION,
            price_reason TEXT,
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
            oi_structure TEXT,
            oi_priority INTEGER,
            oi_hold_state TEXT,
            oi_trend_15m TEXT,
            oi_trend_30m TEXT,
            oi_trend_1h TEXT,
            oi_trend_4h TEXT,
            oi_trend_24h TEXT,
            oi_reason TEXT,
            reason TEXT NOT NULL,
            oi_delta_pct DOUBLE PRECISION,
            oi_acceleration DOUBLE PRECISION,
            oi_prev_avg DOUBLE PRECISION,
            price_delta_pct DOUBLE PRECISION,
            volume_delta_pct DOUBLE PRECISION,
            range_width_pct DOUBLE PRECISION,
            silence_stage INTEGER
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_phase(
            calculated_at TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            phase INTEGER NOT NULL,
            phase_name TEXT NOT NULL,
            phase_status TEXT NOT NULL,
            priority TEXT,
            phase_started_at TIMESTAMPTZ,
            phase_updated_at TIMESTAMPTZ,
            stage1_started_at TIMESTAMPTZ,
            stage2_started_at TIMESTAMPTZ,
            stage3_started_at TIMESTAMPTZ,
            manual_reset_required BOOLEAN DEFAULT FALSE,
            confidence TEXT,
            oi_structure TEXT,
            oi_priority INTEGER,
            oi_hold_state TEXT,
            oi_trend_15m TEXT,
            oi_trend_30m TEXT,
            oi_trend_1h TEXT,
            oi_trend_4h TEXT,
            oi_trend_24h TEXT,
            price_structure TEXT,
            price_quality TEXT,
            price_slope_state TEXT,
            volume_structure TEXT,
            volume_quality TEXT,
            volume_hold_state TEXT,
            transition_reason TEXT,
            reason TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS market_phase_history(
            calculated_at TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            from_phase INTEGER,
            to_phase INTEGER NOT NULL,
            from_phase_name TEXT,
            to_phase_name TEXT NOT NULL,
            phase_status TEXT,
            priority TEXT,
            transition_reason TEXT NOT NULL,
            oi_structure TEXT,
            oi_priority INTEGER,
            oi_hold_state TEXT,
            price_structure TEXT,
            price_quality TEXT,
            volume_structure TEXT,
            volume_quality TEXT
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

        run_runtime_ddl = _runtime_ddl_enabled()
        log(f"DDL migrations enabled: {run_runtime_ddl}")

        if not run_runtime_ddl:
            log("DDL runtime migrations skipped")
            return

        log("DDL deferred: idx_bot_agg_main")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_validation_main ON validation_audit(metric, timeframe, exchange, symbol, ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_raw_oi_main ON oi_5m_сырые(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_raw_price_main ON price_5m_сырые(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_raw_volume_main ON volume_5m_сырые(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_coverage_report_main ON coverage_report(metric, exchange, symbol, coverage_pct)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_gap_report_main ON gap_report(metric, exchange, symbol, gap_start)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_active_symbol_universe_main ON active_symbol_universe(exchange, symbol)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_silence_main ON market_silence(exchange, symbol, timeframe, ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_silence_stage ON market_silence(stage, timeframe)")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS normalized_volume DOUBLE PRECISION")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_percentile INTEGER")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS noise_state TEXT")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_structure TEXT")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_quality TEXT")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_baseline_24h DOUBLE PRECISION")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_hold_state TEXT")
        safe_ddl(cur, "ALTER TABLE market_volume_state ADD COLUMN IF NOT EXISTS volume_reason TEXT")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_volume_state_main ON market_volume_state(exchange, symbol, timeframe, ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_volume_state_name ON market_volume_state(volume_state_name, timeframe)")
        safe_ddl(cur, "ALTER TABLE market_price_state ADD COLUMN IF NOT EXISTS price_structure TEXT")
        safe_ddl(cur, "ALTER TABLE market_price_state ADD COLUMN IF NOT EXISTS price_quality TEXT")
        safe_ddl(cur, "ALTER TABLE market_price_state ADD COLUMN IF NOT EXISTS price_slope_state TEXT")
        safe_ddl(cur, "ALTER TABLE market_price_state ADD COLUMN IF NOT EXISTS price_trend_24h TEXT")
        safe_ddl(cur, "ALTER TABLE market_price_state ADD COLUMN IF NOT EXISTS price_range_from_median_pct DOUBLE PRECISION")
        safe_ddl(cur, "ALTER TABLE market_price_state ADD COLUMN IF NOT EXISTS price_reason TEXT")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_price_state_main ON market_price_state(exchange, symbol, timeframe, ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_price_state_name ON market_price_state(price_state_name, timeframe)")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_structure TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_priority INTEGER")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_hold_state TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_trend_15m TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_trend_30m TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_trend_1h TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_trend_4h TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_trend_24h TEXT")
        safe_ddl(cur, "ALTER TABLE market_oi_slope ADD COLUMN IF NOT EXISTS oi_reason TEXT")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_oi_slope_main ON market_oi_slope(exchange, symbol, timeframe, ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_oi_slope_stage ON market_oi_slope(stage, timeframe)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_phase_main ON market_phase(exchange, symbol, timeframe)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_phase_phase ON market_phase(phase, timeframe, priority)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_phase_history_main ON market_phase_history(exchange, symbol, timeframe, calculated_at)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_phase_latest ON market_phase(exchange, symbol, timeframe, phase_updated_at DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_oi_slope_latest ON market_oi_slope(exchange, symbol, timeframe, ts_close DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_price_state_latest ON market_price_state(exchange, symbol, timeframe, ts_close DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_market_volume_state_latest ON market_volume_state(exchange, symbol, timeframe, ts_close DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_request_failure_report_main ON request_failure_report(exchange, symbol, data_type)")


    log("Postgres: canonical schema + derived tables готовы")

def execute(sql: str, params: tuple = ()) -> None:
    if not DATABASE_URL:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = {DB_STATEMENT_TIMEOUT_MS}")
        cur.execute(sql, params)

def fetch(sql: str, params: tuple = ()) -> list[dict]:
    if not DATABASE_URL:
        return []
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = {DB_STATEMENT_TIMEOUT_MS}")
        cur.execute(sql, params)
        return list(cur.fetchall())

def upsert_oi(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        _executemany_with_lock_retry(cur, """
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
        _executemany_with_lock_retry(cur, """
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
        _executemany_with_lock_retry(cur, """
        INSERT INTO volume_5m_сырые(ts_open, ts_close, exchange, symbol, volume, collected_at)
        VALUES (%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close=EXCLUDED.ts_close,
            volume=EXCLUDED.volume,
            collected_at=NOW()
        """, rows)

def insert_bot_aggregates(rows: list[tuple]) -> None:
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


def replace_bot_aggregates(rows: list[tuple]) -> None:
    execute("DELETE FROM bot_aggregates WHERE ts_close < NOW() - INTERVAL '72 hours'")
    execute("DELETE FROM bot_aggregates WHERE ts_close >= NOW() - INTERVAL '24 hours'")
    insert_bot_aggregates(rows)

def replace_validation(rows: list[tuple]) -> None:
    execute("DELETE FROM validation_audit")
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
    execute("DELETE FROM raw_integrity_report")
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
    execute("DELETE FROM coverage_report")
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
    execute("DELETE FROM gap_report")
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
    execute("DELETE FROM active_symbol_universe")
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





def replace_market_phase(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        print("replace_market_phase skipped: empty rows, old table preserved")
        return
    execute("DELETE FROM market_phase")
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_phase(
            calculated_at, exchange, symbol, timeframe,
            phase, phase_name, phase_status, priority,
            phase_started_at, phase_updated_at,
            stage1_started_at, stage2_started_at, stage3_started_at,
            manual_reset_required, confidence,
            oi_structure, oi_priority, oi_hold_state,
            oi_trend_1h, oi_trend_4h, oi_trend_24h,
            price_structure, price_quality, price_slope_state,
            volume_structure, volume_quality, volume_hold_state,
            transition_reason, reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def insert_market_phase_history(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO market_phase_history(
            calculated_at, exchange, symbol, timeframe,
            from_phase, to_phase, from_phase_name, to_phase_name,
            phase_status, priority, transition_reason,
            oi_structure, oi_priority, oi_hold_state,
            price_structure, price_quality,
            volume_structure, volume_quality
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def dedupe_derived_tables() -> None:
    if not DATABASE_URL:
        print("dedupe_derived_tables skipped: no DATABASE_URL")
        return

    tables = [
        "market_silence",
        "market_volume_state",
        "market_price_state",
        "market_oi_slope",
    ]

    if not _runtime_ddl_enabled():
        log("DDL deferred: derived dedupe + unique indexes skipped")
        return

    for table in tables:
        execute(f"""
            DELETE FROM {table} a
            USING {table} b
            WHERE a.ctid < b.ctid
              AND a.exchange = b.exchange
              AND a.symbol = b.symbol
              AND a.timeframe = b.timeframe
              AND a.ts_close = b.ts_close
        """)
        execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{table}_key
            ON {table}(exchange, symbol, timeframe, ts_close)
        """)
        log(f"dedupe + unique ok: {table}")

def replace_market_silence(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        print("replace_market_silence skipped: empty rows, old table preserved")
        return
    execute("""
        DELETE FROM market_silence
        WHERE ts_close >= NOW() - '24 hours'::interval
    """)
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



def replace_volume_state(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        print("replace_volume_state skipped: empty rows, old table preserved")
        return
    execute("""
        DELETE FROM market_volume_state
        WHERE ts_close >= NOW() - '24 hours'::interval
    """)
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
            volume_structure,
            volume_quality,
            volume_baseline_24h,
            volume_hold_state,
            volume_reason,
            reason,
            volume_delta_pct,
            normalized_volume,
            volume_percentile,
            noise_state,
            market_state,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)


def replace_price_state(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        print("replace_price_state skipped: empty rows, old table preserved")
        return
    execute("""
        DELETE FROM market_price_state
        WHERE ts_close >= NOW() - '24 hours'::interval
    """)
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
            price_structure,
            price_quality,
            price_slope_state,
            price_trend_24h,
            price_range_from_median_pct,
            price_reason,
            reason,
            price_delta_pct,
            range_width_pct,
            market_state,
            invalid_reason
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)




def replace_oi_slope(rows):
    if not DATABASE_URL:
        return

    execute("DELETE FROM market_oi_slope WHERE ts_close >= NOW() - INTERVAL '24 hours'")

    if not rows:
        return

    cols = [
        "calculated_at",
        "ts_close",
        "exchange",
        "symbol",
        "timeframe",
        "stage",
        "stage_name",
        "oi_structure",
        "oi_priority",
        "oi_hold_state",
        "oi_trend_15m",
        "oi_trend_30m",
        "oi_trend_1h",
        "oi_trend_4h",
        "oi_trend_24h",
        "oi_reason",
        "reason",
        "oi_delta_pct",
        "oi_acceleration",
        "oi_prev_avg",
        "price_delta_pct",
        "volume_delta_pct",
        "range_width_pct",
        "silence_stage",
    ]

    expected = len(cols)
    bad = [i for i, r in enumerate(rows[:20]) if len(tuple(r)) != expected]
    if bad:
        raise ValueError(
            f"replace_oi_slope bad row length: expected={expected}, "
            f"bad_indexes={bad}, first_len={len(tuple(rows[0]))}"
        )

    col_sql = ",".join(cols)
    ph_sql = ",".join(["%s"] * expected)

    with _conn() as conn, conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO market_oi_slope ({col_sql}) VALUES ({ph_sql})",
            rows,
        )


def replace_request_failures(rows: list[tuple]) -> None:
    execute("DELETE FROM request_failure_report")
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
        execute(f"DELETE FROM {table} WHERE ts_open < NOW() - (%s || \' days\')::interval", (RAW_RETENTION_DAYS,))

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
