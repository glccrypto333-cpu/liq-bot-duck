from __future__ import annotations
import json
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
    if not DATABASE_URL or not rows:
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

    for attempt in range(5):
        try:
            if _DB_CONN is not None and not _DB_CONN.closed:
                try:
                    with _DB_CONN.cursor() as cur:
                        cur.execute("SELECT 1")
                    return _DB_CONN
                except Exception:
                    try:
                        _DB_CONN.close()
                    except Exception:
                        pass
                    _DB_CONN = None

            _DB_CONN = psycopg.connect(
                DATABASE_URL,
                autocommit=True,
                row_factory=dict_row,
                connect_timeout=5,
            )

            _apply_session_settings(_DB_CONN)

            with _DB_CONN.cursor() as cur:
                cur.execute("SELECT 1")

            return _DB_CONN

        except Exception as e:
            _DB_CONN = None

            if attempt == 4:
                raise

            print(f"DB reconnect retry {attempt + 1}/5: {e}")

            time.sleep(2)


def _fresh_conn():
    conn = psycopg.connect(
        DATABASE_URL,
        autocommit=True,
        row_factory=dict_row,
        connect_timeout=5,
    )
    _apply_session_settings(conn)
    return conn

def init_db() -> None:
    if not DATABASE_URL:
        log("DATABASE_URL не задан, пропускаю init_db")
        return

    with _conn() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_raw(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            oi_open DOUBLE PRECISION NOT NULL,
            oi_high DOUBLE PRECISION NOT NULL,
            oi_low DOUBLE PRECISION NOT NULL,
            oi_close DOUBLE PRECISION NOT NULL,
            cycle_ts TIMESTAMPTZ,
            source TEXT,
            collected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS price_raw(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price_open DOUBLE PRECISION NOT NULL,
            price_high DOUBLE PRECISION NOT NULL,
            price_low DOUBLE PRECISION NOT NULL,
            price_close DOUBLE PRECISION NOT NULL,
            cycle_ts TIMESTAMPTZ,
            source TEXT,
            collected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volume_raw(
            ts_open TIMESTAMPTZ NOT NULL,
            ts_close TIMESTAMPTZ NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            cycle_ts TIMESTAMPTZ,
            source TEXT,
            collected_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS aggregate_windows(
            metric TEXT NOT NULL,
            window_code TEXT NOT NULL,
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
            trajectory_points JSONB,
            source_cycle_ts TIMESTAMPTZ,
            built_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS aggregate_windows_history(
            metric TEXT NOT NULL,
            window_code TEXT NOT NULL,
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
            trajectory_points JSONB,
            source_cycle_ts TIMESTAMPTZ,
            built_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        safe_ddl(cur, "ALTER TABLE aggregate_windows ADD COLUMN IF NOT EXISTS trajectory_points JSONB")
        safe_ddl(cur, "ALTER TABLE aggregate_windows_history ADD COLUMN IF NOT EXISTS trajectory_points JSONB")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_core_state(
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            current_stage INTEGER,
            oi_pattern TEXT,
            oi_pattern_code TEXT,
            oi_pattern_label TEXT,
            oi_direction TEXT,
            oi_angle TEXT,
            oi_stability TEXT,
            oi_retention TEXT,
            oi_breakdown TEXT,
            oi_direction_summary TEXT,
            oi_angle_summary TEXT,
            oi_stability_summary TEXT,
            oi_retention_summary TEXT,
            oi_breakdown_summary TEXT,
            price_state_summary TEXT,
            price_block_level_summary TEXT,
            volume_state_summary TEXT,
            volume_confidence_summary TEXT,
            oi_stage_age_minutes DOUBLE PRECISION,
            oi_transition_permission TEXT,
            blocked_by_price BOOLEAN DEFAULT FALSE,
            blocked_stage_max INTEGER,
            volume_confirmation TEXT,
            decision_reason TEXT,
            block_reason TEXT,
            breakdown_reason TEXT,
            growth_trigger_ts TIMESTAMPTZ,
            oi_slope_class_15m TEXT,
            oi_slope_class_30m TEXT,
            oi_slope_class_1h TEXT,
            oi_slope_class_4h TEXT,
            latest_cycle_ts TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_window_state(
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            window_code TEXT NOT NULL,
            oi_direction TEXT,
            oi_angle TEXT,
            oi_stability TEXT,
            oi_retention TEXT,
            oi_breakdown TEXT,
            oi_pattern TEXT,
            oi_pattern_code TEXT,
            oi_pattern_label TEXT,
            price_state_code TEXT,
            price_state_label TEXT,
            price_block_level TEXT,
            volume_state_code TEXT,
            volume_state_label TEXT,
            volume_confidence_effect TEXT,
            window_growth_pct DOUBLE PRECISION,
            window_weight DOUBLE PRECISION,
            visual_label TEXT,
            cycle_ts TIMESTAMPTZ,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_stage_history(
            id BIGSERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            from_stage INTEGER,
            to_stage INTEGER NOT NULL,
            transition_reason TEXT NOT NULL,
            transition_allowed BOOLEAN,
            stage_age_before_transition DOUBLE PRECISION,
            cycle_ts TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_debug_cases(
            id BIGSERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            cycle_ts TIMESTAMPTZ,
            current_stage INTEGER,
            oi_pattern TEXT,
            decision_snapshot JSONB,
            user_comment TEXT,
            assistant_analysis TEXT,
            status TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS oi_post_stage_analytics(
            id BIGSERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            stage_triggered INTEGER NOT NULL,
            triggered_at TIMESTAMPTZ NOT NULL,
            trigger_price DOUBLE PRECISION,
            trigger_oi DOUBLE PRECISION,
            price_after_1h DOUBLE PRECISION,
            price_after_4h DOUBLE PRECISION,
            price_after_12h DOUBLE PRECISION,
            price_after_24h DOUBLE PRECISION,
            oi_after_1h DOUBLE PRECISION,
            oi_after_4h DOUBLE PRECISION,
            oi_after_12h DOUBLE PRECISION,
            oi_after_24h DOUBLE PRECISION,
            quality_label TEXT,
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS core_state_v2(
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            latest_cycle_ts TIMESTAMPTZ,
            current_stage INTEGER,
            stage_age_minutes DOUBLE PRECISION,
            transition_permission TEXT,
            manual_reset_required BOOLEAN DEFAULT FALSE,
            price_hard_ban BOOLEAN DEFAULT FALSE,
            phase_reason TEXT,
            oi_summary JSONB,
            price_summary JSONB,
            volume_summary JSONB,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS window_state_v2(
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            cycle_ts TIMESTAMPTZ,
            window_code TEXT NOT NULL,
            window_weight DOUBLE PRECISION,
            oi_slope_class TEXT,
            oi_slope_value DOUBLE PRECISION,
            oi_hold_class TEXT,
            oi_pullback_class TEXT,
            oi_smoothness_class TEXT,
            price_regime TEXT,
            price_direction TEXT,
            price_hard_ban BOOLEAN DEFAULT FALSE,
            volume_class TEXT,
            volume_10x_confirmed BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transition_history_v2(
            id BIGSERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            from_stage INTEGER,
            to_stage INTEGER NOT NULL,
            cycle_ts TIMESTAMPTZ,
            transition_allowed BOOLEAN,
            stage_age_before_transition DOUBLE PRECISION,
            reason TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS debug_cases_v2(
            id BIGSERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            cycle_ts TIMESTAMPTZ,
            current_stage INTEGER,
            oi_summary JSONB,
            price_summary JSONB,
            volume_summary JSONB,
            user_comment TEXT,
            status TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS post_stage_analytics_v2(
            id BIGSERIAL PRIMARY KEY,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            stage_triggered INTEGER NOT NULL,
            triggered_at TIMESTAMPTZ NOT NULL,
            trigger_price DOUBLE PRECISION,
            trigger_oi DOUBLE PRECISION,
            price_after_1h DOUBLE PRECISION,
            price_after_4h DOUBLE PRECISION,
            price_after_12h DOUBLE PRECISION,
            price_after_24h DOUBLE PRECISION,
            oi_after_1h DOUBLE PRECISION,
            oi_after_4h DOUBLE PRECISION,
            oi_after_12h DOUBLE PRECISION,
            oi_after_24h DOUBLE PRECISION,
            quality_label TEXT,
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS telegram_stage3_alert_history(
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            created_at_text TEXT,
            alert_key TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            current_stage INTEGER,
            oi_pattern_code TEXT,
            oi_pattern_label TEXT,
            price_state_summary TEXT,
            volume_state_summary TEXT,
            oi_stage_age_minutes DOUBLE PRECISION,
            latest_cycle_ts TIMESTAMPTZ,
            decision_reason TEXT
        )
        """)

        run_runtime_ddl = _runtime_ddl_enabled()

        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_oi_raw_candle ON oi_raw(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_price_raw_candle ON price_raw(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_volume_raw_candle ON volume_raw(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_aggregate_windows_key ON aggregate_windows(metric, window_code, exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_aggregate_windows_history_key ON aggregate_windows_history(metric, window_code, exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_aggregate_windows_history_latest ON aggregate_windows_history(exchange, symbol, window_code, ts_close DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_aggregate_windows_history_ts_close ON aggregate_windows_history(ts_close)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_oi_core_state_key ON oi_core_state(exchange, symbol)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_oi_window_state_key ON oi_window_state(exchange, symbol, window_code)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_core_state_v2_key ON core_state_v2(exchange, symbol)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_window_state_v2_key ON window_state_v2(exchange, symbol, window_code)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_post_stage_v2_key ON post_stage_analytics_v2(exchange, symbol, stage_triggered, triggered_at)")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_pattern_code TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_pattern_label TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_direction_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_angle_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_stability_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_retention_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_breakdown_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS price_state_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS price_block_level_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS volume_state_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS volume_confidence_summary TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS blocked_stage_max INTEGER")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS growth_trigger_ts TIMESTAMPTZ")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_slope_class_15m TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_slope_class_30m TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_slope_class_1h TEXT")
        safe_ddl(cur, "ALTER TABLE oi_core_state ADD COLUMN IF NOT EXISTS oi_slope_class_4h TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS oi_pattern_code TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS oi_pattern_label TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS price_state_code TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS price_state_label TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS price_block_level TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS volume_state_code TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS volume_state_label TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS volume_confidence_effect TEXT")
        safe_ddl(cur, "ALTER TABLE oi_window_state ADD COLUMN IF NOT EXISTS window_weight DOUBLE PRECISION")

        if not run_runtime_ddl:
            log("DDL runtime migrations skipped")
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
        CREATE TABLE IF NOT EXISTS data_quality_quarantine(
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            reason_hint TEXT,
            missing_list TEXT,
            stale_list TEXT,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            restored_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'active',
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

        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_validation_main ON validation_audit(metric, timeframe, exchange, symbol, ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_oi_raw_main ON oi_raw(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_price_raw_main ON price_raw(exchange, symbol, ts_open)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_volume_raw_main ON volume_raw(exchange, symbol, ts_open)")
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
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_aggregate_windows_latest ON aggregate_windows(exchange, symbol, window_code, ts_close DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_aggregate_windows_history_latest ON aggregate_windows_history(exchange, symbol, window_code, ts_close DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_aggregate_windows_history_ts_close ON aggregate_windows_history(ts_close)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_oi_core_stage ON oi_core_state(current_stage)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_oi_stage_history_main ON oi_stage_history(exchange, symbol, created_at DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_oi_debug_cases_main ON oi_debug_cases(exchange, symbol, created_at DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_oi_post_stage_main ON oi_post_stage_analytics(exchange, symbol, triggered_at DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_core_state_v2_stage ON core_state_v2(current_stage)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_transition_history_v2_main ON transition_history_v2(exchange, symbol, created_at DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_debug_cases_v2_main ON debug_cases_v2(exchange, symbol, created_at DESC)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_post_stage_v2_main ON post_stage_analytics_v2(exchange, symbol, triggered_at DESC)")
        safe_ddl(cur, "CREATE UNIQUE INDEX IF NOT EXISTS ux_telegram_stage3_alert_history_key ON telegram_stage3_alert_history(alert_key)")
        safe_ddl(cur, "CREATE INDEX IF NOT EXISTS idx_telegram_stage3_alert_history_created ON telegram_stage3_alert_history(created_at DESC)")


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

def upsert_oi(rows: list[tuple], cycle_ts=None, source: str = "collect") -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        canonical_rows = [
            (
                ts_open, ts_close, exchange, symbol,
                oi_open, oi_high, oi_low, oi_close,
                cycle_ts, source,
            )
            for ts_open, ts_close, exchange, symbol, oi_open, oi_high, oi_low, oi_close in rows
        ]
        _executemany_with_lock_retry(cur, """
        INSERT INTO oi_raw(
            ts_open, ts_close, exchange, symbol,
            oi_open, oi_high, oi_low, oi_close,
            cycle_ts, source, collected_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close=EXCLUDED.ts_close,
            oi_open=EXCLUDED.oi_open,
            oi_high=EXCLUDED.oi_high,
            oi_low=EXCLUDED.oi_low,
            oi_close=EXCLUDED.oi_close,
            cycle_ts=EXCLUDED.cycle_ts,
            source=EXCLUDED.source,
            collected_at=NOW()
        """, canonical_rows)

def upsert_price(rows: list[tuple], cycle_ts=None, source: str = "collect") -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        canonical_rows = [
            (
                ts_open, ts_close, exchange, symbol,
                price_open, price_high, price_low, price_close,
                cycle_ts, source,
            )
            for ts_open, ts_close, exchange, symbol, price_open, price_high, price_low, price_close in rows
        ]
        _executemany_with_lock_retry(cur, """
        INSERT INTO price_raw(
            ts_open, ts_close, exchange, symbol,
            price_open, price_high, price_low, price_close,
            cycle_ts, source, collected_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close=EXCLUDED.ts_close,
            price_open=EXCLUDED.price_open,
            price_high=EXCLUDED.price_high,
            price_low=EXCLUDED.price_low,
            price_close=EXCLUDED.price_close,
            cycle_ts=EXCLUDED.cycle_ts,
            source=EXCLUDED.source,
            collected_at=NOW()
        """, canonical_rows)

def upsert_volume(rows: list[tuple], cycle_ts=None, source: str = "collect") -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        canonical_rows = [
            (
                ts_open, ts_close, exchange, symbol,
                volume, cycle_ts, source,
            )
            for ts_open, ts_close, exchange, symbol, volume in rows
        ]
        _executemany_with_lock_retry(cur, """
        INSERT INTO volume_raw(
            ts_open, ts_close, exchange, symbol,
            volume, cycle_ts, source, collected_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close=EXCLUDED.ts_close,
            volume=EXCLUDED.volume,
            cycle_ts=EXCLUDED.cycle_ts,
            source=EXCLUDED.source,
            collected_at=NOW()
        """, canonical_rows)
def _derived_retention_hours() -> int:
    return int(os.getenv("DERIVED_RETENTION_HOURS", "72"))


def history_retention_hours() -> int:
    return int(os.getenv("AGGREGATE_HISTORY_RETENTION_HOURS", "168"))


def upsert_aggregate_history_rows(rows: list[tuple]) -> int:
    if not DATABASE_URL or not rows:
        return 0

    inserted = 0
    with _conn() as conn, conn.cursor() as cur:
        batch_size = int(os.getenv("AGGREGATE_INSERT_BATCH_SIZE", "5000"))
        insert_sql = """
        INSERT INTO aggregate_windows_history(
            metric, window_code, ts_open, ts_close, exchange, symbol,
            open_value, high_value, low_value, close_value,
            sum_value, avg_value, delta_pct, unique_candles, trajectory_points,
            source_cycle_ts, built_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,NOW())
        ON CONFLICT (metric, window_code, exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close=EXCLUDED.ts_close,
            open_value=EXCLUDED.open_value,
            high_value=EXCLUDED.high_value,
            low_value=EXCLUDED.low_value,
            close_value=EXCLUDED.close_value,
            sum_value=EXCLUDED.sum_value,
            avg_value=EXCLUDED.avg_value,
            delta_pct=EXCLUDED.delta_pct,
            unique_candles=EXCLUDED.unique_candles,
            trajectory_points=EXCLUDED.trajectory_points,
            source_cycle_ts=EXCLUDED.source_cycle_ts,
            built_at=NOW()
        """

        for i in range(0, len(rows), batch_size):
            batch = [
                (
                    metric, timeframe, ts_open, ts_close, exchange, symbol,
                    open_value, high_value, low_value, close_value,
                    sum_value, avg_value, delta_pct, unique_candles,
                    json.dumps(trajectory_points) if trajectory_points is not None else None,
                    ts_close,
                )
                for (
                    metric, timeframe, ts_open, ts_close, exchange, symbol,
                    open_value, high_value, low_value, close_value,
                    sum_value, avg_value, delta_pct, unique_candles, trajectory_points,
                ) in rows[i:i + batch_size]
            ]
            cur.executemany(insert_sql, batch)
            inserted += len(batch)
        conn.commit()
    return inserted


def upsert_aggregate_hot_rows(rows: list[tuple]) -> int:
    if not DATABASE_URL or not rows:
        return 0

    inserted = 0
    with _conn() as conn, conn.cursor() as cur:
        batch_size = int(os.getenv("AGGREGATE_INSERT_BATCH_SIZE", "5000"))
        insert_sql = """
        INSERT INTO aggregate_windows(
            metric, window_code, ts_open, ts_close, exchange, symbol,
            open_value, high_value, low_value, close_value,
            sum_value, avg_value, delta_pct, unique_candles, trajectory_points,
            source_cycle_ts, built_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,NOW())
        ON CONFLICT (metric, window_code, exchange, symbol, ts_open)
        DO UPDATE SET
            ts_close=EXCLUDED.ts_close,
            open_value=EXCLUDED.open_value,
            high_value=EXCLUDED.high_value,
            low_value=EXCLUDED.low_value,
            close_value=EXCLUDED.close_value,
            sum_value=EXCLUDED.sum_value,
            avg_value=EXCLUDED.avg_value,
            delta_pct=EXCLUDED.delta_pct,
            unique_candles=EXCLUDED.unique_candles,
            trajectory_points=EXCLUDED.trajectory_points,
            source_cycle_ts=EXCLUDED.source_cycle_ts,
            built_at=NOW()
        """

        for i in range(0, len(rows), batch_size):
            batch = [
                (
                    metric, timeframe, ts_open, ts_close, exchange, symbol,
                    open_value, high_value, low_value, close_value,
                    sum_value, avg_value, delta_pct, unique_candles,
                    json.dumps(trajectory_points) if trajectory_points is not None else None,
                    ts_close,
                )
                for (
                    metric, timeframe, ts_open, ts_close, exchange, symbol,
                    open_value, high_value, low_value, close_value,
                    sum_value, avg_value, delta_pct, unique_candles, trajectory_points,
                ) in rows[i:i + batch_size]
            ]
            cur.executemany(insert_sql, batch)
            inserted += len(batch)
        conn.commit()
    return inserted


def prune_aggregate_history(hours: int | None = None) -> int:
    retention_hours = int(hours or history_retention_hours())
    rows = execute(
        "DELETE FROM aggregate_windows_history WHERE ts_close < NOW() - (%s || ' hours')::interval",
        (retention_hours,),
    )
    return int(rows or 0)


def replace_aggregate_layers_atomically(rows: list[tuple]) -> None:
    if not DATABASE_URL:
        return
    if not rows:
        raise RuntimeError("replace_aggregate_layers_atomically failed: empty rows")

    conn = _fresh_conn()
    prev_autocommit = conn.autocommit

    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('aggregate_windows_replace'))")
            retention_hours = _derived_retention_hours()
            cur.execute(
                "DELETE FROM aggregate_windows WHERE ts_close < NOW() - (%s || ' hours')::interval",
                (retention_hours,),
            )
            cur.execute(
                "DELETE FROM aggregate_windows WHERE ts_close >= NOW() - (%s || ' hours')::interval",
                (retention_hours,),
            )

            batch_size = int(os.getenv("AGGREGATE_INSERT_BATCH_SIZE", "5000"))
            insert_sql = """
            INSERT INTO aggregate_windows(
                metric, window_code, ts_open, ts_close, exchange, symbol,
                open_value, high_value, low_value, close_value,
                sum_value, avg_value, delta_pct, unique_candles, trajectory_points,
                source_cycle_ts, built_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,NOW())
            ON CONFLICT (metric, window_code, exchange, symbol, ts_open)
            DO UPDATE SET
                ts_close=EXCLUDED.ts_close,
                open_value=EXCLUDED.open_value,
                high_value=EXCLUDED.high_value,
                low_value=EXCLUDED.low_value,
                close_value=EXCLUDED.close_value,
                sum_value=EXCLUDED.sum_value,
                avg_value=EXCLUDED.avg_value,
                delta_pct=EXCLUDED.delta_pct,
                unique_candles=EXCLUDED.unique_candles,
                trajectory_points=EXCLUDED.trajectory_points,
                source_cycle_ts=EXCLUDED.source_cycle_ts,
                built_at=NOW()
            """

            for i in range(0, len(rows), batch_size):
                batch = [
                    (
                        metric, timeframe, ts_open, ts_close, exchange, symbol,
                        open_value, high_value, low_value, close_value,
                        sum_value, avg_value, delta_pct, unique_candles,
                        json.dumps(trajectory_points) if trajectory_points is not None else None,
                        ts_close,
                    )
                    for (
                        metric, timeframe, ts_open, ts_close, exchange, symbol,
                        open_value, high_value, low_value, close_value,
                        sum_value, avg_value, delta_pct, unique_candles, trajectory_points,
                    ) in rows[i:i + batch_size]
                ]
                cur.executemany(insert_sql, batch)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = prev_autocommit

def replace_validation(rows: list[tuple]) -> None:
    execute("DELETE FROM validation_audit")

    if not DATABASE_URL or not rows:
        return

    sql = """
        INSERT INTO validation_audit(
            calculated_at, metric, timeframe, ts_close, exchange, symbol,
            bot_open, audit_open, bot_close, audit_close,
            bot_delta_pct, audit_delta_pct,
            bot_sum, audit_sum, bot_avg, audit_avg,
            drift, unique_candles, validation_status
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    batch_size = 1000

    with _conn() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                cur.executemany(sql, rows[i:i + batch_size])
        conn.commit()

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


def replace_oi_core_state(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO oi_core_state(
            exchange,
            symbol,
            current_stage,
            oi_pattern,
            oi_pattern_code,
            oi_pattern_label,
            oi_direction,
            oi_angle,
            oi_stability,
            oi_retention,
            oi_breakdown,
            oi_direction_summary,
            oi_angle_summary,
            oi_stability_summary,
            oi_retention_summary,
            oi_breakdown_summary,
            price_state_summary,
            price_block_level_summary,
            volume_state_summary,
            volume_confidence_summary,
            oi_stage_age_minutes,
            oi_transition_permission,
            blocked_by_price,
            blocked_stage_max,
            volume_confirmation,
            decision_reason,
            block_reason,
            breakdown_reason,
            growth_trigger_ts,
            oi_slope_class_15m,
            oi_slope_class_30m,
            oi_slope_class_1h,
            oi_slope_class_4h,
            latest_cycle_ts,
            updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol)
        DO UPDATE SET
            current_stage = EXCLUDED.current_stage,
            oi_pattern = EXCLUDED.oi_pattern,
            oi_pattern_code = EXCLUDED.oi_pattern_code,
            oi_pattern_label = EXCLUDED.oi_pattern_label,
            oi_direction = EXCLUDED.oi_direction,
            oi_angle = EXCLUDED.oi_angle,
            oi_stability = EXCLUDED.oi_stability,
            oi_retention = EXCLUDED.oi_retention,
            oi_breakdown = EXCLUDED.oi_breakdown,
            oi_direction_summary = EXCLUDED.oi_direction_summary,
            oi_angle_summary = EXCLUDED.oi_angle_summary,
            oi_stability_summary = EXCLUDED.oi_stability_summary,
            oi_retention_summary = EXCLUDED.oi_retention_summary,
            oi_breakdown_summary = EXCLUDED.oi_breakdown_summary,
            price_state_summary = EXCLUDED.price_state_summary,
            price_block_level_summary = EXCLUDED.price_block_level_summary,
            volume_state_summary = EXCLUDED.volume_state_summary,
            volume_confidence_summary = EXCLUDED.volume_confidence_summary,
            oi_stage_age_minutes = EXCLUDED.oi_stage_age_minutes,
            oi_transition_permission = EXCLUDED.oi_transition_permission,
            blocked_by_price = EXCLUDED.blocked_by_price,
            blocked_stage_max = EXCLUDED.blocked_stage_max,
            volume_confirmation = EXCLUDED.volume_confirmation,
            decision_reason = EXCLUDED.decision_reason,
            block_reason = EXCLUDED.block_reason,
            breakdown_reason = EXCLUDED.breakdown_reason,
            growth_trigger_ts = EXCLUDED.growth_trigger_ts,
            oi_slope_class_15m = EXCLUDED.oi_slope_class_15m,
            oi_slope_class_30m = EXCLUDED.oi_slope_class_30m,
            oi_slope_class_1h = EXCLUDED.oi_slope_class_1h,
            oi_slope_class_4h = EXCLUDED.oi_slope_class_4h,
            latest_cycle_ts = EXCLUDED.latest_cycle_ts,
            updated_at = NOW()
        """, rows)


def replace_oi_window_state(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO oi_window_state(
            exchange,
            symbol,
            window_code,
            oi_direction,
            oi_angle,
            oi_stability,
            oi_retention,
            oi_breakdown,
            oi_pattern,
            oi_pattern_code,
            oi_pattern_label,
            price_state_code,
            price_state_label,
            price_block_level,
            volume_state_code,
            volume_state_label,
            volume_confidence_effect,
            window_growth_pct,
            window_weight,
            visual_label,
            cycle_ts,
            updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, window_code)
        DO UPDATE SET
            oi_direction = EXCLUDED.oi_direction,
            oi_angle = EXCLUDED.oi_angle,
            oi_stability = EXCLUDED.oi_stability,
            oi_retention = EXCLUDED.oi_retention,
            oi_breakdown = EXCLUDED.oi_breakdown,
            oi_pattern = EXCLUDED.oi_pattern,
            oi_pattern_code = EXCLUDED.oi_pattern_code,
            oi_pattern_label = EXCLUDED.oi_pattern_label,
            price_state_code = EXCLUDED.price_state_code,
            price_state_label = EXCLUDED.price_state_label,
            price_block_level = EXCLUDED.price_block_level,
            volume_state_code = EXCLUDED.volume_state_code,
            volume_state_label = EXCLUDED.volume_state_label,
            volume_confidence_effect = EXCLUDED.volume_confidence_effect,
            window_growth_pct = EXCLUDED.window_growth_pct,
            window_weight = EXCLUDED.window_weight,
            visual_label = EXCLUDED.visual_label,
            cycle_ts = EXCLUDED.cycle_ts,
            updated_at = NOW()
        """, rows)


def replace_core_state_v2(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO core_state_v2(
            exchange,
            symbol,
            latest_cycle_ts,
            current_stage,
            stage_age_minutes,
            transition_permission,
            manual_reset_required,
            price_hard_ban,
            phase_reason,
            oi_summary,
            price_summary,
            volume_summary,
            updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,NOW())
        ON CONFLICT (exchange, symbol)
        DO UPDATE SET
            latest_cycle_ts = EXCLUDED.latest_cycle_ts,
            current_stage = EXCLUDED.current_stage,
            stage_age_minutes = EXCLUDED.stage_age_minutes,
            transition_permission = EXCLUDED.transition_permission,
            manual_reset_required = EXCLUDED.manual_reset_required,
            price_hard_ban = EXCLUDED.price_hard_ban,
            phase_reason = EXCLUDED.phase_reason,
            oi_summary = EXCLUDED.oi_summary,
            price_summary = EXCLUDED.price_summary,
            volume_summary = EXCLUDED.volume_summary,
            updated_at = NOW()
        """, rows)


def replace_window_state_v2(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO window_state_v2(
            exchange,
            symbol,
            cycle_ts,
            window_code,
            window_weight,
            oi_slope_class,
            oi_slope_value,
            oi_hold_class,
            oi_pullback_class,
            oi_smoothness_class,
            price_regime,
            price_direction,
            price_hard_ban,
            volume_class,
            volume_10x_confirmed,
            updated_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (exchange, symbol, window_code)
        DO UPDATE SET
            cycle_ts = EXCLUDED.cycle_ts,
            window_weight = EXCLUDED.window_weight,
            oi_slope_class = EXCLUDED.oi_slope_class,
            oi_slope_value = EXCLUDED.oi_slope_value,
            oi_hold_class = EXCLUDED.oi_hold_class,
            oi_pullback_class = EXCLUDED.oi_pullback_class,
            oi_smoothness_class = EXCLUDED.oi_smoothness_class,
            price_regime = EXCLUDED.price_regime,
            price_direction = EXCLUDED.price_direction,
            price_hard_ban = EXCLUDED.price_hard_ban,
            volume_class = EXCLUDED.volume_class,
            volume_10x_confirmed = EXCLUDED.volume_10x_confirmed,
            updated_at = NOW()
        """, rows)


def insert_transition_history_v2(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO transition_history_v2(
            exchange,
            symbol,
            from_stage,
            to_stage,
            cycle_ts,
            transition_allowed,
            stage_age_before_transition,
            reason,
            created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, rows)


def insert_oi_stage_history(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        return
    with _conn() as conn, conn.cursor() as cur:
        cur.executemany("""
        INSERT INTO oi_stage_history(
            exchange,
            symbol,
            from_stage,
            to_stage,
            transition_reason,
            transition_allowed,
            stage_age_before_transition,
            cycle_ts,
            created_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, rows)


def replace_active_universe(rows: list[tuple]) -> None:
    if not DATABASE_URL:
        return
    existing_rows = fetch("SELECT exchange, symbol, source FROM active_symbol_universe")
    existing_map = {
        (str(row.get("exchange")), str(row.get("symbol"))): str(row.get("source") or "")
        for row in existing_rows
    }
    next_map = {
        (str(exchange), str(symbol)): str(source or "")
        for exchange, symbol, source in rows
    }

    to_delete = [
        (exchange, symbol)
        for exchange, symbol in existing_map.keys()
        if (exchange, symbol) not in next_map
    ]
    to_update = [
        (source, exchange, symbol)
        for (exchange, symbol), source in next_map.items()
        if existing_map.get((exchange, symbol)) != source
    ]
    to_insert = [
        (exchange, symbol, source)
        for (exchange, symbol), source in next_map.items()
        if (exchange, symbol) not in existing_map
    ]

    with _conn() as conn, conn.cursor() as cur:
        if to_delete:
            cur.executemany("""
            DELETE FROM active_symbol_universe
            WHERE exchange = %s AND symbol = %s
            """, to_delete)

        if to_update:
            cur.executemany("""
            UPDATE active_symbol_universe
            SET source = %s
            WHERE exchange = %s AND symbol = %s
            """, to_update)

        if to_insert:
            cur.executemany("""
            INSERT INTO active_symbol_universe(exchange, symbol, source, activated_at)
            VALUES (%s,%s,%s,NOW())
            ON CONFLICT (exchange, symbol)
            DO UPDATE SET source = EXCLUDED.source
            """, to_insert)


def active_universe_sql(alias: str = "", include_data_quality_quarantine: bool = True) -> str:
    prefix = f"{alias}." if alias else ""
    base = (
        "EXISTS ("
        "SELECT 1 FROM active_symbol_universe au "
        f"WHERE au.exchange = {prefix}exchange "
        f"AND au.symbol = {prefix}symbol"
        ")"
    )
    if not include_data_quality_quarantine:
        return base
    return (
        f"{base} "
        "AND NOT EXISTS ("
        "SELECT 1 FROM data_quality_quarantine dq "
        f"WHERE dq.exchange = {prefix}exchange "
        f"AND dq.symbol = {prefix}symbol "
        "AND dq.status = 'active'"
        ")"
    )






def prune_inactive_state_rows() -> dict[str, int]:
    if not DATABASE_URL:
        return {}
    with _conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM active_symbol_universe")
        active_count_row = cur.fetchone()
        active_count = int((active_count_row or {}).get("count", 0))
        if active_count <= 0:
            return {}

        counts: dict[str, int] = {}
        delete_specs = [
            (
                "oi_window_state",
                """
                DELETE FROM oi_window_state w
                WHERE NOT EXISTS (
                    SELECT 1 FROM active_symbol_universe au
                    WHERE au.exchange = w.exchange
                      AND au.symbol = w.symbol
                )
                """,
            ),
            (
                "window_state_v2",
                """
                DELETE FROM window_state_v2 w
                WHERE NOT EXISTS (
                    SELECT 1 FROM active_symbol_universe au
                    WHERE au.exchange = w.exchange
                      AND au.symbol = w.symbol
                )
                """,
            ),
            (
                "oi_core_state",
                """
                DELETE FROM oi_core_state c
                WHERE NOT EXISTS (
                    SELECT 1 FROM active_symbol_universe au
                    WHERE au.exchange = c.exchange
                      AND au.symbol = c.symbol
                )
                """,
            ),
            (
                "core_state_v2",
                """
                DELETE FROM core_state_v2 c
                WHERE NOT EXISTS (
                    SELECT 1 FROM active_symbol_universe au
                    WHERE au.exchange = c.exchange
                      AND au.symbol = c.symbol
                )
                """,
            ),
        ]
        for label, sql in delete_specs:
            cur.execute(sql)
            counts[label] = cur.rowcount
        return counts


def replace_market_phase(rows: list[tuple]) -> None:
    if not DATABASE_URL or not rows:
        print("replace_market_phase skipped: empty rows, old table preserved")
        return

    execute("""
        DELETE FROM market_phase a
        USING market_phase b
        WHERE a.exchange = b.exchange
          AND a.symbol = b.symbol
          AND a.timeframe = b.timeframe
          AND a.ctid < b.ctid
    """)

    execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_market_phase_key ON market_phase(exchange, symbol, timeframe)")

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
        ON CONFLICT (exchange, symbol, timeframe)
        DO UPDATE SET
            calculated_at = EXCLUDED.calculated_at,
            phase = EXCLUDED.phase,
            phase_name = EXCLUDED.phase_name,
            phase_status = EXCLUDED.phase_status,
            priority = EXCLUDED.priority,
            phase_started_at = EXCLUDED.phase_started_at,
            phase_updated_at = EXCLUDED.phase_updated_at,
            stage1_started_at = EXCLUDED.stage1_started_at,
            stage2_started_at = EXCLUDED.stage2_started_at,
            stage3_started_at = EXCLUDED.stage3_started_at,
            manual_reset_required = EXCLUDED.manual_reset_required,
            confidence = EXCLUDED.confidence,
            oi_structure = EXCLUDED.oi_structure,
            oi_priority = EXCLUDED.oi_priority,
            oi_hold_state = EXCLUDED.oi_hold_state,
            oi_trend_1h = EXCLUDED.oi_trend_1h,
            oi_trend_4h = EXCLUDED.oi_trend_4h,
            oi_trend_24h = EXCLUDED.oi_trend_24h,
            price_structure = EXCLUDED.price_structure,
            price_quality = EXCLUDED.price_quality,
            price_slope_state = EXCLUDED.price_slope_state,
            volume_structure = EXCLUDED.volume_structure,
            volume_quality = EXCLUDED.volume_quality,
            volume_hold_state = EXCLUDED.volume_hold_state,
            transition_reason = EXCLUDED.transition_reason,
            reason = EXCLUDED.reason
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


def load_data_quality_quarantine_symbols() -> set[tuple[str, str]]:
    if not DATABASE_URL:
        return set()

    rows = fetch("""
        SELECT exchange, symbol
        FROM data_quality_quarantine
        WHERE status = 'active'
        GROUP BY exchange, symbol
    """)

    return {(r["exchange"], r["symbol"]) for r in rows}


def sync_data_quality_quarantine(rows: list[tuple]) -> dict[str, int]:
    if not DATABASE_URL:
        return {"active": 0, "restored": 0}

    active_keys = {(exchange, symbol) for exchange, symbol, *_ in rows}
    active_existing = fetch("""
        SELECT exchange, symbol
        FROM data_quality_quarantine
        WHERE status = 'active'
    """)
    restored_rows = [
        (r["exchange"], r["symbol"])
        for r in active_existing
        if (r["exchange"], r["symbol"]) not in active_keys
    ]

    with _conn() as conn, conn.cursor() as cur:
        if rows:
            cur.executemany("""
            INSERT INTO data_quality_quarantine(
                exchange,
                symbol,
                reason_code,
                reason_hint,
                missing_list,
                stale_list,
                first_seen_at,
                last_seen_at,
                status,
                restored_at
            ) VALUES (%s,%s,%s,%s,%s,%s,NOW(),NOW(),'active',NULL)
            ON CONFLICT(exchange, symbol)
            DO UPDATE SET
                reason_code = EXCLUDED.reason_code,
                reason_hint = EXCLUDED.reason_hint,
                missing_list = EXCLUDED.missing_list,
                stale_list = EXCLUDED.stale_list,
                last_seen_at = NOW(),
                status = 'active',
                restored_at = NULL
            """, rows)

        if restored_rows:
            cur.executemany("""
            UPDATE data_quality_quarantine
            SET status = 'restored',
                restored_at = NOW(),
                last_seen_at = NOW()
            WHERE exchange = %s AND symbol = %s
            """, restored_rows)

    return {"active": len(active_keys), "restored": len(restored_rows)}

def cleanup_old(days: int) -> None:
    raw_days = int(days or RAW_RETENTION_DAYS)

    for table in ["oi_raw", "price_raw", "volume_raw"]:
        rows = execute(
            f"DELETE FROM {table} WHERE ts_open < NOW() - (%s || ' days')::interval",
            (raw_days,),
        )
        print(f"RAW_CLEANUP_TABLE table={table} rows_deleted={int(rows or 0)} retention_days={raw_days}")

    derived_hours = _derived_retention_hours()
    derived_tables = ["aggregate_windows"]

    if os.getenv("CLEANUP_LEGACY_DERIVED_TABLES") == "1":
        derived_tables.extend([
            "market_research",
            "market_price_state",
            "market_volume_state",
            "market_oi_slope",
            "market_silence",
            "market_phase_source",
            "market_oi_slope_staging",
        ])

    for table in derived_tables:
        try:
            rows = execute(
                f"DELETE FROM {table} WHERE ts_close < NOW() - (%s || ' hours')::interval",
                (derived_hours,),
            )
            print(f"DERIVED_CLEANUP_TABLE table={table} rows_deleted={int(rows or 0)} retention_hours={derived_hours}")
        except Exception as e:
            print(f"DERIVED_CLEANUP_TABLE_ERROR table={table} error={type(e).__name__}: {e}")

    try:
        history_hours = history_retention_hours()
        rows = execute(
            "DELETE FROM aggregate_windows_history WHERE ts_close < NOW() - (%s || ' hours')::interval",
            (history_hours,),
        )
        print(f"HISTORY_CLEANUP_TABLE table=aggregate_windows_history rows_deleted={int(rows or 0)} retention_hours={history_hours}")
    except Exception as e:
        print(f"HISTORY_CLEANUP_TABLE_ERROR table=aggregate_windows_history error={type(e).__name__}: {e}")

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
        for table in ["oi_raw", "price_raw", "volume_raw"]:
            cur.execute(
                f"""
                UPDATE {table}
                SET ts_close = ts_open + interval '5 minutes'
                WHERE ts_close IS DISTINCT FROM ts_open + interval '5 minutes'
                """
            )

    log("Postgres: canonical ts_close migration completed")
