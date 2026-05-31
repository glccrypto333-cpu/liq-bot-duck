from pathlib import Path
import csv

from db import fetch

OUT = Path("runtime/fast_snapshot")
OUT.mkdir(parents=True, exist_ok=True)


def write_csv(name, rows):
    rows = list(rows)
    path = OUT / name
    if not rows:
        path.write_text("")
        return
    cols = list(rows[0].keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def q(sql, params=()):
    return fetch(sql, params)


write_csv("health_summary.csv", q("""
WITH h AS (
    SELECT 'oi_raw' AS table_name, COUNT(*) AS rows, MAX(ts_close) AS latest_ts FROM oi_raw
    UNION ALL SELECT 'price_raw', COUNT(*), MAX(ts_close) FROM price_raw
    UNION ALL SELECT 'volume_raw', COUNT(*), MAX(ts_close) FROM volume_raw
    UNION ALL SELECT 'aggregate_windows', COUNT(*), MAX(ts_close) FROM aggregate_windows
    UNION ALL SELECT 'oi_core_state', COUNT(*), MAX(latest_cycle_ts) FROM oi_core_state
    UNION ALL SELECT 'oi_window_state', COUNT(*), MAX(cycle_ts) FROM oi_window_state
    UNION ALL SELECT 'oi_stage_history', COUNT(*), MAX(cycle_ts) FROM oi_stage_history
)
SELECT
    table_name,
    rows,
    latest_ts,
    ROUND(EXTRACT(EPOCH FROM (NOW() - latest_ts)) / 60.0, 2) AS age_minutes,
    CASE
        WHEN rows = 0 THEN 'EMPTY'
        WHEN latest_ts IS NULL THEN 'EMPTY'
        WHEN NOW() - latest_ts > INTERVAL '90 minutes' THEN 'STALE'
        ELSE 'OK'
    END AS status
FROM h
ORDER BY table_name
"""))

write_csv("stage_summary.csv", q("""
SELECT current_stage, COUNT(*) AS cnt, MAX(latest_cycle_ts) AS latest_cycle_ts
FROM oi_core_state
GROUP BY current_stage
ORDER BY current_stage DESC
"""))

write_csv("stage_watch.csv", q("""
SELECT
    exchange,
    symbol,
    current_stage,
    oi_pattern_code,
    oi_pattern_label,
    price_state_summary,
    volume_state_summary,
    oi_stage_age_minutes,
    oi_transition_permission,
    blocked_stage_max,
    latest_cycle_ts
FROM oi_core_state
WHERE current_stage > 0
ORDER BY current_stage DESC, oi_stage_age_minutes DESC, exchange, symbol
LIMIT 300
"""))

write_csv("oi_windows_last_60m.csv", q("""
SELECT *
FROM oi_window_state
WHERE cycle_ts >= NOW() - INTERVAL '60 minutes'
ORDER BY cycle_ts DESC, window_code, ABS(window_growth_pct) DESC
LIMIT 1000
"""))

write_csv("oi_windows_stage_signals.csv", q("""
SELECT
    c.exchange,
    c.symbol,
    c.current_stage,
    c.oi_pattern_code,
    c.price_state_summary,
    c.volume_state_summary,
    c.latest_cycle_ts,
    w.window_code,
    w.oi_pattern_code AS window_pattern_code,
    w.price_state_code,
    w.volume_state_code,
    w.window_growth_pct
FROM oi_core_state c
LEFT JOIN oi_window_state w
  ON w.exchange = c.exchange
 AND w.symbol = c.symbol
WHERE c.current_stage > 0
ORDER BY c.current_stage DESC, c.latest_cycle_ts DESC, w.window_code
LIMIT 1000
"""))

write_csv("stage_history_last_4h.csv", q("""
SELECT *
FROM oi_stage_history
WHERE cycle_ts >= NOW() - INTERVAL '4 hours'
ORDER BY cycle_ts DESC, to_stage DESC
LIMIT 1000
"""))

write_csv("aggregate_windows_last_4h.csv", q("""
SELECT *
FROM aggregate_windows
WHERE ts_close >= NOW() - INTERVAL '4 hours'
ORDER BY ts_close DESC, metric, window_code, exchange, symbol
LIMIT 2000
"""))


write_csv("stage_summary_v2.csv", q("""
SELECT current_stage, COUNT(*) AS cnt, MAX(latest_cycle_ts) AS latest_cycle_ts
FROM core_state_v2
GROUP BY current_stage
ORDER BY current_stage DESC
"""))

write_csv("stage_watch_v2.csv", q("""
SELECT
    exchange,
    symbol,
    current_stage,
    stage_age_minutes,
    transition_permission,
    phase_reason,
    latest_cycle_ts
FROM core_state_v2
WHERE current_stage > 0
ORDER BY current_stage DESC, stage_age_minutes DESC, exchange, symbol
LIMIT 300
"""))

write_csv("window_state_v2_last_60m.csv", q("""
SELECT *
FROM window_state_v2
WHERE cycle_ts >= NOW() - INTERVAL '60 minutes'
ORDER BY cycle_ts DESC, window_code, ABS(oi_slope_value) DESC
LIMIT 1000
"""))

write_csv("transition_history_v2_last_4h.csv", q("""
SELECT *
FROM transition_history_v2
WHERE cycle_ts >= NOW() - INTERVAL '4 hours'
ORDER BY cycle_ts DESC, to_stage DESC
LIMIT 1000
"""))

write_csv("state_sync_summary.csv", q("""
SELECT
    (SELECT COUNT(*) FROM oi_core_state) AS legacy_core_rows,
    (SELECT COUNT(*) FROM core_state_v2) AS v2_core_rows,
    (SELECT MAX(latest_cycle_ts) FROM oi_core_state) AS legacy_core_latest,
    (SELECT MAX(latest_cycle_ts) FROM core_state_v2) AS v2_core_latest,
    (SELECT COUNT(*) FROM oi_window_state) AS legacy_window_rows,
    (SELECT COUNT(*) FROM window_state_v2) AS v2_window_rows,
    (SELECT MAX(cycle_ts) FROM oi_window_state) AS legacy_window_latest,
    (SELECT MAX(cycle_ts) FROM window_state_v2) AS v2_window_latest
"""))

print(f"fast snapshot exported: {OUT}")
