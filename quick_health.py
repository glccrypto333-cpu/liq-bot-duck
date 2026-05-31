import json
import os
from pathlib import Path

from db import execute, fetch

if not os.getenv("DATABASE_URL"):
    raise SystemExit("ERROR: DATABASE_URL is not set")

execute("SET statement_timeout = '10s'")

print("\n=== TABLE HEALTH ===")
for r in fetch("""
SELECT 'oi_raw' AS table_name, COUNT(*) AS rows, MAX(ts_close) AS latest_ts FROM oi_raw
UNION ALL SELECT 'price_raw', COUNT(*), MAX(ts_close) FROM price_raw
UNION ALL SELECT 'volume_raw', COUNT(*), MAX(ts_close) FROM volume_raw
UNION ALL SELECT 'aggregate_windows', COUNT(*), MAX(ts_close) FROM aggregate_windows
UNION ALL SELECT 'oi_core_state', COUNT(*), MAX(latest_cycle_ts) FROM oi_core_state
UNION ALL SELECT 'core_state_v2', COUNT(*), MAX(latest_cycle_ts) FROM core_state_v2
UNION ALL SELECT 'oi_window_state', COUNT(*), MAX(cycle_ts) FROM oi_window_state
UNION ALL SELECT 'window_state_v2', COUNT(*), MAX(cycle_ts) FROM window_state_v2
UNION ALL SELECT 'oi_stage_history', COUNT(*), MAX(cycle_ts) FROM oi_stage_history
UNION ALL SELECT 'transition_history_v2', COUNT(*), MAX(cycle_ts) FROM transition_history_v2
ORDER BY table_name
"""):
    print(dict(r))

print("\n=== ACTIVE STAGES ===")
for r in fetch("""
SELECT current_stage, COUNT(*) AS cnt, MAX(latest_cycle_ts) AS latest_ts
FROM oi_core_state
WHERE current_stage > 0
GROUP BY current_stage
ORDER BY current_stage DESC
"""):
    print(dict(r))

print("\n=== ACTIVE STAGES V2 ===")
for r in fetch("""
SELECT current_stage, COUNT(*) AS cnt, MAX(latest_cycle_ts) AS latest_ts
FROM core_state_v2
WHERE current_stage > 0
GROUP BY current_stage
ORDER BY current_stage DESC
"""):
    print(dict(r))

print("\n=== LEGACY_V2 PARITY ===")
for r in fetch("""
SELECT
    (SELECT COUNT(*) FROM oi_core_state) AS legacy_core_rows,
    (SELECT COUNT(*) FROM core_state_v2) AS v2_core_rows,
    (SELECT MAX(latest_cycle_ts) FROM oi_core_state) AS legacy_core_latest,
    (SELECT MAX(latest_cycle_ts) FROM core_state_v2) AS v2_core_latest,
    (SELECT COUNT(*) FROM oi_window_state) AS legacy_window_rows,
    (SELECT COUNT(*) FROM window_state_v2) AS v2_window_rows,
    (SELECT MAX(cycle_ts) FROM oi_window_state) AS legacy_window_latest,
    (SELECT MAX(cycle_ts) FROM window_state_v2) AS v2_window_latest
"""):
    print(dict(r))

print("\n=== BLOCKED STAGE CASES ===")
blocked = fetch("""
SELECT
    current_stage,
    blocked_stage_max,
    oi_transition_permission,
    COUNT(*) AS cnt
FROM oi_core_state
WHERE blocked_by_price = TRUE
GROUP BY 1,2,3
ORDER BY cnt DESC, current_stage DESC
""")
if not blocked:
    print("OK: no blocked cases")
else:
    for r in blocked:
        print(dict(r))

print("\n=== STAGE 3 ===")
stage3 = fetch("""
SELECT
    exchange,
    symbol,
    current_stage,
    oi_pattern_code,
    price_state_summary,
    volume_state_summary,
    oi_stage_age_minutes,
    latest_cycle_ts
FROM oi_core_state
WHERE current_stage = 3
ORDER BY exchange, symbol
""")
if not stage3:
    print("OK: no stage 3")
else:
    for r in stage3:
        print(dict(r))

print("\n=== WINDOW COVERAGE ===")
for r in fetch("""
SELECT
    window_code,
    COUNT(*) AS rows,
    MAX(cycle_ts) AS latest_ts
FROM oi_window_state
GROUP BY window_code
ORDER BY
    CASE window_code
        WHEN '15м' THEN 1
        WHEN '30м' THEN 2
        WHEN '1ч' THEN 3
        WHEN '4ч' THEN 4
        WHEN '12ч' THEN 5
        WHEN '24ч' THEN 6
        ELSE 9
    END
"""):
    print(dict(r))

print("\n=== RUNTIME REPORTS ===")
reports = Path("runtime_reports")


def read_json(name):
    path = reports / name
    if not path.exists():
        print(f"{name}: missing")
        return {}
    try:
        data = json.loads(path.read_text())
        print(f"{name}: ok")
        return data
    except Exception as exc:
        print(f"{name}: bad_json {type(exc).__name__}: {exc}")
        return {}


runtime = read_json("runtime_health.json")
cycle = read_json("cycle_status.json")

if runtime:
    for key in [
        "rss_health",
        "watchdog_health",
        "collect_seconds",
        "collect_reserve_seconds",
        "collect_reserve_health",
        "runtime_alert_count",
        "runtime_alerts",
        "snapshot_health",
    ]:
        print(f"{key}: {runtime.get(key)}")

if cycle:
    for key in [
        "cycle_health",
        "cycle_elapsed_seconds",
        "cycle_sleep_seconds",
        "cycle_reserve_pct",
        "cycle_latency_class",
        "stop_reason",
        "overrun_streak",
    ]:
        print(f"{key}: {cycle.get(key)}")

health_flags = [
    runtime.get("rss_health"),
    runtime.get("watchdog_health"),
    runtime.get("collect_reserve_health"),
    runtime.get("snapshot_health"),
    cycle.get("cycle_health"),
]

bad = [x for x in health_flags if x and x not in {"ok", "healthy"}]
if bad:
    print(f"RUNTIME_VERDICT: DEGRADED flags={bad}")
else:
    print("RUNTIME_VERDICT: OK")
