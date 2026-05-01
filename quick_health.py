import os
from db import execute, fetch

if not os.getenv("DATABASE_URL"):
    raise SystemExit("ERROR: DATABASE_URL is not set")

execute("SET statement_timeout = '10s'")

print("\n=== TABLE HEALTH ===")
for r in fetch("""
SELECT 'market_research' table_name, COUNT(*) rows, MAX(ts_close) latest_ts FROM market_research
UNION ALL SELECT 'market_silence', COUNT(*), MAX(ts_close) FROM market_silence
UNION ALL SELECT 'market_price_state', COUNT(*), MAX(ts_close) FROM market_price_state
UNION ALL SELECT 'market_volume_state', COUNT(*), MAX(ts_close) FROM market_volume_state
UNION ALL SELECT 'market_oi_slope', COUNT(*), MAX(ts_close) FROM market_oi_slope
UNION ALL SELECT 'market_phase', COUNT(*), MAX(phase_updated_at) FROM market_phase
ORDER BY table_name
"""):
    print(dict(r))

print("\n=== BAD STAGE 2 ===")
bad = fetch("""
SELECT phase, oi_structure, oi_hold_state, oi_trend_1h, COUNT(*) cnt
FROM market_phase
WHERE phase = 2
AND (
    oi_structure IN ('пила','нисходящий OI','всплеск без удержания','тишина')
    OR oi_hold_state NOT IN ('holding','hold','удержание')
)
GROUP BY 1,2,3,4
ORDER BY cnt DESC
""")
if not bad:
    print("OK: no dirty stage 2")
else:
    for r in bad:
        print(dict(r))

print("\n=== STAGE 3 ===")
stage3 = fetch("""
SELECT symbol, timeframe, oi_structure, oi_priority, oi_hold_state, oi_trend_1h, oi_trend_4h
FROM market_phase
WHERE phase = 3
ORDER BY symbol, timeframe
""")
if not stage3:
    print("OK: no stage 3")
else:
    for r in stage3:
        print(dict(r))

print("\n=== OI SLOPE ZERO CHECK ===")
for r in fetch("""
SELECT
    (SELECT COUNT(*) FROM market_research) AS research_rows,
    (SELECT COUNT(*) FROM market_oi_slope) AS oi_slope_rows,
    (SELECT MAX(ts_close) FROM market_research) AS research_latest,
    (SELECT MAX(ts_close) FROM market_oi_slope) AS oi_slope_latest
"""):
    print(dict(r))


print("\n=== RUNTIME REPORTS ===")
from pathlib import Path
import json

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
    keys = [
        "rss_health",
        "watchdog_health",
        "collect_seconds",
        "collect_reserve_seconds",
        "collect_reserve_health",
        "runtime_alert_count",
        "runtime_alerts",
        "snapshot_health",
    ]
    for k in keys:
        print(f"{k}: {runtime.get(k)}")

if cycle:
    keys = [
        "cycle_health",
        "cycle_elapsed_seconds",
        "cycle_sleep_seconds",
        "cycle_reserve_pct",
        "cycle_latency_class",
        "overrun_streak",
    ]
    for k in keys:
        print(f"{k}: {cycle.get(k)}")

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
