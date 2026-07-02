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
runtime_dir = Path("runtime")


def read_json_path(path: Path, label: str) -> dict:
    if not path.exists():
        print(f"{label}: missing")
        return {}
    try:
        data = json.loads(path.read_text())
        print(f"{label}: ok")
        return data
    except Exception as exc:
        print(f"{label}: bad_json {type(exc).__name__}: {exc}")
        return {}


def read_json(name):
    return read_json_path(reports / name, name)


canonical = read_json_path(runtime_dir / "health.json", "runtime/health.json")
runtime = read_json("runtime_health.json")
cycle = read_json("cycle_status.json")

if canonical:
    universe = canonical.get("universe") or {}
    metrics = canonical.get("metrics") or {}
    print("\n=== CANONICAL HEALTH ===")
    for key in ["status", "pid", "started_at", "updated_at", "global_block_reason", "alerts"]:
        print(f"{key}: {canonical.get(key)}")
    for key in [
        "total_symbols",
        "monitored_symbols",
        "listing_health",
        "universe_health",
        "data_quality",
        "stale_windows",
        "incomplete_windows",
        "absent_in_duck",
        "data_quality_quarantine_total",
    ]:
        print(f"universe.{key}: {universe.get(key)}")
    for key in [
        "cycle_health",
        "cycle_latency_class",
        "cycle_elapsed_seconds",
        "cycle_reserve_seconds",
        "cycle_reserve_pct",
        "overrun_streak",
        "signals_observations",
        "signals_waiting_confirmation",
    ]:
        print(f"metrics.{key}: {metrics.get(key)}")

print("\n=== LIVE PROCESS MARKERS ===")
main_pid_path = runtime_dir / "main.pid"
current_log_path = runtime_dir / "current_main_log.path"
main_pid_value = None
if main_pid_path.exists():
    main_pid_value = main_pid_path.read_text().strip()
    print(f"main_pid: {main_pid_value}")
else:
    print("main_pid: missing")

if current_log_path.exists():
    print(f"current_main_log: {current_log_path.read_text().strip()}")
else:
    print("current_main_log: missing")

if runtime:
    for key in [
        "rss_health",
        "watchdog_health",
        "collect_seconds",
        "collect_reserve_seconds",
        "collect_reserve_health",
        "universe_health",
        "universe_alerts",
        "universe_summary",
        "universe_problem_pairs",
        "listing_health",
        "listing_alerts",
        "listing_summary",
        "listing_problem_pairs",
        "symbols_total",
        "symbols_by_exchange",
        "duck_universe_health",
        "duck_listing_health",
        "duck_universe_summary",
        "symbols_incomplete_windows",
        "symbols_stale_windows",
        "symbols_absent_in_duck",
        "data_quality_state",
        "data_quality_alerts",
        "signal_observations_total",
        "signals_already_active",
        "signals_waiting_confirmation",
        "signals_repeat_on_cooldown",
        "new_signals",
        "auto_heal_oi_gaps",
        "auto_heal_listing",
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
    runtime.get("universe_health"),
    runtime.get("listing_health"),
    runtime.get("snapshot_health"),
    cycle.get("cycle_health"),
]

bad = [x for x in health_flags if x and x not in {"ok", "healthy"}]
if canonical:
    universe = canonical.get("universe") or {}
    metrics = canonical.get("metrics") or {}
    if canonical.get("status") != "running":
        bad.append(f"canonical_status={canonical.get('status')}")
    if canonical.get("global_block_reason"):
        bad.append(f"global_block_reason={canonical.get('global_block_reason')}")
    if canonical.get("alerts"):
        bad.append(f"canonical_alerts={canonical.get('alerts')}")
    for label, value in [
        ("canonical_listing_health", universe.get("listing_health")),
        ("canonical_universe_health", universe.get("universe_health")),
        ("canonical_data_quality", universe.get("data_quality")),
        ("canonical_cycle_health", metrics.get("cycle_health")),
    ]:
        if value and value not in {"ok", "healthy"}:
            bad.append(f"{label}={value}")
runtime_pid = runtime.get("pid") if runtime else None
if main_pid_value and runtime_pid and str(runtime_pid) != str(main_pid_value):
    bad.append(f"runtime_snapshot_pid_mismatch={runtime_pid}!={main_pid_value}")
universe_alerts = runtime.get("universe_alerts", []) if runtime else []
if universe_alerts:
    bad.append(f"universe_alerts={universe_alerts}")
problem_pairs = runtime.get("universe_problem_pairs", []) if runtime else []
if problem_pairs:
    print("universe_problem_pairs:")
    for row in problem_pairs[:10]:
        reason = row.get("reason_code")
        level = row.get("reason_level")
        hint = row.get("reason_hint")
        if reason or level or hint:
            print(
                {
                    "exchange": row.get("exchange"),
                    "symbol": row.get("symbol"),
                    "present_cnt": row.get("present_cnt"),
                    "missing_cnt": row.get("missing_cnt"),
                    "max_lag_min": row.get("max_lag_min"),
                    "missing_list": row.get("missing_list"),
                    "stale_list": row.get("stale_list"),
                    "reason_code": reason,
                    "reason_level": level,
                    "reason_hint": hint,
                    "blocking": row.get("blocking"),
                }
            )
        else:
            print(row)
listing_alerts = runtime.get("listing_alerts", []) if runtime else []
if listing_alerts:
    bad.append(f"listing_alerts={listing_alerts}")
listing_problem_pairs = runtime.get("listing_summary", {}).get("listing_problem_pairs", []) if runtime else []
if listing_problem_pairs:
    print("listing_problem_pairs:")
    for row in listing_problem_pairs[:10]:
        print(row)
    if any(row.get("blocking") for row in listing_problem_pairs):
        bad.append("listing_problem_pairs_blocking")

if bad:
    print(f"RUNTIME_VERDICT: DEGRADED flags={bad}")
    raise SystemExit(1)

print("RUNTIME_VERDICT: OK")
