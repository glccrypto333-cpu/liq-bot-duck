from pathlib import Path
from datetime import datetime, timezone
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

def q(sql):
    return fetch(sql)

write_csv("health_summary.csv", q("""
WITH h AS (
    SELECT 'market_research' table_name, COUNT(*) rows, MAX(ts_close) latest_ts FROM market_research
    UNION ALL SELECT 'market_oi_slope', COUNT(*), MAX(ts_close) FROM market_oi_slope
    UNION ALL SELECT 'market_price_state', COUNT(*), MAX(ts_close) FROM market_price_state
    UNION ALL SELECT 'market_volume_state', COUNT(*), MAX(ts_close) FROM market_volume_state
    UNION ALL SELECT 'market_silence', COUNT(*), MAX(ts_close) FROM market_silence
    UNION ALL SELECT 'market_phase', COUNT(*), MAX(phase_updated_at) FROM market_phase
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

write_csv("phase_summary.csv", q("""
SELECT phase, phase_name, COUNT(*) cnt
FROM market_phase
GROUP BY phase, phase_name
ORDER BY phase DESC, cnt DESC
"""))

write_csv("phase_watch.csv", q("""
SELECT *
FROM market_phase
WHERE phase > 0
ORDER BY phase DESC, priority, symbol, timeframe
LIMIT 300
"""))

write_csv("oi_last_60m.csv", q("""
SELECT *
FROM market_oi_slope
WHERE ts_close >= NOW() - INTERVAL '60 minutes'
ORDER BY ts_close DESC, stage DESC, oi_priority DESC
LIMIT 1000
"""))

write_csv("oi_signals_4h.csv", q("""
SELECT *
FROM market_oi_slope
WHERE ts_close >= NOW() - INTERVAL '4 hours'
  AND stage >= 1
ORDER BY stage DESC, oi_priority DESC, ts_close DESC
LIMIT 1000
"""))

write_csv("price_last_15m.csv", q("""
SELECT *
FROM market_price_state
WHERE ts_close >= (SELECT MAX(ts_close) FROM market_price_state) - INTERVAL '15 minutes'
ORDER BY ts_close DESC
LIMIT 1000
"""))

write_csv("volume_last_15m.csv", q("""
SELECT *
FROM market_volume_state
WHERE ts_close >= (SELECT MAX(ts_close) FROM market_volume_state) - INTERVAL '15 minutes'
ORDER BY ts_close DESC
LIMIT 1000
"""))

write_csv("silence_last_15m.csv", q("""
SELECT *
FROM market_silence
WHERE ts_close >= (SELECT MAX(ts_close) FROM market_silence) - INTERVAL '15 minutes'
ORDER BY ts_close DESC, stage DESC, score DESC
LIMIT 1000
"""))

write_csv("silence_signals_4h.csv", q("""
SELECT *
FROM market_silence
WHERE ts_close >= (SELECT MAX(ts_close) FROM market_silence) - INTERVAL '4 hours'
  AND stage >= 1
ORDER BY stage DESC, score DESC, ts_close DESC
LIMIT 1000
"""))

print(f"fast snapshot exported: {OUT}")
