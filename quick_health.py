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
