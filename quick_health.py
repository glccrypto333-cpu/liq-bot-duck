from db import fetch

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
