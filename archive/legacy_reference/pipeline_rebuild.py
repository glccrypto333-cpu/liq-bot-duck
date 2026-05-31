from pathlib import Path
import sys
from datetime import datetime, timezone

from market_silence_engine import rebuild_market_silence
from market_volume_engine import rebuild_volume_state
from market_price_engine import rebuild_price_state
from market_oi_slope_engine import rebuild_oi_slope
from market_phase_engine import rebuild_market_phase
from db import fetch

LOCK = Path("runtime/rebuild.lock")
LOCK.parent.mkdir(parents=True, exist_ok=True)

def log(msg):
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S UTC} | {msg}")

def health():
    for r in fetch("""
    SELECT 'market_research' table_name, COUNT(*) rows, MAX(ts_close) latest_ts FROM market_research
    UNION ALL SELECT 'market_silence', COUNT(*), MAX(ts_close) FROM market_silence
    UNION ALL SELECT 'market_oi_slope', COUNT(*), MAX(ts_close) FROM market_oi_slope
    UNION ALL SELECT 'market_price_state', COUNT(*), MAX(ts_close) FROM market_price_state
    UNION ALL SELECT 'market_phase', COUNT(*), MAX(phase_updated_at) FROM market_phase
    ORDER BY table_name
    """):
        log(dict(r))

if LOCK.exists():
    log(f"LOCK EXISTS: {LOCK}. Another rebuild may be running. Exit.")
    sys.exit(1)

try:
    LOCK.write_text(str(datetime.now(timezone.utc)))

    log("BEFORE HEALTH")
    health()

    log("rebuild_market_silence start")
    silence = rebuild_market_silence()
    log(f"rebuild_market_silence done rows={silence}")

    log("rebuild_volume_state start")
    volume = rebuild_volume_state()
    log(f"rebuild_volume_state done rows={volume}")

    log("rebuild_price_state start")
    price = rebuild_price_state()
    log(f"rebuild_price_state done rows={price}")

    log("rebuild_oi_slope start")
    oi = rebuild_oi_slope()
    log(f"rebuild_oi_slope done rows={oi}")

    log("rebuild_market_phase start")
    phase = rebuild_market_phase()
    log(f"rebuild_market_phase done rows={phase}")

    log("AFTER HEALTH")
    health()

finally:
    if LOCK.exists():
        LOCK.unlink()
        log("lock removed")
