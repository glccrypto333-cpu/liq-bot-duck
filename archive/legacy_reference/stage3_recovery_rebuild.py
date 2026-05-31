from __future__ import annotations

import argparse
import time

from logger import log
from db import fetch
from aggregation_engine import rebuild_bot_aggregates
from research_engine import rebuild_market_research
from market_price_engine import rebuild_price_state
from market_volume_engine import rebuild_volume_state
from market_oi_slope_engine import rebuild_oi_slope
from market_silence_engine import rebuild_market_silence
from market_phase_engine import rebuild_market_phase
from phase_audit import main as phase_audit_main


def _step(name: str, fn, enabled: bool = True) -> int:
    if not enabled:
        print(f"{name}: skipped")
        return -1

    started = time.monotonic()
    print(f"{name}: start")
    rows = fn()
    elapsed = time.monotonic() - started
    print(f"{name}: done rows={rows} seconds={elapsed:.2f}")
    return int(rows or 0)


def _summary() -> None:
    print("\n===== STAGE3 RECOVERY SUMMARY =====")

    for r in fetch("""
        SELECT 'bot_aggregates' table_name, COUNT(*) rows, MAX(ts_close) latest FROM bot_aggregates
        UNION ALL SELECT 'market_research', COUNT(*), MAX(ts_close) FROM market_research
        UNION ALL SELECT 'market_price_state', COUNT(*), MAX(ts_close) FROM market_price_state
        UNION ALL SELECT 'market_volume_state', COUNT(*), MAX(ts_close) FROM market_volume_state
        UNION ALL SELECT 'market_oi_slope', COUNT(*), MAX(ts_close) FROM market_oi_slope
        UNION ALL SELECT 'market_silence', COUNT(*), MAX(ts_close) FROM market_silence
    """):
        print(dict(r))

    print("\n===== MARKET PHASE COUNTS =====")
    for r in fetch("""
        SELECT phase, phase_name, COUNT(*) rows
        FROM market_phase
        GROUP BY 1,2
        ORDER BY phase
    """):
        print(dict(r))


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline Stage 3 recovery rebuild + audit")
    parser.add_argument("--skip-aggregates", action="store_true", help="Skip bot_aggregates rebuild")
    parser.add_argument("--skip-research", action="store_true")
    parser.add_argument("--skip-price", action="store_true")
    parser.add_argument("--skip-volume", action="store_true")
    parser.add_argument("--skip-oi-slope", action="store_true")
    parser.add_argument("--skip-silence", action="store_true")
    parser.add_argument("--skip-phase", action="store_true")
    parser.add_argument("--skip-audit", action="store_true")
    args = parser.parse_args()

    log("stage3 recovery rebuild start")

    _step("aggregates", rebuild_bot_aggregates, not args.skip_aggregates)
    _step("market_research", rebuild_market_research, not args.skip_research)
    _step("market_price_state", rebuild_price_state, not args.skip_price)
    _step("market_volume_state", rebuild_volume_state, not args.skip_volume)
    _step("market_oi_slope", rebuild_oi_slope, not args.skip_oi_slope)
    _step("market_silence", rebuild_market_silence, not args.skip_silence)
    _step("market_phase", rebuild_market_phase, not args.skip_phase)

    _summary()

    if not args.skip_audit:
        print("\n===== PHASE AUDIT =====")
        phase_audit_main()

    log("stage3 recovery rebuild done")
    print("\nSTAGE3_RECOVERY_REBUILD_OK")


if __name__ == "__main__":
    main()
