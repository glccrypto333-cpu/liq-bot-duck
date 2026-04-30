from __future__ import annotations

import argparse
import os
import time

from logger import log
from db import execute
from aggregation_engine import rebuild_bot_aggregates
from research_engine import rebuild_market_research
from market_price_engine import rebuild_price_state
from market_volume_engine import rebuild_volume_state
from market_oi_slope_engine import rebuild_oi_slope
from market_silence_engine import rebuild_market_silence
from market_phase_engine import rebuild_market_phase
from phase_audit import main as phase_audit_main
from phase_snapshot import insert_phase_snapshot
from cleanup_derived_windows import cleanup_derived_windows


LOCK_KEY = 330003


def _step(name: str, fn) -> int:
    started = time.monotonic()
    log(f"hybrid phase job step start: {name}")
    rows = fn()
    elapsed = time.monotonic() - started
    log(f"hybrid phase job step done: {name} rows={rows} seconds={elapsed:.2f}")
    return int(rows or 0)


def _acquire_lock() -> bool:
    row = fetch("SELECT pg_try_advisory_lock(%s) AS locked", (LOCK_KEY,))[0]
    return bool(row["locked"])


def _release_lock() -> None:
    execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))


def main() -> None:
    parser = argparse.ArgumentParser(description="Hybrid offline phase rebuild job")
    parser.add_argument("--skip-audit", action="store_true")
    args = parser.parse_args()

    timeout_ms = int(os.getenv("HYBRID_STATEMENT_TIMEOUT_MS", "120000"))
    execute(f"SET statement_timeout = {timeout_ms}")
    log(f"hybrid phase job start statement_timeout_ms={timeout_ms}")

    counts = {
        "bot_aggregates": _step("bot_aggregates", rebuild_bot_aggregates),
        "market_research": _step("market_research", rebuild_market_research),
        "market_price_state": _step("market_price_state", rebuild_price_state),
        "market_volume_state": _step("market_volume_state", rebuild_volume_state),
        "market_oi_slope": _step("market_oi_slope", rebuild_oi_slope),
        "market_silence": _step("market_silence", rebuild_market_silence),
        "market_phase": _step("market_phase", rebuild_market_phase),
        "cleanup_derived_windows": _step("cleanup_derived_windows", cleanup_derived_windows),
        "phase_snapshot": _step("phase_snapshot", insert_phase_snapshot),
    }

    if not args.skip_audit:
        phase_audit_main()

    log(f"hybrid phase job done: {counts}")
    print("HYBRID_PHASE_JOB_OK")


if __name__ == "__main__":
    main()
