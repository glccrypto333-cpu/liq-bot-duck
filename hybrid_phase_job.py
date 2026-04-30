from __future__ import annotations

import argparse
import time

from logger import log
from db import fetch, execute
from research_engine import rebuild_market_research
from market_price_engine import rebuild_price_state
from market_volume_engine import rebuild_volume_state
from market_oi_slope_engine import rebuild_oi_slope
from market_silence_engine import rebuild_market_silence
from market_phase_engine import rebuild_market_phase
from phase_audit import main as phase_audit_main


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

    if not _acquire_lock():
        raise SystemExit("HYBRID_PHASE_JOB_ALREADY_RUNNING")

    log("hybrid phase job start")

    try:
        counts = {
            "market_research": _step("market_research", rebuild_market_research),
            "market_price_state": _step("market_price_state", rebuild_price_state),
            "market_volume_state": _step("market_volume_state", rebuild_volume_state),
            "market_oi_slope": _step("market_oi_slope", rebuild_oi_slope),
            "market_silence": _step("market_silence", rebuild_market_silence),
            "market_phase": _step("market_phase", rebuild_market_phase),
        }

        if not args.skip_audit:
            phase_audit_main()

        log(f"hybrid phase job done: {counts}")
        print("HYBRID_PHASE_JOB_OK")
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
