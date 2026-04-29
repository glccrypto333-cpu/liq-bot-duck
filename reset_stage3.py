import argparse
import os
from db import execute, fetch

if not os.getenv("DATABASE_URL"):
    raise SystemExit("ERROR: DATABASE_URL is not set")

parser = argparse.ArgumentParser()
parser.add_argument("symbol", nargs="?")
parser.add_argument("timeframe", nargs="?")
parser.add_argument("--all", action="store_true")
args = parser.parse_args()

execute("SET statement_timeout = '10s'")

if args.all:
    rows = fetch("""
        SELECT symbol, timeframe
        FROM market_phase
        WHERE phase = 3
        ORDER BY symbol, timeframe
    """)
    if not rows:
        print("OK: stage 3 empty")
        raise SystemExit(0)

    execute("""
        UPDATE market_phase
        SET phase = 2,
            phase_name = 'stage_2',
            transition_reason = 'manual_reset_all_stage3',
            phase_updated_at = NOW()
        WHERE phase = 3
    """)
    print(f"OK: reset all stage 3 rows = {len(rows)}")
    raise SystemExit(0)

if not args.symbol or not args.timeframe:
    raise SystemExit("Usage: python reset_stage3.py SYMBOL TIMEFRAME OR python reset_stage3.py --all")

symbol = args.symbol.upper()
timeframe = args.timeframe

before = fetch("""
    SELECT symbol, timeframe, phase, phase_name, oi_structure, oi_hold_state
    FROM market_phase
    WHERE symbol = %s AND timeframe = %s
""", (symbol, timeframe))

if not before:
    raise SystemExit(f"NOT FOUND: {symbol} {timeframe}")

if before[0]["phase"] != 3:
    print("SKIP: not stage 3")
    print("CURRENT:", dict(before[0]))
    raise SystemExit(0)

execute("""
    UPDATE market_phase
    SET phase = 2,
        phase_name = 'stage_2',
        transition_reason = 'manual_reset_stage3',
        phase_updated_at = NOW()
    WHERE symbol = %s
      AND timeframe = %s
      AND phase = 3
""", (symbol, timeframe))

after = fetch("""
    SELECT symbol, timeframe, phase, phase_name, transition_reason
    FROM market_phase
    WHERE symbol = %s AND timeframe = %s
""", (symbol, timeframe))

print("BEFORE:", dict(before[0]))
print("AFTER:", dict(after[0]) if after else None)
