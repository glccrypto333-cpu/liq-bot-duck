from __future__ import annotations

import argparse
from datetime import datetime, timezone

from db import fetch, execute, insert_market_phase_history


def _one(exchange: str, symbol: str, timeframe: str):
    rows = fetch(
        """
        SELECT *
        FROM market_phase
        WHERE exchange = %s
          AND symbol = %s
          AND timeframe = %s
        LIMIT 1
        """,
        (exchange, symbol, timeframe),
    )
    return rows[0] if rows else None


def reset_stage3(exchange: str, symbol: str, timeframe: str, reason: str, dry_run: bool = False) -> int:
    now = datetime.now(timezone.utc)

    row = _one(exchange, symbol, timeframe)
    if not row:
        print(f"NOT_FOUND exchange={exchange} symbol={symbol} timeframe={timeframe}")
        return 0

    if int(row["phase"]) != 3:
        print(f"SKIP_NOT_STAGE3 exchange={exchange} symbol={symbol} timeframe={timeframe} phase={row['phase']}")
        return 0

    reset_reason = f"manual_stage3_reset: {reason}"

    print(
        "RESET_STAGE3 "
        f"exchange={exchange} symbol={symbol} timeframe={timeframe} "
        f"old_phase=3 new_phase=0 reason={reset_reason}"
    )

    if dry_run:
        print("DRY_RUN_OK")
        return 1

    execute(
        """
        UPDATE market_phase
        SET
            phase = 0,
            phase_name = 'stage_0_no_interest',
            status = 'cooling',
            priority = 'P4',
            phase_updated_at = %s,
            phase_started_at = %s,
            stage1_started_at = NULL,
            stage2_started_at = NULL,
            stage3_started_at = NULL,
            manual_reset_required = FALSE,
            transition_reason = %s,
            reason = %s
        WHERE exchange = %s
          AND symbol = %s
          AND timeframe = %s
          AND phase = 3
        """,
        (now, now, reset_reason, reset_reason, exchange, symbol, timeframe),
    )

    insert_market_phase_history([
        (
            now,
            exchange,
            symbol,
            timeframe,
            3,
            0,
            row.get("phase_name") or "stage_3_alert_manual_reset",
            "stage_0_no_interest",
            "cooling",
            "P4",
            reset_reason,
            row.get("oi_structure"),
            row.get("oi_priority"),
            row.get("oi_hold_state"),
            row.get("price_structure"),
            row.get("price_quality"),
            row.get("volume_structure"),
            row.get("volume_quality"),
        )
    ])

    print("RESET_STAGE3_OK")
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Manual reset for Stage 3 market phase")
    parser.add_argument("--exchange", choices=["BINANCE", "BYBIT"])
    parser.add_argument("--symbol")
    parser.add_argument("--timeframe")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all", action="store_true", help="Reset all current Stage 3 rows")
    args = parser.parse_args()

    if args.all:
        rows = fetch("""
            SELECT exchange, symbol, timeframe
            FROM market_phase
            WHERE phase = 3
            ORDER BY exchange, symbol, timeframe
        """)

        if not rows:
            print("OK: stage 3 empty")
            return

        total = 0
        for row in rows:
            total += reset_stage3(
                exchange=row["exchange"],
                symbol=row["symbol"],
                timeframe=row["timeframe"],
                reason=args.reason,
                dry_run=args.dry_run,
            )

        print(f"RESET_STAGE3_ALL_OK rows={total}")
        return

    if not args.exchange or not args.symbol or not args.timeframe:
        raise SystemExit("Usage: reset one with --exchange --symbol --timeframe --reason OR reset all with --all --reason")

    reset_stage3(
        exchange=args.exchange,
        symbol=args.symbol.upper(),
        timeframe=args.timeframe,
        reason=args.reason,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
