from __future__ import annotations

import argparse
from datetime import datetime, timezone

from db import execute, fetch

RESET_STAGE3_DISABLED = -1


def _now_utc():
    return datetime.now(timezone.utc)


def _one_v2(exchange: str, symbol: str):
    rows = fetch(
        """
        SELECT *
        FROM core_state_v2
        WHERE exchange = %s
          AND symbol = %s
        LIMIT 1
        """,
        (exchange, symbol),
    )
    return rows[0] if rows else None


def _one_legacy(exchange: str, symbol: str):
    rows = fetch(
        """
        SELECT *
        FROM oi_core_state
        WHERE exchange = %s
          AND symbol = %s
        LIMIT 1
        """,
        (exchange, symbol),
    )
    return rows[0] if rows else None


def _canonical_reason(reason: str) -> str:
    cleaned = str(reason or '').strip() or 'manual_reset'
    return f'manual_reset_stage3:{cleaned}'


def reset_stage3(exchange: str, symbol: str, timeframe: str, reason: str, dry_run: bool = False) -> int:
    symbol = symbol.upper().strip()
    exchange = exchange.upper().strip()
    now = _now_utc()
    reason_text = _canonical_reason(reason)

    row_v2 = _one_v2(exchange, symbol)
    row_legacy = _one_legacy(exchange, symbol)

    if not row_v2 and not row_legacy:
        print(f'NOT_FOUND exchange={exchange} symbol={symbol}')
        return 0

    stage_v2 = int((row_v2 or {}).get('current_stage') or 0)
    stage_legacy = int((row_legacy or {}).get('current_stage') or 0)
    current_stage = stage_v2 if row_v2 is not None else stage_legacy

    if current_stage != 3:
        print(f'SKIP_NOT_STAGE3 exchange={exchange} symbol={symbol} stage={current_stage}')
        return 0

    age_v2 = float((row_v2 or {}).get('stage_age_minutes') or 0)
    age_legacy = float((row_legacy or {}).get('oi_stage_age_minutes') or 0)
    stage_age = age_v2 if row_v2 is not None else age_legacy

    print(
        'RESET_STAGE3_READY '
        f'exchange={exchange} symbol={symbol} timeframe={timeframe or "n/a"} '
        f'reason={reason_text} current_stage=3 age={stage_age}'
    )

    if dry_run:
        print('DRY_RUN_OK')
        return 1

    if row_legacy is not None:
        execute(
            """
            UPDATE oi_core_state
            SET current_stage = 0,
                oi_stage_age_minutes = 0,
                oi_transition_permission = %s,
                blocked_by_price = FALSE,
                blocked_stage_max = NULL,
                decision_reason = %s,
                latest_cycle_ts = %s,
                updated_at = NOW()
            WHERE exchange = %s
              AND symbol = %s
            """,
            ('разрешена_стадия_1', reason_text, now, exchange, symbol),
        )
        execute(
            """
            INSERT INTO oi_stage_history(
                exchange, symbol, from_stage, to_stage, transition_reason,
                transition_allowed, stage_age_before_transition, cycle_ts, created_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (exchange, symbol, 3, 0, reason_text, True, stage_age, now),
        )

    if row_v2 is not None:
        execute(
            """
            UPDATE core_state_v2
            SET current_stage = 0,
                stage_age_minutes = 0,
                transition_permission = %s,
                manual_reset_required = FALSE,
                price_hard_ban = FALSE,
                phase_reason = %s,
                latest_cycle_ts = %s,
                updated_at = NOW()
            WHERE exchange = %s
              AND symbol = %s
            """,
            ('разрешена_стадия_1', reason_text, now, exchange, symbol),
        )
        execute(
            """
            INSERT INTO transition_history_v2(
                exchange, symbol, from_stage, to_stage, cycle_ts,
                transition_allowed, stage_age_before_transition, reason, created_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (exchange, symbol, 3, 0, now, True, stage_age, reason_text),
        )

    print(f'RESET_STAGE3_OK exchange={exchange} symbol={symbol} rows=1')
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description='Canonical Stage 3 reset tool for OI-only runtime')
    parser.add_argument('--exchange', choices=['BINANCE', 'BYBIT'])
    parser.add_argument('--symbol')
    parser.add_argument('--timeframe', default='n/a', help='legacy arg, ignored by canonical reset')
    parser.add_argument('--reason', required=True)
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--all', action='store_true', help='Reset all current Stage 3 rows')
    args = parser.parse_args()

    if args.all:
        rows = fetch(
            """
            SELECT exchange, symbol
            FROM (
                SELECT exchange, symbol FROM core_state_v2 WHERE current_stage = 3
                UNION
                SELECT exchange, symbol FROM oi_core_state WHERE current_stage = 3
            ) s
            ORDER BY exchange, symbol
            """
        )
        if not rows:
            print('OK: stage 3 empty')
            return

        total = 0
        for row in rows:
            total += reset_stage3(
                exchange=row['exchange'],
                symbol=row['symbol'],
                timeframe='n/a',
                reason=args.reason,
                dry_run=args.dry_run,
            )
        print(f'RESET_STAGE3_ALL_DONE rows={total}')
        return

    if not args.exchange or not args.symbol:
        raise SystemExit('Usage: reset one with --exchange --symbol --reason OR reset all with --all --reason')

    reset_stage3(
        exchange=args.exchange,
        symbol=args.symbol.upper(),
        timeframe=args.timeframe,
        reason=args.reason,
        dry_run=args.dry_run,
    )


if __name__ == '__main__':
    main()
