from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import sys

APP_ROOT = Path(__file__).resolve().parents[2]
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from autonomous_oi_replay import (  # noqa: E402
    load_batch_window_updates,
    load_cycle_timestamps,
    load_latest_window_map_for_replay,
)
from autonomous_oi_service import compute_autonomous_oi_snapshot_from_latest_window_map  # noqa: E402


def parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def cycle_record(core_row: tuple, window_map: dict) -> dict:
    fields = {
        "exchange": core_row[0],
        "symbol": core_row[1],
        "target_stage": int(core_row[2] or 0),
        "oi_pattern_code": core_row[4],
        "price_state": core_row[16],
        "volume_state": core_row[18],
        "stage_age_minutes": core_row[20],
        "transition_permission": core_row[21],
        "decision_reason": core_row[25],
        "oi_classes": {"15m": core_row[29], "30m": core_row[30], "1h": core_row[31], "4h": core_row[32]},
        "cycle_ts": core_row[33].isoformat() if core_row[33] else None,
        "windows": {},
    }
    key = (core_row[0], core_row[1])
    for code in ("15м", "30м", "1ч", "4ч"):
        fields["windows"][code] = {}
        for metric in ("OI", "PRICE", "VOLUME"):
            row = (window_map.get(key, {}).get(code, {}) or {}).get(metric)
            if row:
                fields["windows"][code][metric] = {
                    "delta_pct": row.get("delta_pct"),
                    "open_value": row.get("open_value"),
                    "close_value": row.get("close_value"),
                    "unique_candles": row.get("unique_candles"),
                    "source_cycle_ts": row.get("source_cycle_ts").isoformat() if row.get("source_cycle_ts") else None,
                }
    return fields


def reconstruct(exchange: str, symbol: str, from_ts: datetime, to_ts: datetime, window_source: str = "auto") -> dict:
    hours = max(1, math.ceil((to_ts - from_ts).total_seconds() / 3600) + 1)
    cycles, source = load_cycle_timestamps(hours, None, to_ts=to_ts, window_source=window_source)
    cycles = [ts for ts in cycles if from_ts <= ts <= to_ts]
    if not cycles:
        return {"status": "empty", "exchange": exchange, "symbol": symbol, "from": from_ts.isoformat(), "to": to_ts.isoformat(), "cycles": []}
    pair = [(exchange.upper(), symbol.upper())]
    updates = load_batch_window_updates(cycles, window_source=source, tracked_pairs=pair)
    baseline = load_latest_window_map_for_replay(cycles[0], window_source=source, tracked_pairs=pair)
    latest = {k: {w: dict(m) for w, m in v.items()} for k, v in baseline.items()}
    # Seed the phase state from the last canonical transition before the interval.
    # Without this, replay would incorrectly restart stage/trigger age at from_ts.
    from db import fetch
    seed_rows = fetch(
        """
        SELECT from_stage, to_stage, cycle_ts, reason
        FROM transition_history_v2
        WHERE exchange = %s AND symbol = %s AND cycle_ts < %s
        ORDER BY cycle_ts DESC, created_at DESC
        LIMIT 1
        """,
        (exchange.upper(), symbol.upper(), from_ts),
    )
    state_map = {}
    if seed_rows:
        seed = seed_rows[0]
        seed_stage = int(seed.get("to_stage") or 0)
        seed_ts = seed.get("cycle_ts")
        trigger_rows = fetch(
            """
            SELECT cycle_ts
            FROM transition_history_v2
            WHERE exchange = %s AND symbol = %s AND from_stage = 0 AND to_stage = 1 AND cycle_ts < %s
            ORDER BY cycle_ts ASC
            LIMIT 1
            """,
            (exchange.upper(), symbol.upper(), from_ts),
        )
        trigger_ts = trigger_rows[0]["cycle_ts"] if trigger_rows else seed_ts
        age_minutes = max(0.0, (from_ts - seed_ts).total_seconds() / 60.0) if seed_ts else 0.0
        state_map[(exchange.upper(), symbol.upper())] = {
            "exchange": exchange.upper(),
            "symbol": symbol.upper(),
            "current_stage": seed_stage,
            "oi_stage_age_minutes": age_minutes,
            "latest_cycle_ts": seed_ts,
            "growth_trigger_ts": trigger_ts.isoformat() if trigger_ts else None,
        }
    records = []
    for cycle_ts in cycles:
        for row in updates.get(cycle_ts, []):
            latest.setdefault((row["exchange"], row["symbol"]), {}).setdefault(row["window_code"], {})[row["metric"]] = row
        core_rows, _, _, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(latest, cycle_ts=cycle_ts, previous_state_map=state_map)
        for row in core_rows:
            if str(row[0]).upper() == exchange.upper() and str(row[1]).upper() == symbol.upper():
                records.append(cycle_record(row, latest))
    return {"status": "ok", "exchange": exchange.upper(), "symbol": symbol.upper(), "window_source": source, "from": cycles[0].isoformat(), "to": cycles[-1].isoformat(), "cycles": records}


def main() -> None:
    p = argparse.ArgumentParser(description="Reconstruct one Duck exchange/symbol interval without writing production data")
    p.add_argument("--exchange", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--from-ts", required=True, help="ISO timestamp, e.g. 2026-09-01T14:35:00+03:00")
    p.add_argument("--to-ts", required=True)
    p.add_argument("--window-source", choices=["auto", "hot", "history"], default="auto")
    p.add_argument("--json-out")
    args = p.parse_args()
    result = reconstruct(args.exchange, args.symbol, parse_ts(args.from_ts), parse_ts(args.to_ts), args.window_source)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    print(rendered)
    if args.json_out:
        Path(args.json_out).write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
