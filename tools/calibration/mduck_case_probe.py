from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path("/home/alexey/openclaw/apps/liq-bot-duck")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db import fetch  # type: ignore
from phase_common import classify_oi_slope, value_slope_ratio  # type: ignore


WINDOWS = ["15м", "30м", "1ч", "4ч"]


def parse_iso(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def load_cases() -> list[dict]:
    if len(sys.argv) > 1:
        return json.loads(Path(sys.argv[1]).read_text())
    return json.load(sys.stdin)


def fetch_transitions(exchange: str, symbol: str, ts: datetime) -> list[dict]:
    return fetch(
        """
        SELECT from_stage, to_stage, cycle_ts, stage_age_before_transition, reason
        FROM transition_history_v2
        WHERE exchange = %s
          AND symbol = %s
          AND cycle_ts BETWEEN %s AND %s
        ORDER BY cycle_ts
        """,
        (exchange, symbol, ts - timedelta(hours=8), ts + timedelta(hours=2)),
    )


def fetch_oi_rows(exchange: str, symbol: str, ts: datetime) -> list[dict]:
    return fetch(
        """
        SELECT window_code, ts_close, open_value, high_value, low_value, close_value, delta_pct
        FROM aggregate_windows_history
        WHERE metric = 'OI'
          AND exchange = %s
          AND symbol = %s
          AND window_code IN ('15м', '30м', '1ч', '4ч')
          AND ts_close BETWEEN %s AND %s
        ORDER BY ts_close, window_code
        """,
        (exchange, symbol, ts - timedelta(hours=2), ts + timedelta(minutes=30)),
    )


def build_snapshot(rows: list[dict], ref_ts: datetime) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for row in rows:
        row_ts = row["ts_close"]
        if row_ts is None or row_ts > ref_ts:
            continue
        latest[row["window_code"]] = row
    out: dict[str, dict] = {}
    for window in WINDOWS:
        row = latest.get(window)
        if not row:
            out[window] = {"class": "нет_данных"}
            continue
        ratio = value_slope_ratio(row)
        out[window] = {
            "ts_close": row["ts_close"].isoformat(),
            "ratio": round(ratio, 6),
            "class": classify_oi_slope(window, ratio),
            "delta_pct": round(float(row.get("delta_pct") or 0.0), 4),
        }
    return out


def main() -> None:
    cases = load_cases()
    report: list[dict] = []
    for case in cases:
        exchange = case["exchange"]
        symbol = case["symbol"]
        ref_ts = parse_iso(case["ts"])
        transitions = fetch_transitions(exchange, symbol, ref_ts)
        oi_rows = fetch_oi_rows(exchange, symbol, ref_ts)

        points = []
        unique_ts = sorted({row["ts_close"] for row in oi_rows if row.get("ts_close")})
        for point_ts in unique_ts[-8:]:
            points.append(
                {
                    "ts": point_ts.isoformat(),
                    "oi": build_snapshot(oi_rows, point_ts),
                }
            )

        report.append(
            {
                "symbol": symbol,
                "exchange": exchange,
                "ref_ts": ref_ts.isoformat(),
                "transitions": [
                    {
                        "from": row["from_stage"],
                        "to": row["to_stage"],
                        "cycle_ts": row["cycle_ts"].isoformat() if row["cycle_ts"] else None,
                        "age": row["stage_age_before_transition"],
                        "reason": row["reason"],
                    }
                    for row in transitions
                ],
                "snapshots": points,
            }
        )

    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
