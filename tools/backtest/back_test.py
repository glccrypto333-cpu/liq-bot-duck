from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path("/home/alexey/openclaw/apps/liq-bot-duck")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from autonomous_oi_replay import (
    _parse_ts,
    load_batch_window_updates,
    load_latest_window_map_for_replay,
)
from autonomous_oi_service import compute_autonomous_oi_snapshot_from_latest_window_map, load_previous_core_state_map
from db import fetch


MSK = ZoneInfo("Europe/Moscow")


@dataclass
class ReplayPoint:
    cycle_ts: datetime
    stage: int
    transition_permission: str
    reason: str
    trigger_ts: datetime | None
    oi_15m: str | None
    oi_30m: str | None
    oi_1h: str | None
    oi_4h: str | None
    price_state: str | None
    blocked_by_price: bool
    blocked_stage_max: int | None


def parse_user_ts(value: str) -> datetime:
    text = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(timezone.utc)


def fmt_msk(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(MSK).strftime("%Y-%m-%d %H:%M:%S МСК")


def load_cycle_timestamps_between(
    start_ts: datetime,
    end_ts: datetime,
    window_source: str,
) -> tuple[list[datetime], str]:
    def query(table_name: str) -> list[datetime]:
        rows = fetch(
            f"""
            SELECT source_cycle_ts
            FROM {table_name}
            WHERE source_cycle_ts IS NOT NULL
              AND source_cycle_ts >= %s
              AND source_cycle_ts <= %s
            GROUP BY source_cycle_ts
            ORDER BY source_cycle_ts ASC
            """,
            (start_ts, end_ts),
        )
        return [row["source_cycle_ts"] for row in rows]

    if window_source == "hot":
        return query("aggregate_windows"), "hot"
    if window_source == "history":
        return query("aggregate_windows_history"), "history"

    hot = query("aggregate_windows")
    if hot:
        return hot, "hot"
    return query("aggregate_windows_history"), "history"


def build_initial_state(state_mode: str) -> dict[tuple[str, str], dict]:
    if state_mode == "fresh":
        return {}
    if state_mode == "live":
        return load_previous_core_state_map()
    raise ValueError(f"unknown state_mode={state_mode}")


def replay_single_case(
    exchange: str,
    symbol: str,
    start_ts: datetime,
    end_ts: datetime,
    window_source: str,
    state_mode: str,
    control_points: list[datetime],
) -> dict:
    cycles, resolved_window_source = load_cycle_timestamps_between(start_ts, end_ts, window_source)
    if not cycles:
        return {
            "exchange": exchange,
            "symbol": symbol,
            "state_mode": state_mode,
            "window_source": resolved_window_source,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "cycles": 0,
            "error": "нет_циклов_в_интервале",
        }

    tracked_key = (exchange, symbol)
    tracked_pairs = [tracked_key]
    latest_window_map = load_latest_window_map_for_replay(
        cycles[0],
        window_source=resolved_window_source,
        tracked_pairs=tracked_pairs,
    )
    updates_by_cycle = load_batch_window_updates(
        cycles,
        window_source=resolved_window_source,
        tracked_pairs=tracked_pairs,
    )
    state_map = build_initial_state(state_mode)

    first_stage_at: dict[int, datetime | None] = {1: None, 2: None, 3: None}
    transitions: list[dict] = []
    control_targets = sorted(control_points)
    control_index = 0
    control_rows = []
    last_point: ReplayPoint | None = None
    previous_stage: int | None = None

    for cycle_ts in cycles:
        for row in updates_by_cycle.get(cycle_ts, []):
            key = (row["exchange"], row["symbol"])
            latest_window_map.setdefault(key, {})
            latest_window_map[key].setdefault(row["window_code"], {})
            latest_window_map[key][row["window_code"]][row["metric"]] = row

        _core_rows, _window_rows, _history_rows, state_map = compute_autonomous_oi_snapshot_from_latest_window_map(
            latest_window_map,
            cycle_ts=cycle_ts,
            previous_state_map=state_map,
        )

        state = state_map.get(tracked_key)
        if not state:
            continue

        current_stage = int(state.get("current_stage") or 0)
        trigger_text = state.get("growth_trigger_ts")
        trigger_ts = _parse_ts(trigger_text) if trigger_text else None
        point = ReplayPoint(
            cycle_ts=cycle_ts,
            stage=current_stage,
            transition_permission=str(state.get("oi_transition_permission") or ""),
            reason=str(state.get("decision_reason") or ""),
            trigger_ts=trigger_ts,
            oi_15m=state.get("oi_slope_class_15m"),
            oi_30m=state.get("oi_slope_class_30m"),
            oi_1h=state.get("oi_slope_class_1h"),
            oi_4h=state.get("oi_slope_class_4h"),
            price_state=state.get("price_state_summary"),
            blocked_by_price=bool(state.get("blocked_by_price")),
            blocked_stage_max=state.get("blocked_stage_max"),
        )
        while control_index < len(control_targets) and cycle_ts >= control_targets[control_index]:
            control_ts = control_targets[control_index]
            control_rows.append(
                {
                    "control_ts": control_ts.isoformat(),
                    "control_msk": fmt_msk(control_ts),
                    "snapshot": {
                        "cycle_ts": point.cycle_ts.isoformat(),
                        "cycle_msk": fmt_msk(point.cycle_ts),
                        "stage": point.stage,
                        "transition_permission": point.transition_permission,
                        "reason": point.reason,
                        "trigger_msk": fmt_msk(point.trigger_ts),
                        "oi_15m": point.oi_15m,
                        "oi_30m": point.oi_30m,
                        "oi_1h": point.oi_1h,
                        "oi_4h": point.oi_4h,
                        "price_state": point.price_state,
                        "blocked_by_price": point.blocked_by_price,
                        "blocked_stage_max": point.blocked_stage_max,
                    },
                }
            )
            control_index += 1

        for stage_value in (1, 2, 3):
            if current_stage >= stage_value and first_stage_at[stage_value] is None:
                first_stage_at[stage_value] = cycle_ts

        if previous_stage is None:
            previous_stage = current_stage
        elif previous_stage != current_stage:
            transitions.append(
                {
                    "cycle_ts": cycle_ts.isoformat(),
                    "cycle_msk": fmt_msk(cycle_ts),
                    "from_stage": previous_stage,
                    "to_stage": current_stage,
                    "transition_permission": point.transition_permission,
                    "reason": point.reason,
                    "oi_15m": point.oi_15m,
                    "oi_30m": point.oi_30m,
                    "oi_1h": point.oi_1h,
                    "oi_4h": point.oi_4h,
                    "price_state": point.price_state,
                    "blocked_by_price": point.blocked_by_price,
                    "blocked_stage_max": point.blocked_stage_max,
                }
            )
            previous_stage = current_stage

        last_point = point

    while control_index < len(control_targets):
        control_ts = control_targets[control_index]
        control_rows.append(
            {
                "control_ts": control_ts.isoformat(),
                "control_msk": fmt_msk(control_ts),
                "snapshot": None if last_point is None else {
                    "cycle_ts": last_point.cycle_ts.isoformat(),
                    "cycle_msk": fmt_msk(last_point.cycle_ts),
                    "stage": last_point.stage,
                    "transition_permission": last_point.transition_permission,
                    "reason": last_point.reason,
                    "trigger_msk": fmt_msk(last_point.trigger_ts),
                    "oi_15m": last_point.oi_15m,
                    "oi_30m": last_point.oi_30m,
                    "oi_1h": last_point.oi_1h,
                    "oi_4h": last_point.oi_4h,
                    "price_state": last_point.price_state,
                    "blocked_by_price": last_point.blocked_by_price,
                    "blocked_stage_max": last_point.blocked_stage_max,
                },
            }
        )
        control_index += 1

    return {
        "exchange": exchange,
        "symbol": symbol,
        "state_mode": state_mode,
        "window_source": resolved_window_source,
        "start_ts": start_ts.isoformat(),
        "start_msk": fmt_msk(start_ts),
        "end_ts": end_ts.isoformat(),
        "end_msk": fmt_msk(end_ts),
        "cycles": len(cycles),
        "first_stage_1_ts": first_stage_at[1].isoformat() if first_stage_at[1] else None,
        "first_stage_1_msk": fmt_msk(first_stage_at[1]),
        "first_stage_2_ts": first_stage_at[2].isoformat() if first_stage_at[2] else None,
        "first_stage_2_msk": fmt_msk(first_stage_at[2]),
        "first_stage_3_ts": first_stage_at[3].isoformat() if first_stage_at[3] else None,
        "first_stage_3_msk": fmt_msk(first_stage_at[3]),
        "final_stage": None if last_point is None else last_point.stage,
        "final_cycle_ts": None if last_point is None else last_point.cycle_ts.isoformat(),
        "final_cycle_msk": None if last_point is None else fmt_msk(last_point.cycle_ts),
        "final_reason": None if last_point is None else last_point.reason,
        "final_permission": None if last_point is None else last_point.transition_permission,
        "transitions": transitions,
        "control_points": control_rows,
    }


def print_human_report(report: dict) -> None:
    print("BACK TEST")
    print(f"Биржа: {report['exchange']}")
    print(f"Монета: {report['symbol']}")
    print(f"Режим состояния: {'чистый_реплей' if report['state_mode'] == 'fresh' else 'живое_накопленное'}")
    print(f"Источник окон: {report['window_source']}")
    print(f"Начало: {report.get('start_msk')}")
    print(f"Конец: {report.get('end_msk')}")
    print(f"Циклов: {report.get('cycles')}")
    if report.get("error"):
        print(f"Ошибка: {report['error']}")
        return

    print(f"Первая стадия 1: {report.get('first_stage_1_msk') or 'нет'}")
    print(f"Первая стадия 2: {report.get('first_stage_2_msk') or 'нет'}")
    print(f"Первая стадия 3: {report.get('first_stage_3_msk') or 'нет'}")
    print(f"Финальная стадия: {report.get('final_stage')}")
    print(f"Финальный цикл: {report.get('final_cycle_msk')}")
    print(f"Финальное разрешение: {report.get('final_permission')}")
    print(f"Финальная причина: {report.get('final_reason')}")

    print("")
    print("Переходы")
    if not report["transitions"]:
        print("- переходов нет")
    else:
        for item in report["transitions"]:
            print(
                f"- {item['cycle_msk']} | {item['from_stage']} -> {item['to_stage']} | "
                f"15м={item['oi_15m']} 30м={item['oi_30m']} 1ч={item['oi_1h']} 4ч={item['oi_4h']} | "
                f"цена={item['price_state']} | {item['reason']}"
            )

    if report["control_points"]:
        print("")
        print("Контрольные точки")
        for item in report["control_points"]:
            snap = item["snapshot"]
            if snap is None:
                print(f"- {item['control_msk']} | нет снимка")
                continue
            print(
                f"- {item['control_msk']} | стадия={snap['stage']} | "
                f"15м={snap['oi_15m']} 30м={snap['oi_30m']} 1ч={snap['oi_1h']} 4ч={snap['oi_4h']} | "
                f"цена={snap['price_state']} | {snap['reason']}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Канонический исторический прогон MightyDuck")
    parser.add_argument("--exchange", required=True, help="Биржа, например BYBIT или BINANCE")
    parser.add_argument("--symbol", required=True, help="Символ, например BELUSDT")
    parser.add_argument("--from-ts", required=True, help="Стартовое время. Если зона не указана, считается МСК")
    parser.add_argument("--to-ts", help="Конечное время. Если не указано, берется from-ts + hours")
    parser.add_argument("--hours", type=int, default=24, help="Длина прогона в часах, если to-ts не указан")
    parser.add_argument("--window-source", choices=["auto", "hot", "history"], default="auto")
    parser.add_argument("--state-mode", choices=["fresh", "live"], default="fresh")
    parser.add_argument("--control-ts", action="append", default=[], help="Контрольная точка в МСК")
    parser.add_argument("--report-json", help="Куда сохранить json-отчет")
    args = parser.parse_args()

    start_ts = parse_user_ts(args.from_ts)
    end_ts = parse_user_ts(args.to_ts) if args.to_ts else start_ts + timedelta(hours=args.hours)
    control_points = [parse_user_ts(value) for value in args.control_ts]

    report = replay_single_case(
        exchange=args.exchange.upper(),
        symbol=args.symbol.upper(),
        start_ts=start_ts,
        end_ts=end_ts,
        window_source=args.window_source,
        state_mode=args.state_mode,
        control_points=control_points,
    )

    print_human_report(report)
    if args.report_json:
        path = Path(args.report_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print("")
        print(f"JSON-отчет: {path}")


if __name__ == "__main__":
    main()
