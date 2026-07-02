from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

MSK = timezone(timedelta(hours=3))


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _fmt_minutes(value: Any) -> str:
    try:
        total_minutes = int(round(float(value or 0)))
    except Exception:
        return "н/д"
    hours, minutes = divmod(max(total_minutes, 0), 60)
    if hours <= 0:
        return f"{minutes}м"
    return f"{hours}ч {minutes}м"


def _parse_dt(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _format_ts_moscow_short(value: Any) -> str:
    dt = _parse_dt(value)
    if not dt:
        return "н/д"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(MSK).strftime("%d.%m %H:%M МСК")


def _find_first_transition(history_rows: list[dict[str, Any]], to_stage: int) -> dict[str, Any] | None:
    for row in history_rows or []:
        if _safe_int(row.get("to_stage")) == to_stage:
            return row
    return None


def _find_last_transition(history_rows: list[dict[str, Any]], to_stage: int) -> dict[str, Any] | None:
    for row in history_rows or []:
        if _safe_int(row.get("to_stage")) == to_stage:
            return row
    return None


def _phase_zero_line(history_rows: list[dict[str, Any]], humanize_reason: Callable[[Any], str] | None = None) -> str | None:
    row = _find_first_transition(history_rows, 1)
    if not row:
        return None
    age_text = _fmt_minutes(row.get("stage_age_before_transition"))
    ts_text = _format_ts_moscow_short(row.get("cycle_ts"))
    return f"<b>Фаза 0</b> - {age_text} - запрет снят: {ts_text}"


def build_phase_history_lines(
    history_rows: list[dict[str, Any]],
    *,
    current_stage: int,
    current_age_minutes: Any,
    humanize_reason: Callable[[Any], str] | None = None,
) -> list[str]:
    lines = ["<b>История по фазам</b>"]
    phase0 = _phase_zero_line(history_rows, humanize_reason)
    if phase0:
        lines.append(phase0)

    for stage in range(1, current_stage):
        entry_row = _find_first_transition(history_rows, stage)
        exit_row = _find_first_transition(history_rows, stage + 1)
        if not entry_row:
            continue
        age_minutes = exit_row.get("stage_age_before_transition") if exit_row else current_age_minutes
        line = f"<b>Фаза {stage}</b> - {_fmt_minutes(age_minutes)} - вход: {_format_ts_moscow_short(entry_row.get('cycle_ts'))}"
        lines.append(line)

    current_entry = _find_last_transition(history_rows, current_stage)
    if current_stage > 0:
        lines.append(
            f"<b>Фаза {current_stage}</b> - {_fmt_minutes(current_age_minutes)} - "
            f"вход: {_format_ts_moscow_short(current_entry.get('cycle_ts') if current_entry else None)}"
        )
    return lines
