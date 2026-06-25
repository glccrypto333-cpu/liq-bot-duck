from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

МОСКВА = timezone(timedelta(hours=3))


def сейчас_мск() -> datetime:
    return datetime.now(МОСКВА)


def iso_мск(value: datetime | None = None) -> str:
    dt = value or сейчас_мск()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(МОСКВА).isoformat()


def текст_мск(value: datetime | None = None, fmt: str = "%Y-%m-%d %H:%M:%S МСК") -> str:
    dt = value or сейчас_мск()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(МОСКВА).strftime(fmt)


def в_мск(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(МОСКВА)
