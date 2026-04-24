from __future__ import annotations

def изменение_в_процентах(start: float | None, end: float | None) -> float | None:
    if start is None or end is None or start == 0:
        return None
    return ((end - start) / start) * 100.0

def abs_diff(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return abs(a - b)

def rel_diff_pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    if b == 0:
        return 0.0 if a == 0 else None
    return abs((a - b) / b) * 100.0
