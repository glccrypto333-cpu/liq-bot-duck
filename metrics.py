from __future__ import annotations

def изменение_в_процентах(начало: float | None, конец: float | None) -> float | None:
    if начало is None or конец is None or начало == 0:
        return None
    return ((конец - начало) / начало) * 100.0

def абсолютное_расхождение(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return abs(a - b)

def относительное_расхождение_pct(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    if b == 0:
        return 0.0 if a == 0 else None
    return abs((a - b) / b) * 100.0

def класс_надёжности(оценка_качества: float, тип_состояния: str) -> str:
    if тип_состояния == "полная_сверка":
        if оценка_качества >= 85:
            return "A"
        if оценка_качества >= 70:
            return "B"
        if оценка_качества >= 50:
            return "C"
        return "мусор"
    if тип_состояния == "одна_биржа":
        if оценка_качества >= 75:
            return "одна_биржа"
        if оценка_качества >= 55:
            return "наблюдение"
        return "мусор"
    if тип_состояния == "слабая_межоконная_согласованность":
        if оценка_качества >= 65:
            return "слабая_согласованность"
        return "мусор"
    return "мусор"
