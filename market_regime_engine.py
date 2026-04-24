from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from db import fetch, replace_market_regime
from logger import log


def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _confidence(score: float) -> str:
    if score >= 75.0:
        return "высокая"
    if score >= 50.0:
        return "средняя"
    return "низкая"


def _reason_for_invalid(row: dict) -> str:
    reason = row.get("invalid_reason")
    if reason:
        return f"данные невалидны: {reason}"
    return "данные невалидны: причина не указана"


def _classify(row: dict) -> tuple[str, str, str]:
    # Фаза 2. Это НЕ торговый сигнал. Это диагностика режима.
    market_state = row["market_state"]

    continuation = _safe_float(row.get("continuation_score"))
    exhaustion = _safe_float(row.get("exhaustion_score"))
    compression = _safe_float(row.get("compression_score"))
    oi_delta = _safe_float(row.get("oi_delta_pct"))
    price_delta = _safe_float(row.get("price_delta_pct"))
    volume_delta = _safe_float(row.get("volume_delta_pct"))
    range_width = _safe_float(row.get("range_width_pct"))

    if market_state == "invalid_data":
        return "invalid_data", "низкая", _reason_for_invalid(row)

    if market_state == "продолжение":
        return (
            "continuation",
            _confidence(continuation),
            (
                "продолжение: удержание движения, "
                f"оценка продолжения={continuation:.2f}, "
                f"OI={oi_delta:.4f}%, цена={price_delta:.4f}%, объём={volume_delta:.4f}%"
            ),
        )

    if market_state == "выдох":
        return (
            "exhaustion",
            _confidence(exhaustion),
            (
                "выдох: импульс после топлива, "
                f"оценка выдоха={exhaustion:.2f}, "
                f"OI={oi_delta:.4f}%, цена={price_delta:.4f}%, объём={volume_delta:.4f}%"
            ),
        )

    if market_state == "сжатие":
        return (
            "compression",
            _confidence(compression),
            (
                "сжатие: узкий диапазон и накопление, "
                f"оценка сжатия={compression:.2f}, "
                f"ширина={range_width:.4f}%, OI={oi_delta:.4f}%"
            ),
        )

    if market_state == "диапазон":
        score = max(
            0.0,
            100.0
            - abs(price_delta) * 80.0
            - abs(oi_delta) * 40.0
            - max(0.0, range_width - 1.2) * 30.0,
        )
        return (
            "range",
            _confidence(score),
            (
                "диапазон: цена и OI без сильного импульса, "
                f"оценка диапазона={score:.2f}, "
                f"ширина={range_width:.4f}%, OI={oi_delta:.4f}%, цена={price_delta:.4f}%"
            ),
        )

    return (
        "neutral",
        "низкая",
        (
            "нейтрально: нет сильного сценария, "
            f"продолжение={continuation:.2f}, выдох={exhaustion:.2f}, сжатие={compression:.2f}"
        ),
    )


def rebuild_market_regime() -> int:
    rows = fetch(
        '''
        SELECT
            calculated_at,
            ts_close,
            exchange,
            symbol,
            timeframe,
            oi_delta_pct,
            price_delta_pct,
            volume_delta_pct,
            oi_velocity,
            oi_acceleration,
            range_width_pct,
            continuation_score,
            exhaustion_score,
            compression_score,
            market_state,
            invalid_reason
        FROM market_research
        ORDER BY exchange, symbol, timeframe, ts_close
        '''
    )

    calculated_at = datetime.now(timezone.utc)
    out = []

    for r in rows:
        scenario, confidence, reason = _classify(r)

        out.append(
            (
                calculated_at,
                r["ts_close"],
                r["exchange"],
                r["symbol"],
                r["timeframe"],
                r["market_state"],
                scenario,
                confidence,
                reason,
                r.get("oi_delta_pct"),
                r.get("price_delta_pct"),
                r.get("volume_delta_pct"),
                r.get("range_width_pct"),
                r.get("continuation_score"),
                r.get("exhaustion_score"),
                r.get("compression_score"),
                r.get("invalid_reason"),
            )
        )

    replace_market_regime(out)

    counts = Counter(x[6] for x in out)
    summary = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
    log(f"market regime rebuilt: rows={len(out)} {summary}")

    return len(out)
