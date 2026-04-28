from __future__ import annotations

from datetime import datetime, timezone, timedelta

from db import fetch, replace_market_phase, insert_market_phase_history


PHASE_NAMES = {
    0: "stage_0_no_interest",
    1: "stage_1_watch",
    2: "stage_2_strong_watch",
    3: "stage_3_alert_manual_reset",
}

GOOD_STAGE1_OI = {"спокойный боковик", "плавный набор", "удержание после роста"}
BAD_OI = {"нисходящий OI", "пила", "всплеск без удержания", "перегрев", "распределение"}
STAGE2_OI = {"ступенчатый набор", "ускорение", "агрессивный набор", "удержание после роста"}
STAGE3_OI = {"агрессивный набор", "ускорение"}

OK_PRICE = {"сжатие", "спокойный боковик", "широкий боковик", "возврат"}
BAD_PRICE = {"импульс вверх", "импульс вниз", "расширение вверх", "расширение вниз", "памп", "дамп"}

VOLUME_BOOST = {"всплеск объема", "аномальный объем"}
VOLUME_OK = {"обычный объем", "объем падает", "объем растет", "всплеск объема"}
VOLUME_FADE = {"объем падает"}

POSITIVE_OI_TRENDS = {"плавный рост", "устойчивый рост", "агрессивный рост"}
STRONG_OI_TRENDS = {"устойчивый рост", "агрессивный рост"}

STAGE1_MIN_AGE = timedelta(hours=1)
STAGE2_MIN_AGE = timedelta(minutes=15)


def _v(row, key, default=None):
    try:
        return row[key]
    except Exception:
        return default


def _as_int(value, default: int = 5) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _age_ok(started_at, now: datetime, minimum: timedelta) -> bool:
    if not started_at:
        return False
    try:
        return now - started_at >= minimum
    except Exception:
        return False


def _priority(phase: int, oi_priority, oi_structure, volume_structure, price_structure) -> str:
    oi_p = _as_int(oi_priority)

    if phase >= 3:
        return "P1"

    if phase == 2 and oi_p <= 2:
        return "P1"

    if phase == 2:
        return "P2"

    if phase == 1 and oi_p <= 2:
        return "P2"

    if phase == 1:
        return "P3"

    if oi_structure in BAD_OI or price_structure in BAD_PRICE:
        return "P5"

    return "P4"


def _status(phase: int, oi_structure, oi_hold_state, volume_hold_state, oi_trend_1h=None, volume_structure=None) -> str:
    if phase == 0:
        return "cooling"

    if oi_structure in {"перегрев", "распределение", "всплеск без удержания"}:
        return "exhausted"

    if oi_structure == "удержание после роста":
        return "holding"

    if oi_hold_state in {"holding", "hold", "удержание", "trying_hold"}:
        return "holding"

    if volume_hold_state in {"holding", "hold", "удержание", "trying_hold"}:
        return "holding"

    if oi_trend_1h in {"нисходящий", "снижение"} or volume_structure in VOLUME_FADE:
        return "cooling"

    return "active"


def _confidence(phase: int, oi_priority, oi_structure, volume_structure, price_structure) -> str:
    oi_p = _as_int(oi_priority)

    if phase == 3:
        if oi_p <= 1 and volume_structure in VOLUME_BOOST and price_structure not in BAD_PRICE:
            return "HIGH"
        return "MEDIUM"

    if phase == 2:
        if oi_p <= 2 and price_structure not in BAD_PRICE:
            return "MEDIUM"
        return "LOW"

    if phase == 1:
        return "LOW"

    return "NONE"


def _dmd_level(phase: int, oi_priority, volume_structure) -> str:
    oi_p = _as_int(oi_priority)

    if phase == 3 and oi_p <= 1 and volume_structure in VOLUME_BOOST:
        return "DMD_HIGH"

    if phase >= 2 and oi_p <= 2:
        return "DMD_MEDIUM"

    if phase >= 1:
        return "DMD_LOW"

    return "DMD_NONE"


def _decide_phase(prev_phase: int, row, now: datetime) -> tuple[int, str]:
    oi_structure = _v(row, "oi_structure")
    oi_quality = _v(row, "oi_quality")
    oi_priority = _as_int(_v(row, "oi_priority", 5))
    oi_hold_state = _v(row, "oi_hold_state")
    oi_trend_1h = _v(row, "oi_trend_1h")
    oi_trend_4h = _v(row, "oi_trend_4h")
    price_structure = _v(row, "price_structure")
    volume_structure = _v(row, "volume_structure")

    stage1_started_at = _v(row, "stage1_started_at")
    stage2_started_at = _v(row, "stage2_started_at")

    stage1_age_ok = _age_ok(stage1_started_at, now, STAGE1_MIN_AGE)
    stage2_age_ok = _age_ok(stage2_started_at, now, STAGE2_MIN_AGE)

    # 2 -> 0 / 1 -> 0: мусорный OI ломает фазу.
    if oi_structure in BAD_OI:
        return 0, f"bad_oi_structure={oi_structure}"

    # Price сам не управляет фазой, но хаос без OI-поддержки отправляет в 0.
    if price_structure in BAD_PRICE and oi_structure not in STAGE2_OI:
        return 0, f"bad_price_without_oi_support price={price_structure} oi={oi_structure}"

    # 2 -> 3: только после Stage 2 >= 15 минут.
    stage3_ok = (
        prev_phase == 2
        and stage2_age_ok
        and oi_priority <= 2
        and (
            oi_structure in STAGE3_OI
            or oi_quality == "агрессивный набор"
            or oi_trend_1h in STRONG_OI_TRENDS
            or oi_trend_4h in STRONG_OI_TRENDS
        )
    )

    if stage3_ok:
        return 3, "stage2_to_stage3: stage2_age_ok strong_oi_slope quality_ok"

    # 1 -> 2: Stage 1 >= 1 час, OI P1/P2/P3, slope устойчивый или есть удержание.
    stage2_ok = (
        prev_phase in {1, 2}
        and (
            prev_phase == 2
            or (
                prev_phase == 1
                and stage1_age_ok
            )
        )
        and oi_priority <= 3
        and (
            oi_structure in STAGE2_OI
            or oi_trend_1h in POSITIVE_OI_TRENDS
            or oi_trend_4h in POSITIVE_OI_TRENDS
            or oi_hold_state in {"holding", "hold", "удержание", "trying_hold"}
        )
    )

    if stage2_ok:
        return 2, "stage1_to_stage2_or_hold: stage1_age_ok oi_slope_or_hold"

    # 2 -> 1: OI остыл, но мусора нет.
    if prev_phase == 2 and oi_structure in GOOD_STAGE1_OI:
        return 1, "downgrade_stage2_to_stage1: oi_cooled_no_bad_oi"

    # 0 -> 1: OI ожил, структура не мусор, price не хаос.
    stage1_ok = (
        oi_structure in GOOD_STAGE1_OI
        and price_structure in OK_PRICE
        and volume_structure in VOLUME_OK.union({None})
    )

    if stage1_ok:
        return 1, "stage0_to_stage1_or_hold: early_oi_interest"

    return 0, "no_valid_phase_conditions"


def rebuild_market_phase() -> int:
    now = datetime.now(timezone.utc)

    rows = fetch("""
        WITH latest_oi AS (
            SELECT DISTINCT ON (exchange, symbol, timeframe)
                *
            FROM market_oi_slope
            ORDER BY exchange, symbol, timeframe, ts_close DESC
        ),
        latest_price AS (
            SELECT DISTINCT ON (exchange, symbol, timeframe)
                *
            FROM market_price_state
            ORDER BY exchange, symbol, timeframe, ts_close DESC
        ),
        latest_volume AS (
            SELECT DISTINCT ON (exchange, symbol, timeframe)
                *
            FROM market_volume_state
            ORDER BY exchange, symbol, timeframe, ts_close DESC
        ),
        prev_phase AS (
            SELECT DISTINCT ON (exchange, symbol, timeframe)
                *
            FROM market_phase
            ORDER BY exchange, symbol, timeframe, phase_updated_at DESC NULLS LAST
        )
        SELECT
            oi.exchange,
            oi.symbol,
            oi.timeframe,
            oi.ts_close,

            COALESCE(pp.phase, 0) AS prev_phase,
            pp.phase_name AS prev_phase_name,
            pp.phase_started_at,
            pp.stage1_started_at,
            pp.stage2_started_at,
            pp.stage3_started_at,

            oi.oi_structure,
            oi.oi_quality,
            oi.oi_priority,
            oi.oi_hold_state,
            oi.oi_trend_1h,
            oi.oi_trend_4h,
            oi.oi_trend_24h,

            pr.price_structure,
            pr.price_quality,
            pr.price_slope_state,

            vo.volume_structure,
            vo.volume_quality,
            vo.volume_hold_state
        FROM latest_oi oi
        LEFT JOIN latest_price pr
          ON pr.exchange=oi.exchange AND pr.symbol=oi.symbol AND pr.timeframe=oi.timeframe
        LEFT JOIN latest_volume vo
          ON vo.exchange=oi.exchange AND vo.symbol=oi.symbol AND vo.timeframe=oi.timeframe
        LEFT JOIN prev_phase pp
          ON pp.exchange=oi.exchange AND pp.symbol=oi.symbol AND pp.timeframe=oi.timeframe
    """)

    phase_rows = []
    history_rows = []

    for r in rows:
        prev_phase = _as_int(_v(r, "prev_phase", 0), 0)

        # Stage 3 не снимается автоматически.
        # Это правило должно стоять ДО расчета новой фазы,
        # чтобы история не получала ложные переходы 3 -> 2 или 3 -> 0.
        if prev_phase == 3:
            new_phase = 3
            transition_reason = "stage3_locked_until_manual_reset"
        else:
            new_phase, transition_reason = _decide_phase(prev_phase, r, now)

        # Прямые 0->3 и 1->3 запрещены.
        if new_phase == 3 and prev_phase != 2:
            new_phase = 2
            transition_reason = "blocked_direct_transition_to_stage3"

        phase_name = PHASE_NAMES[new_phase]

        status = _status(
            new_phase,
            _v(r, "oi_structure"),
            _v(r, "oi_hold_state"),
            _v(r, "volume_hold_state"),
            _v(r, "oi_trend_1h"),
            _v(r, "volume_structure"),
        )

        priority = _priority(
            new_phase,
            _v(r, "oi_priority"),
            _v(r, "oi_structure"),
            _v(r, "volume_structure"),
            _v(r, "price_structure"),
        )

        confidence = _confidence(
            new_phase,
            _v(r, "oi_priority"),
            _v(r, "oi_structure"),
            _v(r, "volume_structure"),
            _v(r, "price_structure"),
        )

        dmd = _dmd_level(new_phase, _v(r, "oi_priority"), _v(r, "volume_structure"))

        prev_started = _v(r, "phase_started_at")
        stage1_started = _v(r, "stage1_started_at")
        stage2_started = _v(r, "stage2_started_at")
        stage3_started = _v(r, "stage3_started_at")

        phase_started_at = prev_started if new_phase == prev_phase and prev_started else now

        if new_phase == 0:
            stage1_started = None
            stage2_started = None
            stage3_started = None

        if new_phase == 1 and not stage1_started:
            stage1_started = now

        if new_phase == 2 and not stage2_started:
            stage2_started = now

        if new_phase == 3 and not stage3_started:
            stage3_started = now

        reason = (
            f"{transition_reason}; "
            f"oi_structure={_v(r,'oi_structure')}; "
            f"oi_quality={_v(r,'oi_quality')}; "
            f"oi_priority={_v(r,'oi_priority')}; "
            f"oi_hold_state={_v(r,'oi_hold_state')}; "
            f"oi_trend_1h={_v(r,'oi_trend_1h')}; "
            f"oi_trend_4h={_v(r,'oi_trend_4h')}; "
            f"price_structure={_v(r,'price_structure')}; "
            f"volume_structure={_v(r,'volume_structure')}; "
            f"volume_hold_state={_v(r,'volume_hold_state')}"
        )

        phase_rows.append((
            now,
            _v(r, "exchange"),
            _v(r, "symbol"),
            _v(r, "timeframe"),
            new_phase,
            phase_name,
            status,
            priority,
            phase_started_at,
            now,
            stage1_started,
            stage2_started,
            stage3_started,
            new_phase == 3,
            dmd,
            confidence,
            _v(r, "oi_structure"),
            _v(r, "oi_quality"),
            _v(r, "oi_priority"),
            _v(r, "oi_hold_state"),
            _v(r, "oi_trend_1h"),
            _v(r, "oi_trend_4h"),
            _v(r, "oi_trend_24h"),
            _v(r, "price_structure"),
            _v(r, "price_quality"),
            _v(r, "price_slope_state"),
            _v(r, "volume_structure"),
            _v(r, "volume_quality"),
            _v(r, "volume_hold_state"),
            transition_reason,
            reason,
        ))

        if new_phase != prev_phase:
            history_rows.append((
                now,
                _v(r, "exchange"),
                _v(r, "symbol"),
                _v(r, "timeframe"),
                prev_phase,
                new_phase,
                _v(r, "prev_phase_name") or PHASE_NAMES.get(prev_phase),
                phase_name,
                status,
                priority,
                transition_reason,
                _v(r, "oi_structure"),
                _v(r, "oi_quality"),
                _v(r, "oi_priority"),
                _v(r, "oi_hold_state"),
                _v(r, "price_structure"),
                _v(r, "price_quality"),
                _v(r, "volume_structure"),
                _v(r, "volume_quality"),
            ))

    replace_market_phase(phase_rows)
    insert_market_phase_history(history_rows)

    print(f"market phase rebuilt: rows={len(phase_rows)} transitions={len(history_rows)}")
    return len(phase_rows)
