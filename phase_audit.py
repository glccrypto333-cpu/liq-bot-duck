from __future__ import annotations

from db import fetch


CHECKS = {
    "bad_stage1_oi": """
        SELECT phase, oi_structure, oi_hold_state, oi_trend_1h, oi_trend_4h, price_structure, COUNT(*) cnt
        FROM market_phase
        WHERE phase = 1
          AND (
            oi_structure IN ('нисходящий OI','пила')
            OR oi_trend_1h IN ('нисходящий','снижение','down','strong_down')
            OR oi_trend_4h IN ('нисходящий','снижение','down','strong_down')
          )
        GROUP BY 1,2,3,4,5,6
        ORDER BY cnt DESC
    """,

    "bad_stage2_core": """
        SELECT phase, oi_structure, oi_hold_state, oi_trend_1h, price_structure, COUNT(*) cnt
        FROM market_phase
        WHERE phase = 2
          AND (
            oi_hold_state NOT IN ('holding','hold','удержание')
            OR oi_structure IN ('нисходящий OI','тишина','пила','всплеск без удержания')
            OR oi_trend_1h NOT IN ('плавный рост','устойчивый рост','агрессивный рост')
          )
        GROUP BY 1,2,3,4,5
        ORDER BY cnt DESC
    """,

    "bad_stage3_core": """
        SELECT phase, oi_structure, oi_hold_state, oi_trend_1h, oi_trend_4h, price_structure, COUNT(*) cnt
        FROM market_phase
        WHERE phase = 3
          AND (
            oi_hold_state NOT IN ('holding','hold','удержание')
            OR oi_structure IN ('нисходящий OI','тишина','пила','всплеск без удержания','распределение','перегрев')
            OR oi_trend_1h NOT IN ('плавный рост','устойчивый рост','агрессивный рост')
          )
        GROUP BY 1,2,3,4,5,6
        ORDER BY cnt DESC
    """,

    "stage3_price_squeeze_warning": """
        SELECT exchange, symbol, timeframe, phase, oi_structure, oi_hold_state, oi_trend_1h, price_structure, phase_updated_at
        FROM market_phase
        WHERE phase = 3
          AND price_structure IN ('импульс вниз','расширение вниз','дамп')
        ORDER BY phase_updated_at DESC
        LIMIT 50
    """,

    "stage3_transition_audit": """
        SELECT old_phase, new_phase, old_phase_name, new_phase_name, transition_reason, COUNT(*) cnt
        FROM market_phase_history
        WHERE new_phase = 3
        GROUP BY 1,2,3,4,5
        ORDER BY cnt DESC
    """,
}


def main() -> None:
    has_errors = False

    for name, sql in CHECKS.items():
        rows = fetch(sql)
        print(f"\n===== {name} rows={len(rows)} =====")

        for r in rows[:30]:
            print(dict(r))

        if name.startswith("bad_") and rows:
            has_errors = True

    if has_errors:
        raise SystemExit("PHASE_AUDIT_FAILED")

    print("\nPHASE_AUDIT_OK")


if __name__ == "__main__":
    main()
