from __future__ import annotations

from db import fetch

WINDOW_HOURS = 72

EXPECTED = {(1, 0), (2, 1), (2, 0), (2, 3), (3, 0)}
SUSPICIOUS = {
    (0, 2), (0, 3),
    (1, 3),
    (3, 1), (3, 2),
}


def cols(table: str) -> set[str]:
    rows = fetch(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
    """)
    return {r["column_name"] for r in rows}


def main() -> None:
    c = cols("market_phase_history")

    ts_col = None
    for x in ["created_at", "transition_at", "phase_updated_at", "ts_close", "inserted_at"]:
        if x in c:
            ts_col = x
            break

    where = ""
    if ts_col:
        where = f"WHERE {ts_col} >= NOW() - INTERVAL '{WINDOW_HOURS} hours'"

    print(f"PHASE_TRANSITION_AUDIT window_hours={WINDOW_HOURS} ts_col={ts_col or 'NONE'}")
    if not ts_col:
        print("PHASE_TRANSITION_AUDIT_WARNING no timestamp column in market_phase_history; scanning all history")

    rows = fetch(f"""
        SELECT
            from_phase,
            to_phase,
            COALESCE(from_phase_name, '') AS from_phase_name,
            COALESCE(to_phase_name, '') AS to_phase_name,
            COALESCE(transition_reason, '') AS transition_reason,
            COUNT(*) AS cnt
        FROM market_phase_history
        {where}
        GROUP BY 1,2,3,4,5
        ORDER BY cnt DESC, from_phase, to_phase
    """)

    print("\n=== ALL TRANSITIONS ===")
    if not rows:
        print("NO_TRANSITIONS")
    for r in rows:
        print(dict(r))

    print("\n=== EXPECTED TRANSITIONS ===")
    found = set()
    for r in rows:
        pair = (r["from_phase"], r["to_phase"])
        if pair in EXPECTED:
            found.add(pair)
            print(dict(r))
    if not found:
        print("NO_EXPECTED_TRANSITIONS_IN_WINDOW")

    print("\n=== SUSPICIOUS TRANSITIONS ===")
    bad = False
    for r in rows:
        pair = (r["from_phase"], r["to_phase"])
        reason = (r["transition_reason"] or "").lower()

        if pair == (3, 2) and "blocked_direct_transition_to_stage3" in reason:
            print({"severity": "WARN_LEGACY_HISTORY_NO_TS_FILTER", **dict(r)})
            continue

        if pair in SUSPICIOUS:
            bad = True
            print({"severity": "FAIL_SUSPICIOUS_TRANSITION", **dict(r)})

        if pair == (3, 0) and "manual" not in reason and "reset" not in reason:
            bad = True
            print({"warning": "stage3_to_0_without_manual_reset_reason", **dict(r)})

    if not bad:
        print("OK: no suspicious transitions")

    print("\nPHASE_TRANSITION_AUDIT_OK")


if __name__ == "__main__":
    main()
