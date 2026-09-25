from autonomous_oi_service import build_quarantine_lifecycle


def test_quarantine_lifecycle_counts_unique_pairs_not_issue_categories():
    problems = [
        {"exchange": "BYBIT", "symbol": "CYPHUSDT"},
        {"exchange": "BYBIT", "symbol": "HUTUSDT"},
        {"exchange": "BYBIT", "symbol": "PATHUSDT"},
    ]
    quality_rows = [
        {"exchange": "BYBIT", "symbol": "CYPHUSDT", "reason": "карантин_качества_данных"},
        {"exchange": "BYBIT", "symbol": "HUTUSDT", "reason": "карантин_качества_данных"},
        {"exchange": "BYBIT", "symbol": "PATHUSDT", "reason": "карантин_качества_данных"},
    ]

    lifecycle = build_quarantine_lifecycle(problems, quality_rows)

    assert lifecycle["active_total"] == 3
    assert lifecycle["data_quality_active_total"] == 3


def test_quarantine_lifecycle_excludes_nonblocking_warmup_pairs():
    lifecycle = build_quarantine_lifecycle(
        [{"exchange": "BYBIT", "symbol": "CYPHUSDT", "blocking": False}],
        [],
    )

    assert lifecycle["active_total"] == 0
