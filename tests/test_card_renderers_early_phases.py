from card_renderers import build_phase_history_lines


def test_early_phase_entries_are_labeled_in_history() -> None:
    rows = [
        {"to_stage": 3, "cycle_ts": "2026-09-02T05:35:00+00:00", "stage_age_before_transition": 30, "reason": "переход_2_3:ранний_выпуск_A"},
        {"to_stage": 2, "cycle_ts": "2026-09-02T05:05:00+00:00", "stage_age_before_transition": 30, "reason": "переход_1_2:ранний_выпуск_A"},
        {"to_stage": 1, "cycle_ts": "2026-08-31T23:55:00+00:00", "stage_age_before_transition": 50, "reason": "разрешен_вход_в_1"},
    ]
    lines = build_phase_history_lines(rows, current_stage=3, current_age_minutes=0)
    assert "<b>Фаза 1</b>" in lines[2]
    assert "<b>Фаза 2 Ранний</b>" in lines[3]
    assert "<b>Фаза 3 Ранний</b>" in lines[4]


def test_regular_phase_entries_keep_regular_labels() -> None:
    rows = [
        {"to_stage": 3, "cycle_ts": "2026-09-02T05:35:00+00:00", "stage_age_before_transition": 30, "reason": "переход_2_3:обычный"},
        {"to_stage": 2, "cycle_ts": "2026-09-02T05:05:00+00:00", "stage_age_before_transition": 30, "reason": "переход_1_2:обычный"},
        {"to_stage": 1, "cycle_ts": "2026-08-31T23:55:00+00:00", "stage_age_before_transition": 50, "reason": "разрешен_вход_в_1"},
    ]
    lines = build_phase_history_lines(rows, current_stage=3, current_age_minutes=0)
    assert all("Ранний" not in line for line in lines)


def test_phase_history_shows_volume_unlock_timestamp():
    from datetime import datetime, timezone

    lines = build_phase_history_lines(
        [
            {"to_stage": 1, "cycle_ts": "2026-09-19T23:50:00+00:00"},
            {"to_stage": 2, "cycle_ts": "2026-09-22T17:10:00+00:00", "reason": "ранний"},
            {"to_stage": 3, "cycle_ts": "2026-09-22T17:30:00+00:00", "reason": "ранний"},
        ],
        current_stage=3,
        current_age_minutes=0,
        volume_unlocked_at=datetime(2026, 9, 22, 17, 35, tzinfo=timezone.utc),
    )
    assert lines[-1] == "<b>Объёмы &gt;100%</b> - подтверждено: 22.09 20:35 МСК"
