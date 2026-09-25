import sys
sys.path.insert(0, '.')

from phase_service import early_stage3_block_reason


def oi(a='good_up', b='good_up', h='strong_up', q='good_up'):
    return {
        'oi_slope_class_15m': a,
        'oi_slope_class_30m': b,
        'oi_slope_class_1h': h,
        'oi_slope_class_4h': q,
    }


def test_early_stage3_reports_trigger_age_block():
    reason = early_stage3_block_reason(
        target_stage=2,
        oi_summary=oi(),
        stage2_age_minutes=15.0,
        trigger_age_minutes=29.0,
    )
    assert reason == 'trigger_age_below_30m:29.00'


def test_early_stage3_reports_first_failing_oi_gate():
    reason = early_stage3_block_reason(
        target_stage=2,
        oi_summary=oi(a='weak_up'),
        stage2_age_minutes=15.0,
        trigger_age_minutes=40.0,
    )
    assert reason == 'oi_15m_not_mature:weak_up'
