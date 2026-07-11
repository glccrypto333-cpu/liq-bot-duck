from chart_screenshot.coinglass import dropdown_timeframe_has_reliable_evidence


def test_dropdown_timeframe_requires_matching_legend_evidence() -> None:
    assert dropdown_timeframe_has_reliable_evidence("4H", "4h")
    assert not dropdown_timeframe_has_reliable_evidence("4H", None)
    assert not dropdown_timeframe_has_reliable_evidence("4H", "5")
