import asyncio

from chart_screenshot.coinglass import _pick_dropdown_timeframe, dropdown_timeframe_has_reliable_evidence
from chart_screenshot.coinglass import is_coinglass_application_error


def test_application_error_page_is_detected():
    assert is_coinglass_application_error("Application error: a client-side exception has occurred") is True


def test_normal_page_is_not_application_error():
    assert is_coinglass_application_error("GBP GBP Live Price Charts | CoinGlass") is False


def test_dropdown_timeframe_requires_matching_legend_evidence() -> None:
    assert dropdown_timeframe_has_reliable_evidence("4H", "4h")
    assert not dropdown_timeframe_has_reliable_evidence("4H", None)
    assert not dropdown_timeframe_has_reliable_evidence("4H", "5")


class _DelayedDropdownPage:
    def __init__(self) -> None:
        self.calls = 0

    async def evaluate(self, script: str, label: str) -> bool:
        self.calls += 1
        return self.calls == 2


def test_dropdown_selection_retries_a_delayed_item(monkeypatch) -> None:
    async def _no_wait(_: float) -> None:
        return None

    monkeypatch.setattr("chart_screenshot.coinglass.asyncio.sleep", _no_wait)
    page = _DelayedDropdownPage()

    assert asyncio.run(_pick_dropdown_timeframe(page, "4H")) is True
    assert page.calls == 2
