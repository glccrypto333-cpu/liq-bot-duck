from datetime import datetime, timezone
import importlib.util

spec = importlib.util.spec_from_file_location("reconstruct_interval", "/home/alexey/openclaw/apps/liq-bot-duck/tools/backtest/reconstruct_interval.py")


def test_cycle_record_preserves_matrix_and_windows():
    mod = importlib.util.module_from_spec(spec)
    # Test only the pure formatter; remote integration test runs the CLI.
    spec.loader.exec_module(mod)
    ts = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
    row = ("BINANCE", "BELUSDT", 3, "label", "code", "label", "вверх", "сильный", None, None, "нет", "вверх", "сильный", None, None, "нет", "цена_не_блокирует", False, "подтверждающий", "нейтрально", 15.0, "разрешено", False, 2, "нейтрально", "ранний выпуск A", "", "", None, "strong_up", "good_up", "strong_up", "good_up", ts)
    wm = {("BINANCE", "BELUSDT"): {"15м": {"OI": {"delta_pct": 7.1, "source_cycle_ts": ts, "open_value": 1.0, "close_value": 2.0, "unique_candles": 3}}}}
    out = mod.cycle_record(row, wm)
    assert out["target_stage"] == 3
    assert out["decision_reason"] == "ранний выпуск A"
    assert out["windows"]["15м"]["OI"]["delta_pct"] == 7.1
