from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


BYBIT_PUBLIC_LINEAR_WS = "wss://stream.bybit.com/v5/public/linear"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Shadow-only OI stream collector. Does not write production DB tables.",
    )
    parser.add_argument("--exchange", choices=["BYBIT"], default="BYBIT")
    parser.add_argument(
        "--symbols",
        default="MAGMAUSDT,PTBUSDT,COOKIEUSDT,HMSTRUSDT",
        help="Comma-separated symbols for shadow observation.",
    )
    parser.add_argument("--duration-minutes", type=float, default=15.0)
    parser.add_argument(
        "--out-dir",
        default="/home/alexey/Codex/MightyDuck/reports/ws_shadow",
        help="Directory for JSONL shadow ticks.",
    )
    args = parser.parse_args()

    try:
        import websockets  # type: ignore[import-not-found]
    except ModuleNotFoundError:
        print(
            "websockets package is not installed in this Python environment. "
            "Create an isolated shadow venv or approve adding the dependency before live stream tests.",
            file=sys.stderr,
        )
        return 2

    symbols = [symbol.strip().upper() for symbol in args.symbols.split(",") if symbol.strip()]
    if not symbols:
        print("No symbols provided.", file=sys.stderr)
        return 2

    return asyncio.run(
        collect_bybit_shadow(
            websockets=websockets,
            symbols=symbols,
            duration_minutes=args.duration_minutes,
            out_dir=Path(args.out_dir),
        ),
    )


async def collect_bybit_shadow(*, websockets, symbols: list[str], duration_minutes: float, out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_label = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"bybit_oi_shadow_{ts_label}.jsonl"
    topics = [f"tickers.{symbol}" for symbol in symbols]
    deadline = asyncio.get_running_loop().time() + duration_minutes * 60.0

    async with websockets.connect(BYBIT_PUBLIC_LINEAR_WS, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(json.dumps({"op": "subscribe", "args": topics}))

        with out_path.open("a", encoding="utf-8") as handle:
            while asyncio.get_running_loop().time() < deadline:
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=30)
                now_iso = datetime.now(timezone.utc).isoformat()
                message = json.loads(raw_message)
                record = normalize_bybit_ticker_message(message, received_at=now_iso)
                if record is None:
                    continue
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                handle.flush()

    print(f"shadow_ticks_written={out_path}")
    return 0


def normalize_bybit_ticker_message(message: dict, *, received_at: str) -> dict | None:
    topic = message.get("topic")
    data = message.get("data")
    if not topic or not isinstance(data, dict):
        return None

    symbol = str(data.get("symbol") or topic.removeprefix("tickers.")).upper()
    open_interest = data.get("openInterest")
    if open_interest is None:
        return None

    try:
        oi_value = float(open_interest)
    except (TypeError, ValueError):
        return None

    return {
        "exchange": "BYBIT",
        "symbol": symbol,
        "received_at": received_at,
        "event_ts_ms": message.get("ts"),
        "open_interest": oi_value,
        "raw_topic": topic,
    }


if __name__ == "__main__":
    raise SystemExit(main())
