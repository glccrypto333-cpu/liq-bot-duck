from __future__ import annotations

from datetime import datetime, timezone, timedelta
from time_utils import iso_мск
from pathlib import Path
import csv
import json
import resource
import zipfile

from config import (
    ПАПКА_ДАННЫХ,
    APP_VERSION,
    QUICK_EXPORT_CANDLES,
    RESEARCH_EXPORT_DAYS,
    RESEARCH_30D_EXPORT_DAYS,
)
from db import active_universe_sql, fetch


RUNTIME_REPORTS_DIR = Path(__file__).resolve().parent / "runtime_reports"

WINDOW_ORDER = {
    "15м": 1,
    "30м": 2,
    "1ч": 3,
    "4ч": 4,
    "12ч": 5,
    "24ч": 6,
}


def _safe_fetch(sql: str, params: tuple = ()) -> list[dict]:
    try:
        rows = fetch(sql, params) or []
        return [row for row in rows if row is not None]
    except Exception:
        return []


def _write_dict_csv(path: Path, rows: list[dict], preferred: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [dict(row) for row in (rows or []) if row is not None]

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    keys: list[str] = []
    if preferred:
        for key in preferred:
            if any(key in row for row in rows):
                keys.append(key)
    for row in rows:
        for key in row.keys():
            if key not in keys:
                keys.append(key)

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _zip(zip_path: Path, files: list[Path]) -> None:
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for file in files:
            if file.exists():
                z.write(file, arcname=file.name)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _runtime_memory_mb() -> float:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if usage > 10_000_000:
            return usage / 1024 / 1024
        return usage / 1024
    except Exception:
        return 0.0


def _window_sort(value: str) -> int:
    return WINDOW_ORDER.get(str(value or ""), 99)


def _mode_since(mode: str) -> tuple[datetime, str, str]:
    now = datetime.now(timezone.utc)
    if mode == "research_30d":
        return now - timedelta(days=RESEARCH_30D_EXPORT_DAYS), "research_30d", f"{RESEARCH_30D_EXPORT_DAYS}d"
    if mode == "research_7d":
        return now - timedelta(days=RESEARCH_EXPORT_DAYS), "research_7d", f"{RESEARCH_EXPORT_DAYS}d"
    return now - timedelta(minutes=QUICK_EXPORT_CANDLES * 5), "quick", f"{QUICK_EXPORT_CANDLES * 5}m"


def _raw_market_rows(since: datetime) -> list[dict]:
    return _safe_fetch(
        """
        SELECT
            o.ts_open,
            o.ts_close,
            o.exchange,
            o.symbol,
            o.oi_open,
            o.oi_high,
            o.oi_low,
            o.oi_close,
            p.price_open,
            p.price_high,
            p.price_low,
            p.price_close,
            v.volume,
            o.cycle_ts,
            o.source,
            o.collected_at
        FROM oi_raw o
        LEFT JOIN price_raw p
          ON p.exchange = o.exchange
         AND p.symbol = o.symbol
         AND p.ts_close = o.ts_close
        LEFT JOIN volume_raw v
          ON v.exchange = o.exchange
         AND v.symbol = o.symbol
         AND v.ts_close = o.ts_close
        WHERE o.ts_close >= %s
        ORDER BY o.ts_close DESC, o.exchange, o.symbol
        """,
        (since,),
    )


def _aggregate_rows(since: datetime) -> list[dict]:
    return _safe_fetch(
        """
        SELECT *
        FROM aggregate_windows
        WHERE ts_close >= %s
        ORDER BY ts_close DESC, metric, window_code, exchange, symbol
        """,
        (since,),
    )


def _window_rows(since: datetime) -> list[dict]:
    rows = _safe_fetch(
        """
        SELECT c.current_stage, w.*
        FROM oi_window_state w
        LEFT JOIN oi_core_state c
          ON c.exchange = w.exchange
         AND c.symbol = w.symbol
        WHERE w.cycle_ts >= %s
        ORDER BY w.cycle_ts DESC, w.exchange, w.symbol, w.window_code
        """,
        (since,),
    )
    return sorted(rows, key=lambda r: (
        str(r.get("exchange") or ""),
        str(r.get("symbol") or ""),
        _window_sort(str(r.get("window_code") or "")),
        str(r.get("cycle_ts") or ""),
    ))


def _stage_summary_rows(core_rows: list[dict]) -> list[dict]:
    summary: dict[int, dict] = {}
    for row in core_rows:
        stage = int(row.get("current_stage") or 0)
        item = summary.setdefault(stage, {
            "current_stage": stage,
            "rows": 0,
            "max_stage_age_minutes": 0.0,
            "latest_cycle_ts": None,
        })
        item["rows"] += 1
        try:
            item["max_stage_age_minutes"] = max(item["max_stage_age_minutes"], float(row.get("oi_stage_age_minutes") or 0.0))
        except Exception:
            pass
        latest = row.get("latest_cycle_ts")
        if latest and (item["latest_cycle_ts"] is None or str(latest) > str(item["latest_cycle_ts"])):
            item["latest_cycle_ts"] = latest
    return sorted(summary.values(), key=lambda r: r["current_stage"], reverse=True)


def _window_summary_rows(window_rows: list[dict]) -> list[dict]:
    summary: dict[tuple[str, str], dict] = {}
    for row in window_rows:
        key = (str(row.get("window_code") or ""), str(row.get("oi_pattern_code") or ""))
        item = summary.setdefault(key, {
            "window_code": key[0],
            "oi_pattern_code": key[1],
            "oi_pattern_label": row.get("oi_pattern_label"),
            "rows": 0,
            "avg_growth_pct": 0.0,
        })
        item["rows"] += 1
        try:
            item["avg_growth_pct"] += float(row.get("window_growth_pct") or 0.0)
        except Exception:
            pass
    out = []
    for item in summary.values():
        item["avg_growth_pct"] = round(item["avg_growth_pct"] / max(item["rows"], 1), 4)
        out.append(item)
    return sorted(out, key=lambda r: (_window_sort(r["window_code"]), -r["rows"], r["oi_pattern_code"]))


def _top_window_rows(window_rows: list[dict], window_code: str, limit: int = 100) -> list[dict]:
    rows = [row for row in window_rows if row.get("window_code") == window_code]
    rows.sort(
        key=lambda r: (
            -(int(r.get("current_stage") or 0) if r.get("current_stage") is not None else 0),
            -abs(float(r.get("window_growth_pct") or 0.0)),
            str(r.get("exchange") or ""),
            str(r.get("symbol") or ""),
        )
    )
    return rows[:limit]


def _table_health_rows() -> list[dict]:
    return _safe_fetch(
        """
        WITH h AS (
            SELECT 'oi_raw' AS table_name, COUNT(*) AS rows, MAX(ts_close) AS latest_ts FROM oi_raw
            UNION ALL SELECT 'price_raw', COUNT(*), MAX(ts_close) FROM price_raw
            UNION ALL SELECT 'volume_raw', COUNT(*), MAX(ts_close) FROM volume_raw
            UNION ALL SELECT 'aggregate_windows', COUNT(*), MAX(ts_close) FROM aggregate_windows
            UNION ALL SELECT 'oi_core_state', COUNT(*), MAX(latest_cycle_ts) FROM oi_core_state
            UNION ALL SELECT 'oi_window_state', COUNT(*), MAX(cycle_ts) FROM oi_window_state
            UNION ALL SELECT 'oi_stage_history', COUNT(*), MAX(cycle_ts) FROM oi_stage_history
            UNION ALL SELECT 'validation_audit', COUNT(*), MAX(ts_close) FROM validation_audit
            UNION ALL SELECT 'coverage_report', COUNT(*), NULL::timestamptz FROM coverage_report
            UNION ALL SELECT 'gap_report', COUNT(*), NULL::timestamptz FROM gap_report
            UNION ALL SELECT 'active_symbol_universe', COUNT(*), MAX(activated_at) FROM active_symbol_universe
            UNION ALL SELECT 'request_failure_report', COUNT(*), MAX(calculated_at) FROM request_failure_report
        )
        SELECT
            table_name,
            rows,
            latest_ts,
            CASE
                WHEN latest_ts IS NULL THEN NULL
                ELSE ROUND(EXTRACT(EPOCH FROM (NOW() - latest_ts)) / 60.0, 2)
            END AS age_minutes,
            CASE
                WHEN rows = 0 THEN 'EMPTY'
                WHEN latest_ts IS NULL THEN 'OK'
                WHEN NOW() - latest_ts > INTERVAL '90 minutes' THEN 'STALE'
                ELSE 'OK'
            END AS status
        FROM h
        ORDER BY table_name
        """
    )


def _storage_manifest_text(files: list[Path], mode: str, range_label: str) -> str:
    lines = [
        f"generated_at_utc={iso_мск()}",
        f"app_version={APP_VERSION}",
        f"mode={mode}",
        f"range={range_label}",
        "main_downloads=market_research_bundle.zip, audit_report.txt, research_report.txt",
        "surface=oi_only_runtime",
        "pipeline=oi_raw/price_raw/volume_raw -> aggregate_windows -> oi_core_state/oi_window_state/oi_stage_history",
        "",
        "files:",
    ]
    for file in files:
        if file.exists():
            lines.append(f"- {file.name} size={file.stat().st_size}")
    return "\n".join(lines) + "\n"
def _storage_health_text(files: list[Path]) -> str:
    now = datetime.now(timezone.utc)
    lines = [
        f"generated_at_utc={now.isoformat()}",
        f"export_process_rss_mb={round(_runtime_memory_mb(), 2)}",
        "",
        "artifact,status,size,age_minutes",
    ]
    for file in files:
        if not file.exists():
            lines.append(f"{file.name},MISSING,0,")
            continue
        mtime = datetime.fromtimestamp(file.stat().st_mtime, timezone.utc)
        age = round((now - mtime).total_seconds() / 60.0, 2)
        status = "STALE" if age > 180 else "OK"
        lines.append(f"{file.name},{status},{file.stat().st_size},{age}")
    return "\n".join(lines) + "\n"


def _runtime_health_text(table_health: list[dict], core_rows: list[dict], stage_history_rows: list[dict]) -> str:
    runtime = _read_json(RUNTIME_REPORTS_DIR / "runtime_health.json")
    cycle = _read_json(RUNTIME_REPORTS_DIR / "cycle_status.json")
    watchdog = _read_text(RUNTIME_REPORTS_DIR / "watchdog_status.txt").strip()
    snapshot = _read_text(RUNTIME_REPORTS_DIR / "snapshot_status.txt").strip()

    active_stage_rows = [row for row in core_rows if int(row.get("current_stage") or 0) > 0]
    stage3_rows = [row for row in core_rows if int(row.get("current_stage") or 0) == 3]

    lines = [
        f"generated_at_utc={iso_мск()}",
        f"app_version={APP_VERSION}",
        f"canonical_tables_ok={sum(1 for row in table_health if row.get('status') == 'OK')}",
        f"canonical_tables_stale={sum(1 for row in table_health if row.get('status') == 'STALE')}",
        f"core_rows={len(core_rows)}",
        f"active_stage_rows={len(active_stage_rows)}",
        f"stage3_rows={len(stage3_rows)}",
        f"stage_history_rows={len(stage_history_rows)}",
        f"rss_mb={runtime.get('rss_mb', 'n/a')}",
        f"rss_peak_mb={runtime.get('rss_peak_mb', 'n/a')}",
        f"rss_health={runtime.get('rss_health', 'unknown')}",
        f"watchdog_health={runtime.get('watchdog_health', 'unknown')}",
        f"collect_reserve_health={runtime.get('collect_reserve_health', 'unknown')}",
        f"snapshot_health={runtime.get('snapshot_health', 'unknown')}",
        f"cycle_health={cycle.get('cycle_health', 'unknown')}",
        f"cycle_elapsed_seconds={cycle.get('cycle_elapsed_seconds', 'n/a')}",
        f"cycle_sleep_seconds={cycle.get('cycle_sleep_seconds', 'n/a')}",
        f"cycle_reserve_pct={cycle.get('cycle_reserve_pct', 'n/a')}",
        f"latest_core_cycle_ts={max([str(row.get('latest_cycle_ts') or '') for row in core_rows] or [''])}",
        "",
        "watchdog_status:",
        watchdog or "missing",
        "",
        "snapshot_status:",
        snapshot or "missing",
    ]
    return "\n".join(lines) + "\n"


def _research_report_text(
    mode: str,
    range_label: str,
    raw_rows: list[dict],
    aggregate_rows: list[dict],
    core_rows: list[dict],
    window_rows: list[dict],
    stage_history_rows: list[dict],
) -> str:
    active_stage_rows = [row for row in core_rows if int(row.get("current_stage") or 0) > 0]
    blocked_rows = [row for row in core_rows if row.get("blocked_by_price")]
    top_stage = sorted(active_stage_rows, key=lambda r: (
        -int(r.get("current_stage") or 0),
        -float(r.get("oi_stage_age_minutes") or 0.0),
        str(r.get("exchange") or ""),
        str(r.get("symbol") or ""),
    ))[:15]

    lines = [
        f"generated_at_utc={iso_мск()}",
        f"app_version={APP_VERSION}",
        f"mode={mode}",
        f"range={range_label}",
        f"raw_market_5m_rows={len(raw_rows)}",
        f"aggregate_windows_rows={len(aggregate_rows)}",
        f"oi_core_state_rows={len(core_rows)}",
        f"oi_window_state_rows={len(window_rows)}",
        f"oi_stage_history_rows={len(stage_history_rows)}",
        f"active_stage_rows={len(active_stage_rows)}",
        f"blocked_by_price_rows={len(blocked_rows)}",
        "",
        "stage_distribution:",
    ]

    for row in _stage_summary_rows(core_rows):
        lines.append(
            f"- stage={row['current_stage']} rows={row['rows']} max_age_m={row['max_stage_age_minutes']} latest={row['latest_cycle_ts']}"
        )

    lines.extend(["", "top_stage_watch:"])
    for row in top_stage:
        lines.append(
            "- "
            f"{row.get('exchange')} {row.get('symbol')} "
            f"stage={row.get('current_stage')} "
            f"pattern={row.get('oi_pattern_label') or row.get('oi_pattern_code')} "
            f"price={row.get('price_state_summary')} "
            f"volume={row.get('volume_state_summary')} "
            f"age={row.get('oi_stage_age_minutes')}m "
            f"reason={row.get('decision_reason')}"
        )

    return "\n".join(lines) + "\n"


def rebuild_exports(mode: str = "quick") -> Path:
    since, suffix, range_label = _mode_since(mode)

    raw_rows = _raw_market_rows(since)
    aggregate_rows = _aggregate_rows(since)
    audit_rows = _safe_fetch(
        "SELECT * FROM validation_audit WHERE ts_close >= %s ORDER BY ts_close DESC, metric, timeframe, exchange, symbol",
        (since,),
    )
    coverage_rows = _safe_fetch("SELECT * FROM coverage_report ORDER BY metric, exchange, symbol")
    gap_rows = _safe_fetch("SELECT * FROM gap_report ORDER BY metric, exchange, symbol, gap_start")
    active_universe_rows = _safe_fetch(
        f"SELECT * FROM {active_universe_sql()} ORDER BY exchange, symbol"
    )
    request_failure_rows = _safe_fetch(
        "SELECT * FROM request_failure_report WHERE calculated_at >= %s ORDER BY calculated_at DESC, exchange, symbol, data_type",
        (since,),
    )
    core_rows = _safe_fetch(
        """
        SELECT *
        FROM oi_core_state
        ORDER BY current_stage DESC, oi_stage_age_minutes DESC NULLS LAST, latest_cycle_ts DESC, exchange, symbol
        """
    )
    window_rows = _window_rows(since)
    stage_history_rows = _safe_fetch(
        """
        SELECT *
        FROM oi_stage_history
        WHERE cycle_ts >= %s
        ORDER BY cycle_ts DESC, exchange, symbol, to_stage DESC
        """,
        (since,),
    )
    table_health_rows = _table_health_rows()

    data_dir = Path(ПАПКА_ДАННЫХ)
    raw_path = data_dir / "raw_market_5m.csv"
    aggregate_path = data_dir / "aggregate_windows.csv"
    audit_path = data_dir / "validation_audit.csv"
    coverage_path = data_dir / "coverage_report.csv"
    gap_path = data_dir / "gap_report.csv"
    active_universe_path = data_dir / "active_universe_report.csv"
    request_failures_path = data_dir / "request_failure_report.csv"
    core_path = data_dir / "oi_core_state.csv"
    window_path = data_dir / "oi_window_state.csv"
    stage_history_path = data_dir / "oi_stage_history.csv"
    stage_summary_path = data_dir / "oi_stage_summary.csv"
    window_summary_path = data_dir / "oi_window_summary.csv"
    top_15m_path = data_dir / "top_oi_15m.csv"
    top_30m_path = data_dir / "top_oi_30m.csv"
    top_1h_path = data_dir / "top_oi_1h.csv"
    top_4h_path = data_dir / "top_oi_4h.csv"
    table_health_path = data_dir / "table_health.csv"
    audit_report_path = data_dir / "audit_report.txt"
    research_report_path = data_dir / "research_report.txt"
    manifest_path = data_dir / "storage_manifest.txt"
    storage_health_path = data_dir / "storage_health_report.txt"
    runtime_health_path = data_dir / "runtime_health_report.txt"
    runtime_timing_path = data_dir / "runtime_timing_report.txt"

    _write_dict_csv(raw_path, raw_rows)
    _write_dict_csv(aggregate_path, aggregate_rows)
    _write_dict_csv(audit_path, audit_rows)
    _write_dict_csv(coverage_path, coverage_rows)
    _write_dict_csv(gap_path, gap_rows)
    _write_dict_csv(active_universe_path, active_universe_rows)
    _write_dict_csv(request_failures_path, request_failure_rows)
    _write_dict_csv(core_path, core_rows)
    _write_dict_csv(window_path, window_rows)
    _write_dict_csv(stage_history_path, stage_history_rows)
    _write_dict_csv(stage_summary_path, _stage_summary_rows(core_rows))
    _write_dict_csv(window_summary_path, _window_summary_rows(window_rows))
    _write_dict_csv(top_15m_path, _top_window_rows(window_rows, "15м"))
    _write_dict_csv(top_30m_path, _top_window_rows(window_rows, "30м"))
    _write_dict_csv(top_1h_path, _top_window_rows(window_rows, "1ч"))
    _write_dict_csv(top_4h_path, _top_window_rows(window_rows, "4ч"))
    _write_dict_csv(table_health_path, table_health_rows)

    _write_text(audit_report_path, _storage_health_text([table_health_path, audit_path, coverage_path, gap_path]))
    _write_text(
        research_report_path,
        _research_report_text(mode, range_label, raw_rows, aggregate_rows, core_rows, window_rows, stage_history_rows),
    )
    _write_text(runtime_health_path, _runtime_health_text(table_health_rows, core_rows, stage_history_rows))

    runtime_timing_source = Path("runtime/runtime_timing_report.txt")
    runtime_timing_text = _read_text(runtime_timing_source)
    if not runtime_timing_text:
        runtime_timing_text = (
            f"generated_at={iso_мск()}\n"
            f"status=missing_runtime_timing_source\n"
        )
    _write_text(runtime_timing_path, runtime_timing_text)

    bundle_files = [
        raw_path,
        aggregate_path,
        audit_path,
        coverage_path,
        gap_path,
        active_universe_path,
        request_failures_path,
        core_path,
        window_path,
        stage_history_path,
        stage_summary_path,
        window_summary_path,
        top_15m_path,
        top_30m_path,
        top_1h_path,
        top_4h_path,
        table_health_path,
        audit_report_path,
        research_report_path,
        storage_health_path,
        runtime_health_path,
        runtime_timing_path,
    ]

    _write_text(manifest_path, _storage_manifest_text(bundle_files, mode, range_label))
    _write_text(storage_health_path, _storage_health_text(bundle_files + [manifest_path]))

    bundle_path = data_dir / "market_research_bundle.zip"
    mode_bundle_path = data_dir / f"market_research_bundle_{suffix}.zip"
    _zip(bundle_path, bundle_files + [manifest_path])
    _zip(mode_bundle_path, bundle_files + [manifest_path])

    return mode_bundle_path if suffix != "quick" else bundle_path
