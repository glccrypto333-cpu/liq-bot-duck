#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Always pin the default verification contour explicitly so suite flags
# are not misread by the sourced env-loader as a DB target name.
source "${SCRIPT_DIR}/load_verification_db_env.sh" hot

cd "${SCRIPT_DIR}"
mkdir -p reports

HOURS=36
LIMIT_CYCLES=90
FOLLOW_STEPS=4
MAX_STAGE2_ROWS=12
SHOW_CYCLES=8
BASKET_PER_BUCKET=2

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hours)
      HOURS="$2"
      shift 2
      ;;
    --limit-cycles)
      LIMIT_CYCLES="$2"
      shift 2
      ;;
    --follow-steps)
      FOLLOW_STEPS="$2"
      shift 2
      ;;
    --max-stage2-rows)
      MAX_STAGE2_ROWS="$2"
      shift 2
      ;;
    --show-cycles)
      SHOW_CYCLES="$2"
      shift 2
      ;;
    --basket-per-bucket)
      BASKET_PER_BUCKET="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

STAMP="$(date -u +%Y%m%d_%H%M%S)"
REPORT="reports/verification_suite_${STAMP}.txt"
SUMMARY="${REPORT%.txt}_summary.txt"
export REPORT SUMMARY BASKET_PER_BUCKET

section() {
  printf '
=== %s ===
' "$1" | tee -a "$REPORT"
}

run_and_capture() {
  local title="$1"
  shift
  section "$title"
  "$@" | tee -a "$REPORT"
}

CASE_EXPORTS="$(${DUCK_VERIFICATION_PYTHON} - <<'PY2'
import os
from shlex import quote
from db import fetch

bucket_size = int(os.environ.get('BASKET_PER_BUCKET', '2'))

def first_value(sql, params=(), key=None):
    rows = fetch(sql, params)
    if not rows:
        return ''
    row = rows[0]
    if key is None:
        return next(iter(row.values())) if row else ''
    return row.get(key) or ''

def many_values(sql, params=(), key=None, limit=2):
    rows = fetch(sql, params)
    values = []
    for row in rows[:limit]:
        value = row.get(key) if key is not None else next(iter(row.values()))
        if value and value not in values:
            values.append(str(value))
    return values

strong_values = many_values(
    """
    select symbol
    from oi_post_stage_analytics
    where quality_label in ('flat', 'positive')
    group by symbol
    order by count(*) desc, max(triggered_at) desc
    limit 20
    """,
    key='symbol',
    limit=bucket_size,
)
negative_values = many_values(
    """
    select symbol
    from oi_post_stage_analytics
    where quality_label = 'negative'
    group by symbol
    order by count(*) desc, max(triggered_at) desc
    limit 20
    """,
    key='symbol',
    limit=bucket_size,
)
partial_values = many_values(
    """
    select symbol
    from oi_post_stage_analytics
    where quality_label = 'partial'
    group by symbol
    order by count(*) desc, max(triggered_at) desc
    limit 20
    """,
    key='symbol',
    limit=bucket_size,
)
compare_symbol = (negative_values or strong_values or partial_values or [''])[0]
compare_from_ts = first_value(
    """
    select min(triggered_at)::text as triggered_at
    from oi_post_stage_analytics
    where symbol = %s
    """,
    (compare_symbol,),
    key='triggered_at',
) if compare_symbol else ''
tracked = []
for bucket in (strong_values, negative_values, partial_values):
    for item in bucket:
        if item and item not in tracked:
            tracked.append(item)
print(f"STRONG_SYMBOLS={quote(' '.join(strong_values))}")
print(f"NEGATIVE_SYMBOLS={quote(' '.join(negative_values))}")
print(f"PARTIAL_SYMBOLS={quote(' '.join(partial_values))}")
print(f"COMPARE_SYMBOL={quote(str(compare_symbol))}")
print(f"COMPARE_FROM_TS={quote(str(compare_from_ts))}")
print(f"TRACKED_SYMBOLS={quote(' '.join(tracked))}")
print(f"TRACKED_COUNT={quote(str(len(tracked)))}")
PY2
)"
eval "$CASE_EXPORTS"

section "suite_meta"
{
  echo "report=$REPORT"
  echo "summary=$SUMMARY"
  echo "hours=$HOURS"
  echo "limit_cycles=$LIMIT_CYCLES"
  echo "follow_steps=$FOLLOW_STEPS"
  echo "max_stage2_rows=$MAX_STAGE2_ROWS"
  echo "show_cycles=$SHOW_CYCLES"
  echo "basket_per_bucket=$BASKET_PER_BUCKET"
  echo "tracked_count=${TRACKED_COUNT:-0}"
  echo "strong_symbols=${STRONG_SYMBOLS:-}"
  echo "negative_symbols=${NEGATIVE_SYMBOLS:-}"
  echo "partial_symbols=${PARTIAL_SYMBOLS:-}"
  echo "compare_symbol=${COMPARE_SYMBOL:-}"
  echo "compare_from_ts=${COMPARE_FROM_TS:-}"
  echo "tracked_symbols=${TRACKED_SYMBOLS:-}"
} | tee -a "$REPORT"

run_and_capture "runtime_snapshot" ${DUCK_VERIFICATION_PYTHON} - <<'PY2'
import json
from pathlib import Path
runtime_dir = Path('runtime_reports')
for name in ('runtime_health.json', 'cycle_status.json'):
    path = runtime_dir / name
    print(name)
    if not path.exists():
        print({'missing': True})
        continue
    data = json.loads(path.read_text())
    if name == 'runtime_health.json':
        keys = ['ts', 'rss_mb', 'rss_peak_mb', 'rss_health', 'watchdog_health', 'cycle_health', 'cycle_latency_class', 'collect_reserve_health']
    else:
        keys = ['ts', 'cycle_health', 'sleep_seconds', 'stop_reason']
    print({k: data.get(k) for k in keys})
PY2

run_and_capture "verification_db_health" "${DUCK_VERIFICATION_PYTHON}" verification_db_health.py --skip-indexes --skip-sizes
run_and_capture "verification_stage_summary" "${DUCK_VERIFICATION_PYTHON}" verification_stage_summary.py
run_and_capture "replay_batch" bash run_verification_replay.sh --mode batch --hours "$HOURS" --limit-cycles "$LIMIT_CYCLES" --show-cycles "$SHOW_CYCLES"
run_and_capture "stage2_entries" "${DUCK_VERIFICATION_PYTHON}" verification_stage2_entries.py --hours "$HOURS" --limit-cycles "$LIMIT_CYCLES" --exchange ALL --max-rows "$MAX_STAGE2_ROWS"

if [[ -n "${TRACKED_SYMBOLS:-}" ]]; then
  read -r -a SYMBOL_ARRAY <<< "$TRACKED_SYMBOLS"
  run_and_capture "stage_persistence" "${DUCK_VERIFICATION_PYTHON}" verification_stage_persistence.py --hours "$HOURS" --limit-cycles "$LIMIT_CYCLES" --symbols "${SYMBOL_ARRAY[@]}"
  run_and_capture "stage_trace" "${DUCK_VERIFICATION_PYTHON}" verification_stage_trace.py --hours "$HOURS" --limit-cycles "$LIMIT_CYCLES" --exchange ALL --symbols "${SYMBOL_ARRAY[@]}"
  run_and_capture "stage3_follow" "${DUCK_VERIFICATION_PYTHON}" verification_stage3_follow.py --hours "$HOURS" --limit-cycles "$LIMIT_CYCLES" --exchange ALL --follow-steps "$FOLLOW_STEPS" --symbols "${SYMBOL_ARRAY[@]}"
  if [[ -n "${COMPARE_FROM_TS:-}" ]]; then
    run_and_capture "stage_compare" "${DUCK_VERIFICATION_PYTHON}" verification_stage_compare.py --hours "$HOURS" --limit-cycles "$LIMIT_CYCLES" --exchange ALL --from-ts "$COMPARE_FROM_TS" --symbols "${SYMBOL_ARRAY[@]}"
  fi
fi

run_and_capture "post_stage_quality" ${DUCK_VERIFICATION_PYTHON} - <<'PY2'
from db import fetch
for title, sql in (
    (
        'quality_by_stage',
        """
        select stage_triggered, quality_label, count(*) as c
        from oi_post_stage_analytics
        group by stage_triggered, quality_label
        order by stage_triggered, quality_label
        """,
    ),
    (
        'top_negative_symbols',
        """
        select symbol, count(*) as c
        from oi_post_stage_analytics
        where quality_label = 'negative'
        group by symbol
        order by c desc, symbol
        limit 10
        """,
    ),
):
    print(title)
    for row in fetch(sql):
        print(row)
PY2

section "suite_done"
echo "$REPORT" | tee -a "$REPORT"

${DUCK_VERIFICATION_PYTHON} - <<'PY2'
from pathlib import Path
import os
report = Path(os.environ['REPORT'])
summary = Path(os.environ['SUMMARY'])
text = report.read_text()
lines = text.splitlines()
sections = {}
current = None
for line in lines:
    if line.startswith('=== ') and line.endswith(' ==='):
        current = line[4:-4].strip()
        sections[current] = []
        continue
    if current is not None:
        sections[current].append(line)
keep_order = ['suite_meta','runtime_snapshot','verification_db_health','verification_stage_summary','replay_batch','stage_persistence','stage3_follow','post_stage_quality','suite_done']
out = []
out.append(f'full_report={report}')
out.append(f'summary_generated_from_lines={len(lines)}')
out.append('')
for name in keep_order:
    block = sections.get(name)
    if not block:
        continue
    out.append(f'=== {name} ===')
    if name == 'verification_db_health':
        for row in block:
            if row.startswith('freshness_summary') or row.startswith('row_counts') or row.startswith('legacy_tables_present') or row.startswith('{'):
                out.append(row)
    elif name == 'replay_batch':
        for row in block:
            if row.startswith('REPLAY_OK') or row.startswith('FINAL ') or row.startswith('MATURITY') or row.startswith('CYCLE '):
                out.append(row)
    elif name == 'stage_persistence':
        for row in block:
            if row.startswith('SYMBOL ') or row.startswith('  stage'):
                out.append(row)
    else:
        out.extend(block)
    out.append('')
summary.write_text('\n'.join(out).rstrip() + '\n')
print(summary)
PY2
