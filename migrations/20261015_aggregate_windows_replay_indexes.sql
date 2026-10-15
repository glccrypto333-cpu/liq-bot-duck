-- Run with psql -v ON_ERROR_STOP=1 against the Duck database.
-- CONCURRENTLY is deliberate: these indexes are created on the live market-data table.
SET lock_timeout = '30s';
SET statement_timeout = '20min';
SET maintenance_work_mem = '64MB';
SET max_parallel_maintenance_workers = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_aggregate_windows_source_cycle
    ON public.aggregate_windows (source_cycle_ts, metric, window_code, exchange, symbol)
    WHERE source_cycle_ts IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_aggregate_windows_latest
    ON public.aggregate_windows (exchange, symbol, window_code, ts_close DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_aggregate_windows_pair_metric_latest
    ON public.aggregate_windows (exchange, symbol, metric, window_code, ts_close DESC);
