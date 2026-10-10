CREATE INDEX ASYNC IF NOT EXISTS idx_dispatch_backlog_run_key
ON dispatch_backlog (run_key, key);
