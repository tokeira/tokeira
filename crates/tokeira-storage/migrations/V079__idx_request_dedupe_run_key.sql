CREATE INDEX ASYNC IF NOT EXISTS idx_request_dedupe_run_key
ON request_dedupe (run_key, key);
