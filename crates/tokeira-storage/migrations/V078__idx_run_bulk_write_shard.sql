CREATE INDEX ASYNC IF NOT EXISTS idx_run_bulk_write_shard
ON run_bulk_write (shard_id, run_key);
