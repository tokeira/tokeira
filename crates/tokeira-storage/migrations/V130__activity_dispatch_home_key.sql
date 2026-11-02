CREATE INDEX ASYNC IF NOT EXISTS idx_activity_dispatch_home_key ON activity_dispatch (shard_id, run_key, key);
