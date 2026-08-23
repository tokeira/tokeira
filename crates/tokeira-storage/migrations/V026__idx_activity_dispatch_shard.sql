CREATE INDEX ASYNC IF NOT EXISTS idx_activity_dispatch_shard ON activity_dispatch (shard_id, dispatch_at, created_at);
