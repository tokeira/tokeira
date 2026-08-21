CREATE INDEX ASYNC idx_activity_dispatch_shard ON activity_dispatch (shard_id, dispatch_at, created_at);
