CREATE INDEX ASYNC IF NOT EXISTS idx_activity_state_shard
    ON activity_state (shard_id);
