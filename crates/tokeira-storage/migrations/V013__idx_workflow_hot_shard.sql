CREATE INDEX ASYNC IF NOT EXISTS idx_workflow_hot_shard
    ON workflow_hot (shard_id);
