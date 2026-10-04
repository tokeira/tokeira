CREATE INDEX ASYNC IF NOT EXISTS idx_workflow_hot_recovery ON workflow_hot (shard_id, recovery_needed, run_key);
