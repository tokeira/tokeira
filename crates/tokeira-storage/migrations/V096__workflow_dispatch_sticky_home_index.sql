CREATE INDEX ASYNC IF NOT EXISTS idx_workflow_dispatch_sticky_home
ON workflow_dispatch (shard_id, run_key)
WHERE sticky = true;
