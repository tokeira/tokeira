CREATE INDEX ASYNC IF NOT EXISTS idx_workflow_dispatch_queue
ON workflow_dispatch (
    queue_namespace, queue_key, routing_mode, deployment_key, build_key,
    priority_key, scheduled_at, run_key
)
WHERE sticky = false;
