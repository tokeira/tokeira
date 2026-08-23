CREATE INDEX ASYNC IF NOT EXISTS idx_activity_dispatch_queue ON activity_dispatch (queue_namespace, queue_name, task_kind, deployment, build_id, dispatch_at, created_at);
