CREATE INDEX ASYNC IF NOT EXISTS idx_dispatch_backlog_queue_order ON dispatch_backlog (queue_namespace, queue_name, task_kind, deployment, build_id, priority_key, fair_pass, insertion_tie);
