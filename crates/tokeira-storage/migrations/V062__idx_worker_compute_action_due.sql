CREATE INDEX ASYNC idx_worker_compute_action_due ON worker_compute_action (namespace_id, due_bucket, status, next_attempt_at, action_id)
