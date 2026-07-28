CREATE INDEX ASYNC idx_worker_task_provenance_expiry
ON worker_task_provenance (expires_at, token_digest)
