CREATE INDEX ASYNC IF NOT EXISTS idx_activity_state_queue
    ON activity_state (queue_namespace, queue_name);
