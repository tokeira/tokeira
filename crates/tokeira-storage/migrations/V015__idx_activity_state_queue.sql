CREATE INDEX ASYNC idx_activity_state_queue
    ON activity_state (queue_namespace, queue_name);
