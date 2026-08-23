CREATE INDEX ASYNC IF NOT EXISTS idx_vis_execution_ns_type
    ON vis_execution (namespace_id, workflow_type);
