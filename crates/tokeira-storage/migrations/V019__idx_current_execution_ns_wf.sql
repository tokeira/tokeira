CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx_current_execution_ns_wf ON current_execution (namespace_id, workflow_id);
