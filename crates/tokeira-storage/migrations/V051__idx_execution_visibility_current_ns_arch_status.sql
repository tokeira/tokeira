CREATE INDEX ASYNC idx_execution_visibility_current_ns_arch_status
    ON execution_visibility_current (namespace_id, archetype_id, status_keyword, start_time, run_key);
