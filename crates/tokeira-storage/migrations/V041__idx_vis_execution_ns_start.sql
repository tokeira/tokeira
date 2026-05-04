CREATE INDEX ASYNC idx_vis_execution_ns_start ON vis_execution (namespace_id, start_time DESC, run_key DESC);
