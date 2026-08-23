CREATE INDEX ASYNC IF NOT EXISTS idx_vis_execution_ns_close
    ON vis_execution (namespace_id, close_time, start_time, run_key);
