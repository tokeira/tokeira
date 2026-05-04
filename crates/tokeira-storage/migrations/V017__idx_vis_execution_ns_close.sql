CREATE INDEX ASYNC idx_vis_execution_ns_close
    ON vis_execution (namespace_id, close_time DESC NULLS LAST, start_time DESC, run_key DESC);
