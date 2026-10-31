CREATE INDEX ASYNC IF NOT EXISTS run_operation_home_idx ON run_operation (source_home, kind, source_run_key, initiating_event_id);
