CREATE INDEX ASYNC IF NOT EXISTS idx_execution_visibility_attr_lookup
    ON execution_visibility_attr_index (namespace_id, attr_id, attr_type, keyword_value, run_key);
