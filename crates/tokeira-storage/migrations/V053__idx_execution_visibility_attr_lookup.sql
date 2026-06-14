CREATE INDEX ASYNC idx_execution_visibility_attr_lookup
    ON execution_visibility_attr_index (namespace_id, archetype_id, attr_id, attr_type, keyword_value, run_key);
