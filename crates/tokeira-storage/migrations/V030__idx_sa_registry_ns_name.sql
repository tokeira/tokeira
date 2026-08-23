CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx_sa_registry_ns_name ON sa_registry (namespace_id, attr_name);
