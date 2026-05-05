CREATE TABLE IF NOT EXISTS budget_allocation (
    id              INTEGER          PRIMARY KEY,
    version         BIGINT           NOT NULL DEFAULT 0,
    allocator_id    UUID,
    allocated_at    TIMESTAMPTZ,
    rate_budget     DOUBLE PRECISION NOT NULL DEFAULT 100.0,
    capacity_budget BIGINT           NOT NULL DEFAULT 10000
);
