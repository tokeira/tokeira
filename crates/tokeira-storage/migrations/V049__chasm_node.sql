CREATE TABLE IF NOT EXISTS chasm_node (
    namespace_id                UUID        NOT NULL,
    business_id                 TEXT        NOT NULL,
    run_id                      UUID        NOT NULL,
    encoded_path                BYTEA       NOT NULL,
    archetype_id                BIGINT      NOT NULL,
    failover_version            BIGINT      NOT NULL,
    transition_count            BIGINT      NOT NULL,
    initial_failover_version    BIGINT      NOT NULL,
    initial_transition_count    BIGINT      NOT NULL,
    metadata                    BYTEA       NOT NULL,
    data                        BYTEA,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, business_id, run_id, encoded_path)
);
