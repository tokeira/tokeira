CREATE TABLE IF NOT EXISTS request_dedupe (
    key                       UUID        NOT NULL,
    namespace_id              UUID        NOT NULL,
    workflow_id               TEXT        NOT NULL,
    request_id                TEXT        NOT NULL,
    run_key                   UUID        NOT NULL,
    run_id                    UUID        NOT NULL,
    first_seen_transition_seq BIGINT      NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (key)
);
