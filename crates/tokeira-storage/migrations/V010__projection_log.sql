CREATE TABLE IF NOT EXISTS projection_log (
    partition_id    INTEGER     NOT NULL,
    fanout          SMALLINT    NOT NULL,
    run_key         UUID        NOT NULL,
    transition_seq  BIGINT      NOT NULL,
    context_data    BYTEA       NOT NULL,
    ops_data        BYTEA       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (partition_id, fanout, run_key, transition_seq)
);
