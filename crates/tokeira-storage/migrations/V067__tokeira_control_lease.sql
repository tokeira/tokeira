CREATE TABLE IF NOT EXISTS tokeira_control_lease (
    claim_name TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    cluster_arn TEXT NOT NULL,
    owner_id TEXT,
    fence_token BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (claim_name)
);
