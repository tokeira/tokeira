CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx_shard_lease_epoch
ON shard_lease (shard_id, epoch);
