use anyhow::Result;
use async_trait::async_trait;
use tokeira_storage::ProjectionRecord;

/// Projection sink contract.
///
/// A sink should be idempotent with respect to replay of the same record. The
/// worker intentionally provides records in log order for a partition/fanout
/// stream, but operational reality means restarts and retries still happen.
#[async_trait]
pub trait ProjectionSink: Send + Sync {
    async fn apply(&self, record: &ProjectionRecord, partition_id: u32) -> Result<()>;

    // TODO(projection): add batched apply once the SQL sink exists.
    // TODO(projection): add checkpoint persistence hooks for sinks that want to
    // own their own cursor durability.
}
