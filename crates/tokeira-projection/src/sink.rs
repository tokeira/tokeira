use anyhow::Result;
use async_trait::async_trait;
use tokeira_storage::ProjectionRecord;

/// What applying one image did to its run's visibility row.
///
/// Either outcome leaves the stored row at least as new as the image, so the image
/// has nothing left to contribute (`projection-passes` criterion 2.9). A failure is
/// an error, never an outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The image became the stored row.
    Advanced,
    /// The stored row was already at least as new, so nothing was written.
    Current,
}

/// Projection sink contract.
///
/// A sink should be idempotent with respect to replay of the same record. The
/// worker intentionally provides records in log order for a partition/fanout
/// stream, but operational reality means restarts and retries still happen.
#[async_trait]
pub trait ProjectionSink: Send + Sync {
    async fn apply(&self, record: &ProjectionRecord, partition_id: u32) -> Result<ApplyOutcome>;

    // TODO(projection): add batched apply once the SQL sink exists.
    // TODO(projection): add checkpoint persistence hooks for sinks that want to
    // own their own cursor durability.
}
