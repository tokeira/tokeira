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
/// A sink applies images in any order and any number of times: an image becomes the
/// stored row only when it is strictly newer, by `(authority_epoch,
/// transition_seq)`, so a repeat or an older image changes nothing. Projectors rely
/// on that, applying only each run's newest image in a page, and so do CHASM's
/// direct applies and its visibility repair.
#[async_trait]
pub trait ProjectionSink: Send + Sync {
    async fn apply(&self, record: &ProjectionRecord, partition_id: u32) -> Result<ApplyOutcome>;
}
