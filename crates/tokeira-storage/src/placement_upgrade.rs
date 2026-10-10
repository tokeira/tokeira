//! Stopped-cluster execution-home placement preparation.
//!
//! A page's physical changes and durable cursor commit together. All starters
//! write the same revision, so a stale page cannot commit after completion.
//! This protocol does not fence older binaries: stopping them remains an
//! operational precondition, supplemented by a live-lease check in storage.

use std::time::Duration;

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_kernel::WorkflowState;
use tokeira_types::{NamespaceId, RunKey, ShardId, WorkflowId};
use uuid::Uuid;

use crate::RunRepository;

/// Maximum metadata rows screened in a page with no relocation.
pub const SCREEN_KEYS: usize = 1_000;
/// Maximum source keys processed by a page that relocates data.
pub const MOVE_KEYS: usize = 64;
/// Stable identity of this one-time placement algorithm.
pub const PLACEMENT_UPGRADE: &str = "execution-home-placement-v1";

/// Physical timer position. Ordering matches the DSQL primary key, including
/// the source shard so a copied destination cannot hide another source row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimerPosition {
    /// Physical shard UUID, preserved even for an unrecognized legacy encoding.
    pub shard: Uuid,
    /// Existing timer deadline; relocation never restarts it.
    pub fire_at: OffsetDateTime,
    /// Run owning the timer.
    pub run_key: RunKey,
    /// Timer identity within its run.
    pub timer_id: String,
}

/// Durable phase and exclusive continuation of the startup migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementPhase {
    /// Inspect all hot rows, including rows omitted by the recovery index.
    Hot(Option<RunKey>),
    /// Inspect timers independently of hot-row placement.
    Timers(Option<TimerPosition>),
    /// No old page can write after the revision establishing this phase.
    Complete,
}

impl PlacementPhase {
    /// Low-cardinality progress label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Hot(_) => "hot",
            Self::Timers(_) => "timers",
            Self::Complete => "complete",
        }
    }
}

/// Committed totals and continuation, kept separate from workflow state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementProgress {
    /// Current scan phase.
    pub phase: PlacementPhase,
    /// Hot rows examined, including unchanged rows.
    pub hot_examined: u64,
    /// Hot rows whose physical shard changed.
    pub hot_moved: u64,
    /// Timer keys examined; a moved destination may be encountered again.
    pub timers_examined: u64,
    /// Timer source rows removed after installing equivalent destinations.
    pub timers_moved: u64,
}

impl Default for PlacementProgress {
    fn default() -> Self {
        Self {
            phase: PlacementPhase::Hot(None),
            hot_examined: 0,
            hot_moved: 0,
            timers_examined: 0,
            timers_moved: 0,
        }
    }
}

impl PlacementProgress {
    /// Whether physical placement is ready for runtime construction.
    pub fn complete(&self) -> bool {
        matches!(self.phase, PlacementPhase::Complete)
    }
}

/// One transaction's outcome; a lost page contributes no progress.
#[derive(Debug, Clone)]
pub enum PlacementPage {
    /// Durable progress, including a previously completed marker.
    Committed(PlacementProgress),
    /// Retry from a fresh marker after contention or a transient connection error.
    Retry,
}

/// Complete physical preparation before constructing any runtime tasks.
///
/// Dropping this future cancels retries and backoff. An interrupted or uncertain
/// page resumes from durable progress; no process-owned lease is needed.
/// Callers must keep every older node stopped throughout this operation.
pub async fn prepare_execution_placement<R: RunRepository + ?Sized>(
    repo: &R,
) -> Result<PlacementProgress> {
    let mut lost = 0u32;
    loop {
        match repo.prepare_placement_page().await? {
            PlacementPage::Committed(progress) => {
                tracing::info!(
                    phase = progress.phase.label(),
                    hot_examined = progress.hot_examined,
                    hot_moved = progress.hot_moved,
                    timers_examined = progress.timers_examined,
                    timers_moved = progress.timers_moved,
                    "execution-home placement preparation committed"
                );
                if progress.complete() {
                    return Ok(progress);
                }
                lost = 0;
            }
            PlacementPage::Retry => {
                lost = lost.saturating_add(1).min(7);
                let ceiling = (10u64 << lost).min(1_000);
                // Per-attempt randomness prevents simultaneous starters from
                // falling into the same retry cadence on the singleton.
                let jitter = (Uuid::new_v4().as_u128() % u128::from(ceiling)) as u64;
                tokio::time::sleep(Duration::from_millis(ceiling / 2 + jitter)).await;
            }
        }
    }
}

/// Validate the authoritative identity only when physical data will move.
/// Diagnostics identify the exact row and check; there is no skip override.
pub(crate) fn validate_identity(
    run_key: RunKey,
    stored: Uuid,
    home: ShardId,
    namespace: NamespaceId,
    workflow: &WorkflowId,
    state: &WorkflowState,
) -> Result<()> {
    ensure!(
        state.run_key == run_key
            && state.namespace_id == namespace
            && state.workflow_id == *workflow,
        "placement upgrade run={run_key:?} stored_shard={stored} computed_home={home:?}: authoritative identity disagrees with hot columns"
    );
    Ok(())
}

/// Reversible encoding already used by hot rows, leases and timers.
pub(crate) fn shard_uuid(shard: ShardId) -> Uuid {
    let mut bytes = *b"tokeira-shard-id";
    bytes[12..16].copy_from_slice(&shard.0.to_be_bytes());
    Uuid::from_bytes(bytes)
}

/// Refuse unexplained placement rather than making an actual home unknowable
/// to its later acquisition walk.
pub(crate) fn validate_source(
    key: RunKey,
    stored: Uuid,
    home: ShardId,
    shard_count: u32,
) -> Result<()> {
    let legacy = ShardId((key.0.as_u128() as u32) % shard_count);
    ensure!(
        stored == shard_uuid(legacy),
        "placement upgrade run={key:?} stored_shard={stored} computed_home={home:?}: source is neither execution home nor legacy run-key shard"
    );
    Ok(())
}
