//! Durable, derived workflow-task intent shared by both storage backends.
//!
//! Rows describe only running, unstarted normal tasks. They are maintained with
//! hot state, never claimed by consumers, and can be reconstructed from state
//! without consulting previous rows or a deployment registry. Lookup digests
//! bound index width; full coordinates remain the authority for queue matching.

use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{Priority, WorkflowState, WorkflowTaskType};
use tokeira_types::{
    BuildId, DeploymentId, ExecutionStatus, LogicalTaskSeq, NamespaceId, RunKey, ShardId,
    StickyAffinity, TaskQueueName, WorkerIdentity,
};

/// A repair transaction lost OCC; retry from a fresh state read and acquisition check.
#[derive(Debug, thiserror::Error)]
#[error("workflow dispatch repair serialization conflict")]
pub struct WorkflowDispatchRepairConflict;

/// Identity of a single startable generation within a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTaskIncarnation {
    /// Durable run identity; reset successors have a different key.
    pub run_key: RunKey,
    /// Monotonic task generation, independent of worker-visible attempts.
    pub logical_seq: LogicalTaskSeq,
}

/// Stored routing coordinates, without freezing a registry-selected target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowDispatchRouting {
    /// Resolve the current run policy through the existing deployment resolver.
    Live,
    /// Explicit queue coordinates already resolved in authoritative state.
    Exact {
        /// Stored deployment coordinate, including a present empty string.
        deployment: DeploymentId,
        /// Optional build coordinate; absence differs from a present empty value.
        build_id: Option<BuildId>,
    },
}

impl WorkflowDispatchRouting {
    pub(crate) fn coordinates(&self) -> (i16, Option<&str>, Option<&str>) {
        match self {
            Self::Live => (0, None, None),
            Self::Exact {
                deployment,
                build_id,
            } => (
                1,
                Some(&deployment.0),
                build_id.as_ref().map(|value| value.0.as_str()),
            ),
        }
    }
}

/// One read-only normal-queue scan range, spanning all execution homes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDiscoveryRange {
    /// Namespace isolation is explicit even when lookup digests collide.
    pub namespace_id: NamespaceId,
    /// Full normal task-queue name, compared after digest lookup.
    pub queue_name: TaskQueueName,
    /// Live policy or exact stored coordinates selected by poll demand.
    pub routing: WorkflowDispatchRouting,
}

impl WorkflowDiscoveryRange {
    /// Compare raw coordinates after lookup; a digest collision cannot route work.
    #[must_use]
    pub fn matches(&self, row: &WorkflowDispatchRow) -> bool {
        !row.sticky
            && self.namespace_id == row.namespace_id
            && self.queue_name == row.queue_name
            && self.routing == row.routing
    }

    pub(crate) fn lookup_keys(&self) -> [String; 3] {
        lookup_keys(&self.queue_name.0, &self.routing)
    }
}

/// Strict keyset continuation; no delivery or traversal state is persisted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct WorkflowDispatchPosition {
    /// Normalized priority band; smaller values precede larger ones.
    pub priority_key: i16,
    /// Original schedule time, independent of discovery time.
    pub scheduled_at: OffsetDateTime,
    /// Unique tie-breaker within the selected range.
    pub run_key: RunKey,
}

/// A short page of candidates, including any digest collisions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDispatchPage {
    /// Raw rows that callers must match against their selected range.
    pub candidates: Vec<WorkflowDispatchRow>,
    /// Last examined row, even when every candidate is later rejected.
    pub last_examined: Option<WorkflowDispatchPosition>,
    /// True when the bounded query returned fewer rows than requested.
    pub exhausted: bool,
}

/// Narrow durable intent; no queue claim or acknowledgement is represented here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDispatchRow {
    /// Run and startable task generation.
    pub incarnation: WorkflowTaskIncarnation,
    /// Placement used by the hot-state write, never the lane-local run hash.
    pub execution_home: ShardId,
    /// Owning namespace.
    pub namespace_id: NamespaceId,
    /// Sticky destination when recoverable affinity holds, otherwise normal.
    pub queue_name: TaskQueueName,
    /// Normal destination retained for sticky fallback.
    pub normal_queue_name: TaskQueueName,
    /// Sticky rows are absent from the periodic normal-queue index.
    pub sticky: bool,
    /// Stored coordinates or policy requiring live resolution.
    pub routing: WorkflowDispatchRouting,
    /// Original schedule timestamp.
    pub scheduled_at: OffsetDateTime,
    /// Normalized workflow priority band, independent of fairness counters.
    pub priority_key: i16,
    /// Raw priority and fairness metadata preserved for delivery policy.
    pub priority: Option<Priority>,
    /// Sticky worker hint, present only for a recoverable sticky destination.
    pub sticky_worker: Option<WorkerIdentity>,
    /// Absolute pending deadline, retained even after affinity is cleared.
    pub schedule_to_start_deadline: Option<OffsetDateTime>,
}

impl WorkflowDispatchRow {
    /// Position consumed after examining this row, including a rejected collision.
    #[must_use]
    pub fn position(&self) -> WorkflowDispatchPosition {
        WorkflowDispatchPosition {
            priority_key: self.priority_key,
            scheduled_at: self.scheduled_at,
            run_key: self.incarnation.run_key,
        }
    }

    pub(crate) fn lookup_keys(&self) -> [String; 3] {
        lookup_keys(&self.queue_name.0, &self.routing)
    }

    /// Reject representations that either backend cannot persist losslessly.
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.incarnation.logical_seq.0 > 0,
            "workflow dispatch sequence must be positive"
        );
        i64::try_from(self.incarnation.logical_seq.0)?;
        ensure!(
            (1..=5).contains(&self.priority_key),
            "invalid workflow dispatch priority band"
        );
        ensure!(
            self.sticky == self.sticky_worker.is_some(),
            "invalid workflow dispatch sticky worker"
        );
        ensure!(
            !self.sticky || self.schedule_to_start_deadline.is_some(),
            "sticky dispatch requires a recoverable deadline"
        );
        self.validate_size()?;
        Ok(())
    }

    fn validate_size(&self) -> Result<()> {
        let (_, deployment, build) = self.routing.coordinates();
        let priority = self
            .priority
            .as_ref()
            .map(postcard::to_allocvec)
            .transpose()?;
        let columns = [
            self.queue_name.0.len(),
            self.normal_queue_name.0.len(),
            deployment.map_or(0, str::len),
            build.map_or(0, str::len),
            self.sticky_worker
                .as_ref()
                .map_or(0, |worker| worker.0.len()),
            priority.as_ref().map_or(0, Vec::len),
        ];
        ensure!(
            columns.iter().all(|bytes| *bytes <= 1024 * 1024),
            "workflow dispatch column exceeds DSQL's 1 MiB limit"
        );
        // Conservative room for scalar columns, digests and row/index overhead.
        // One-run repair modifies just this row. Even charging old + new images
        // stays below the separate 10 MiB / 3,000-row transaction service limits,
        // including an orphan deletion (at most one existing 2 MiB row).
        ensure!(
            columns.iter().sum::<usize>() + 4096 <= 2 * 1024 * 1024,
            "workflow dispatch row exceeds the conservative 2 MiB encoding budget"
        );
        Ok(())
    }
}

/// Derive complete intent from authoritative state and its execution placement.
///
/// Transient retries are normal tasks. Speculative tasks keep their independent
/// delivery path and never acquire durable dispatch rows.
#[must_use]
pub fn derive_workflow_dispatch(
    state: &WorkflowState,
    execution_home: ShardId,
) -> Option<WorkflowDispatchRow> {
    let pending = state.pending_workflow_task.as_ref()?;
    if state.status != ExecutionStatus::Running
        || pending.started_event_id.is_some()
        || pending.task_type != WorkflowTaskType::Normal
    {
        return None;
    }
    let sticky = recoverable_sticky(state);
    let priority_key = state
        .priority
        .as_ref()
        .map_or(0, |priority| priority.priority_key);
    Some(WorkflowDispatchRow {
        incarnation: WorkflowTaskIncarnation {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
        },
        execution_home,
        namespace_id: state.namespace_id,
        queue_name: sticky.map_or_else(
            || state.task_queue.clone(),
            |affinity| affinity.sticky_queue.clone(),
        ),
        normal_queue_name: state.task_queue.clone(),
        sticky: sticky.is_some(),
        routing: state
            .deployment
            .as_ref()
            .map_or(WorkflowDispatchRouting::Live, |deployment| {
                WorkflowDispatchRouting::Exact {
                    deployment: deployment.clone(),
                    build_id: state.build_id.clone(),
                }
            }),
        scheduled_at: dispatch_timestamp(pending.scheduled_at),
        // Same five-band/default-3 policy as runtime/task_ordering.rs; keep the
        // raw Priority alongside it so task-queue fairness policy remains live.
        priority_key: if priority_key == 0 {
            3
        } else {
            priority_key.clamp(1, 5) as i16
        },
        priority: state.priority.clone(),
        sticky_worker: sticky.map(|affinity| affinity.worker_identity.clone()),
        schedule_to_start_deadline: pending.schedule_to_start_deadline.map(dispatch_timestamp),
    })
}

pub(crate) fn recoverable_sticky(state: &WorkflowState) -> Option<&StickyAffinity> {
    state
        .pending_workflow_task
        .as_ref()?
        .schedule_to_start_deadline?;
    state
        .sticky
        .as_ref()
        .filter(|affinity| !affinity.sticky_queue.0.is_empty())
}

fn dispatch_timestamp(value: OffsetDateTime) -> OffsetDateTime {
    // SQLx encodes TIMESTAMPTZ as whole microseconds since 2000, truncating
    // toward zero. Normalize both backends identically; authoritative state
    // retains its original deadline and precision outside this derived view.
    let epoch = OffsetDateTime::UNIX_EPOCH + Duration::seconds(946_684_800);
    epoch
        + Duration::microseconds(
            i64::try_from((value - epoch).whole_microseconds())
                .expect("time's supported date range fits SQL timestamp microseconds"),
        )
}

fn lookup_keys(queue: &str, routing: &WorkflowDispatchRouting) -> [String; 3] {
    let (_, deployment, build_id) = routing.coordinates();
    [
        lookup_digest(b"queue", Some(queue)),
        lookup_digest(b"deployment", deployment),
        lookup_digest(b"build", build_id),
    ]
}

fn lookup_digest(domain: &[u8], value: Option<&str>) -> String {
    let mut digest = Sha256::new();
    digest.update(b"tokeira-workflow-dispatch-v1\0");
    digest.update((domain.len() as u64).to_be_bytes());
    digest.update(domain);
    digest.update([u8::from(value.is_some())]);
    if let Some(value) = value {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_normalization_matches_sqlx_on_both_sides_of_its_epoch() {
        let epoch = OffsetDateTime::UNIX_EPOCH + Duration::seconds(946_684_800);
        for nanos in [-1999, -1001, -999, 0, 999, 1001, 1999] {
            assert_eq!(
                dispatch_timestamp(epoch + Duration::nanoseconds(nanos)),
                epoch + Duration::microseconds(nanos / 1000)
            );
        }
    }

    #[test]
    fn lookup_digest_vectors_preserve_domain_presence_and_long_names() {
        assert_eq!(
            lookup_digest(b"queue", Some("orders")),
            "a4344c14c544ebddc185e4be48efce10b522dbd2348426c28510e5ba429831a1"
        );
        assert_eq!(
            lookup_digest(b"deployment", None),
            "66b404f520ce20aac790f3828d065b5a3d0050eb65da83d05c89339eb588e6b6"
        );
        assert_eq!(
            lookup_digest(b"deployment", Some("")),
            "7f17f7a7602e923a376bdfc843ff80a37ae6ce7688cc63c5b8eeddc53bac3696"
        );
        assert_eq!(
            lookup_digest(b"build", Some("")),
            "9fd325349ff21c53f538da13aa37921db0e079107dd40d85a6a8368dba1584b0"
        );
        assert_eq!(
            lookup_digest(b"queue", Some(&"x".repeat(8192))),
            "6d41bfc6864e83ec3f729c7b991dcc6c6c6b4e879746106c1c58bcbf00dadb6d"
        );
    }

    #[test]
    fn dispatch_schema_keeps_the_normal_index_partial_and_home_index_complete() {
        let table = include_str!("../migrations/V074__workflow_dispatch.sql");
        let queue = include_str!("../migrations/V075__workflow_dispatch_queue_index.sql");
        let home = include_str!("../migrations/V076__workflow_dispatch_home_index.sql");
        assert!(table.contains("run_key UUID PRIMARY KEY"));
        assert!(!table.contains("CHECK"));
        assert!(queue.contains("CREATE INDEX ASYNC"));
        assert!(queue.contains("WHERE sticky = false"));
        assert!(queue.contains("priority_key, scheduled_at, run_key"));
        assert!(home.contains("CREATE INDEX ASYNC"));
        assert!(home.contains("(shard_id, run_key)"));
        assert!(!home.contains("WHERE"));
    }
}
