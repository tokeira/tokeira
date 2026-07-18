//! The engine→projection visibility adapter (spec task 24.2).
//!
//! Implements the engine's [`VisibilitySink`] hook by converting a component's
//! typed [`VisibilitySnapshot`] into a projection [`ProjectionRecord`] and applying
//! it to the shared visibility store through the same [`ProjectionSink`] the
//! workflow projection worker uses. The write happens post-commit, **off the
//! correctness path** (Requirement 10.15; design.md "writes the snapshot to the
//! shared visibility store"; `reference/DECISION-visibility-engine-adapter.md`).
//!
//! Workflows reach the shared index via `projection_log → worker`; activities reach
//! it directly here. Both apply the same monotonic, idempotent, apply-iff-newer
//! contract keyed on `(authority_epoch, source_transition_seq)`, so two producers on
//! one index is sound — the "one log, two producers" argument (design.md), not a
//! double-write hazard.

use std::sync::Arc;

use async_trait::async_trait;
use tokeira_chasm::{ExecutionKey, LifecycleState, VersionedTransition, VisibilitySnapshot};
use tokeira_projection::ProjectionSink;
use tokeira_storage::{ProjectionContext, ProjectionRecord};
use tokeira_types::{
    ArchetypeId, ExecutionStatus, NamespaceId, RunId, RunKey, TaskQueueName, TransitionSeq,
    VisibilityLifecycleState, WorkflowId, WorkflowType,
};
use uuid::Uuid;

use super::VisibilitySink;

/// Reserved system fields a component must not spoof through a user search
/// attribute (Requirement 10.10). The typed snapshot carries these as struct
/// fields; a like-named entry in `search_attributes` is rejected.
const RESERVED_FIELDS: &[&str] = &[
    "archetype",
    "status",
    "lifecycle_state",
    "namespace",
    "run_id",
    "business_id",
];

/// Adapts CHASM engine transition-close events into writes on the shared visibility
/// store, reusing the projection apply path (row + search-attribute index + rollup).
pub struct ProjectionVisibilitySink {
    sink: Arc<dyn ProjectionSink>,
    partition_count: u32,
}

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for ProjectionVisibilitySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionVisibilitySink")
            .field("partition_count", &self.partition_count)
            .finish_non_exhaustive()
    }
}

impl ProjectionVisibilitySink {
    /// Build an adapter over a projection sink. `partition_count` matches the
    /// deployment's projection partitioning; for a direct store write it only keeps
    /// the record's `partition_id` metric coherent (no worker consumes it).
    pub fn new(sink: Arc<dyn ProjectionSink>, partition_count: u32) -> Self {
        Self {
            sink,
            partition_count: partition_count.max(1),
        }
    }
}

#[async_trait]
impl VisibilitySink for ProjectionVisibilitySink {
    async fn record(
        &self,
        key: &ExecutionKey,
        archetype_id: u32,
        version: VersionedTransition,
        snapshot: VisibilitySnapshot,
    ) -> anyhow::Result<()> {
        let record = build_record(key, archetype_id, version, snapshot, self.partition_count)?;
        let partition_id = record.partition_id;
        self.sink.apply(&record, partition_id).await
    }
}

/// Map a component's typed snapshot plus the engine-stamped identity/version into a
/// projection record. See `reference/DECISION-visibility-engine-adapter.md` for the
/// per-field rationale (e.g. `run_key = run_id`, `execution_status` is a workflow-
/// internal placeholder while `status_keyword` is authoritative — 23.7).
///
/// Shared with the repair scanner ([`super::repair`]), which rebuilds the same record
/// from persisted node state — so the post-commit and repair paths produce byte-identical
/// projections.
pub(crate) fn build_record(
    key: &ExecutionKey,
    archetype_id: u32,
    version: VersionedTransition,
    snapshot: VisibilitySnapshot,
    partition_count: u32,
) -> anyhow::Result<ProjectionRecord> {
    for name in snapshot.search_attributes.0.keys() {
        if RESERVED_FIELDS.contains(&name.as_str()) {
            anyhow::bail!("search attribute `{name}` collides with a reserved system field");
        }
    }

    let namespace_id = NamespaceId(parse_uuid("namespace_id", &key.namespace_id)?);
    let run_uuid = parse_uuid("run_id", &key.run_id)?;
    let run_key = RunKey(run_uuid);
    let run_id = RunId(run_uuid);
    let business_id = key.business_id.clone();

    let lifecycle_state = match snapshot.lifecycle_state {
        LifecycleState::Running => VisibilityLifecycleState::Open,
        LifecycleState::Completed | LifecycleState::Failed => VisibilityLifecycleState::Closed,
    };
    let start_time = snapshot
        .start_time_unix_nanos
        .and_then(to_offset)
        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH);
    let close_time = snapshot.close_time_unix_nanos.and_then(to_offset);
    let update_time = close_time.unwrap_or(start_time);
    let transition_count = version.transition_count;

    let context = ProjectionContext {
        archetype_id: ArchetypeId(archetype_id),
        namespace_id,
        business_id: business_id.clone(),
        authority_epoch: version.namespace_failover_version,
        status_keyword: snapshot.status_keyword,
        lifecycle_state,
        // Generic business identity mirrors `workflow_id`; the typed enum status is
        // a workflow-internal placeholder (status_keyword is the query key — 23.7).
        workflow_id: WorkflowId(business_id.clone()),
        run_id,
        workflow_type: WorkflowType(snapshot.execution_type.unwrap_or_default()),
        task_queue: TaskQueueName(snapshot.task_queue.unwrap_or_default()),
        execution_status: ExecutionStatus::Running,
        start_time,
        update_time,
        execution_time: None,
        close_time,
        history_length: transition_count,
        execution_duration: None,
        state_transition_count: transition_count,
        transition_count,
        history_size_bytes: 0,
        parent_workflow_id: None,
        parent_run_id: None,
        root_workflow_id: Some(WorkflowId(business_id)),
        root_run_id: Some(run_id),
        search_attr_generation: 0,
        memo: snapshot.memo,
        search_attributes: snapshot.search_attributes,
    };

    let partition_id =
        (u128::from_le_bytes(run_uuid.into_bytes()) % u128::from(partition_count)) as u32;
    Ok(ProjectionRecord {
        partition_id,
        fanout: 1,
        run_key,
        transition_seq: TransitionSeq(transition_count.max(0) as u64),
        context,
    })
}

fn parse_uuid(field: &str, value: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(value)
        .map_err(|e| anyhow::anyhow!("execution key {field} `{value}` is not a UUID: {e}"))
}

fn to_offset(unix_nanos: i64) -> Option<time::OffsetDateTime> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(unix_nanos)).ok()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokeira_types::{SearchAttrValue, SearchAttributes};
    use uuid::Uuid;

    use super::*;

    fn snapshot(status: &str, lifecycle: LifecycleState) -> VisibilitySnapshot {
        VisibilitySnapshot {
            status_keyword: status.to_owned(),
            lifecycle_state: lifecycle,
            execution_type: Some("MyActivity".to_owned()),
            task_queue: Some("tq-1".to_owned()),
            start_time_unix_nanos: Some(100),
            close_time_unix_nanos: None,
            search_attributes: SearchAttributes::default(),
            memo: Default::default(),
        }
    }

    fn key(namespace: &str, run: &str) -> ExecutionKey {
        ExecutionKey::new(namespace, "activity-biz", run)
    }

    fn version(transition_count: i64) -> VersionedTransition {
        VersionedTransition {
            namespace_failover_version: 0,
            transition_count,
        }
    }

    #[test]
    fn maps_running_activity_to_open_row_keyed_by_run_id() {
        let ns = Uuid::from_u128(1);
        let run = Uuid::from_u128(2);
        let record = build_record(
            &key(&ns.to_string(), &run.to_string()),
            7,
            version(3),
            snapshot("Running", LifecycleState::Running),
            4,
        )
        .expect("maps");

        // run_key is the run_id UUID; identity comes from the execution key.
        assert_eq!(record.run_key, RunKey(run));
        assert_eq!(record.context.run_id, RunId(run));
        assert_eq!(record.context.namespace_id, NamespaceId(ns));
        assert_eq!(record.context.archetype_id, ArchetypeId(7));
        assert_eq!(record.context.business_id, "activity-biz");
        assert_eq!(
            record.context.workflow_id,
            WorkflowId("activity-biz".into())
        );
        // status_keyword is authoritative; the typed enum is a placeholder (23.7).
        assert_eq!(record.context.status_keyword, "Running");
        assert_eq!(record.context.execution_status, ExecutionStatus::Running);
        assert_eq!(
            record.context.lifecycle_state,
            VisibilityLifecycleState::Open
        );
        // execution_type rides the generic workflow_type column; task_queue carried.
        assert_eq!(
            record.context.workflow_type,
            WorkflowType("MyActivity".into())
        );
        assert_eq!(record.context.task_queue, TaskQueueName("tq-1".into()));
        // version stamping: (failover, transition_count) -> (authority_epoch, seq).
        assert_eq!(record.context.authority_epoch, 0);
        assert_eq!(record.context.transition_count, 3);
        assert_eq!(record.transition_seq, TransitionSeq(3));
        assert!(record.partition_id < 4);
        assert!(record.context.close_time.is_none());
    }

    #[test]
    fn closed_activity_maps_to_closed_with_close_time() {
        let mut snap = snapshot("Completed", LifecycleState::Completed);
        snap.close_time_unix_nanos = Some(500);
        let record = build_record(
            &key(
                &Uuid::from_u128(1).to_string(),
                &Uuid::from_u128(2).to_string(),
            ),
            7,
            version(9),
            snap,
            1,
        )
        .expect("maps");
        assert_eq!(
            record.context.lifecycle_state,
            VisibilityLifecycleState::Closed
        );
        assert!(record.context.close_time.is_some());
    }

    #[test]
    fn rejects_reserved_system_field_in_search_attributes() {
        let mut snap = snapshot("Running", LifecycleState::Running);
        let mut map = BTreeMap::new();
        map.insert(
            "status".to_owned(),
            SearchAttrValue::Keyword("spoof".into()),
        );
        snap.search_attributes = SearchAttributes(map);
        let err = build_record(
            &key(
                &Uuid::from_u128(1).to_string(),
                &Uuid::from_u128(2).to_string(),
            ),
            7,
            version(1),
            snap,
            1,
        )
        .expect_err("reserved field must be rejected");
        assert!(err.to_string().contains("reserved system field"));
    }

    #[test]
    fn rejects_non_uuid_execution_key() {
        let err = build_record(
            &key("not-a-uuid", &Uuid::from_u128(2).to_string()),
            7,
            version(1),
            snapshot("Running", LifecycleState::Running),
            1,
        )
        .expect_err("non-uuid namespace must be rejected");
        assert!(err.to_string().contains("is not a UUID"));
    }
}
