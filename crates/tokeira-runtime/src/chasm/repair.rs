//! The visibility repair scanner (spec task 26.1).
//!
//! Guarantees a committed transition cannot permanently lack a visibility projection
//! (Requirement 10.11). The snapshot is fully recomputable from persisted node state
//! (the close-time precondition, 26.1 A), so the scanner rebuilds each execution's
//! snapshot from its root node and re-applies it. Re-apply is version-keyed
//! (apply-iff-newer), so a still-projected execution is a no-op and a dropped one is
//! repaired — the C2.5 "transition-derived, repairable" shape, not full C3
//! ("visibility = fold(history)").
//!
//! This is the durability shape `tokeira-runtime/AGENTS.md` §3 sanctions: the
//! authoritative record lives on the node and the projection is a derived effect a
//! sweeper reconstructs — never a load-bearing queue write (which is why this is a
//! scanner, not an in-commit outbox).

use std::sync::Arc;

use anyhow::Result;
use tokeira_chasm::{ChasmNode, ExecutionKey, VisibilitySnapshot};
use tokeira_projection::ProjectionSink;
use tokeira_storage::ChasmNodeRepository;

use super::visibility_adapter::build_record;

/// Rebuilds a component's [`VisibilitySnapshot`] from its persisted root-node data,
/// dispatched on archetype id. The bootstrap supplies this (it knows the concrete
/// component types); it returns `None` for an archetype it does not handle or a node
/// that contributes no visibility. Keeping the decode here (rather than a registry
/// vtable) avoids forcing every registered component to be a `VisibilityContributor`.
pub type SnapshotRebuilder = Arc<dyn Fn(u32, &[u8]) -> Option<VisibilitySnapshot> + Send + Sync>;

/// What one [`VisibilityRepairScanner::repair_once`] pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairStats {
    /// Executions visited.
    pub scanned: usize,
    /// Executions whose snapshot was rebuilt and re-applied to the index (the apply
    /// itself is iff-newer, so this counts attempts, not net index changes).
    pub rebuilt: usize,
}

/// Reconstructs visibility projections from authoritative node state and re-applies
/// them iff-newer (Requirement 10.11).
pub struct VisibilityRepairScanner {
    nodes: Arc<dyn ChasmNodeRepository>,
    sink: Arc<dyn ProjectionSink>,
    rebuild: SnapshotRebuilder,
    partition_count: u32,
}

impl VisibilityRepairScanner {
    pub fn new(
        nodes: Arc<dyn ChasmNodeRepository>,
        sink: Arc<dyn ProjectionSink>,
        rebuild: SnapshotRebuilder,
        partition_count: u32,
    ) -> Self {
        Self {
            nodes,
            sink,
            rebuild,
            partition_count: partition_count.max(1),
        }
    }

    /// One repair pass over every committed execution, in the node store's
    /// deterministic order. Rebuilds each snapshot from persisted root state and
    /// re-applies it (apply-iff-newer). A single malformed execution is logged and
    /// skipped, never aborting the pass.
    pub async fn repair_once(&self) -> Result<RepairStats> {
        let executions = self.nodes.scan_executions().await?;
        let scanned = executions.len();
        let mut rebuilt = 0usize;
        for (key, root) in executions {
            match self.repair_execution(&key, &root).await {
                Ok(true) => rebuilt += 1,
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(?error, ?key, "visibility repair skipped an execution");
                }
            }
        }
        Ok(RepairStats { scanned, rebuilt })
    }

    async fn repair_execution(&self, key: &ExecutionKey, root: &ChasmNode) -> Result<bool> {
        let archetype_id = root.metadata.component_type_id;
        let version = root.metadata.versioned_transition;
        let Some(data) = root.data.as_deref() else {
            return Ok(false);
        };
        let Some(snapshot) = (self.rebuild)(archetype_id, data) else {
            return Ok(false);
        };
        let record = build_record(key, archetype_id, version, snapshot, self.partition_count)?;
        let partition_id = record.partition_id;
        // Version-keyed apply: a no-op when the index already has this version or
        // newer, a repair when it is missing or behind.
        self.sink.apply(&record, partition_id).await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_chasm::{LifecycleState, NodeMetadata, VersionedTransition};
    use tokeira_projection::{InMemoryVisibilityStore, VisibilitySink, VisibilityStore};
    use tokeira_storage::{ExpectedVersion, InMemoryChasmNodeStore, NodeWrite};
    use tokeira_types::RunKey;
    use uuid::Uuid;

    /// Test rebuilder: the node's root `data` is the `status_keyword` bytes.
    fn rebuilder() -> SnapshotRebuilder {
        Arc::new(|_archetype_id, data: &[u8]| {
            let status = String::from_utf8(data.to_vec()).ok()?;
            let lifecycle = if status == "Running" {
                LifecycleState::Running
            } else {
                LifecycleState::Completed
            };
            Some(VisibilitySnapshot {
                status_keyword: status,
                lifecycle_state: lifecycle,
                execution_type: Some("Test".to_owned()),
                task_queue: None,
                start_time_unix_nanos: Some(1),
                close_time_unix_nanos: None,
                search_attributes: Default::default(),
                memo: Default::default(),
            })
        })
    }

    fn root_node(status: &str) -> tokeira_chasm::ChasmNode {
        // A single fixed version per execution (convergence, not version races, is
        // the property under test). archetype id 1, root component node.
        let vt = VersionedTransition::new(0, 5);
        tokeira_chasm::ChasmNode {
            metadata: NodeMetadata::new(1, Some(LifecycleState::Completed), vt),
            data: Some(status.as_bytes().to_vec()),
        }
    }

    // Feature: chasm-foundation, Property 14: Repair convergence
    // **Validates: Requirements 10.11**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_repair_convergence(
            statuses in prop::collection::vec(
                prop_oneof![Just("Running"), Just("Completed"), Just("Failed")],
                1..8,
            ),
            drop_seed in any::<u64>(),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let nodes = Arc::new(InMemoryChasmNodeStore::new());
                let store = InMemoryVisibilityStore::default();
                let sink: Arc<dyn ProjectionSink> = Arc::new(VisibilitySink::new(store.clone()));
                let ns = Uuid::from_u128(1);

                // Seed authoritative node state (distinct run ids per execution).
                for (i, status) in statuses.iter().enumerate() {
                    let run = Uuid::from_u128(1000 + i as u128);
                    let key =
                        ExecutionKey::new(ns.to_string(), format!("act-{i}"), run.to_string());
                    nodes
                        .persist_dirty(
                            &key,
                            vec![NodeWrite {
                                encoded_path: Vec::new(),
                                node: root_node(status),
                                expected: ExpectedVersion::Absent,
                            }],
                        )
                        .await
                        .unwrap();
                }

                let scanner =
                    VisibilityRepairScanner::new(nodes.clone(), sink.clone(), rebuilder(), 1);

                // First pass projects everything from authoritative state.
                let first = scanner.repair_once().await.unwrap();
                prop_assert_eq!(first.scanned, statuses.len());

                // Drop an arbitrary subset of projections (loss after commit).
                for i in 0..statuses.len() {
                    if (drop_seed >> (i % 64)) & 1 == 1 {
                        store
                            .delete_execution(RunKey(Uuid::from_u128(1000 + i as u128)))
                            .await
                            .unwrap();
                    }
                }

                // Repair drives the index back to the fold of the latest snapshots,
                // regardless of which projections were dropped (idempotent on the rest).
                scanner.repair_once().await.unwrap();

                for (i, status) in statuses.iter().enumerate() {
                    let row = store.get_row(RunKey(Uuid::from_u128(1000 + i as u128))).await;
                    prop_assert!(row.is_some(), "execution {} missing after repair", i);
                    prop_assert_eq!(row.unwrap().status_keyword, *status);
                }
                Ok(())
            })?;
        }
    }
}
