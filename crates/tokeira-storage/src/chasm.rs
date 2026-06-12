//! The CHASM node store: encoded-path-keyed node rows with write-only-dirty-node
//! persistence and optimistic-concurrency (CAS) fencing.
//!
//! This is the storage half of the CHASM substrate (design "Storage Design";
//! Requirement 9). The pure [`tokeira_chasm`] crate computes *what* a transition
//! changes (a [`TransitionResult`](tokeira_chasm::TransitionResult) carrying the
//! dirty-node set); this module persists exactly those nodes, each write fenced on
//! the node's prior [`VersionedTransition`] (Requirement 9.3–9.6). It never decides
//! transition correctness — that is the pure crate's job — and it never owns the
//! *when*/*under-what-fence* of a commit, which is the runtime's job
//! (`crates/tokeira-storage/AGENTS.md`).
//!
//! The trait is backend-agnostic. The in-memory [`InMemoryChasmNodeStore`] is the
//! verification vehicle the CHASM engine integration tests run over (design
//! Verification; spec task 15.1); the DSQL implementation persists the same
//! semantics against the `chasm_node` table (migration `V049`) and lives behind the
//! `dsql` feature.
//!
//! ## The CAS-fenced, all-or-nothing batch
//!
//! [`persist_dirty`](ChasmNodeRepository::persist_dirty) takes the whole dirty-node
//! batch for one transition and applies it atomically: every write is checked
//! against its [`ExpectedVersion`] first, and if **any** check fails the batch is
//! rejected as [`NodePersistOutcome::Conflict`] with **no** partial write
//! (Requirement 9.5, 9.6). The runtime responds to a conflict by reloading the
//! execution and re-running the transition — never by force-overwriting. This is
//! the same fenced-commit posture as the workflow `RunRepository`, specialized to
//! the per-node VT stamp.

use std::{collections::HashMap, sync::Mutex};

use anyhow::Result;
use async_trait::async_trait;
use tokeira_chasm::{ChasmNode, ExecutionKey, VersionedTransition};

/// The compare-and-set precondition for persisting one dirty node (Requirement
/// 9.4). It fences a write on the node's prior last-update [`VersionedTransition`]
/// so a stale transition cannot clobber a newer one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// The node must **not** already exist — the create path for a node the
    /// transition brought into being this commit.
    Absent,
    /// The node's stored last-update VT must equal this value — the update path.
    /// A mismatch means another transition advanced the node first (conflict).
    Vt(VersionedTransition),
}

/// One dirty node to persist, paired with its CAS precondition (Requirement 9.3,
/// 9.4). `node` already carries the committing VT stamp (the pure crate stamped it
/// at [`close_transaction`](tokeira_chasm::NodeTree::close_transaction)); `expected`
/// is the *prior* VT the fence checks against.
#[derive(Debug, Clone)]
pub struct NodeWrite {
    /// The node's encoded path (its key within the execution).
    pub encoded_path: Vec<u8>,
    /// The node to write, already stamped with the committing VT.
    pub node: ChasmNode,
    /// The CAS precondition fencing this write.
    pub expected: ExpectedVersion,
}

/// The result of a [`persist_dirty`](ChasmNodeRepository::persist_dirty) batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodePersistOutcome {
    /// Every node's fence held; the batch was applied as one atomic unit.
    Applied,
    /// At least one node's fence failed; nothing was written. The runtime should
    /// reload the execution and re-run the transition (Requirement 9.5).
    Conflict {
        /// Human-readable description of the first failing fence.
        reason: String,
    },
}

/// The durable store for CHASM execution node trees (Requirement 9).
///
/// Implementations persist nodes keyed by `(ExecutionKey, encoded_path)`, support
/// prefix range scans over `encoded_path` within one execution (Requirement 4.4),
/// and fence each write on the node's prior VT (Requirement 9.4).
#[async_trait]
pub trait ChasmNodeRepository: Send + Sync {
    /// Persist the dirty-node batch for one transition atomically, CAS-fenced.
    ///
    /// Applies all writes or none: if any node's [`ExpectedVersion`] does not match
    /// the stored state, returns [`NodePersistOutcome::Conflict`] with no partial
    /// write (Requirement 9.3, 9.5, 9.6).
    async fn persist_dirty(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
    ) -> Result<NodePersistOutcome>;

    /// Load every node of an execution, in encoded-path order (a whole-tree range
    /// scan). Empty when the execution does not exist.
    async fn load_execution(&self, key: &ExecutionKey) -> Result<Vec<(Vec<u8>, ChasmNode)>>;

    /// Load the subtree rooted at `encoded_prefix` (the node itself and all
    /// descendants) as a single prefix range scan over `encoded_path`
    /// (Requirement 4.4). Pass the empty slice to load the whole execution.
    async fn load_subtree(
        &self,
        key: &ExecutionKey,
        encoded_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, ChasmNode)>>;

    /// Range-delete the entire node subtree of an execution (Requirement 6.1,
    /// `DeleteExecution`). Idempotent: deleting an absent execution is a no-op.
    async fn delete_execution(&self, key: &ExecutionKey) -> Result<()>;
}

/// In-memory [`ChasmNodeRepository`] for tests, examples, and the CHASM engine
/// integration suite (spec task 15.1). It realizes the full dirty-only-write, CAS,
/// and prefix-range-scan semantics so behaviour proven here matches the DSQL
/// backend; it is **not** a concurrency or scale reference for a cluster.
///
/// Nodes are held per execution in a `BTreeMap` keyed by encoded path, so range
/// scans follow the [path encoder](tokeira_chasm::PathEncoder) sort contract.
#[derive(Debug, Default)]
pub struct InMemoryChasmNodeStore {
    // `ExecutionKey` is `Hash`/`Eq` but not `Ord`, so the outer map is a `HashMap`;
    // the inner per-execution map is a `BTreeMap` so encoded-path range scans are
    // contiguous and ordered.
    executions: Mutex<HashMap<ExecutionKey, std::collections::BTreeMap<Vec<u8>, ChasmNode>>>,
}

impl InMemoryChasmNodeStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ChasmNodeRepository for InMemoryChasmNodeStore {
    async fn persist_dirty(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
    ) -> Result<NodePersistOutcome> {
        let mut executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        let tree = executions.entry(key.clone()).or_default();

        // Phase 1 — check every fence before mutating anything, so the batch is
        // all-or-nothing (Requirement 9.6). A failed fence yields Conflict with no
        // partial write.
        for write in &batch {
            match write.expected {
                ExpectedVersion::Absent => {
                    if tree.contains_key(&write.encoded_path) {
                        return Ok(NodePersistOutcome::Conflict {
                            reason: format!(
                                "node at {:?} expected absent but already exists",
                                write.encoded_path
                            ),
                        });
                    }
                }
                ExpectedVersion::Vt(expected) => match tree.get(&write.encoded_path) {
                    Some(existing) if existing.metadata.versioned_transition == expected => {}
                    Some(existing) => {
                        return Ok(NodePersistOutcome::Conflict {
                            reason: format!(
                                "node at {:?} VT {:?} does not match expected {expected:?}",
                                write.encoded_path, existing.metadata.versioned_transition
                            ),
                        });
                    }
                    None => {
                        return Ok(NodePersistOutcome::Conflict {
                            reason: format!(
                                "node at {:?} expected VT {expected:?} but is absent",
                                write.encoded_path
                            ),
                        });
                    }
                },
            }
        }

        // Phase 2 — every fence held; apply the whole batch.
        for write in batch {
            tree.insert(write.encoded_path, write.node);
        }
        Ok(NodePersistOutcome::Applied)
    }

    async fn load_execution(&self, key: &ExecutionKey) -> Result<Vec<(Vec<u8>, ChasmNode)>> {
        let executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        Ok(executions
            .get(key)
            .map(|tree| tree.iter().map(|(p, n)| (p.clone(), n.clone())).collect())
            .unwrap_or_default())
    }

    async fn load_subtree(
        &self,
        key: &ExecutionKey,
        encoded_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, ChasmNode)>> {
        let end = tokeira_chasm::path::subtree_range_end(encoded_prefix);
        let executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        Ok(executions
            .get(key)
            .map(|tree| {
                tree.range(encoded_prefix.to_vec()..end)
                    .map(|(p, n)| (p.clone(), n.clone()))
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn delete_execution(&self, key: &ExecutionKey) -> Result<()> {
        let mut executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        executions.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_chasm::{ChasmNode, NodeMetadata, NodeTree, RetainAllValidator};

    fn key() -> ExecutionKey {
        ExecutionKey::new("ns", "wf-1", "run-1")
    }

    fn vt(failover: i64, count: i64) -> VersionedTransition {
        VersionedTransition::new(failover, count)
    }

    // Build a one-transition dirty batch from a node tree, capturing the prior-VT
    // fences the engine would supply (Absent for created nodes here).
    fn first_commit_batch() -> (ExecutionKey, Vec<NodeWrite>) {
        let mut tree = NodeTree::new();
        tree.create_node(
            b"$state".to_vec(),
            7,
            Some(tokeira_chasm::LifecycleState::Running),
            Some(vec![1]),
        )
        .expect("create");
        let result = tree
            .close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close");
        let batch = result
            .dirty_nodes
            .into_iter()
            .map(|(encoded_path, node)| NodeWrite {
                encoded_path,
                node,
                expected: ExpectedVersion::Absent,
            })
            .collect();
        (key(), batch)
    }

    #[tokio::test]
    async fn persist_then_load_round_trips() {
        let store = InMemoryChasmNodeStore::new();
        let (key, batch) = first_commit_batch();
        assert_eq!(
            store.persist_dirty(&key, batch).await.unwrap(),
            NodePersistOutcome::Applied
        );
        let loaded = store.load_execution(&key).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, b"$state".to_vec());
        assert_eq!(loaded[0].1.metadata.versioned_transition, vt(1, 1));
    }

    #[tokio::test]
    async fn create_fence_rejects_existing_node() {
        let store = InMemoryChasmNodeStore::new();
        let (key, batch) = first_commit_batch();
        store.persist_dirty(&key, batch.clone()).await.unwrap();
        // Re-applying the same Absent-fenced create must conflict.
        let outcome = store.persist_dirty(&key, batch).await.unwrap();
        assert!(matches!(outcome, NodePersistOutcome::Conflict { .. }));
    }

    #[tokio::test]
    async fn cas_fence_rejects_stale_update() {
        let store = InMemoryChasmNodeStore::new();
        let (key, batch) = first_commit_batch();
        store.persist_dirty(&key, batch).await.unwrap();

        // An update fenced on the wrong prior VT is rejected with no write.
        let stale = vec![NodeWrite {
            encoded_path: b"$state".to_vec(),
            node: ChasmNode {
                metadata: NodeMetadata::new(7, None, vt(1, 2)),
                data: Some(vec![9]),
            },
            expected: ExpectedVersion::Vt(vt(9, 9)),
        }];
        assert!(matches!(
            store.persist_dirty(&key, stale).await.unwrap(),
            NodePersistOutcome::Conflict { .. }
        ));
        // The original node is untouched.
        let loaded = store.load_execution(&key).await.unwrap();
        assert_eq!(loaded[0].1.data, Some(vec![1]));
    }

    #[tokio::test]
    async fn batch_is_all_or_nothing() {
        let store = InMemoryChasmNodeStore::new();
        let key = key();
        // First node would succeed (Absent), second fails (Absent but we pre-seed).
        store
            .persist_dirty(
                &key,
                vec![NodeWrite {
                    encoded_path: b"$existing".to_vec(),
                    node: ChasmNode {
                        metadata: NodeMetadata::new(1, None, vt(1, 1)),
                        data: None,
                    },
                    expected: ExpectedVersion::Absent,
                }],
            )
            .await
            .unwrap();

        let mixed = vec![
            NodeWrite {
                encoded_path: b"$new".to_vec(),
                node: ChasmNode {
                    metadata: NodeMetadata::new(1, None, vt(1, 2)),
                    data: None,
                },
                expected: ExpectedVersion::Absent,
            },
            NodeWrite {
                encoded_path: b"$existing".to_vec(),
                node: ChasmNode {
                    metadata: NodeMetadata::new(1, None, vt(1, 2)),
                    data: None,
                },
                expected: ExpectedVersion::Absent, // conflicts: already exists
            },
        ];
        assert!(matches!(
            store.persist_dirty(&key, mixed).await.unwrap(),
            NodePersistOutcome::Conflict { .. }
        ));
        // The would-be-first write ($new) must NOT have landed.
        assert!(store.load_subtree(&key, b"$new").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn subtree_scan_returns_only_descendants() {
        let store = InMemoryChasmNodeStore::new();
        let key = key();
        let nodes = [
            b"$state".to_vec(),
            b"$attempts".to_vec(),
            b"$attempts#0001".to_vec(),
            b"$attempts#0002".to_vec(),
        ];
        let batch = nodes
            .iter()
            .map(|p| NodeWrite {
                encoded_path: p.clone(),
                node: ChasmNode {
                    metadata: NodeMetadata::new(1, None, vt(1, 1)),
                    data: None,
                },
                expected: ExpectedVersion::Absent,
            })
            .collect();
        store.persist_dirty(&key, batch).await.unwrap();

        let subtree: Vec<Vec<u8>> = store
            .load_subtree(&key, b"$attempts")
            .await
            .unwrap()
            .into_iter()
            .map(|(p, _)| p)
            .collect();
        assert_eq!(
            subtree,
            vec![
                b"$attempts".to_vec(),
                b"$attempts#0001".to_vec(),
                b"$attempts#0002".to_vec()
            ]
        );
    }

    #[tokio::test]
    async fn delete_removes_the_execution() {
        let store = InMemoryChasmNodeStore::new();
        let (key, batch) = first_commit_batch();
        store.persist_dirty(&key, batch).await.unwrap();
        store.delete_execution(&key).await.unwrap();
        assert!(store.load_execution(&key).await.unwrap().is_empty());
    }
}
