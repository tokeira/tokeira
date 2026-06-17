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
use tokeira_chasm::{ChasmNode, ExecutionKey, LifecycleState, VersionedTransition};

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

/// The authoritative current-run pointer value for one `(namespace_id, business_id)`
/// — the CHASM analog of the workflow `current_execution` row (migration `V003`;
/// `activity-executions-first-class` design Item 1). Resolves a bare-id (empty
/// `run_id`) request to a concrete run. `status` lets the Start path apply the id
/// reuse/conflict policy without loading the run; `vt_epoch` is the run's committing
/// `VersionedTransition` — the optimistic fence for a superseding advance, the analog
/// of v1.31.0's `last_write_version` conditional update on the current-execution row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentRun {
    /// The current run's id.
    pub run_id: String,
    /// The current run's lifecycle status (live vs terminal — see [`LifecycleState`]).
    pub status: LifecycleState,
    /// The current run's committing VersionedTransition (the advance fence).
    pub vt_epoch: VersionedTransition,
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

    /// Persist the dirty-node batch for a **new run** and set the
    /// `(namespace_id, business_id)` current-run pointer to it, in one atomic unit
    /// (`activity-executions-first-class` Req 1, 2). The pointer write is
    /// co-transactional with the node batch — the analog of v1.31.0 writing the
    /// `current_executions` row inside the entity-create transaction — so a run's
    /// nodes and its current-run pointer never tear. Node fences behave exactly as in
    /// [`persist_dirty`](Self::persist_dirty); on a node conflict nothing is written
    /// and the pointer is left unchanged.
    async fn persist_new_execution(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
    ) -> Result<NodePersistOutcome>;

    /// Resolve the current run for `(namespace_id, business_id)` — the run a bare-id
    /// (empty `run_id`) request addresses (Req 1). `None` when the id has never had a
    /// run or its run was deleted. Authoritative; never derived from the visibility
    /// projection (a bare-id read is a read-your-write against authoritative state).
    async fn current_run(
        &self,
        namespace_id: &str,
        business_id: &str,
    ) -> Result<Option<CurrentRun>>;

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

    /// Enumerate every execution's **root component node** (the node at the empty
    /// `ROOT_PATH`), in a deterministic `(namespace_id, business_id, run_id)` order.
    ///
    /// This is the read side of the visibility **repair scanner** (Req 10.11): the
    /// scanner rebuilds each execution's visibility snapshot from its persisted root
    /// state and re-applies it iff-newer, so a committed transition can never
    /// permanently lack a projection. Ordered output is required — an unordered scan
    /// is a determinism hazard (`tokeira-runtime/AGENTS.md`).
    async fn scan_executions(&self) -> Result<Vec<(ExecutionKey, ChasmNode)>>;
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
    // The current-run pointer: `(namespace_id, business_id) -> CurrentRun`. Held under
    // its own lock; the only path that writes nodes and the pointer together
    // (`persist_new_execution`) acquires `executions` first, then `current_runs`, so
    // the two never tear and the consistent lock order rules out deadlock.
    current_runs: Mutex<HashMap<(String, String), CurrentRun>>,
}

impl InMemoryChasmNodeStore {
    /// Construct an empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Check every node fence, then apply the batch to `tree`, all-or-nothing
/// (Requirement 9.6). Returns `Some(reason)` on the first failed fence (no write),
/// `None` once the whole batch is applied. Shared by `persist_dirty` and
/// `persist_new_execution` so both have one fence-then-apply implementation.
fn check_and_apply_node_batch(
    tree: &mut std::collections::BTreeMap<Vec<u8>, ChasmNode>,
    batch: Vec<NodeWrite>,
) -> Option<String> {
    for write in &batch {
        match write.expected {
            ExpectedVersion::Absent => {
                if tree.contains_key(&write.encoded_path) {
                    return Some(format!(
                        "node at {:?} expected absent but already exists",
                        write.encoded_path
                    ));
                }
            }
            ExpectedVersion::Vt(expected) => match tree.get(&write.encoded_path) {
                Some(existing) if existing.metadata.versioned_transition == expected => {}
                Some(existing) => {
                    return Some(format!(
                        "node at {:?} VT {:?} does not match expected {expected:?}",
                        write.encoded_path, existing.metadata.versioned_transition
                    ));
                }
                None => {
                    return Some(format!(
                        "node at {:?} expected VT {expected:?} but is absent",
                        write.encoded_path
                    ));
                }
            },
        }
    }
    for write in batch {
        tree.insert(write.encoded_path, write.node);
    }
    None
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
        Ok(match check_and_apply_node_batch(tree, batch) {
            Some(reason) => NodePersistOutcome::Conflict { reason },
            None => NodePersistOutcome::Applied,
        })
    }

    async fn persist_new_execution(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
    ) -> Result<NodePersistOutcome> {
        // Lock order: `executions` first, then `current_runs`. This is the only path
        // that holds both, so the node batch and the pointer write land as one atomic
        // unit and the consistent order rules out deadlock.
        let mut executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        let tree = executions.entry(key.clone()).or_default();
        if let Some(reason) = check_and_apply_node_batch(tree, batch) {
            return Ok(NodePersistOutcome::Conflict { reason });
        }
        let mut current_runs = self
            .current_runs
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        current_runs.insert((key.namespace_id.clone(), key.business_id.clone()), current);
        Ok(NodePersistOutcome::Applied)
    }

    async fn current_run(
        &self,
        namespace_id: &str,
        business_id: &str,
    ) -> Result<Option<CurrentRun>> {
        let current_runs = self
            .current_runs
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        Ok(current_runs
            .get(&(namespace_id.to_owned(), business_id.to_owned()))
            .cloned())
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
        // Clear the current-run pointer iff it points at the deleted run, so a
        // subsequent bare-id read is NotFound (read-your-write; Req 1.5). Deleting a
        // superseded (non-current) run leaves the pointer untouched.
        let mut current_runs = self
            .current_runs
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        let ptr_key = (key.namespace_id.clone(), key.business_id.clone());
        if current_runs
            .get(&ptr_key)
            .is_some_and(|c| c.run_id == key.run_id)
        {
            current_runs.remove(&ptr_key);
        }
        Ok(())
    }

    async fn scan_executions(&self) -> Result<Vec<(ExecutionKey, ChasmNode)>> {
        let executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        // The root component node is at the empty `ROOT_PATH` (b""), the minimum
        // encoded path; the inner `BTreeMap`'s first entry.
        let mut out: Vec<(ExecutionKey, ChasmNode)> = executions
            .iter()
            .filter_map(|(key, tree)| {
                tree.get(b"".as_slice())
                    .map(|node| (key.clone(), node.clone()))
            })
            .collect();
        // `ExecutionKey` is not `Ord`; sort by its fields for deterministic output
        // (the scanner must not emit in `HashMap` order — `AGENTS.md` determinism).
        out.sort_by(|(a, _), (b, _)| {
            (&a.namespace_id, &a.business_id, &a.run_id).cmp(&(
                &b.namespace_id,
                &b.business_id,
                &b.run_id,
            ))
        });
        Ok(out)
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
