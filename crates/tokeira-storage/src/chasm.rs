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
//! `dsql` feature. Current-run pointers are keyed by namespace, archetype and
//! business id, and are committed with new nodes. During migration, root-checked
//! legacy fallback keeps activities reachable until the bootstrap backfill's
//! durable completion marker disables it.
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

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Mutex,
};

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
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

/// The authoritative current-run pointer value for one `(namespace_id, archetype_id, business_id)`
/// — the CHASM analog of the workflow `current_execution` row (migration `V003`;
/// `activity-executions-first-class` design Item 1). Resolves a bare-id (empty
/// `run_id`) request to a concrete run. `status` is advisory for scans; the Start
/// path reads the live root for reuse/conflict policy. `vt_epoch` is the run's committing
/// `VersionedTransition` — the optimistic fence for a superseding advance, the analog
/// of v1.31.0's `last_write_version` conditional update on the current-execution row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentRun {
    /// The current run's id.
    pub run_id: String,
    /// The current run's **create** request id — the request id of the Start that
    /// created this run. Immutable for the run's lifetime. Used by the Start path to
    /// (a) idempotently return the existing run when a retried Start carries the same
    /// request id, and (b) populate the `StartRequestId` of the targeted release's
    /// `ActivityExecutionAlreadyStarted` error on a rejected conflicting Start
    /// (the analog of v1.31.0's `currentExecutionInfo.createRequestID`,
    /// `service/history/chasm_engine.go` @ v1.31.0).
    pub request_id: String,
    /// The current run's lifecycle status (live vs terminal — see [`LifecycleState`]).
    /// Advisory: it records the status at the time the pointer was last written, not
    /// maintained on close. The Start path reads the run's live root lifecycle for
    /// the authoritative reuse/conflict decision rather than trusting this field.
    pub status: LifecycleState,
    /// The current run's committing VersionedTransition (the advance fence).
    pub vt_epoch: VersionedTransition,
}

/// Durable proof that legacy pointers have been copied before request admission.
pub const CHASM_CURRENT_EXECUTION_BACKFILL_MARKER: &str = "chasm_current_execution_backfill";

/// A scoped pointer and the execution it addresses. Pointer status is advisory,
/// as on [`CurrentRun`]; callers deciding lifecycle policy must load the root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentExecution {
    /// Namespace, business id and current run id.
    pub key: ExecutionKey,
    /// Root archetype owning this business-id space.
    pub archetype_id: u32,
    /// Persisted pointer value.
    pub current: CurrentRun,
}

/// Exclusive keyset position for a current-pointer scan. The next page starts
/// strictly after this key, including when the cursor's row no longer exists.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CurrentExecutionCursor {
    /// Namespace owning the pointer.
    pub namespace_id: String,
    /// Root archetype owning the business-id space.
    pub archetype_id: u32,
    /// Business id within the namespace and archetype.
    pub business_id: String,
}

/// Complete the legacy activity-pointer copy before serving requests. Existing
/// scoped pointers win, so restarting a partially finished backfill is harmless.
/// A failed batch leaves the marker unset; callers may retry the whole driver.
/// Returns this invocation's copied rows (zero when already marked complete).
/// `batch` must be nonzero; zero must never masquerade as a completed backfill.
pub async fn run_current_execution_backfill(
    repo: &dyn ChasmNodeRepository,
    archetype_id: u32,
    batch: usize,
) -> Result<u64> {
    anyhow::ensure!(batch > 0, "CHASM backfill batch must be nonzero");
    if repo
        .backfill_marker_set(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
        .await?
    {
        return Ok(0);
    }
    let mut copied = 0;
    loop {
        let count = repo
            .backfill_current_executions(archetype_id, batch)
            .await?;
        if count == 0 {
            repo.set_backfill_marker(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
                .await?;
            return Ok(copied);
        }
        copied += u64::try_from(count)?;
    }
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
    /// `(namespace_id, archetype_id, business_id)` current-run pointer to it, in one atomic unit
    /// (`activity-executions-first-class` Req 1, 2). The pointer write is
    /// co-transactional with the node batch — the analog of v1.31.0 writing the
    /// `current_executions` row inside the entity-create transaction — so a run's
    /// nodes and its current-run pointer never tear. Node fences behave exactly as in
    /// [`persist_dirty`](Self::persist_dirty); on a node conflict nothing is written
    /// and the pointer is left unchanged.
    async fn persist_new_execution(
        &self,
        key: &ExecutionKey,
        archetype_id: u32,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
    ) -> Result<NodePersistOutcome>;

    /// Resolve the current run for `(namespace_id, archetype_id, business_id)` — the run a bare-id
    /// (empty `run_id`) request addresses (Req 1). `None` when the id has never had a
    /// run or its run was deleted. Authoritative; never derived from the visibility
    /// projection (a bare-id read is a read-your-write against authoritative state).
    /// Before the backfill marker is set, an absent scoped pointer falls back to
    /// the legacy table only if its root exists and has the requested archetype.
    async fn current_run(
        &self,
        namespace_id: &str,
        archetype_id: u32,
        business_id: &str,
    ) -> Result<Option<CurrentRun>>;

    /// Scan scoped pointers with the given advisory status, ordered by namespace,
    /// archetype and business id. `after` is exclusive; zero limit returns no rows.
    /// Pages observe current state independently, not a snapshot across calls.
    async fn scan_current_executions(
        &self,
        status: LifecycleState,
        after: Option<CurrentExecutionCursor>,
        limit: usize,
    ) -> Result<Vec<CurrentExecution>>;

    /// Copy at most `batch` (capped at 500) remaining legacy pointers under the activity archetype,
    /// in old-key order, without overwriting scoped pointers. Returns rows copied;
    /// zero means exhausted. Rejects a zero batch. Call only during bootstrap,
    /// before admitting mutations; legacy writers must have stopped.
    async fn backfill_current_executions(&self, archetype_id: u32, batch: usize) -> Result<usize>;

    /// Count scoped pointers per archetype, in ascending archetype-id order.
    async fn distinct_archetypes(&self) -> Result<Vec<(u32, u64)>>;

    /// Whether a named durable backfill completion marker exists.
    async fn backfill_marker_set(&self, name: &str) -> Result<bool>;

    /// Idempotently record completion, only after every backfill batch committed.
    async fn set_backfill_marker(&self, name: &str) -> Result<()>;

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
    // Every path needing both locks acquires executions before pointers. Acquire
    // both before changing either so a failed lock/fence cannot partially commit.
    pointers: Mutex<InMemoryPointers>,
}

#[derive(Debug, Default)]
struct InMemoryPointers {
    current: BTreeMap<CurrentExecutionCursor, CurrentRun>,
    // Read-only legacy state; only test fixtures populate this map.
    legacy: BTreeMap<(String, String), CurrentRun>,
    markers: HashSet<String>,
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
        archetype_id: u32,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
    ) -> Result<NodePersistOutcome> {
        let mut executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        let mut pointers = self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?;
        let tree = executions.entry(key.clone()).or_default();
        if let Some(reason) = check_and_apply_node_batch(tree, batch) {
            return Ok(NodePersistOutcome::Conflict { reason });
        }
        pointers.current.insert(
            CurrentExecutionCursor {
                namespace_id: key.namespace_id.clone(),
                archetype_id,
                business_id: key.business_id.clone(),
            },
            current,
        );
        Ok(NodePersistOutcome::Applied)
    }

    async fn current_run(
        &self,
        namespace_id: &str,
        archetype_id: u32,
        business_id: &str,
    ) -> Result<Option<CurrentRun>> {
        let executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        let pointers = self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?;
        let cursor = CurrentExecutionCursor {
            namespace_id: namespace_id.to_owned(),
            archetype_id,
            business_id: business_id.to_owned(),
        };
        if let Some(current) = pointers.current.get(&cursor) {
            return Ok(Some(current.clone()));
        }
        if pointers
            .markers
            .contains(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
        {
            return Ok(None);
        }
        // The legacy key has no archetype. The root supplies it so fallback never
        // leaks another archetype's run, and a deleted legacy run stays absent.
        Ok(pointers
            .legacy
            .get(&(namespace_id.to_owned(), business_id.to_owned()))
            .filter(|current| {
                executions
                    .get(&ExecutionKey::new(
                        namespace_id,
                        business_id,
                        &current.run_id,
                    ))
                    .and_then(|tree| tree.get(b"".as_slice()))
                    .is_some_and(|root| root.metadata.component_type_id == archetype_id)
            })
            .cloned())
    }

    async fn scan_current_executions(
        &self,
        status: LifecycleState,
        after: Option<CurrentExecutionCursor>,
        limit: usize,
    ) -> Result<Vec<CurrentExecution>> {
        let pointers = self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?;
        Ok(pointers
            .current
            .iter()
            .filter(|(key, current)| {
                after.as_ref().is_none_or(|after| *key > after) && current.status == status
            })
            .take(limit)
            .map(|(key, current)| CurrentExecution {
                key: ExecutionKey::new(&key.namespace_id, &key.business_id, &current.run_id),
                archetype_id: key.archetype_id,
                current: current.clone(),
            })
            .collect())
    }

    async fn backfill_current_executions(&self, archetype_id: u32, batch: usize) -> Result<usize> {
        anyhow::ensure!(batch > 0, "CHASM backfill batch must be nonzero");
        let executions = self
            .executions
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm node store mutex poisoned"))?;
        let mut pointers = self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?;
        // Already copied keys are the durable progress record. Filtering before
        // limiting both resumes after a crash and advances past a full first page.
        let rows: Vec<_> = pointers
            .legacy
            .iter()
            .filter_map(|((namespace_id, business_id), current)| {
                let cursor = CurrentExecutionCursor {
                    namespace_id: namespace_id.clone(),
                    archetype_id,
                    business_id: business_id.clone(),
                };
                let root_matches = executions
                    .get(&ExecutionKey::new(
                        namespace_id,
                        business_id,
                        &current.run_id,
                    ))
                    .and_then(|tree| tree.get(b"".as_slice()))
                    .is_some_and(|root| root.metadata.component_type_id == archetype_id);
                (!pointers.current.contains_key(&cursor) && root_matches)
                    .then(|| (cursor, current.clone()))
            })
            .take(batch.min(500))
            .collect();
        let count = rows.len();
        pointers.current.extend(rows);
        Ok(count)
    }

    async fn distinct_archetypes(&self) -> Result<Vec<(u32, u64)>> {
        let pointers = self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?;
        let mut counts = BTreeMap::new();
        for key in pointers.current.keys() {
            *counts.entry(key.archetype_id).or_insert(0) += 1;
        }
        Ok(counts.into_iter().collect())
    }

    async fn backfill_marker_set(&self, name: &str) -> Result<bool> {
        Ok(self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?
            .markers
            .contains(name))
    }

    async fn set_backfill_marker(&self, name: &str) -> Result<()> {
        self.pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?
            .markers
            .insert(name.to_owned());
        Ok(())
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
        let mut pointers = self
            .pointers
            .lock()
            .map_err(|_| anyhow::anyhow!("chasm pointer store mutex poisoned"))?;
        executions.remove(key);
        // A delete has no archetype argument: the full run key identifies its
        // pointer. Superseded runs and other archetypes' runs remain untouched.
        pointers.current.retain(|cursor, current| {
            cursor.namespace_id != key.namespace_id
                || cursor.business_id != key.business_id
                || current.run_id != key.run_id
        });
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

#[cfg(test)]
pub(crate) mod pointer_tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_chasm::{BusinessIdConflictPolicy, BusinessIdReusePolicy, NodeMetadata};
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    pub(crate) struct Scenario {
        pub(crate) archetypes: [u32; 2],
        pub(crate) legacy: Vec<Option<LifecycleState>>,
        starts: Vec<Start>,
        marker_at: usize,
        batch: usize,
    }

    #[derive(Debug, Clone)]
    struct Start {
        second: bool,
        business: usize,
        request: String,
        reuse: u8,
        conflict: u8,
        close: Option<LifecycleState>,
    }

    fn lifecycle() -> impl Strategy<Value = LifecycleState> {
        prop_oneof![
            Just(LifecycleState::Running),
            Just(LifecycleState::Completed),
            Just(LifecycleState::Failed)
        ]
    }

    pub(crate) fn scenarios() -> impl Strategy<Value = Scenario> {
        scenario_strategy(6, 32)
    }

    #[cfg(feature = "dsql")]
    pub(crate) fn dsql_scenarios() -> impl Strategy<Value = Scenario> {
        // Keep 100 real-database cases within the integration test time budget;
        // the same model covers all policies and marker positions with short traces.
        scenario_strategy(3, 6)
    }

    fn scenario_strategy(businesses: usize, steps: usize) -> impl Strategy<Value = Scenario> {
        (
            any::<u32>(),
            1_u32..=u32::MAX,
            prop::collection::vec(prop::option::of(lifecycle()), 1..businesses),
            prop::collection::vec(
                (
                    any::<bool>(),
                    0_usize..5,
                    "[a-c]{0,2}",
                    0_u8..3,
                    0_u8..3,
                    prop::option::of(lifecycle()),
                ),
                1..steps,
            ),
            any::<usize>(),
            1_usize..4,
        )
            .prop_map(|(first, mask, legacy, starts, marker_at, batch)| Scenario {
                archetypes: [first, first ^ mask],
                marker_at: marker_at % (starts.len() + 1),
                legacy,
                starts: starts
                    .into_iter()
                    .map(
                        |(second, business, request, reuse, conflict, close)| Start {
                            second,
                            business,
                            request,
                            reuse,
                            conflict,
                            close,
                        },
                    )
                    .collect(),
                batch,
            })
    }

    pub(crate) fn root(archetype: u32, status: LifecycleState, count: i64) -> NodeWrite {
        NodeWrite {
            encoded_path: Vec::new(),
            node: ChasmNode {
                metadata: NodeMetadata::new(
                    archetype,
                    Some(status),
                    VersionedTransition::new(1, count),
                ),
                data: Some(vec![42]),
            },
            expected: ExpectedVersion::Absent,
        }
    }

    pub(crate) fn current(run_id: String, request_id: &str, status: LifecycleState) -> CurrentRun {
        CurrentRun {
            run_id,
            request_id: request_id.to_owned(),
            status,
            vt_epoch: VersionedTransition::new(1, 1),
        }
    }

    pub(crate) fn legacy_rows(
        scenario: &Scenario,
        namespace: &str,
    ) -> Vec<(ExecutionKey, CurrentRun)> {
        scenario
            .legacy
            .iter()
            .enumerate()
            .filter_map(|(index, status)| {
                status.map(|status| {
                    let run_id = Uuid::from_u128(index as u128 + 1).to_string();
                    let key = ExecutionKey::new(namespace, format!("business-{index}"), &run_id);
                    (key, current(run_id, "a", status))
                })
            })
            .collect()
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Created,
        Existing(String),
        Rejected(String),
        Unsupported,
    }

    // Policy is deliberately a test-side adapter: storage owns identity, never
    // lifecycle semantics. The table model below is independent of repository reads.
    async fn start(
        repo: &dyn ChasmNodeRepository,
        key: &ExecutionKey,
        archetype: u32,
        request: &str,
        reuse: u8,
        conflict: u8,
    ) -> Result<Outcome> {
        if let Some(pointer) = repo
            .current_run(&key.namespace_id, archetype, &key.business_id)
            .await?
        {
            let existing = ExecutionKey::new(&key.namespace_id, &key.business_id, &pointer.run_id);
            let nodes = repo.load_execution(&existing).await?;
            let node = &nodes
                .iter()
                .find(|(path, _)| path.is_empty())
                .expect("current root")
                .1;
            anyhow::ensure!(
                node.metadata.component_type_id == archetype,
                "cross-archetype root"
            );
            if !request.is_empty() && pointer.request_id == request {
                return Ok(Outcome::Existing(pointer.run_id));
            }
            let reuse = [
                BusinessIdReusePolicy::AllowDuplicate,
                BusinessIdReusePolicy::AllowDuplicateFailedOnly,
                BusinessIdReusePolicy::RejectDuplicate,
            ][usize::from(reuse)];
            let conflict = [
                BusinessIdConflictPolicy::Fail,
                BusinessIdConflictPolicy::UseExisting,
                BusinessIdConflictPolicy::TerminateExisting,
            ][usize::from(conflict)];
            if node.metadata.lifecycle_state == Some(LifecycleState::Running) {
                return Ok(match conflict {
                    BusinessIdConflictPolicy::Fail => Outcome::Rejected(pointer.run_id),
                    BusinessIdConflictPolicy::UseExisting => Outcome::Existing(pointer.run_id),
                    BusinessIdConflictPolicy::TerminateExisting => Outcome::Unsupported,
                });
            }
            if reuse == BusinessIdReusePolicy::RejectDuplicate
                || (reuse == BusinessIdReusePolicy::AllowDuplicateFailedOnly
                    && node.metadata.lifecycle_state == Some(LifecycleState::Completed))
            {
                return Ok(Outcome::Rejected(pointer.run_id));
            }
        }
        let pointer = current(key.run_id.clone(), request, LifecycleState::Running);
        anyhow::ensure!(
            repo.persist_new_execution(
                key,
                archetype,
                vec![root(archetype, LifecycleState::Running, 1)],
                pointer
            )
            .await?
                == NodePersistOutcome::Applied,
            "start fence"
        );
        Ok(Outcome::Created)
    }

    pub(crate) async fn exercise(
        repo: &dyn ChasmNodeRepository,
        scenario: &Scenario,
        namespace: &str,
    ) -> Result<()> {
        let mut model: BTreeMap<(u32, String), (CurrentRun, LifecycleState)> =
            legacy_rows(scenario, namespace)
                .into_iter()
                .map(|(key, pointer)| {
                    (
                        (scenario.archetypes[0], key.business_id),
                        (pointer.clone(), pointer.status),
                    )
                })
                .collect();
        for step in 0..=scenario.starts.len() {
            if step == scenario.marker_at {
                run_current_execution_backfill(repo, scenario.archetypes[0], scenario.batch)
                    .await?;
                anyhow::ensure!(
                    repo.backfill_marker_set(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
                        .await?,
                    "missing marker"
                );
                anyhow::ensure!(
                    run_current_execution_backfill(repo, scenario.archetypes[0], scenario.batch)
                        .await?
                        == 0,
                    "driver not idempotent"
                );
            }
            for archetype in scenario.archetypes {
                for business in 0..scenario.legacy.len() {
                    let business = format!("business-{business}");
                    let expected = model
                        .get(&(archetype, business.clone()))
                        .map(|(pointer, _)| pointer.clone());
                    anyhow::ensure!(
                        repo.current_run(namespace, archetype, &business).await? == expected,
                        "pointer mismatch at step {step}, archetype {archetype}, business {business}"
                    );
                }
            }
            let Some(Start {
                second,
                business,
                request,
                reuse,
                conflict,
                close,
            }) = scenario.starts.get(step)
            else {
                break;
            };
            let archetype = scenario.archetypes[usize::from(*second)];
            let business = format!("business-{}", business % scenario.legacy.len());
            let key = ExecutionKey::new(
                namespace,
                &business,
                Uuid::from_u128(step as u128 + 100).to_string(),
            );
            // service/history/chasm_engine.go @ v1.31.0: request-id idempotence
            // precedes the live-conflict / terminal-reuse matrix.
            let expected = match model.get(&(archetype, business.clone())) {
                None => Outcome::Created,
                Some((pointer, _)) if !request.is_empty() && request == &pointer.request_id => {
                    Outcome::Existing(pointer.run_id.clone())
                }
                Some((pointer, LifecycleState::Running)) => match conflict {
                    0 => Outcome::Rejected(pointer.run_id.clone()),
                    1 => Outcome::Existing(pointer.run_id.clone()),
                    _ => Outcome::Unsupported,
                },
                Some((pointer, state)) => match (reuse, state) {
                    (0, _) | (1, LifecycleState::Failed) => Outcome::Created,
                    _ => Outcome::Rejected(pointer.run_id.clone()),
                },
            };
            let outcome = start(repo, &key, archetype, request, *reuse, *conflict).await?;
            anyhow::ensure!(
                outcome == expected,
                "outcome mismatch at step {step}: {outcome:?} != {expected:?}"
            );
            if outcome == Outcome::Created {
                model.insert(
                    (archetype, business.clone()),
                    (
                        current(key.run_id.clone(), request, LifecycleState::Running),
                        LifecycleState::Running,
                    ),
                );
            }
            if let (Some(status), Some((pointer, lifecycle))) =
                (close, model.get_mut(&(archetype, business)))
            {
                let current_key = ExecutionKey::new(namespace, &key.business_id, &pointer.run_id);
                let nodes = repo.load_execution(&current_key).await?;
                let prior = nodes[0].1.metadata.versioned_transition;
                let mut write = root(archetype, *status, prior.transition_count + 1);
                write.expected = ExpectedVersion::Vt(prior);
                anyhow::ensure!(
                    repo.persist_dirty(&current_key, vec![write]).await?
                        == NodePersistOutcome::Applied,
                    "close fence"
                );
                *lifecycle = *status;
            }
        }
        let scanned = repo
            .scan_current_executions(LifecycleState::Running, None, usize::MAX / 2)
            .await?;
        let expected_running = model
            .values()
            .filter(|(pointer, _)| pointer.status == LifecycleState::Running)
            .count();
        anyhow::ensure!(scanned.len() == expected_running, "scan status mismatch");
        let mut after = None;
        let mut paged = Vec::new();
        loop {
            let page = repo
                .scan_current_executions(LifecycleState::Running, after, scenario.batch)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(CurrentExecutionCursor {
                namespace_id: last.key.namespace_id.clone(),
                archetype_id: last.archetype_id,
                business_id: last.key.business_id.clone(),
            });
            paged.extend(page);
            anyhow::ensure!(
                paged.len() <= model.len(),
                "keyset cursor failed to advance"
            );
        }
        anyhow::ensure!(
            paged == scanned,
            "keyset scan skipped or repeated a pointer"
        );
        let mut counts = BTreeMap::new();
        for (archetype, _) in model.keys() {
            *counts.entry(*archetype).or_insert(0_u64) += 1;
        }
        anyhow::ensure!(
            repo.distinct_archetypes().await? == counts.into_iter().collect::<Vec<_>>(),
            "archetype counts mismatch"
        );
        Ok(())
    }

    async fn seed(
        store: &InMemoryChasmNodeStore,
        key: &ExecutionKey,
        archetype: u32,
        pointer: CurrentRun,
    ) {
        store
            .persist_dirty(key, vec![root(archetype, pointer.status, 1)])
            .await
            .unwrap();
        store
            .pointers
            .lock()
            .unwrap()
            .legacy
            .insert((key.namespace_id.clone(), key.business_id.clone()), pointer);
    }

    // Feature: chasm-extension-archetypes, Property 8: archetype-scoped business ids
    // Interleaving archetypes and backfill preserves independent single-archetype outcomes.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn archetype_scoped_business_ids(scenario in scenarios()) {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let store = InMemoryChasmNodeStore::new();
                let namespace = Uuid::new_v4().to_string();
                for (key, pointer) in legacy_rows(&scenario, &namespace) {
                    seed(&store, &key, scenario.archetypes[0], pointer).await;
                }
                exercise(&store, &scenario, &namespace).await.unwrap();
            });
        }
    }

    #[tokio::test]
    async fn backfill_resumes_and_preserves_newer_pointers() {
        let store = InMemoryChasmNodeStore::new();
        for id in 0..7 {
            let key = ExecutionKey::new("ns", id.to_string(), "old");
            seed(
                &store,
                &key,
                7,
                current("old".into(), "old-request", LifecycleState::Running),
            )
            .await;
        }
        assert_eq!(store.backfill_current_executions(7, 2).await.unwrap(), 2);
        assert!(
            !store
                .backfill_marker_set(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
                .await
                .unwrap()
        );
        let key = ExecutionKey::new("ns", "3", "new");
        let new = current("new".into(), "new-request", LifecycleState::Failed);
        store
            .persist_new_execution(
                &key,
                7,
                vec![root(7, LifecycleState::Failed, 1)],
                new.clone(),
            )
            .await
            .unwrap();
        assert_eq!(
            run_current_execution_backfill(&store, 7, 2).await.unwrap(),
            4
        );
        assert_eq!(store.current_run("ns", 7, "3").await.unwrap(), Some(new));
        assert_eq!(store.distinct_archetypes().await.unwrap(), vec![(7, 7)]);
        assert_eq!(
            run_current_execution_backfill(&store, 7, 2).await.unwrap(),
            0
        );
        assert_eq!(store.pointers.lock().unwrap().legacy.len(), 7);
    }

    #[tokio::test]
    async fn fallback_requires_matching_root_and_unset_marker() {
        let store = InMemoryChasmNodeStore::new();
        let key = ExecutionKey::new("ns", "business", "old");
        let pointer = current("old".into(), "request", LifecycleState::Completed);
        seed(&store, &key, 7, pointer.clone()).await;
        assert_eq!(
            store.current_run("ns", 7, "business").await.unwrap(),
            Some(pointer)
        );
        assert_eq!(store.current_run("ns", 8, "business").await.unwrap(), None);
        store
            .set_backfill_marker(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
            .await
            .unwrap();
        store
            .set_backfill_marker(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
            .await
            .unwrap();
        assert_eq!(store.current_run("ns", 7, "business").await.unwrap(), None);
        assert!(!store.backfill_marker_set("different-marker").await.unwrap());
    }

    #[tokio::test]
    async fn deleted_legacy_root_cannot_be_resurrected() {
        let store = InMemoryChasmNodeStore::new();
        let key = ExecutionKey::new("ns", "business", "old");
        seed(
            &store,
            &key,
            7,
            current("old".into(), "request", LifecycleState::Running),
        )
        .await;
        store.delete_execution(&key).await.unwrap();
        assert_eq!(store.current_run("ns", 7, "business").await.unwrap(), None);
        assert_eq!(
            run_current_execution_backfill(&store, 7, 1).await.unwrap(),
            0
        );
        assert_eq!(store.pointers.lock().unwrap().legacy.len(), 1);
    }

    #[tokio::test]
    async fn pointer_and_nodes_commit_atomically_and_delete_by_run() {
        let store = InMemoryChasmNodeStore::new();
        let old = ExecutionKey::new("ns", "business", "old");
        for (archetype, run) in [(7, "old"), (8, "other"), (7, "new")] {
            let key = ExecutionKey::new("ns", "business", run);
            let pointer = current(run.into(), run, LifecycleState::Running);
            assert_eq!(
                store
                    .persist_new_execution(
                        &key,
                        archetype,
                        vec![root(archetype, LifecycleState::Running, 1)],
                        pointer
                    )
                    .await
                    .unwrap(),
                NodePersistOutcome::Applied
            );
        }
        let invalid = current("bad".into(), "bad", LifecycleState::Completed);
        assert!(matches!(
            store
                .persist_new_execution(
                    &old,
                    7,
                    vec![root(7, LifecycleState::Completed, 1)],
                    invalid
                )
                .await
                .unwrap(),
            NodePersistOutcome::Conflict { .. }
        ));
        store.delete_execution(&old).await.unwrap();
        assert_eq!(
            store
                .current_run("ns", 7, "business")
                .await
                .unwrap()
                .unwrap()
                .run_id,
            "new"
        );
        store
            .delete_execution(&ExecutionKey::new("ns", "business", "new"))
            .await
            .unwrap();
        assert_eq!(store.current_run("ns", 7, "business").await.unwrap(), None);
        assert_eq!(
            store
                .current_run("ns", 8, "business")
                .await
                .unwrap()
                .unwrap()
                .run_id,
            "other"
        );
        assert!(store.pointers.lock().unwrap().legacy.is_empty());
        assert!(
            store
                .scan_current_executions(LifecycleState::Running, None, 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn scan_orders_namespaces_archetypes_and_business_ids_with_exclusive_cursors() {
        let store = InMemoryChasmNodeStore::new();
        for (namespace, archetype, business, status) in [
            ("b", 1, "a", LifecycleState::Running),
            ("a", u32::MAX, "a", LifecycleState::Running),
            ("a", 0, "z", LifecycleState::Running),
            ("a", 0, "a", LifecycleState::Running),
            ("a", 0, "b", LifecycleState::Completed),
        ] {
            let key = ExecutionKey::new(namespace, business, format!("{archetype}-{business}"));
            store
                .persist_new_execution(
                    &key,
                    archetype,
                    vec![root(archetype, status, 1)],
                    current(key.run_id.clone(), "request", status),
                )
                .await
                .unwrap();
        }
        let all = store
            .scan_current_executions(LifecycleState::Running, None, 10)
            .await
            .unwrap();
        let keys: Vec<_> = all
            .iter()
            .map(|row| {
                (
                    row.key.namespace_id.as_str(),
                    row.archetype_id,
                    row.key.business_id.as_str(),
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                ("a", 0, "a"),
                ("a", 0, "z"),
                ("a", u32::MAX, "a"),
                ("b", 1, "a")
            ]
        );
        let after = CurrentExecutionCursor {
            namespace_id: "a".into(),
            archetype_id: 0,
            business_id: "missing".into(),
        };
        assert_eq!(
            store
                .scan_current_executions(LifecycleState::Running, Some(after), 2)
                .await
                .unwrap(),
            all[1..3]
        );
        assert_eq!(
            store
                .scan_current_executions(LifecycleState::Completed, None, 10)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .scan_current_executions(LifecycleState::Failed, None, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn zero_batch_cannot_mark_an_incomplete_backfill() {
        let store = InMemoryChasmNodeStore::new();
        assert!(store.backfill_current_executions(7, 0).await.is_err());
        assert!(run_current_execution_backfill(&store, 7, 0).await.is_err());
        assert!(
            !store
                .backfill_marker_set(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
                .await
                .unwrap()
        );
    }

    #[test]
    fn legacy_pointer_sql_is_read_only() {
        fn check(path: &std::path::Path) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    check(&path);
                } else if path.extension().is_some_and(|ext| ext == "rs") {
                    let source = std::fs::read_to_string(&path).unwrap();
                    let production = source.split("#[cfg(test)]").next().unwrap();
                    let normalized = production
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ")
                        .to_ascii_lowercase();
                    for verb in [
                        "insert into",
                        "update",
                        "delete from",
                        "truncate",
                        "merge into",
                    ] {
                        assert!(
                            !normalized.contains(&format!("{verb} chasm_current_run")),
                            "legacy write in {}",
                            path.display()
                        );
                    }
                }
            }
        }
        check(&std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"));
    }
}
