//! The persisted node, the execution key, and the node tree.
//!
//! An Execution is a **tree of nodes** rooted at an `ExecutionKey`
//! (`{ namespace_id, business_id, run_id }`). Each `Field`/`Map` child persists
//! as its own `ChasmNode { metadata, data }`, keyed by its
//! [encoded path](crate::path) (Requirement 4.1, 2.7). `metadata` is always
//! present (it carries the node's type id, lifecycle, and task outboxes); `data`
//! is present only for data fields.
//!
//! This module also owns the **transition close** algorithm (`close_transaction`):
//! it tracks the nodes mutated during a transition, stamps every dirty node with a
//! new [`VersionedTransition`](crate::versioned_transition), re-validates the task
//! outboxes, and returns the dirty-node set plus the surviving tasks as one atomic
//! unit for the runtime to persist and dispatch (Requirement 5.1, 5.2, 7.3–7.5).
//! Computing the result is pure; the I/O of persisting it lives in the runtime.
//!
//! Implemented by tasks 5.1, 5.2 of the `chasm-foundation` spec; this is a
//! skeleton.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    component::LifecycleState,
    error::ChasmError,
    path::subtree_range_end,
    task::{OutboxValidator, ScheduledTask, TaskId, TaskKind, TaskOutbox, TaskValidity},
    versioned_transition::{Staleness, VersionedTransition},
};

/// The identity of an Execution: the tuple that names the root of a node tree.
///
/// Mirrors upstream CHASM's `ExecutionKey` (`chasm/ref.go @ v1.31.0`): a
/// `namespace_id`, a user-meaningful `business_id` (e.g. a workflow id or an
/// activity id), and a `run_id` naming a single instance. The `business_id`
/// persists across resets/continuation; the `run_id` changes per instance. These
/// three components form the leading part of every node's storage key
/// (Requirement 4.1, 9.1), so they are ordinary owned strings here — the storage
/// layer maps `namespace_id`/`run_id` onto its `UUID` columns and `business_id`
/// onto `TEXT`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionKey {
    /// The namespace the execution belongs to.
    pub namespace_id: String,
    /// The user-defined business identifier; stable across resets/continuation.
    pub business_id: String,
    /// The single-instance run identifier; changes on reset or follow-up run.
    pub run_id: String,
}

impl ExecutionKey {
    /// Construct an execution key from its three components.
    pub fn new(
        namespace_id: impl Into<String>,
        business_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace_id: namespace_id.into(),
            business_id: business_id.into(),
            run_id: run_id.into(),
        }
    }
}

/// Read-only summary of an execution surfaced to component code through
/// [`Context`](crate::context::Context).
///
/// Mirrors upstream `ExecutionInfo` (`chasm/context.go @ v1.31.0`): the running
/// state-transition count, an approximate persisted size, and the close time once
/// the execution has closed. `close_time_unix_nanos` is `None` while the execution
/// is open. Time is carried as a plain `i64` of Unix nanoseconds rather than a
/// `time` type so the pure crate keeps its minimal dependency set (Requirement
/// 1.1, 1.2); the runtime converts to/from wall-clock types at its boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExecutionInfo {
    /// The number of committed state transitions on this execution so far.
    pub state_transition_count: i64,
    /// An approximate persisted size of the execution's node tree, in bytes.
    pub approximate_state_size: u64,
    /// The execution's close time as Unix nanoseconds, or `None` while open.
    pub close_time_unix_nanos: Option<i64>,
}

impl ExecutionInfo {
    /// True once the execution has closed (a close time has been recorded).
    pub fn is_closed(&self) -> bool {
        self.close_time_unix_nanos.is_some()
    }
}

/// The per-node metadata that travels in the node row's `metadata` column
/// (Storage Design). It carries everything about a node *except* its data payload:
/// the component/field type id, the (optional) lifecycle of a component node, the
/// node's last-update and creation [`VersionedTransition`] stamps, and the task
/// [`TaskOutbox`].
///
/// `versioned_transition` is the node's last-update clock — the value the storage
/// layer fences its compare-and-set persist on. `initial_versioned_transition` is
/// fixed at creation and is the node-identity half of `(path, initial_vt)`
/// (Requirement 8.5), so it must never change after the creating transition.
/// `lifecycle_state` is `Some` for component nodes and `None` for plain data-leaf
/// nodes. `next_task_offset` is the per-node monotonic counter that assigns stable,
/// unique [`TaskId`] offsets (see [`TaskId`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeMetadata {
    /// Registry type id of the component (or `0` for the reserved legacy-workflow
    /// archetype) / field this node represents.
    pub component_type_id: u32,
    /// The lifecycle of a component node; `None` for a data-leaf node.
    pub lifecycle_state: Option<LifecycleState>,
    /// The node's last-update stamp; the storage CAS fences persist on it.
    pub versioned_transition: VersionedTransition,
    /// The node's creation stamp; immutable after create (node-identity half).
    pub initial_versioned_transition: VersionedTransition,
    /// The node's pure/side-effect task outbox.
    pub outbox: TaskOutbox,
    /// Per-node monotonic counter assigning unique task offsets.
    pub next_task_offset: u32,
}

impl NodeMetadata {
    /// Construct metadata for a freshly created node. Both VT stamps are left at
    /// the supplied `initial_vt` placeholder; [`NodeTree::close_transaction`]
    /// re-stamps them with the committing transition's VT.
    pub fn new(
        component_type_id: u32,
        lifecycle_state: Option<LifecycleState>,
        initial_vt: VersionedTransition,
    ) -> Self {
        Self {
            component_type_id,
            lifecycle_state,
            versioned_transition: initial_vt,
            initial_versioned_transition: initial_vt,
            outbox: TaskOutbox::new(),
            next_task_offset: 0,
        }
    }
}

/// A single persisted node: its [`NodeMetadata`] plus its optional `data` payload
/// (Requirement 9.2). `data` is the serialized proto bytes of a data-field node and
/// is `None` for a pure component/structural node. This is the unit the storage
/// node table stores one-per-row, keyed by encoded path; its serde round-trip is
/// **Property 1** (node serialization round-trip).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChasmNode {
    /// The node's metadata (type id, lifecycle, VT stamps, task outbox).
    pub metadata: NodeMetadata,
    /// The node's serialized data payload, if it is a data-field node.
    pub data: Option<Vec<u8>>,
}

/// A task added during the current transition that has not yet been assigned its
/// stable [`TaskId`]. Identity is assigned at [`NodeTree::close_transaction`] from
/// the committing VT and the owning node's offset counter.
#[derive(Debug, Clone)]
struct PendingTask {
    kind: TaskKind,
    task_type_id: u32,
    payload: Vec<u8>,
    fire_at_unix_nanos: Option<i64>,
}

/// A surviving side-effect task to dispatch post-commit, paired with the encoded
/// path of the node that owns it. Returned from [`NodeTree::close_transaction`] so
/// the runtime can enqueue it *after* the commit lands (Requirement 7.6, 7.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchableTask {
    /// Encoded path of the owning node.
    pub node_path: Vec<u8>,
    /// The surviving side-effect task.
    pub task: ScheduledTask,
}

/// The atomic result of closing a transition (Requirement 5.1, 5.2, 7.6).
///
/// It is one indivisible unit handed to the runtime: the set of nodes the
/// transition dirtied (to persist, fenced on their prior VT), the side-effect tasks
/// that survived re-validation and were newly scheduled (to dispatch post-commit),
/// and the single earliest surviving pure-task deadline tree-wide (to arm one
/// physical timer). The pure crate computes all three; the runtime performs the
/// I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionResult {
    /// The dirtied nodes, in encoded-path order, to persist as one fenced batch.
    pub dirty_nodes: Vec<(Vec<u8>, ChasmNode)>,
    /// Newly scheduled side-effect tasks that survived validation, to dispatch
    /// post-commit.
    pub side_effect_tasks: Vec<DispatchableTask>,
    /// The earliest surviving pure-task `fire_at` across the whole tree, or `None`
    /// if no valid pure task is outstanding. The runtime arms at most one physical
    /// timer at this deadline (Requirement 7.6).
    pub earliest_pure_deadline_unix_nanos: Option<i64>,
}

/// The in-memory node tree for one execution: the mutable working set a transition
/// reads and writes, plus the dirty/created/pending bookkeeping that
/// [`close_transaction`](NodeTree::close_transaction) consumes.
///
/// Nodes are keyed by their [encoded path](crate::path), and the map is a
/// `BTreeMap` so iteration and range scans follow the path encoder's sort
/// contract — a subtree, a collection, or an ancestor chain is one contiguous
/// range (Requirement 4.1, 4.4). The tree is **pure**: it models *what* a
/// transition changes and *what* it schedules; the storage layer loads/persists
/// rows and the runtime performs dispatch.
///
/// Within a transition the tree records which nodes were mutated (`dirty`), which
/// were created (`created`, so their `initial_versioned_transition` is stamped),
/// and which tasks were scheduled but not yet given identity (`pending_tasks`).
/// `close_transaction` merges, re-validates, stamps, and clears this bookkeeping.
#[derive(Debug, Default)]
pub struct NodeTree {
    nodes: BTreeMap<Vec<u8>, ChasmNode>,
    dirty: BTreeSet<Vec<u8>>,
    created: BTreeSet<Vec<u8>>,
    pending_tasks: BTreeMap<Vec<u8>, Vec<PendingTask>>,
    execution_vt: VersionedTransition,
}

impl NodeTree {
    /// Construct an empty node tree with a zero execution clock. The first
    /// committed transition advances it (Requirement 5.4).
    pub fn new() -> Self {
        Self::default()
    }

    /// The execution's current [`VersionedTransition`] — the clock the last
    /// committed transition stamped.
    pub fn execution_vt(&self) -> VersionedTransition {
        self.execution_vt
    }

    /// Seed a node loaded from storage **without** marking it dirty. Used by the
    /// storage layer to populate the working set before a transition runs.
    pub fn load_node(&mut self, encoded_path: Vec<u8>, node: ChasmNode) {
        self.nodes.insert(encoded_path, node);
    }

    /// Set the execution clock to the value loaded from storage. Used alongside
    /// [`load_node`](NodeTree::load_node) when reconstituting an execution.
    pub fn set_loaded_execution_vt(&mut self, vt: VersionedTransition) {
        self.execution_vt = vt;
    }

    /// The node at `encoded_path`, if present in the working set.
    pub fn node(&self, encoded_path: &[u8]) -> Option<&ChasmNode> {
        self.nodes.get(encoded_path)
    }

    /// True iff a node exists at `encoded_path`.
    pub fn contains(&self, encoded_path: &[u8]) -> bool {
        self.nodes.contains_key(encoded_path)
    }

    /// Number of nodes currently in the working set.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// True iff the working set holds no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Create a new node at `encoded_path` during the current transition, marking
    /// it dirty and recording it as created (so its
    /// `initial_versioned_transition` is stamped at close).
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if a node already exists at `encoded_path` — a
    /// create must not silently clobber an existing node (node identity is
    /// positional).
    pub fn create_node(
        &mut self,
        encoded_path: Vec<u8>,
        component_type_id: u32,
        lifecycle_state: Option<LifecycleState>,
        data: Option<Vec<u8>>,
    ) -> Result<(), ChasmError> {
        if self.nodes.contains_key(&encoded_path) {
            return Err(ChasmError::Internal(format!(
                "create_node: a node already exists at path {encoded_path:?}"
            )));
        }
        let node = ChasmNode {
            // The placeholder VT is overwritten at close; using the current
            // execution clock keeps the node well-ordered if read before close.
            metadata: NodeMetadata::new(component_type_id, lifecycle_state, self.execution_vt),
            data,
        };
        self.nodes.insert(encoded_path.clone(), node);
        self.created.insert(encoded_path.clone());
        self.dirty.insert(encoded_path);
        Ok(())
    }

    /// Replace the data payload of the node at `encoded_path` and mark it dirty.
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if no node exists at `encoded_path`.
    pub fn set_data(
        &mut self,
        encoded_path: &[u8],
        data: Option<Vec<u8>>,
    ) -> Result<(), ChasmError> {
        let node = self.node_mut(encoded_path)?;
        node.data = data;
        self.dirty.insert(encoded_path.to_vec());
        Ok(())
    }

    /// Set the lifecycle of the component node at `encoded_path` and mark it dirty.
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if no node exists at `encoded_path`.
    pub fn set_lifecycle(
        &mut self,
        encoded_path: &[u8],
        lifecycle: LifecycleState,
    ) -> Result<(), ChasmError> {
        let node = self.node_mut(encoded_path)?;
        node.metadata.lifecycle_state = Some(lifecycle);
        self.dirty.insert(encoded_path.to_vec());
        Ok(())
    }

    /// Mark the node at `encoded_path` dirty (e.g. after an in-place data
    /// mutation the caller performed through another path).
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if no node exists at `encoded_path`.
    pub fn mark_dirty(&mut self, encoded_path: &[u8]) -> Result<(), ChasmError> {
        if !self.nodes.contains_key(encoded_path) {
            return Err(ChasmError::Internal(format!(
                "mark_dirty: no node at path {encoded_path:?}"
            )));
        }
        self.dirty.insert(encoded_path.to_vec());
        Ok(())
    }

    /// Schedule a task into the outbox of the node at `encoded_path`. The task is
    /// staged and assigned its stable [`TaskId`] at [`close_transaction`](NodeTree::close_transaction);
    /// the owning node is marked dirty because its outbox (carried in `metadata`)
    /// changes (Requirement 7.2).
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if no node exists at `encoded_path`.
    pub fn add_task(
        &mut self,
        encoded_path: &[u8],
        kind: TaskKind,
        task_type_id: u32,
        payload: Vec<u8>,
        fire_at_unix_nanos: Option<i64>,
    ) -> Result<(), ChasmError> {
        if !self.nodes.contains_key(encoded_path) {
            return Err(ChasmError::Internal(format!(
                "add_task: no node at path {encoded_path:?}"
            )));
        }
        self.pending_tasks
            .entry(encoded_path.to_vec())
            .or_default()
            .push(PendingTask {
                kind,
                task_type_id,
                payload,
                fire_at_unix_nanos,
            });
        self.dirty.insert(encoded_path.to_vec());
        Ok(())
    }

    /// Iterate the nodes in the subtree rooted at `encoded_prefix` (the node itself
    /// and all descendants), in encoded-path order — a single contiguous range
    /// scan (Requirement 4.4). Pass the root's encoding (the empty slice) to walk
    /// the whole execution.
    pub fn subtree(&self, encoded_prefix: &[u8]) -> impl Iterator<Item = (&Vec<u8>, &ChasmNode)> {
        let end = subtree_range_end(encoded_prefix);
        self.nodes.range(encoded_prefix.to_vec()..end)
    }

    /// Close the current transition atomically (Requirement 5.1, 5.2, 7.3–7.6).
    ///
    /// The committing `next_vt` MUST be [`Advanced`](Staleness::Advanced) past the
    /// execution's current clock — this is the structural guarantee behind VT
    /// monotonicity (Property 3). The close then, in order:
    ///
    /// 1. **Merges** staged tasks into their nodes' outboxes, assigning each a
    ///    stable `(next_vt, offset)` [`TaskId`] from the owning node's counter.
    /// 2. **Re-validates** every task in every node through `validator`
    ///    (validate-then-drop): a [`TaskValidity::Drop`] task is removed, and a
    ///    node whose outbox shrank is marked dirty so the drop is persisted
    ///    (Requirement 7.3, 7.4).
    /// 3. **Stamps** every dirty node's `versioned_transition` with `next_vt`, and
    ///    a created node's `initial_versioned_transition` too (Requirement 5.1).
    /// 4. Computes the **tree-wide earliest** surviving pure-task deadline
    ///    (Requirement 7.6) and collects the **newly scheduled** side-effect tasks
    ///    that survived (Requirement 7.8).
    ///
    /// It then advances the execution clock to `next_vt` and clears the
    /// per-transition bookkeeping. Field writes and task schedules thus commit (or,
    /// on an error before the clock advances, roll back) together as one unit.
    ///
    /// # Errors
    ///
    /// [`ChasmError::Internal`] if `next_vt` does not strictly advance the
    /// execution clock (a non-monotonic commit is a substrate/caller bug).
    pub fn close_transaction(
        &mut self,
        next_vt: VersionedTransition,
        validator: &dyn OutboxValidator,
    ) -> Result<TransitionResult, ChasmError> {
        if next_vt.staleness_check(&self.execution_vt) != Staleness::Advanced {
            return Err(ChasmError::Internal(format!(
                "close_transaction: next VT {next_vt:?} does not advance execution VT {:?}",
                self.execution_vt
            )));
        }

        // Step 1 — merge staged tasks, assigning stable (VT, offset) identity, and
        // remember the newly scheduled side-effect tasks for the dispatch set.
        let mut new_side_effect_ids: Vec<(Vec<u8>, TaskId)> = Vec::new();
        let pending = std::mem::take(&mut self.pending_tasks);
        for (path, tasks) in pending {
            // The node is guaranteed present: add_task verified it and nothing
            // removes nodes mid-transition.
            let node = self
                .nodes
                .get_mut(&path)
                .ok_or_else(|| ChasmError::Internal("close: pending task node vanished".into()))?;
            for pending_task in tasks {
                let offset = node.metadata.next_task_offset;
                node.metadata.next_task_offset += 1;
                let scheduled = ScheduledTask {
                    kind: pending_task.kind,
                    task_type_id: pending_task.task_type_id,
                    payload: pending_task.payload,
                    fire_at_unix_nanos: pending_task.fire_at_unix_nanos,
                    id: TaskId::new(next_vt, offset),
                };
                if scheduled.is_side_effect() {
                    new_side_effect_ids.push((path.clone(), scheduled.id));
                }
                node.metadata.outbox.push(scheduled);
            }
        }

        // Step 2 — re-validate every task tree-wide; drop the stale ones and mark
        // any node whose outbox shrank as dirty so the drop is persisted.
        let mut newly_dirtied: Vec<Vec<u8>> = Vec::new();
        for (path, node) in self.nodes.iter_mut() {
            let before = node.metadata.outbox.len();
            node.metadata
                .outbox
                .pure_tasks
                .retain(|t| validator.validate(path, t) == TaskValidity::Valid);
            node.metadata
                .outbox
                .side_effect_tasks
                .retain(|t| validator.validate(path, t) == TaskValidity::Valid);
            if node.metadata.outbox.len() != before {
                newly_dirtied.push(path.clone());
            }
        }
        for path in newly_dirtied {
            self.dirty.insert(path);
        }

        // Step 3 — stamp every dirty node with the committing VT (and a created
        // node's immutable initial VT).
        for path in &self.dirty {
            if let Some(node) = self.nodes.get_mut(path) {
                node.metadata.versioned_transition = next_vt;
                if self.created.contains(path) {
                    node.metadata.initial_versioned_transition = next_vt;
                }
            }
        }

        // Step 4 — snapshot dirty nodes, the tree-wide earliest pure deadline, and
        // the surviving newly scheduled side-effect tasks.
        let dirty_nodes: Vec<(Vec<u8>, ChasmNode)> = self
            .dirty
            .iter()
            .filter_map(|path| self.nodes.get(path).map(|n| (path.clone(), n.clone())))
            .collect();

        let earliest_pure_deadline_unix_nanos = self
            .nodes
            .values()
            .filter_map(|n| n.metadata.outbox.earliest_pure_deadline())
            .min();

        let mut side_effect_tasks: Vec<DispatchableTask> = Vec::new();
        for (path, id) in new_side_effect_ids {
            if let Some(node) = self.nodes.get(&path)
                && let Some(task) = node
                    .metadata
                    .outbox
                    .side_effect_tasks
                    .iter()
                    .find(|t| t.id == id)
            {
                side_effect_tasks.push(DispatchableTask {
                    node_path: path,
                    task: task.clone(),
                });
            }
        }

        // Advance the clock and clear per-transition bookkeeping.
        self.execution_vt = next_vt;
        self.dirty.clear();
        self.created.clear();

        Ok(TransitionResult {
            dirty_nodes,
            side_effect_tasks,
            earliest_pure_deadline_unix_nanos,
        })
    }

    /// Mutable access to a node, erroring if absent.
    fn node_mut(&mut self, encoded_path: &[u8]) -> Result<&mut ChasmNode, ChasmError> {
        self.nodes
            .get_mut(encoded_path)
            .ok_or_else(|| ChasmError::Internal(format!("no node at path {encoded_path:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_key_round_trips() {
        let key = ExecutionKey::new("ns", "wf-1", "run-1");
        let json = serde_json::to_string(&key).expect("serialize");
        let back: ExecutionKey = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(key, back);
        assert_eq!(back.business_id, "wf-1");
    }

    #[test]
    fn execution_info_open_until_close_time_set() {
        let mut info = ExecutionInfo::default();
        assert!(!info.is_closed());
        assert_eq!(info.state_transition_count, 0);
        info.close_time_unix_nanos = Some(1_700_000_000_000_000_000);
        assert!(info.is_closed());
    }

    use crate::task::{RetainAllValidator, TaskKind, TaskValidity};

    fn vt(failover: i64, count: i64) -> VersionedTransition {
        VersionedTransition::new(failover, count)
    }

    #[test]
    fn create_stamps_initial_and_last_vt_on_close() {
        let mut tree = NodeTree::new();
        tree.create_node(
            b"$state".to_vec(),
            7,
            Some(LifecycleState::Running),
            Some(vec![1]),
        )
        .expect("create");
        let result = tree
            .close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close");
        assert_eq!(result.dirty_nodes.len(), 1);
        let node = tree.node(b"$state").expect("present");
        assert_eq!(node.metadata.versioned_transition, vt(1, 1));
        assert_eq!(node.metadata.initial_versioned_transition, vt(1, 1));
        assert_eq!(tree.execution_vt(), vt(1, 1));
    }

    #[test]
    fn second_transition_advances_last_vt_but_not_initial() {
        let mut tree = NodeTree::new();
        tree.create_node(
            b"$state".to_vec(),
            7,
            Some(LifecycleState::Running),
            Some(vec![1]),
        )
        .expect("create");
        tree.close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close 1");
        tree.set_data(b"$state", Some(vec![2])).expect("mutate");
        tree.close_transaction(vt(1, 2), &RetainAllValidator)
            .expect("close 2");
        let node = tree.node(b"$state").expect("present");
        // Last-update VT advanced; node identity (initial VT) is immutable.
        assert_eq!(node.metadata.versioned_transition, vt(1, 2));
        assert_eq!(node.metadata.initial_versioned_transition, vt(1, 1));
    }

    #[test]
    fn close_rejects_non_advancing_vt() {
        let mut tree = NodeTree::new();
        tree.create_node(b"$state".to_vec(), 7, Some(LifecycleState::Running), None)
            .expect("create");
        tree.close_transaction(vt(1, 5), &RetainAllValidator)
            .expect("close");
        // Same VT does not advance.
        assert!(matches!(
            tree.close_transaction(vt(1, 5), &RetainAllValidator),
            Err(ChasmError::Internal(_))
        ));
        // A behind VT does not advance.
        assert!(matches!(
            tree.close_transaction(vt(1, 4), &RetainAllValidator),
            Err(ChasmError::Internal(_))
        ));
    }

    #[test]
    fn dirty_only_writes_persist_exactly_mutated_nodes() {
        // Property 4 shape: only the dirtied node appears in the result.
        let mut tree = NodeTree::new();
        tree.create_node(
            b"$state".to_vec(),
            1,
            Some(LifecycleState::Running),
            Some(vec![0]),
        )
        .expect("create state");
        tree.create_node(b"$input".to_vec(), 2, None, Some(vec![9]))
            .expect("create input");
        tree.close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close 1");

        // Mutate only $state in the next transition.
        tree.set_data(b"$state", Some(vec![1])).expect("mutate");
        let result = tree
            .close_transaction(vt(1, 2), &RetainAllValidator)
            .expect("close 2");
        assert_eq!(result.dirty_nodes.len(), 1);
        assert_eq!(result.dirty_nodes[0].0, b"$state".to_vec());
        // $input keeps its original stamp (was not rewritten).
        assert_eq!(
            tree.node(b"$input").unwrap().metadata.versioned_transition,
            vt(1, 1)
        );
    }

    #[test]
    fn add_task_assigns_identity_and_returns_dispatch_set() {
        let mut tree = NodeTree::new();
        tree.create_node(b"$state".to_vec(), 1, Some(LifecycleState::Running), None)
            .expect("create");
        tree.add_task(b"$state", TaskKind::Pure, 10, vec![1], Some(500))
            .expect("pure task");
        tree.add_task(b"$state", TaskKind::SideEffect, 11, vec![2], None)
            .expect("side-effect task");
        let result = tree
            .close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close");

        // Pure task drives the tree-wide earliest deadline.
        assert_eq!(result.earliest_pure_deadline_unix_nanos, Some(500));
        // Side-effect task is dispatchable post-commit.
        assert_eq!(result.side_effect_tasks.len(), 1);
        assert_eq!(result.side_effect_tasks[0].node_path, b"$state".to_vec());

        // Both tasks carry a stable (VT, offset) identity with the committing VT.
        let node = tree.node(b"$state").unwrap();
        assert_eq!(
            node.metadata.outbox.pure_tasks[0].id.versioned_transition,
            vt(1, 1)
        );
        assert_eq!(node.metadata.outbox.pure_tasks[0].id.offset, 0);
        assert_eq!(node.metadata.outbox.side_effect_tasks[0].id.offset, 1);
    }

    #[test]
    fn validate_then_drop_reaps_stale_tasks() {
        // Property 5 shape: a Drop verdict removes the task from the outbox and it
        // never appears in the dispatch set; a Valid task is retained.
        let mut tree = NodeTree::new();
        tree.create_node(b"$state".to_vec(), 1, Some(LifecycleState::Running), None)
            .expect("create");
        tree.add_task(b"$state", TaskKind::SideEffect, 11, vec![2], None)
            .expect("task");
        // Drop every side-effect task on this close.
        let drop_side_effects = |_p: &[u8], t: &ScheduledTask| {
            if t.is_side_effect() {
                TaskValidity::Drop
            } else {
                TaskValidity::Valid
            }
        };
        let result = tree
            .close_transaction(vt(1, 1), &drop_side_effects)
            .expect("close");
        assert!(result.side_effect_tasks.is_empty());
        assert!(tree.node(b"$state").unwrap().metadata.outbox.is_empty());
    }

    #[test]
    fn earliest_pure_deadline_is_tree_wide_minimum() {
        // Property 6 shape: the earliest pure deadline is the min across all nodes.
        let mut tree = NodeTree::new();
        tree.create_node(b"$a".to_vec(), 1, Some(LifecycleState::Running), None)
            .expect("create a");
        tree.create_node(b"$b".to_vec(), 2, Some(LifecycleState::Running), None)
            .expect("create b");
        tree.add_task(b"$a", TaskKind::Pure, 10, vec![], Some(900))
            .expect("a task");
        tree.add_task(b"$b", TaskKind::Pure, 10, vec![], Some(300))
            .expect("b task");
        let result = tree
            .close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close");
        assert_eq!(result.earliest_pure_deadline_unix_nanos, Some(300));
    }

    #[test]
    fn node_round_trips_through_serde() {
        // Property 1 shape: a node with metadata (incl. outbox) and data round-trips.
        let mut tree = NodeTree::new();
        tree.create_node(
            b"$state".to_vec(),
            7,
            Some(LifecycleState::Completed),
            Some(vec![4, 5, 6]),
        )
        .expect("create");
        tree.add_task(b"$state", TaskKind::Pure, 10, vec![1], Some(500))
            .expect("task");
        tree.close_transaction(vt(2, 3), &RetainAllValidator)
            .expect("close");
        let node = tree.node(b"$state").expect("present").clone();
        let json = serde_json::to_string(&node).expect("serialize");
        let back: ChasmNode = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(node, back);
    }

    #[test]
    fn subtree_range_scan_returns_only_descendants() {
        // A loaded tree; the subtree scan over "$attempts" returns the node and its
        // collection children, not the sibling "$state".
        let mut tree = NodeTree::new();
        for (path, ty) in [
            (b"$state".to_vec(), 1u32),
            (b"$attempts".to_vec(), 2),
            (b"$attempts#0001".to_vec(), 3),
            (b"$attempts#0002".to_vec(), 3),
        ] {
            tree.load_node(
                path,
                ChasmNode {
                    metadata: NodeMetadata::new(ty, None, vt(1, 1)),
                    data: None,
                },
            );
        }
        let paths: Vec<Vec<u8>> = tree.subtree(b"$attempts").map(|(p, _)| p.clone()).collect();
        assert_eq!(
            paths,
            vec![
                b"$attempts".to_vec(),
                b"$attempts#0001".to_vec(),
                b"$attempts#0002".to_vec()
            ]
        );
    }
}
