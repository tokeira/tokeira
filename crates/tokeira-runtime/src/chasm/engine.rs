//! The concrete CHASM engine: transition orchestration, persistence, long-poll,
//! dispatch, the single physical timer, and the visibility hook.
//!
//! [`ChasmEngine`] is the runtime side of the substrate. It loads an execution's
//! node tree from the [`ChasmNodeRepository`], drives a transition through the pure
//! [`NodeTree::close_transaction`], persists the dirty-node set CAS-fenced, and
//! then applies the derived effects post-commit: dispatch surviving side-effect
//! tasks, arm at most one physical timer at the earliest pure-task deadline, emit
//! search attributes to visibility, and wake long-poll waiters. The pure crate
//! decides *what*; the engine performs the I/O *when* and *under what fence*
//! (`crates/tokeira-runtime/AGENTS.md`).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokeira_chasm::{
    ChasmError, Context, DispatchableTask, ExecutionInfo, ExecutionKey, LifecycleState,
    MutableContext, NodeTree, Registry, RetainAllValidator, Staleness, TransitionResult,
    VersionedTransition, VisibilitySnapshot,
};
use tokeira_storage::{ChasmNodeRepository, ExpectedVersion, NodePersistOutcome, NodeWrite};
use tokio::sync::Notify;

use super::{
    CommitOutcome, Engine, NotifyEvent, PollOutcome, PollRequest, ReadOutcome, StagedTask,
    StartRequest, UpdateOutcome, UpdateRequest,
};

/// The encoded path of an execution's root component node. The MVP materializes a
/// component from this single node (see the module doc); it is the empty key, so a
/// whole-execution range scan begins at it.
pub const ROOT_PATH: &[u8] = b"";

/// The sink for post-commit side-effect dispatch (Requirement 7.6, 7.8). The real
/// implementation enqueues to matching; the engine only hands it the surviving
/// tasks **after** the commit lands, so dispatch stays a derived effect.
#[async_trait]
pub trait DispatchSink: Send + Sync {
    /// Dispatch the surviving side-effect tasks of a committed transition for the
    /// given execution. The `key` identifies which execution the tasks belong to —
    /// a [`DispatchableTask`] carries only its owning node path, so the sink needs
    /// the key to route the task (e.g. onto a per-execution or per-task-queue
    /// queue) and to build a worker task token that addresses the execution.
    async fn dispatch(
        &self,
        key: &ExecutionKey,
        tasks: Vec<DispatchableTask>,
    ) -> anyhow::Result<()>;
}

/// The sink for derived visibility writes (Requirement 10.3). The real
/// implementation writes to `tokeira-projection`; it is off the correctness path.
///
/// The hook carries the component's typed [`VisibilitySnapshot`] (produced on
/// transition close) plus the fields only the engine knows — the `archetype_id`
/// and the committed [`VersionedTransition`] — which the projection adapter stamps
/// onto the record before writing (`reference/DECISION-visibility-engine-adapter.md`).
#[async_trait]
pub trait VisibilitySink: Send + Sync {
    /// Record a component's typed visibility snapshot for a committed transition.
    async fn record(
        &self,
        key: &ExecutionKey,
        archetype_id: u32,
        version: VersionedTransition,
        snapshot: VisibilitySnapshot,
    ) -> anyhow::Result<()>;
}

/// A [`DispatchSink`] that collects dispatched tasks in memory for tests.
#[derive(Debug, Default)]
pub struct CollectingDispatchSink {
    /// Every `(execution, task)` handed to the sink, in dispatch order.
    pub dispatched: Mutex<Vec<(ExecutionKey, DispatchableTask)>>,
}

#[async_trait]
impl DispatchSink for CollectingDispatchSink {
    async fn dispatch(
        &self,
        key: &ExecutionKey,
        tasks: Vec<DispatchableTask>,
    ) -> anyhow::Result<()> {
        let mut guard = self
            .dispatched
            .lock()
            .map_err(|_| anyhow::anyhow!("dispatch sink mutex poisoned"))?;
        for task in tasks {
            guard.push((key.clone(), task));
        }
        Ok(())
    }
}

/// A [`VisibilitySink`] that collects emitted attributes in memory for tests.
#[derive(Debug, Default)]
pub struct CollectingVisibilitySink {
    /// Every `(execution, snapshot)` pair recorded, in commit order.
    pub recorded: Mutex<Vec<(ExecutionKey, VisibilitySnapshot)>>,
}

#[async_trait]
impl VisibilitySink for CollectingVisibilitySink {
    async fn record(
        &self,
        key: &ExecutionKey,
        _archetype_id: u32,
        _version: VersionedTransition,
        snapshot: VisibilitySnapshot,
    ) -> anyhow::Result<()> {
        self.recorded
            .lock()
            .map_err(|_| anyhow::anyhow!("visibility sink mutex poisoned"))?
            .push((key.clone(), snapshot));
        Ok(())
    }
}

/// A [`VisibilitySink`] that drops everything. For deployments that do not yet
/// project CHASM component search attributes — the projection plane is off the
/// correctness path, so discarding is safe (visibility is a derived read model
/// that can be rebuilt from authoritative history).
#[derive(Debug, Default)]
pub struct NoopVisibilitySink;

#[async_trait]
impl VisibilitySink for NoopVisibilitySink {
    async fn record(
        &self,
        _key: &ExecutionKey,
        _archetype_id: u32,
        _version: VersionedTransition,
        _snapshot: VisibilitySnapshot,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Engine tuning, characterized by behaviour, not deployment (`AGENTS`
/// Configuration). `long_poll_buffer` is subtracted from `long_poll_timeout` so a
/// poll returns [`PollOutcome::Empty`] slightly before the client's deadline,
/// leaving room to resubmit (Requirement 6.6).
#[derive(Debug, Clone)]
pub struct ChasmEngineConfig {
    /// How long a [`poll_component`](Engine::poll_component) blocks before
    /// returning empty.
    pub long_poll_timeout: Duration,
    /// Slack subtracted from the timeout so the client can resubmit in time.
    pub long_poll_buffer: Duration,
    /// Bound on optimistic-concurrency reload-and-retry attempts (Requirement
    /// 9.5).
    pub max_commit_retries: u32,
}

impl Default for ChasmEngineConfig {
    fn default() -> Self {
        Self {
            long_poll_timeout: Duration::from_secs(20),
            long_poll_buffer: Duration::from_secs(1),
            max_commit_retries: 16,
        }
    }
}

/// The runtime read/write context handed to component code during a transition.
///
/// It is the concrete [`MutableContext`] the typed engine builds around one
/// execution: reads expose the execution's identity, summary, and logical clock;
/// writes **stage** tasks and a dirty flag that the engine applies to the node tree
/// after the closure returns (so the borrow of the tree and the borrow of the
/// component do not collide). A read path uses the same type but only as
/// `&dyn Context`, which exposes no mutation.
pub struct TransitionContext {
    key: ExecutionKey,
    info: ExecutionInfo,
    now: i64,
    staged_tasks: Vec<StagedTask>,
    dirtied: bool,
}

impl TransitionContext {
    /// Build a context for one execution at `execution_vt` and logical time `now`.
    pub(crate) fn new(key: ExecutionKey, execution_vt: VersionedTransition, now: i64) -> Self {
        Self {
            key,
            info: ExecutionInfo {
                state_transition_count: execution_vt.transition_count,
                approximate_state_size: 0,
                close_time_unix_nanos: None,
            },
            now,
            staged_tasks: Vec::new(),
            dirtied: false,
        }
    }

    /// The tasks the closure staged, consumed by the engine to build the
    /// [`UpdateRequest`].
    pub(crate) fn take_staged_tasks(&mut self) -> Vec<StagedTask> {
        std::mem::take(&mut self.staged_tasks)
    }
}

impl Context for TransitionContext {
    fn execution_key(&self) -> &ExecutionKey {
        &self.key
    }

    fn execution_info(&self) -> ExecutionInfo {
        self.info
    }

    fn now_unix_nanos(&self) -> i64 {
        self.now
    }
}

impl MutableContext for TransitionContext {
    fn add_task(
        &mut self,
        kind: tokeira_chasm::TaskKind,
        task_type_id: u32,
        payload: Vec<u8>,
        fire_at_unix_nanos: Option<i64>,
    ) -> Result<(), ChasmError> {
        self.staged_tasks.push(StagedTask {
            kind,
            task_type_id,
            payload,
            fire_at_unix_nanos,
        });
        Ok(())
    }

    fn mark_dirty(&mut self) -> Result<(), ChasmError> {
        // The engine always rewrites and re-stamps the root data node on an update,
        // so the flag is advisory; recording it keeps the contract honest and lets
        // future multi-node materialization act on it.
        self.dirtied = true;
        Ok(())
    }
}

/// The concrete CHASM engine (Requirement 6).
///
/// Holds the node repository, the immutable component [`Registry`], the dispatch
/// and visibility sinks, and the per-execution long-poll/timer registries. It is
/// cheap to clone-share behind an `Arc`.
pub struct ChasmEngine {
    repo: Arc<dyn ChasmNodeRepository>,
    registry: Arc<Registry>,
    dispatch: Arc<dyn DispatchSink>,
    visibility: Arc<dyn VisibilitySink>,
    config: ChasmEngineConfig,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Per-execution wake handles for monotonic long-poll (Requirement 6.5).
    pollers: Mutex<HashMap<ExecutionKey, Arc<Notify>>>,
    /// At most one armed physical-timer deadline per execution (Requirement 7.6).
    /// Engine-local, non-replicated state that never bumps the VT (Requirement
    /// 7.7).
    timers: Mutex<HashMap<ExecutionKey, Option<i64>>>,
}

impl ChasmEngine {
    /// Construct an engine with default tuning and a system-time logical clock.
    pub fn new(
        repo: Arc<dyn ChasmNodeRepository>,
        registry: Arc<Registry>,
        dispatch: Arc<dyn DispatchSink>,
        visibility: Arc<dyn VisibilitySink>,
    ) -> Self {
        Self {
            repo,
            registry,
            dispatch,
            visibility,
            config: ChasmEngineConfig::default(),
            clock: Arc::new(default_now_unix_nanos),
            pollers: Mutex::new(HashMap::new()),
            timers: Mutex::new(HashMap::new()),
        }
    }

    /// Override the engine tuning.
    pub fn with_config(mut self, config: ChasmEngineConfig) -> Self {
        self.config = config;
        self
    }

    /// Override the logical clock (tests inject a fixed clock for determinism).
    pub fn with_clock(mut self, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.clock = clock;
        self
    }

    /// The component registry this engine resolves archetypes against.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// The engine tuning.
    pub fn config(&self) -> &ChasmEngineConfig {
        &self.config
    }

    /// The engine's current logical time, in Unix nanoseconds.
    pub fn now(&self) -> i64 {
        (self.clock)()
    }

    /// The armed physical-timer deadline for an execution, if any (test/inspection
    /// surface for "single earliest pure timer", Requirement 7.6).
    pub fn armed_timer(&self, key: &ExecutionKey) -> Option<i64> {
        self.timers
            .lock()
            .ok()
            .and_then(|timers| timers.get(key).copied().flatten())
    }

    /// Load an execution's node tree plus the per-node baseline VTs that fence the
    /// next commit. The execution clock is reconstructed as the maximum node VT
    /// (every committed transition stamps at least one node with the committing
    /// VT, so the max equals the execution clock).
    async fn load_tree(
        &self,
        key: &ExecutionKey,
    ) -> Result<(NodeTree, HashMap<Vec<u8>, VersionedTransition>), ChasmError> {
        let nodes = self
            .repo
            .load_execution(key)
            .await
            .map_err(|e| ChasmError::Internal(format!("load execution: {e}")))?;
        let mut tree = NodeTree::new();
        let mut baseline = HashMap::with_capacity(nodes.len());
        let mut max_vt = VersionedTransition::default();
        for (path, node) in nodes {
            let vt = node.metadata.versioned_transition;
            if vt.staleness_check(&max_vt) == Staleness::Advanced {
                max_vt = vt;
            }
            baseline.insert(path.clone(), vt);
            tree.load_node(path, node);
        }
        tree.set_loaded_execution_vt(max_vt);
        Ok((tree, baseline))
    }

    /// Persist the dirty-node set CAS-fenced on each node's baseline VT
    /// (Requirement 9.3, 9.4).
    async fn commit(
        &self,
        key: &ExecutionKey,
        baseline: &HashMap<Vec<u8>, VersionedTransition>,
        result: &TransitionResult,
    ) -> Result<NodePersistOutcome, ChasmError> {
        let batch: Vec<NodeWrite> = result
            .dirty_nodes
            .iter()
            .map(|(path, node)| NodeWrite {
                encoded_path: path.clone(),
                node: node.clone(),
                expected: match baseline.get(path) {
                    Some(vt) => ExpectedVersion::Vt(*vt),
                    None => ExpectedVersion::Absent,
                },
            })
            .collect();
        self.repo
            .persist_dirty(key, batch)
            .await
            .map_err(|e| ChasmError::Internal(format!("persist dirty nodes: {e}")))
    }

    /// Apply the derived effects of a committed transition (Requirement 7.6, 7.8,
    /// 10.3): dispatch surviving side-effect tasks, arm the single physical timer,
    /// emit visibility, and wake long-poll waiters.
    async fn post_commit(
        &self,
        key: &ExecutionKey,
        result: TransitionResult,
        archetype_id: u32,
        version: VersionedTransition,
        visibility: Option<VisibilitySnapshot>,
    ) -> Result<(), ChasmError> {
        if !result.side_effect_tasks.is_empty() {
            self.dispatch
                .dispatch(key, result.side_effect_tasks)
                .await
                .map_err(|e| ChasmError::Internal(format!("dispatch side-effect tasks: {e}")))?;
        }
        self.arm_timer(key, result.earliest_pure_deadline_unix_nanos);
        if let Some(snapshot) = visibility {
            // Visibility is a derived read model strictly off the correctness path
            // (Requirement 10.15): the authoritative transition has already
            // committed, so a failed projection write must not fail the operation.
            // It is best-effort here; the task-26 repair/outbox makes it durable
            // (Requirement 10.11).
            if let Err(error) = self.visibility.record(key, archetype_id, version, snapshot).await {
                tracing::warn!(
                    ?error,
                    ?key,
                    "visibility projection write failed (off correctness path)"
                );
            }
        }
        self.wake_pollers(key);
        Ok(())
    }

    /// Arm at most one physical timer per execution at `deadline` (Requirement
    /// 7.6). Replacing the entry is what enforces "at most one armed timer".
    fn arm_timer(&self, key: &ExecutionKey, deadline: Option<i64>) {
        if let Ok(mut timers) = self.timers.lock() {
            timers.insert(key.clone(), deadline);
        }
    }

    /// The wake handle for an execution's long-pollers, created on first use.
    fn poller_notify(&self, key: &ExecutionKey) -> Arc<Notify> {
        let mut pollers = self.pollers.lock().expect("poller registry not poisoned");
        pollers.entry(key.clone()).or_default().clone()
    }

    /// Wake every long-poller waiting on an execution after its VT advanced.
    fn wake_pollers(&self, key: &ExecutionKey) {
        if let Ok(pollers) = self.pollers.lock()
            && let Some(notify) = pollers.get(key)
        {
            notify.notify_waiters();
        }
    }

    /// Build a root [`ComponentRef`]: the addressable return-address to the root
    /// component as of `execution_vt`, created at `initial_vt`.
    fn root_ref(
        &self,
        key: &ExecutionKey,
        archetype_id: u32,
        execution_vt: VersionedTransition,
        initial_vt: VersionedTransition,
    ) -> tokeira_chasm::ComponentRef {
        tokeira_chasm::ComponentRef::new(
            key.clone(),
            archetype_id,
            execution_vt,
            vec![],
            initial_vt,
        )
    }

    /// Read the root component's snapshot, or `None` if the execution does not
    /// exist. Shared by [`read_component`](Engine::read_component) and the long-poll
    /// resolution path.
    async fn read_root(&self, key: &ExecutionKey) -> Result<Option<ReadOutcome>, ChasmError> {
        let (tree, _) = self.load_tree(key).await?;
        match tree.node(ROOT_PATH) {
            Some(root) => Ok(Some(ReadOutcome {
                data: root.data.clone(),
                lifecycle: root.metadata.lifecycle_state,
                execution_vt: tree.execution_vt(),
            })),
            None => Ok(None),
        }
    }
}

/// Compute the next execution VT: same failover version, transition count + 1. The
/// MVP keeps `namespace_failover_version` constant; namespace-failover bumps ride
/// the same clock when wired (Requirement 5.4).
fn next_vt(current: VersionedTransition) -> VersionedTransition {
    VersionedTransition::new(
        current.namespace_failover_version,
        current.transition_count + 1,
    )
}

/// Default logical clock: wall-clock Unix nanoseconds.
fn default_now_unix_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[async_trait]
impl Engine for ChasmEngine {
    async fn start_execution(
        &self,
        req: StartRequest,
    ) -> Result<tokeira_chasm::ComponentRef, ChasmError> {
        let (mut tree, baseline) = self.load_tree(&req.key).await?;
        if !tree.is_empty() {
            return Err(ChasmError::BusinessIdConflict(format!(
                "execution {:?} already exists",
                req.key
            )));
        }
        tree.create_node(
            ROOT_PATH.to_vec(),
            req.archetype_id,
            Some(LifecycleState::Running),
            Some(req.data),
        )?;
        let committed_vt = next_vt(tree.execution_vt());
        let result = tree.close_transaction(committed_vt, &RetainAllValidator)?;
        match self.commit(&req.key, &baseline, &result).await? {
            NodePersistOutcome::Applied => {}
            NodePersistOutcome::Conflict { reason } => {
                return Err(ChasmError::BusinessIdConflict(reason));
            }
        }
        self.post_commit(&req.key, result, req.archetype_id, committed_vt, req.visibility)
            .await?;
        Ok(self.root_ref(&req.key, req.archetype_id, committed_vt, committed_vt))
    }

    async fn update_component(&self, req: UpdateRequest) -> Result<CommitOutcome, ChasmError> {
        let (mut tree, baseline) = self.load_tree(&req.key).await?;
        let root = tree.node(ROOT_PATH).ok_or(ChasmError::ExecutionNotFound)?;
        // Reject any mutating transition on a closed execution (Requirement 2.4).
        if root
            .metadata
            .lifecycle_state
            .is_some_and(LifecycleState::is_closed)
        {
            return Err(ChasmError::ExecutionClosed);
        }
        let archetype_id = root.metadata.component_type_id;
        let initial_vt = root.metadata.initial_versioned_transition;

        // Fence: if the live clock has moved past what the mutation was computed
        // against, reject so the caller reloads and recomputes (Requirement 9.5).
        if tree.execution_vt() != req.expected_execution_vt {
            return Ok(CommitOutcome::Conflict);
        }

        tree.set_data(ROOT_PATH, Some(req.new_root_data))?;
        tree.set_lifecycle(ROOT_PATH, req.new_lifecycle)?;
        for task in req.tasks {
            tree.add_task(
                ROOT_PATH,
                task.kind,
                task.task_type_id,
                task.payload,
                task.fire_at_unix_nanos,
            )?;
        }

        let committed_vt = next_vt(tree.execution_vt());
        let result = tree.close_transaction(committed_vt, &RetainAllValidator)?;
        match self.commit(&req.key, &baseline, &result).await? {
            NodePersistOutcome::Applied => {}
            NodePersistOutcome::Conflict { .. } => return Ok(CommitOutcome::Conflict),
        }
        let closed = req.new_lifecycle.is_closed();
        self.post_commit(&req.key, result, archetype_id, committed_vt, req.visibility)
            .await?;
        Ok(CommitOutcome::Applied(UpdateOutcome {
            reference: self.root_ref(&req.key, archetype_id, committed_vt, initial_vt),
            execution_vt: committed_vt,
            closed,
        }))
    }

    async fn read_component(&self, key: &ExecutionKey) -> Result<ReadOutcome, ChasmError> {
        self.read_root(key)
            .await?
            .ok_or(ChasmError::ExecutionNotFound)
    }

    async fn poll_component(&self, req: PollRequest) -> Result<PollOutcome, ChasmError> {
        let notify = self.poller_notify(&req.key);
        let deadline = Instant::now()
            + self
                .config
                .long_poll_timeout
                .saturating_sub(self.config.long_poll_buffer);
        loop {
            // Register the wake future *before* checking state so a commit between
            // the check and the await is not a lost wakeup.
            let notified = notify.notified();

            if let Some(outcome) = self.read_root(&req.key).await?
                && outcome.execution_vt.staleness_check(&req.since) == Staleness::Advanced
            {
                return Ok(PollOutcome::Advanced(outcome));
            }

            let now = Instant::now();
            if now >= deadline {
                return Ok(PollOutcome::Empty);
            }
            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
            tokio::select! {
                _ = notified => {}
                _ = sleep => return Ok(PollOutcome::Empty),
            }
        }
    }

    async fn delete_execution(&self, key: &ExecutionKey) -> Result<(), ChasmError> {
        self.repo
            .delete_execution(key)
            .await
            .map_err(|e| ChasmError::Internal(format!("delete execution: {e}")))?;
        self.arm_timer(key, None);
        self.wake_pollers(key);
        Ok(())
    }

    async fn notify_execution(
        &self,
        key: &ExecutionKey,
        _event: NotifyEvent,
    ) -> Result<(), ChasmError> {
        self.wake_pollers(key);
        Ok(())
    }
}
