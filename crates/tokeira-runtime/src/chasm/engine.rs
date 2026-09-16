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
    BusinessIdConflictPolicy, BusinessIdReusePolicy, ChasmError, Context, DispatchableTask,
    ExecutionInfo, ExecutionKey, LifecycleState, MutableContext, NodeTree, Registry,
    RegistryOutboxValidator, ScheduledTask, Staleness, TaskId, TaskOutcome, TaskValidity,
    TransitionResult, VersionedTransition, VisibilitySnapshot,
};
use tokeira_storage::{
    ChasmNodeRepository, CurrentRun, ExpectedVersion, NodePersistOutcome, NodeWrite,
};
use tokio::sync::Notify;

use super::{
    CommitOutcome, Engine, NotifyEvent, PollOutcome, PollRequest, ReadOutcome, StagedTask,
    StartOutcome, StartRequest, UpdateOutcome, UpdateRequest,
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

/// Whether an external delivery committed its transition or was already obsolete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeApplied {
    /// The outcome and task removal committed under the same fence.
    Applied(UpdateOutcome),
    /// The exact side-effect task is no longer held; state was not changed.
    NotHeld,
    /// The execution has no root node.
    ExecutionMissing,
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
pub(super) struct TransitionContext {
    key: ExecutionKey,
    info: ExecutionInfo,
    now: i64,
    staged_tasks: Vec<StagedTask>,
    resolved: Vec<TaskId>,
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
            resolved: Vec::new(),
            dirtied: false,
        }
    }

    /// The tasks the closure staged, consumed by the engine to build the
    /// [`UpdateRequest`].
    pub(crate) fn take_staged_tasks(&mut self) -> Vec<StagedTask> {
        std::mem::take(&mut self.staged_tasks)
    }

    /// Consume task resolutions for the same fenced transition as the mutation.
    pub(crate) fn take_resolved_tasks(&mut self) -> Vec<TaskId> {
        std::mem::take(&mut self.resolved)
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
    fn resolve_task(&mut self, id: TaskId) {
        self.resolved.push(id);
    }
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

// Manual impl: composed of trait objects with no `Debug` bound.
impl std::fmt::Debug for ChasmEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChasmEngine")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
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

    /// Snapshot every execution that currently has an armed timer deadline, as
    /// `(key, deadline)` pairs. The CHASM timer sweeper scans this each tick to find
    /// due executions; it is a point-in-time copy so the sweeper never holds the
    /// lock across an `await`. Engine-local, non-replicated state (Requirement 7.7).
    pub fn armed_timers_snapshot(&self) -> Vec<(ExecutionKey, i64)> {
        let mut snapshot: Vec<_> = self
            .timers
            .lock()
            .map(|timers| {
                timers
                    .iter()
                    .filter_map(|(key, deadline)| deadline.map(|d| (key.clone(), d)))
                    .collect()
            })
            .unwrap_or_default();
        snapshot.sort_by(|(a, _), (b, _)| {
            (&a.namespace_id, &a.business_id, &a.run_id).cmp(&(
                &b.namespace_id,
                &b.business_id,
                &b.run_id,
            ))
        });
        snapshot
    }

    /// Re-arm (or clear, with `None`) an execution's physical timer to `deadline`.
    /// The sweeper calls this after evaluating timeouts so the next wake is the
    /// state-derived next deadline rather than a stale pure-task deadline — firing
    /// is a derived effect re-computable from node state (Requirement 7.6, 7.7;
    /// `crates/tokeira-runtime/AGENTS.md`).
    pub fn set_armed_timer(&self, key: &ExecutionKey, deadline: Option<i64>) {
        self.arm_timer(key, deadline);
    }

    /// Load an execution's node tree plus the per-node baseline VTs that fence the
    /// next commit. The execution clock is reconstructed as the maximum node VT
    /// (every committed transition stamps at least one node with the committing
    /// VT, so the max equals the execution clock).
    /// Resolve the current run for `(namespace_id, archetype_id, business_id)` — the run a bare-id
    /// (empty `run_id`) request addresses (`activity-executions-first-class` Req 1).
    /// Authoritative: delegates to the node store's current-run pointer, never the
    /// visibility projection.
    pub async fn current_run(
        &self,
        namespace_id: &str,
        archetype_id: u32,
        business_id: &str,
    ) -> Result<Option<CurrentRun>, ChasmError> {
        self.repo
            .current_run(namespace_id, archetype_id, business_id)
            .await
            .map_err(|e| ChasmError::Internal(format!("resolve current run: {e}")))
    }

    /// Apply an external outcome only while the exact side-effect task is held.
    /// Component bytes and removal commit under one fence, so duplicates and late
    /// deliveries are inert. Conflicts reload and rerun the pure handler up to the
    /// configured bound; handler errors persist nothing.
    pub async fn apply_side_effect_outcome(
        &self,
        target: &ExecutionKey,
        task_type_id: u32,
        task_id: TaskId,
        outcome: TaskOutcome,
    ) -> Result<OutcomeApplied, ChasmError> {
        let attempts = self.config.max_commit_retries.max(1);
        for _ in 0..attempts {
            let (mut tree, baseline) = self.load_tree(target).await?;
            let Some(root) = tree.node(ROOT_PATH) else {
                return Ok(OutcomeApplied::ExecutionMissing);
            };
            let Some(task) = root
                .metadata
                .outbox
                .side_effect_tasks
                .iter()
                .find(|task| task.id == task_id && task.task_type_id == task_type_id)
                .cloned()
            else {
                return Ok(OutcomeApplied::NotHeld);
            };
            let archetype_id = root.metadata.component_type_id;
            let initial_vt = root.metadata.initial_versioned_transition;
            let data = root.data.as_deref().ok_or_else(|| {
                ChasmError::Validation("CHASM root has no component data".to_owned())
            })?;
            let mut ctx = TransitionContext::new(target.clone(), tree.execution_vt(), self.now());
            let data =
                self.registry
                    .apply_outcome(archetype_id, data, &task, &outcome, &mut ctx)?;
            tree.set_data(ROOT_PATH, Some(data))?;
            self.apply_context(&mut tree, &mut ctx)?;
            // This engine-owned drop is the delivery fence even if a handler
            // forgets to request resolution itself.
            tree.resolve_task(ROOT_PATH, task_id)?;
            let (lifecycle, visibility) = self.derive_root(&mut tree, &ctx)?;
            let vt = next_vt(tree.execution_vt());
            let result = self.close_root(&mut tree, vt, &ctx)?;
            if matches!(
                self.commit(target, &baseline, &result).await?,
                NodePersistOutcome::Conflict { .. }
            ) {
                continue;
            }
            self.post_commit(target, result, archetype_id, vt, visibility)
                .await?;
            return Ok(OutcomeApplied::Applied(UpdateOutcome {
                reference: self.root_ref(target, archetype_id, vt, initial_vt),
                execution_vt: vt,
                closed: lifecycle.is_closed(),
            }));
        }
        Err(ChasmError::RetriesExhausted { attempts })
    }

    /// Execute the currently held due pure tasks in `(deadline, id)` order under
    /// one fenced transition. Validation is repeated against each preceding
    /// handler's result, and tasks resolved by an earlier handler are skipped.
    /// Newly staged tasks wait for a later pass, bounding each transition's work.
    pub async fn execute_due_pure_tasks(
        &self,
        key: &ExecutionKey,
        now: i64,
    ) -> Result<Option<i64>, ChasmError> {
        let attempts = self.config.max_commit_retries.max(1);
        for _ in 0..attempts {
            let (mut tree, baseline) = self.load_tree(key).await?;
            let Some(root) = tree.node(ROOT_PATH) else {
                return Ok(None);
            };
            let archetype_id = root.metadata.component_type_id;
            let mut due: Vec<ScheduledTask> = root
                .metadata
                .outbox
                .pure_tasks
                .iter()
                .filter(|task| task.fire_at_unix_nanos.is_some_and(|at| at <= now))
                .cloned()
                .collect();
            due.sort_by_key(|task| {
                (
                    task.fire_at_unix_nanos,
                    task.id.versioned_transition.namespace_failover_version,
                    task.id.versioned_transition.transition_count,
                    task.id.offset,
                )
            });
            if due.is_empty() {
                return Ok(root.metadata.outbox.earliest_pure_deadline());
            }
            let mut ctx = TransitionContext::new(key.clone(), tree.execution_vt(), now);
            for task in due {
                let root = tree.node(ROOT_PATH).ok_or(ChasmError::ExecutionNotFound)?;
                if !root
                    .metadata
                    .outbox
                    .pure_tasks
                    .iter()
                    .any(|held| held.id == task.id)
                {
                    continue;
                }
                let data = root.data.as_deref().ok_or_else(|| {
                    ChasmError::Validation("CHASM root has no component data".to_owned())
                })?;
                if self
                    .registry
                    .validate_task(archetype_id, data, &task, &ctx)?
                    == TaskValidity::Valid
                {
                    let data = self
                        .registry
                        .execute_pure(archetype_id, data, &task, &mut ctx)?;
                    tree.set_data(ROOT_PATH, Some(data))?;
                    self.apply_context(&mut tree, &mut ctx)?;
                }
                tree.resolve_task(ROOT_PATH, task.id)?;
            }
            let (_, visibility) = self.derive_root(&mut tree, &ctx)?;
            let vt = next_vt(tree.execution_vt());
            let result = self.close_root(&mut tree, vt, &ctx)?;
            let next = result.earliest_pure_deadline_unix_nanos;
            if matches!(
                self.commit(key, &baseline, &result).await?,
                NodePersistOutcome::Conflict { .. }
            ) {
                continue;
            }
            self.post_commit(key, result, archetype_id, vt, visibility)
                .await?;
            return Ok(next);
        }
        Err(ChasmError::RetriesExhausted { attempts })
    }

    fn derive_root(
        &self,
        tree: &mut NodeTree,
        ctx: &dyn Context,
    ) -> Result<(LifecycleState, Option<VisibilitySnapshot>), ChasmError> {
        let root = tree.node(ROOT_PATH).ok_or(ChasmError::ExecutionNotFound)?;
        let id = root.metadata.component_type_id;
        let data = root
            .data
            .as_deref()
            .ok_or_else(|| ChasmError::Validation("CHASM root has no component data".to_owned()))?;
        let lifecycle = self.registry.lifecycle_state(id, data, ctx)?;
        let visibility = self.registry.visibility_snapshot(id, data)?;
        tree.set_lifecycle(ROOT_PATH, lifecycle)?;
        Ok((lifecycle, visibility))
    }

    /// Snapshot a root for archetype routing; callers must still fence any writes
    /// because this read is only a derived scheduling hint.
    pub async fn root_node(
        &self,
        key: &ExecutionKey,
    ) -> Result<Option<tokeira_chasm::ChasmNode>, ChasmError> {
        let (tree, _) = self.load_tree(key).await?;
        Ok(tree.node(ROOT_PATH).cloned())
    }

    fn close_root(
        &self,
        tree: &mut NodeTree,
        vt: VersionedTransition,
        ctx: &dyn Context,
    ) -> Result<TransitionResult, ChasmError> {
        let root = tree.node(ROOT_PATH).ok_or(ChasmError::ExecutionNotFound)?;
        let component_type_id = root.metadata.component_type_id;
        let data = root
            .data
            .clone()
            .ok_or_else(|| ChasmError::Validation("CHASM root has no component data".to_owned()))?;
        tree.close_transaction(
            vt,
            &RegistryOutboxValidator {
                registry: &self.registry,
                component_type_id,
                data: &data,
                ctx,
            },
        )
    }

    fn apply_context(
        &self,
        tree: &mut NodeTree,
        ctx: &mut TransitionContext,
    ) -> Result<(), ChasmError> {
        for id in ctx.take_resolved_tasks() {
            tree.resolve_task(ROOT_PATH, id)?;
        }
        for task in ctx.take_staged_tasks() {
            tree.add_task(
                ROOT_PATH,
                task.kind,
                task.task_type_id,
                task.payload,
                task.fire_at_unix_nanos,
            )?;
        }
        Ok(())
    }

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
        // Executors can synchronously commit an outcome on this same execution.
        // Publish our timer hint first, so a nested transition's newer retry timer
        // is not replaced by this transition's already-obsolete deadline.
        self.arm_timer(key, result.earliest_pure_deadline_unix_nanos);
        if !result.side_effect_tasks.is_empty()
            && let Err(error) = self.dispatch.dispatch(key, result.side_effect_tasks).await
        {
            // The commit stands. Returning an error could invite replay of an
            // already-applied command; the rebuild scanner retries delivery.
            tracing::warn!(
                ?error,
                ?key,
                "CHASM dispatch failed; committed tasks remain pending"
            );
        }
        if let Some(snapshot) = visibility {
            // The snapshot carries the close time from the component's persisted state
            // (recorded on the terminal transition), so the runtime no longer stamps
            // it — that keeps every snapshot input recomputable from node state for
            // the repair scanner (Req 10.11). Visibility is a derived read model
            // strictly off the correctness path (Requirement 10.15): the authoritative
            // transition has already committed, so a failed projection write must not
            // fail the operation. Best-effort here; the scanner makes it durable.
            if let Err(error) = self
                .visibility
                .record(key, archetype_id, version, snapshot)
                .await
            {
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

impl ChasmEngine {
    async fn check_current_run_collision(&self, req: &StartRequest) -> Result<(), ChasmError> {
        // Creation may have won after admission but before the node load. Only a
        // current run can resolve on policy reload; an older run id cannot become
        // fresh through retries and must report the original business-id conflict.
        if self
            .current_run(
                &req.key.namespace_id,
                req.archetype_id,
                &req.key.business_id,
            )
            .await?
            .is_some_and(|current| current.run_id == req.key.run_id)
        {
            return Ok(());
        }
        Err(ChasmError::BusinessIdConflict(format!(
            "run id `{}` already exists for business id `{}` and is not current",
            req.key.run_id, req.key.business_id,
        )))
    }

    pub(super) async fn start_with_initializer(
        &self,
        req: StartRequest,
        mut initialize: impl FnMut(
            &mut StartRequest,
            &mut TransitionContext,
        ) -> Result<LifecycleState, ChasmError>
        + Send,
    ) -> Result<StartOutcome, ChasmError> {
        let attempts = self.config.max_commit_retries.max(1);
        for _ in 0..attempts {
            // A failed pointer fence invalidates policy admission as well as the
            // create. Reload both the pointer and live root; the winner may have
            // our request id, or require an entirely different policy verdict.
            let mut req = req.clone();
            // Business-id reuse/conflict enforcement against the current run for this id
            // (`service/history/chasm_engine.go:1014-1090 @ v1.31.0`). The current-run
            // pointer is the authority; the run's *live* root lifecycle — not the
            // advisory pointer status — decides live-vs-terminal, so a just-closed run is
            // governed by the reuse policy rather than the conflict policy.
            let expected_current = self
                .current_run(
                    &req.key.namespace_id,
                    req.archetype_id,
                    &req.key.business_id,
                )
                .await?;
            if let Some(current) = expected_current.as_ref() {
                let current_key = ExecutionKey::new(
                    req.key.namespace_id.clone(),
                    req.key.business_id.clone(),
                    current.run_id.clone(),
                );
                let current_root = self.read_root(&current_key).await?;
                let current_lifecycle = current_root.as_ref().and_then(|r| r.lifecycle);
                let current_vt = current_root
                    .as_ref()
                    .map(|r| r.execution_vt)
                    .unwrap_or_default();
                let live = matches!(current_lifecycle, Some(LifecycleState::Running));
                // Idempotent retry: a Start carrying the same request id as the run that
                // created the current run returns that run unchanged, ahead of any policy
                // branch (`Fail/SecondStartWithSameRequestIdReturnsExistingRun @ v1.31.0`).
                let same_request_id = req
                    .request_id
                    .as_deref()
                    .is_some_and(|id| !id.is_empty() && id == current.request_id);

                let mut return_existing = same_request_id;
                if !same_request_id {
                    if live {
                        match req.policy.conflict {
                            BusinessIdConflictPolicy::Fail => {
                                return Err(already_started(current));
                            }
                            BusinessIdConflictPolicy::UseExisting => return_existing = true,
                            BusinessIdConflictPolicy::TerminateExisting => {
                                // The targeted release also answers Unimplemented here
                                // (`chasm_engine.go:1041 @ v1.31.0`); the activity edge never
                                // maps a request to this variant, so it is unreachable in
                                // practice but kept faithful.
                                return Err(ChasmError::Unsupported(
                                    "ID Conflict Policy Terminate Existing is not yet supported"
                                        .to_owned(),
                                ));
                            }
                        }
                    } else {
                        match req.policy.reuse {
                            // Terminal current run: a fresh run is admitted (fall through).
                            BusinessIdReusePolicy::AllowDuplicate => {}
                            BusinessIdReusePolicy::AllowDuplicateFailedOnly => {
                                // Reject only if the terminal run completed *successfully*;
                                // a failed/canceled/terminated/timed-out run (mapped to
                                // `Failed`) may be retried (`chasm_engine.go:1070 @ v1.31.0`).
                                if matches!(current_lifecycle, Some(LifecycleState::Completed)) {
                                    return Err(already_started(current));
                                }
                            }
                            BusinessIdReusePolicy::RejectDuplicate => {
                                return Err(already_started(current));
                            }
                        }
                    }
                }

                if return_existing {
                    return Ok(StartOutcome {
                        reference: self.root_ref(
                            &current_key,
                            req.archetype_id,
                            current_vt,
                            current_vt,
                        ),
                        created: false,
                    });
                }
            }

            let (mut tree, baseline) = self.load_tree(&req.key).await?;
            if tree.node(ROOT_PATH).is_some() {
                self.check_current_run_collision(&req).await?;
                continue;
            }
            // A fresh run_id means the node tree is empty, so the Absent node fences
            // below also reject a same-(namespace, business, run) collision. The pointer
            // advance is co-transactional with the root-node create
            // (`activity-executions-first-class` Req 1, 2).
            // The initializer runs only after policy/idempotency admission and before
            // any write. Its state and tasks share the atomic root/pointer create, so
            // rejection or failed persistence cannot leave a half-initialized run.
            let mut ctx = TransitionContext::new(req.key.clone(), tree.execution_vt(), self.now());
            let lifecycle = initialize(&mut req, &mut ctx)?;
            tree.create_node(
                ROOT_PATH.to_vec(),
                req.archetype_id,
                Some(lifecycle),
                Some(req.data),
            )?;
            self.apply_context(&mut tree, &mut ctx)?;
            let committed_vt = next_vt(tree.execution_vt());
            let result = self.close_root(&mut tree, committed_vt, &ctx)?;
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
            let current = CurrentRun {
                run_id: req.key.run_id.clone(),
                // The create request id pins id-reuse idempotency and the AlreadyStarted
                // `StartRequestId`; an absent originating id records as empty (no idempotency key).
                request_id: req.request_id.clone().unwrap_or_default(),
                status: lifecycle,
                vt_epoch: committed_vt,
            };
            match self
                .repo
                .persist_new_execution(&req.key, req.archetype_id, batch, current, expected_current)
                .await
                .map_err(|e| ChasmError::Internal(format!("persist new execution: {e}")))?
            {
                NodePersistOutcome::Applied => {}
                NodePersistOutcome::Conflict { .. } => continue,
            }

            self.post_commit(
                &req.key,
                result,
                req.archetype_id,
                committed_vt,
                req.visibility,
            )
            .await?;
            return Ok(StartOutcome {
                reference: self.root_ref(&req.key, req.archetype_id, committed_vt, committed_vt),
                created: true,
            });
        }
        Err(ChasmError::RetriesExhausted { attempts })
    }
}

/// Build the typed already-started error from the current run, carrying its run id
/// and create request id so the edge can surface the targeted release's
/// `ActivityExecutionAlreadyStarted` with `RunId`/`StartRequestId`
/// (`chasm/lib/activity/handler.go:91 @ v1.31.0`). The message mirrors the
/// serviceerror's fixed text; the structured ids are the load-bearing detail.
fn already_started(current: &CurrentRun) -> ChasmError {
    ChasmError::BusinessIdAlreadyStarted {
        run_id: current.run_id.clone(),
        request_id: current.request_id.clone(),
        message: "activity execution already started".to_owned(),
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
    async fn start_execution(&self, req: StartRequest) -> Result<StartOutcome, ChasmError> {
        self.start_with_initializer(req, |_, _| Ok(LifecycleState::Running))
            .await
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

        for id in req.resolved {
            tree.resolve_task(ROOT_PATH, id)?;
        }

        let committed_vt = next_vt(tree.execution_vt());
        let ctx = TransitionContext::new(req.key.clone(), tree.execution_vt(), self.now());
        let result = self.close_root(&mut tree, committed_vt, &ctx)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chasm::{
        TypedEngine,
        test_support::{self as ts, ConflictingStore, Root, Work},
    };
    use proptest::prelude::*;
    use prost::Message;
    use std::sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    };
    use tokeira_chasm::{BusinessIdPolicy, Task, TaskKind, task_type_id_for_fqn};

    #[tokio::test]
    async fn reused_run_id_conflicts_but_a_current_collision_reloads() {
        let repo = Arc::new(ConflictingStore::default());
        let engine = ts::engine(repo, Arc::new(AtomicI64::new(0)), ts::sink());
        let key = ts::key(0);
        let first = ts::start(&engine, key.clone()).await;
        TypedEngine::<Root>::new(engine.clone())
            .update(&first, |root, _| {
                root.data.closed = true;
                Ok(())
            })
            .await
            .unwrap();
        let next = ExecutionKey::new(&key.namespace_id, &key.business_id, "next");
        let winner = TypedEngine::<Root>::new(engine.clone())
            .start(
                next.clone(),
                ts::Data::default(),
                Some("winner".into()),
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap();
        TypedEngine::<Root>::new(engine.clone())
            .update(&winner.reference, |root, _| {
                root.data.closed = true;
                Ok(())
            })
            .await
            .unwrap();
        let error = TypedEngine::<Root>::new(engine.clone())
            .start(
                key.clone(),
                ts::Data::default(),
                Some("new-request".into()),
                BusinessIdPolicy::default(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, ChasmError::BusinessIdConflict(message) if message.contains(&key.run_id) && message.contains(&key.business_id))
        );
        let request = StartRequest {
            key: next.clone(),
            archetype_id: winner.reference.archetype_id,
            data: Vec::new(),
            request_id: Some("winner".into()),
            policy: BusinessIdPolicy::default(),
            visibility: None,
        };
        engine.check_current_run_collision(&request).await.unwrap();
        let repeated = engine.start_execution(request).await.unwrap();
        assert!(!repeated.created);
        assert_eq!(repeated.reference.execution_key, next);
    }

    #[tokio::test]
    async fn atomic_start_retries_pristine_input_and_stops_at_bound() {
        for conflicts in [1, 3] {
            let repo = Arc::new(ConflictingStore::default());
            repo.create_conflicts.store(conflicts, Ordering::SeqCst);
            let dispatch = Arc::new(CollectingDispatchSink::default());
            let engine = Arc::new(
                ChasmEngine::new(
                    repo.clone(),
                    ts::registry(),
                    dispatch.clone(),
                    Arc::new(NoopVisibilitySink),
                )
                .with_config(ChasmEngineConfig {
                    max_commit_retries: 3,
                    ..Default::default()
                }),
            );
            let mut calls = 0;
            let outcome = TypedEngine::<Root>::new(engine.clone())
                .start_with(
                    ts::key(0),
                    ts::Data::default(),
                    Some("request".into()),
                    BusinessIdPolicy::default(),
                    |component, ctx| {
                        calls += 1;
                        assert!(component.data.pure.is_empty());
                        component.data.pure.push(7);
                        ctx.add_task(
                            TaskKind::SideEffect,
                            task_type_id_for_fqn(Work::<true>::FQN),
                            Work::<true> {
                                token: 7,
                                ..Default::default()
                            }
                            .encode_to_vec(),
                            None,
                        )
                    },
                )
                .await;
            if conflicts == 1 {
                assert!(outcome.unwrap().created);
                assert_eq!(calls, 2);
                assert_eq!(dispatch.dispatched.lock().unwrap().len(), 1);
            } else {
                assert!(matches!(
                    outcome,
                    Err(ChasmError::RetriesExhausted { attempts: 3 })
                ));
                assert_eq!(calls, 3);
                assert!(repo.load_execution(&ts::key(0)).await.unwrap().is_empty());
                assert!(dispatch.dispatched.lock().unwrap().is_empty());
            }
        }
    }

    // Feature: chasm-extension-archetypes, Property 6: outcome application fence
    // Only a held task can apply; conflicts, repeats and after-drop deliveries are inert.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn outcomes_apply_once_under_conflicts(count in 1usize..8, deliveries in prop::collection::vec((0u8..6, any::<u8>(), 0usize..20), 1..40)) {
            ts::runtime().block_on(async {
                let repo = Arc::new(ConflictingStore::default());
                let engine = ts::engine(repo.clone(), Arc::new(AtomicI64::new(0)), ts::sink());
                let key = ts::key(0);
                let reference = ts::start(&engine, key.clone()).await;
                let tasks: Vec<_> = (0..count).map(|i| Work::<true> { token: i as u32, ..Default::default() }).collect();
                ts::stage(&engine, &reference, &tasks).await;
                let root = engine.root_node(&key).await.unwrap().unwrap();
                let ids: Vec<_> = root.metadata.outbox.side_effect_tasks.iter().map(|t| t.id).collect();
                let mut held = vec![true; count];
                let mut expected = Vec::new();
                let task_type = task_type_id_for_fqn(Work::<true>::FQN);
                for (action, index, conflicts) in deliveries {
                    let index = usize::from(index) % count;
                    if action == 3 {
                        repo.conflicts.store(0, Ordering::SeqCst);
                        TypedEngine::<Root>::new(engine.clone()).update(&reference, |_, ctx| { ctx.resolve_task(ids[index]); Ok(()) }).await.unwrap();
                        held[index] = false;
                        continue;
                    }
                    let before = repo.load_execution(&key).await.unwrap();
                    repo.conflicts.store(conflicts, Ordering::SeqCst);
                    let target = if action == 4 { ts::key(999) } else { key.clone() };
                    let kind = if action == 5 { 999 } else { task_type };
                    let result = engine.apply_side_effect_outcome(&target, kind, ids[index], TaskOutcome::Completed { payload: vec![1] }).await;
                    let applied = action < 3 && held[index] && conflicts < 16;
                    if applied {
                        prop_assert!(matches!(result, Ok(OutcomeApplied::Applied(_))));
                        held[index] = false;
                        expected.push(index as u32);
                    } else {
                        if action == 4 { prop_assert_eq!(result.unwrap(), OutcomeApplied::ExecutionMissing); }
                        else if action == 5 || !held[index] { prop_assert_eq!(result.unwrap(), OutcomeApplied::NotHeld); }
                        else { prop_assert!(matches!(result, Err(ChasmError::RetriesExhausted { .. })), "retry bound"); }
                        prop_assert_eq!(repo.load_execution(&key).await.unwrap(), before);
                    }
                    prop_assert_eq!(ts::data(&engine, &key).await.outcomes, expected.clone());
                }
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn unregistered_task_aborts_root_change_and_resolution() {
        let repo = Arc::new(ConflictingStore::default());
        let engine = ts::engine(repo.clone(), Arc::new(AtomicI64::new(0)), ts::sink());
        let key = ts::key(0);
        let reference = ts::start(&engine, key.clone()).await;
        ts::stage(&engine, &reference, &[Work::<true>::default()]).await;
        let before = repo.load_execution(&key).await.unwrap();
        let id = before[0].1.metadata.outbox.side_effect_tasks[0].id;
        let result = TypedEngine::<Root>::new(engine.clone())
            .update(&reference, |root, ctx| {
                root.data.outcomes.push(100);
                ctx.resolve_task(id);
                ctx.add_task(tokeira_chasm::TaskKind::SideEffect, 999, vec![], None)
            })
            .await;
        assert!(matches!(
            result,
            Err(ChasmError::UnknownTaskType {
                task_type_id: 999,
                ..
            })
        ));
        assert_eq!(repo.load_execution(&key).await.unwrap(), before);
    }

    #[tokio::test]
    async fn generic_handlers_close_roots_without_visibility() {
        for effect in [false, true] {
            let repo = Arc::new(ConflictingStore::default());
            let engine = ts::engine(repo.clone(), Arc::new(AtomicI64::new(0)), ts::sink());
            let key = ts::key(0);
            let reference = ts::start(&engine, key.clone()).await;
            TypedEngine::<Root>::new(engine.clone())
                .update(&reference, |root, _| {
                    root.data.hidden = true;
                    Ok(())
                })
                .await
                .unwrap();
            if effect {
                ts::stage(
                    &engine,
                    &reference,
                    &[Work::<true> {
                        close: true,
                        ..Default::default()
                    }],
                )
                .await;
                let task = engine
                    .root_node(&key)
                    .await
                    .unwrap()
                    .unwrap()
                    .metadata
                    .outbox
                    .side_effect_tasks[0]
                    .clone();
                let result = engine
                    .apply_side_effect_outcome(
                        &key,
                        task.task_type_id,
                        task.id,
                        TaskOutcome::Terminated,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    result,
                    OutcomeApplied::Applied(UpdateOutcome { closed: true, .. })
                ));
            } else {
                ts::stage(
                    &engine,
                    &reference,
                    &[Work::<false> {
                        deadline: 1,
                        close: true,
                        ..Default::default()
                    }],
                )
                .await;
                assert_eq!(engine.execute_due_pure_tasks(&key, 1).await.unwrap(), None);
            }
            let root = engine.root_node(&key).await.unwrap().unwrap();
            assert_eq!(
                root.metadata.lifecycle_state,
                Some(LifecycleState::Completed)
            );
            assert!(root.metadata.outbox.is_empty());
        }
    }

    #[tokio::test]
    async fn handler_errors_roll_back_the_whole_batch() {
        let repo = Arc::new(ConflictingStore::default());
        let engine = ts::engine(repo.clone(), Arc::new(AtomicI64::new(0)), ts::sink());
        let key = ts::key(0);
        let reference = ts::start(&engine, key.clone()).await;
        ts::stage(
            &engine,
            &reference,
            &[
                Work::<false> {
                    token: 1,
                    deadline: 1,
                    ..Default::default()
                },
                Work::<false> {
                    token: 2,
                    deadline: 2,
                    fail: true,
                    ..Default::default()
                },
            ],
        )
        .await;
        ts::stage(
            &engine,
            &reference,
            &[Work::<true> {
                fail: true,
                ..Default::default()
            }],
        )
        .await;
        let before = repo.load_execution(&key).await.unwrap();
        assert!(engine.execute_due_pure_tasks(&key, 2).await.is_err());
        assert_eq!(repo.load_execution(&key).await.unwrap(), before);
        let task = &before[0].1.metadata.outbox.side_effect_tasks[0];
        assert!(
            engine
                .apply_side_effect_outcome(
                    &key,
                    task.task_type_id,
                    task.id,
                    TaskOutcome::Terminated
                )
                .await
                .is_err()
        );
        assert_eq!(repo.load_execution(&key).await.unwrap(), before);
    }
}
