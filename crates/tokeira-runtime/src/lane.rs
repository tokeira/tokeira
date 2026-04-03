use anyhow::{Result, anyhow};
use async_trait::async_trait;
use smallvec::SmallVec;
use tokeira_kernel::{Command, DispatchOp, Kernel};
use tokeira_storage::{CommitResult, RunRepository};
use tokeira_types::RunKey;
use tokio::sync::{mpsc, oneshot};

/// Configuration knobs for a single lane executor.
///
/// See [`spawn_lane`] and the
/// [runtime architecture](../../../docs/crates/runtime.md)
/// for how these values influence command processing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneConfig {
    /// Maximum optimistic-concurrency-control retries
    /// before surfacing a conflict error to the caller.
    pub max_occ_retries: u32,
    /// Maximum commands drained from the channel for the
    /// same run in a single activation before yielding.
    pub max_drain_per_activation: u32,
}

impl Default for LaneConfig {
    fn default() -> Self {
        Self {
            max_occ_retries: 5,
            max_drain_per_activation: 16,
        }
    }
}

/// Publishes dispatch operations produced by a committed
/// transition (workflow tasks, activity tasks, etc.).
///
/// Implementations are expected to be cheap and
/// non-blocking; the lane holds no locks while calling
/// [`publish`](DispatchPublisher::publish).
#[async_trait]
pub trait DispatchPublisher: Send + Sync {
    /// Publish a batch of [`DispatchOp`]s for `run_key`.
    async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()>;
}

/// A lane is a single serial command processor.
///
/// Insight: lanes are *execution locality* devices. They reduce lock pressure
/// and make it obvious which piece of code serializes commands for a run, but
/// they do not define correctness. If a run moves between lanes later, the run's
/// durable state remains the source of truth.
pub struct LaneHandle {
    tx: mpsc::Sender<LaneMessage>,
}

impl LaneHandle {
    /// Submit a command for `run_key` and wait for the
    /// commit result.
    ///
    /// The command is serialized through the lane's
    /// single-threaded executor, so callers never need
    /// external locking on the run.
    pub async fn submit(
        &self,
        run_key: RunKey,
        command: Command,
    ) -> Result<CommitResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(LaneMessage {
                run_key,
                command,
                reply_tx,
            })
            .await?;
        reply_rx.await?
    }
}

struct LaneMessage {
    run_key: RunKey,
    command: Command,
    reply_tx: oneshot::Sender<Result<CommitResult>>,
}

/// Spawn a new lane executor as a background Tokio task.
///
/// Each lane owns a bounded channel and processes commands
/// serially. Commands for the same run are coalesced within
/// a single activation up to
/// [`LaneConfig::max_drain_per_activation`].
///
/// Returns a [`LaneHandle`] that callers use to submit
/// commands.
pub fn spawn_lane<K, R, P>(
    kernel: K,
    repo: R,
    publisher: P,
    config: LaneConfig,
) -> LaneHandle
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
    P: DispatchPublisher + 'static,
{
    let (tx, mut rx) = mpsc::channel::<LaneMessage>(1024);
    let requeue_tx = tx.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let buffered =
                run_activation(&kernel, &repo, &publisher, &mut rx, message, &config)
                    .await;
            for message in buffered {
                if requeue_tx.send(message).await.is_err() {
                    break;
                }
            }
        }
    });
    LaneHandle { tx }
}

async fn run_activation<K, R, P>(
    kernel: &K,
    repo: &R,
    publisher: &P,
    rx: &mut mpsc::Receiver<LaneMessage>,
    first_message: LaneMessage,
    config: &LaneConfig,
) -> Vec<LaneMessage>
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
    P: DispatchPublisher + 'static,
{
    let active_run_key = first_message.run_key;
    let mut current = Some(first_message);
    let mut buffered = Vec::new();
    let mut drained = 0usize;
    let drain_limit = config.max_drain_per_activation.max(1) as usize;

    while let Some(message) = current.take() {
        let result = handle_message(
            kernel,
            repo,
            message.run_key,
            message.command,
            config.max_occ_retries,
        )
        .await;

        let stop_draining = result.is_err();
        let reply = match result {
            Ok((commit_result, dispatch_ops)) => {
                if !dispatch_ops.is_empty() {
                    if let Err(error) =
                        publisher.publish(message.run_key, &dispatch_ops).await
                    {
                        tracing::warn!(?error, run_key = ?message.run_key, "failed to publish dispatch ops");
                    }
                }
                Ok(commit_result)
            }
            Err(error) => Err(error),
        };
        let _ = message.reply_tx.send(reply);
        drained += 1;

        if stop_draining || drained >= drain_limit {
            break;
        }

        match rx.try_recv() {
            Ok(next) if next.run_key == active_run_key => {
                current = Some(next);
            }
            Ok(other) => {
                buffered.push(other);
                break;
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => break,
        }
    }

    buffered
}

async fn handle_message<K, R>(
    kernel: &K,
    repo: &R,
    run_key: RunKey,
    command: Command,
    max_retries: u32,
) -> Result<(CommitResult, SmallVec<[DispatchOp; 4]>)>
where
    K: Kernel + Send + Sync + 'static,
    R: RunRepository + 'static,
{
    let mut attempts = 0u32;
    loop {
        let loaded = repo.load_run(run_key).await?;
        let transition = kernel
            .apply(loaded, command.clone())
            .map_err(|reject| anyhow!("kernel rejected command: {reject}"))?;
        let dispatch_ops = transition.dispatch_ops.clone();

        match repo.commit_transition(run_key, transition).await? {
            CommitResult::Applied { new_state } => {
                return Ok((CommitResult::Applied { new_state }, dispatch_ops));
            }
            CommitResult::Duplicate => {
                return Ok((CommitResult::Duplicate, SmallVec::new()));
            }
            CommitResult::Conflict { reason } => {
                if attempts >= max_retries {
                    return Err(anyhow!(
                        "lane OCC retry exhausted after {} conflicts for {:?}: {}",
                        attempts + 1,
                        run_key,
                        reason
                    ));
                }
                attempts += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex},
    };

    use proptest::prelude::*;
    use smallvec::smallvec;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityState, HistoryEvent, LoadedRun, PendingWorkflowTask, ProjectionOp,
        Reject, RequestDedupeOp, TimerOp, Transition, WorkflowState,
    };
    use tokeira_storage::{
        BacklogEntry, CommitResult, DispatchableActivityTask, DispatchableWorkflowTask,
        DueTimer, LeaseOutcome, LeaseRepository, ProjectionBatch, ProjectionLog,
        ProjectionRecord, RequestRecord, TransitionAuditRecord,
    };
    use tokeira_types::{
        ExecutionRef, ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payloads,
        ProjectionCursor, QueueKey, RequestContext, RequestId, RunId, RunKey,
        SearchAttributes, ShardEpoch, ShardId, TaskKind, TaskQueueName,
        TransitionSeq as DurableTransitionSeq, WorkerIdentity, WorkflowId, WorkflowType,
    };
    use tokio::runtime::Runtime;
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;

    #[derive(Clone)]
    struct MockKernel {
        state: Arc<Mutex<MockKernelState>>,
    }

    struct MockKernelState {
        applied_commands: Vec<Command>,
        loaded_runs: Vec<LoadedRun>,
        dispatch_ops: SmallVec<[DispatchOp; 4]>,
        reject: bool,
    }

    impl MockKernel {
        fn new(dispatch_ops: SmallVec<[DispatchOp; 4]>) -> Self {
            Self {
                state: Arc::new(Mutex::new(MockKernelState {
                    applied_commands: Vec::new(),
                    loaded_runs: Vec::new(),
                    dispatch_ops,
                    reject: false,
                })),
            }
        }

        fn with_reject(self) -> Self {
            self.state.lock().unwrap().reject = true;
            self
        }

        fn snapshot(&self) -> (Vec<Command>, Vec<LoadedRun>) {
            let state = self.state.lock().unwrap();
            (state.applied_commands.clone(), state.loaded_runs.clone())
        }
    }

    impl Kernel for MockKernel {
        fn apply(
            &self,
            loaded: LoadedRun,
            command: Command,
        ) -> Result<Transition, Reject> {
            let mut state = self.state.lock().unwrap();
            state.applied_commands.push(command);
            state.loaded_runs.push(loaded.clone());
            if state.reject {
                return Err(Reject::WorkflowPaused);
            }

            let LoadedRun::Existing(current) = loaded else {
                panic!("tests expect an existing run");
            };

            let mut next_state = current.clone();
            next_state.transition_seq = current.transition_seq.next();
            next_state.last_event_id += 1;

            Ok(Transition {
                expected_seq: current.transition_seq,
                next_state,
                history_events: smallvec![HistoryEvent {
                    event_id: current.last_event_id + 1,
                    happened_at: OffsetDateTime::now_utc(),
                    kind: tokeira_kernel::HistoryEventKind::WorkflowExecutionSignaled {
                        signal_name: "test".to_string(),
                        input: Payloads::default(),
                        request_id: "req".to_string(),
                        identity: None,
                    },
                }],
                request_dedupe_ops: SmallVec::<[RequestDedupeOp; 1]>::new(),
                activity_ops: SmallVec::<[tokeira_kernel::ActivityOp; 4]>::new(),
                timer_ops: SmallVec::<[TimerOp; 4]>::new(),
                dispatch_ops: state.dispatch_ops.clone(),
                projection_ops: SmallVec::<[ProjectionOp; 8]>::new(),
            })
        }
    }

    #[derive(Clone)]
    struct MockRepo {
        state: Arc<AsyncMutex<MockRepoState>>,
    }

    struct MockRepoState {
        loaded: LoadedRun,
        load_calls: usize,
        commit_calls: usize,
        commit_behaviors: VecDeque<CommitBehavior>,
    }

    #[derive(Clone, Copy)]
    enum CommitBehavior {
        Applied,
        Conflict,
        Duplicate,
        Error,
    }

    impl MockRepo {
        fn new(initial: LoadedRun, commit_behaviors: Vec<CommitBehavior>) -> Self {
            Self {
                state: Arc::new(AsyncMutex::new(MockRepoState {
                    loaded: initial,
                    load_calls: 0,
                    commit_calls: 0,
                    commit_behaviors: commit_behaviors.into(),
                })),
            }
        }

        async fn snapshot(&self) -> (usize, usize, LoadedRun) {
            let state = self.state.lock().await;
            (state.load_calls, state.commit_calls, state.loaded.clone())
        }
    }

    #[async_trait]
    impl RunRepository for MockRepo {
        async fn resolve_execution(
            &self,
            _execution: &ExecutionRef,
        ) -> Result<Option<RunKey>> {
            Ok(None)
        }

        async fn load_run(&self, _run_key: RunKey) -> Result<LoadedRun> {
            let mut state = self.state.lock().await;
            state.load_calls += 1;
            Ok(state.loaded.clone())
        }

        async fn read_history(
            &self,
            _run_key: RunKey,
            _after_event_id: i64,
            _limit: usize,
        ) -> Result<Vec<HistoryEvent>> {
            Ok(Vec::new())
        }

        async fn lookup_request_dedupe(
            &self,
            _execution: &ExecutionRef,
            _request_id: &RequestId,
        ) -> Result<Option<RequestRecord>> {
            Ok(None)
        }

        async fn read_transition_audit(
            &self,
            _run_key: RunKey,
        ) -> Result<Vec<TransitionAuditRecord>> {
            Ok(Vec::new())
        }

        async fn commit_transition(
            &self,
            _run_key: RunKey,
            transition: Transition,
        ) -> Result<CommitResult> {
            let mut state = self.state.lock().await;
            state.commit_calls += 1;
            match state
                .commit_behaviors
                .pop_front()
                .unwrap_or(CommitBehavior::Applied)
            {
                CommitBehavior::Applied => {
                    state.loaded = LoadedRun::Existing(transition.next_state.clone());
                    Ok(CommitResult::Applied {
                        new_state: transition.next_state,
                    })
                }
                CommitBehavior::Conflict => Ok(CommitResult::Conflict {
                    reason: "conflict".to_string(),
                }),
                CommitBehavior::Duplicate => Ok(CommitResult::Duplicate),
                CommitBehavior::Error => Err(anyhow!("commit failed")),
            }
        }

        async fn list_dispatchable_workflow_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableWorkflowTask>> {
            Ok(Vec::new())
        }

        async fn list_dispatchable_activity_tasks(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<DispatchableActivityTask>> {
            Ok(Vec::new())
        }

        async fn persist_to_backlog(&self, _entries: Vec<BacklogEntry>) -> Result<()> {
            Ok(())
        }

        async fn drain_backlog(
            &self,
            _queue: &QueueKey,
            _limit: usize,
        ) -> Result<Vec<BacklogEntry>> {
            Ok(Vec::new())
        }

        async fn list_due_timers(
            &self,
            _now: OffsetDateTime,
            _limit: usize,
        ) -> Result<Vec<DueTimer>> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ProjectionLog for MockRepo {
        async fn read_from(
            &self,
            _cursor: &ProjectionCursor,
            _limit: usize,
        ) -> Result<ProjectionBatch> {
            Ok(ProjectionBatch {
                records: Vec::<ProjectionRecord>::new(),
                next_cursor: ProjectionCursor::beginning(0, 1),
            })
        }
    }

    #[async_trait]
    impl LeaseRepository for MockRepo {
        async fn try_acquire_bundle(
            &self,
            _bundle: ShardId,
            _owner: String,
        ) -> Result<LeaseOutcome> {
            Ok(LeaseOutcome::Acquired {
                epoch: ShardEpoch::ZERO,
            })
        }

        async fn renew_bundle(
            &self,
            _bundle: ShardId,
            _owner: String,
            _epoch: ShardEpoch,
        ) -> Result<LeaseOutcome> {
            Ok(LeaseOutcome::Renewed {
                epoch: ShardEpoch::ZERO,
            })
        }
    }

    #[derive(Clone)]
    struct MockPublisher {
        state: Arc<AsyncMutex<MockPublisherState>>,
    }

    #[derive(Default)]
    struct MockPublisherState {
        publishes: Vec<(RunKey, Vec<DispatchOp>)>,
        fail: bool,
    }

    impl MockPublisher {
        fn new() -> Self {
            Self {
                state: Arc::new(AsyncMutex::new(MockPublisherState::default())),
            }
        }

        async fn with_failure(self) -> Self {
            self.state.lock().await.fail = true;
            self
        }

        async fn snapshot(&self) -> Vec<(RunKey, Vec<DispatchOp>)> {
            self.state.lock().await.publishes.clone()
        }
    }

    #[async_trait]
    impl DispatchPublisher for MockPublisher {
        async fn publish(&self, run_key: RunKey, ops: &[DispatchOp]) -> Result<()> {
            let mut state = self.state.lock().await;
            state.publishes.push((run_key, ops.to_vec()));
            if state.fail {
                return Err(anyhow!("publisher failed"));
            }
            Ok(())
        }
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        let namespace_id = NamespaceId::new();
        WorkflowState {
            run_key,
            namespace_id,
            workflow_id: WorkflowId("workflow".to_string()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("example".to_string()),
            task_queue: TaskQueueName("queue-a".to_string()),
            status: ExecutionStatus::Running,
            transition_seq: DurableTransitionSeq::ZERO,
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq::ONE,
            pending_workflow_task: Some(PendingWorkflowTask {
                logical_seq: LogicalTaskSeq::ONE,
                scheduled_event_id: 1,
                started_event_id: None,
                attempt: 1,
            }),
            sticky: None,
            pause_info: None,
            wft_stamp: 0,
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            workflow_execution_timeout: None,
            workflow_run_timeout: None,
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            activities: BTreeMap::<String, ActivityState>::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            pending_nexus_operations: BTreeMap::new(),
            versioning_override: None,
            completion_callbacks: Vec::new(),
            started_at: OffsetDateTime::now_utc(),
            closed_at: None,
        }
    }

    fn sample_command(label: &str) -> Command {
        Command::Signal(tokeira_kernel::SignalRequest {
            signal_name: label.to_string(),
            input: Payloads::default(),
            request: RequestContext {
                request_id: RequestId(format!("req-{label}")),
                caller_identity: None,
                received_at: OffsetDateTime::now_utc(),
            },
            now: OffsetDateTime::now_utc(),
        })
    }

    fn sample_dispatch_ops(namespace_id: NamespaceId) -> SmallVec<[DispatchOp; 4]> {
        smallvec![DispatchOp::EnqueueWorkflowTask {
            queue: QueueKey {
                namespace_id,
                task_queue: TaskQueueName("queue-a".to_string()),
                task_kind: TaskKind::Workflow,
                deployment: None,
                build_id: None,
            },
            logical_seq: LogicalTaskSeq::ONE,
            sticky_preferred: Some(WorkerIdentity("worker-a".to_string())),
        }]
    }

    fn lane_message(
        run_key: RunKey,
        label: &str,
    ) -> (LaneMessage, oneshot::Receiver<Result<CommitResult>>) {
        let (reply_tx, reply_rx) = oneshot::channel();
        (
            LaneMessage {
                run_key,
                command: sample_command(label),
                reply_tx,
            },
            reply_rx,
        )
    }

    #[test]
    fn lane_config_defaults() {
        let config = LaneConfig::default();
        assert_eq!(config.max_occ_retries, 5);
        assert_eq!(config.max_drain_per_activation, 16);
    }

    #[test]
    fn lane_config_edge_values_are_representable() {
        let config = LaneConfig {
            max_occ_retries: 0,
            max_drain_per_activation: 1,
        };
        assert_eq!(config.max_occ_retries, 0);
        assert_eq!(config.max_drain_per_activation, 1);
    }

    proptest! {
        #[test]
        fn property_reload_and_recompute_on_conflict(conflicts in 0u32..4) {
            let rt = Runtime::new().unwrap();
            let (result, load_calls, commit_calls, command_len, loaded_len) = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    std::iter::repeat(CommitBehavior::Conflict)
                        .take(conflicts as usize)
                        .chain(std::iter::once(CommitBehavior::Applied))
                        .collect(),
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));

                let (result, _) = handle_message(&kernel, &repo, run_key, sample_command("a"), 8).await.unwrap();
                let (load_calls, commit_calls, _) = repo.snapshot().await;
                let (commands, loaded_runs) = kernel.snapshot();
                (result, load_calls, commit_calls, commands.len(), loaded_runs.len())
            });
            let applied = matches!(result, CommitResult::Applied { .. });
            prop_assert!(applied);
            prop_assert_eq!(load_calls, conflicts as usize + 1);
            prop_assert_eq!(commit_calls, conflicts as usize + 1);
            prop_assert_eq!(command_len, conflicts as usize + 1);
            prop_assert_eq!(loaded_len, conflicts as usize + 1);
        }

        #[test]
        fn property_same_command_across_retries(conflicts in 0u32..4) {
            let rt = Runtime::new().unwrap();
            let commands = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    std::iter::repeat(CommitBehavior::Conflict)
                        .take(conflicts as usize)
                        .chain(std::iter::once(CommitBehavior::Applied))
                        .collect(),
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
                let command = sample_command("stable");

                let _ = handle_message(&kernel, &repo, run_key, command.clone(), 8).await.unwrap();
                kernel.snapshot().0
            });
            prop_assert!(!commands.is_empty());
            let expected = commands[0].clone();
            for seen in commands {
                prop_assert_eq!(seen, expected.clone());
            }
        }

        #[test]
        fn property_retry_bound_and_exhaustion(max_retries in 0u32..8) {
            let rt = Runtime::new().unwrap();
            let (message, load_calls, commit_calls) = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    std::iter::repeat(CommitBehavior::Conflict)
                        .take(max_retries as usize + 1)
                        .collect(),
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));

                let error = handle_message(&kernel, &repo, run_key, sample_command("bound"), max_retries)
                    .await
                    .expect_err("retry exhaustion should surface as an error");
                let (load_calls, commit_calls, _) = repo.snapshot().await;
                (error.to_string(), load_calls, commit_calls)
            });
            prop_assert!(message.contains("retry exhausted"));
            prop_assert_eq!(load_calls, max_retries as usize + 1);
            prop_assert_eq!(commit_calls, max_retries as usize + 1);
        }

        #[test]
        fn property_duplicate_passthrough_without_retry(seed in 0u8..4) {
            let rt = Runtime::new().unwrap();
            let (result, ops, load_calls, commit_calls) = rt.block_on(async move {
                let run_key = RunKey::new();
                let state = sample_state(run_key);
                let repo = MockRepo::new(
                    LoadedRun::Existing(state.clone()),
                    vec![CommitBehavior::Duplicate],
                );
                let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));

                let (result, ops) = handle_message(
                    &kernel,
                    &repo,
                    run_key,
                    sample_command(&format!("dup-{seed}")),
                    5,
                )
                .await
                .unwrap();

                let (load_calls, commit_calls, _) = repo.snapshot().await;
                (result, ops, load_calls, commit_calls)
            });
            let _ = seed;
            prop_assert_eq!(result, CommitResult::Duplicate);
            prop_assert!(ops.is_empty());
            prop_assert_eq!(load_calls, 1);
            prop_assert_eq!(commit_calls, 1);
        }
    }

    #[tokio::test]
    async fn run_activation_coalesces_same_run_and_uses_fresh_state() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied, CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (_foreign, _foreign_reply) = lane_message(RunKey::new(), "foreign");
        let (second, second_reply) = lane_message(run_key, "second");
        let (tx, mut rx) = mpsc::channel(8);
        tx.send(second).await.unwrap();
        tx.send(_foreign).await.unwrap();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 5,
                max_drain_per_activation: 4,
            },
        )
        .await;

        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(matches!(
            second_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert_eq!(buffered.len(), 1);

        let (commands, loaded_runs) = kernel.snapshot();
        assert_eq!(commands.len(), 2);
        assert_eq!(loaded_runs.len(), 2);
        assert_eq!(
            loaded_runs,
            vec![
                LoadedRun::Existing(state.clone()),
                LoadedRun::Existing({
                    let mut next = state.clone();
                    next.transition_seq = state.transition_seq.next();
                    next.last_event_id = 1;
                    next
                }),
            ]
        );
        assert_eq!(publisher.snapshot().await.len(), 2);
    }

    #[tokio::test]
    async fn run_activation_honors_drain_limit() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![
                CommitBehavior::Applied,
                CommitBehavior::Applied,
                CommitBehavior::Applied,
            ],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (second, second_reply) = lane_message(run_key, "second");
        let (third, _third_reply) = lane_message(run_key, "third");
        let (tx, mut rx) = mpsc::channel(8);
        tx.send(second).await.unwrap();
        tx.send(third).await.unwrap();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 5,
                max_drain_per_activation: 2,
            },
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(matches!(
            second_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn run_activation_stops_drain_on_error() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![
                CommitBehavior::Applied,
                CommitBehavior::Error,
                CommitBehavior::Applied,
            ],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (second, second_reply) = lane_message(run_key, "second");
        let (third, _third_reply) = lane_message(run_key, "third");
        let (tx, mut rx) = mpsc::channel(8);
        tx.send(second).await.unwrap();
        tx.send(third).await.unwrap();

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert!(second_reply.await.unwrap().is_err());
        assert!(rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn run_activation_publishes_dispatch_ops_and_swallow_publisher_errors() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let dispatch_ops = sample_dispatch_ops(state.namespace_id);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel = MockKernel::new(dispatch_ops.clone());
        let publisher = MockPublisher::new().with_failure().await;
        let (first, first_reply) = lane_message(run_key, "first");
        let (_tx, mut rx) = mpsc::channel(8);

        let buffered = run_activation(
            &kernel,
            &repo,
            &publisher,
            &mut rx,
            first,
            &LaneConfig::default(),
        )
        .await;

        assert!(buffered.is_empty());
        assert!(matches!(
            first_reply.await.unwrap().unwrap(),
            CommitResult::Applied { .. }
        ));
        assert_eq!(
            publisher.snapshot().await,
            vec![(run_key, dispatch_ops.into_vec())]
        );
    }

    #[tokio::test]
    async fn run_activation_does_not_publish_when_commit_fails() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Conflict],
        );
        let kernel = MockKernel::new(sample_dispatch_ops(state.namespace_id));
        let publisher = MockPublisher::new();
        let (first, first_reply) = lane_message(run_key, "first");
        let (_tx, mut rx) = mpsc::channel(8);

        let _ = run_activation(
            &kernel,
            &repo,
            &publisher,
            &mut rx,
            first,
            &LaneConfig {
                max_occ_retries: 0,
                max_drain_per_activation: 16,
            },
        )
        .await;

        assert!(first_reply.await.unwrap().is_err());
        assert!(publisher.snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn handle_message_returns_kernel_reject_without_retry() {
        let run_key = RunKey::new();
        let state = sample_state(run_key);
        let repo = MockRepo::new(
            LoadedRun::Existing(state.clone()),
            vec![CommitBehavior::Applied],
        );
        let kernel =
            MockKernel::new(sample_dispatch_ops(state.namespace_id)).with_reject();

        let error = handle_message(&kernel, &repo, run_key, sample_command("reject"), 5)
            .await
            .expect_err("reject should surface as error");
        assert!(error.to_string().contains("kernel rejected command"));

        let (load_calls, commit_calls, _) = repo.snapshot().await;
        assert_eq!(load_calls, 1);
        assert_eq!(commit_calls, 0);
    }
}
