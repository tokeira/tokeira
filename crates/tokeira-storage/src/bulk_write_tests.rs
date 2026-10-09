//! Shared contracts for the paged bulk writes (`bounded-bulk-writes`), run
//! against the in-memory store in the default suite and against an ephemeral
//! Aurora DSQL cluster in the live suite.
//!
//! Each case asserts the bounded behaviour. On a store that writes the whole set
//! in one transaction, each fails with DSQL's refusal, or with the in-memory
//! store's model of it.

use anyhow::Result;
use async_trait::async_trait;
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, LoadedRun, RequestDedupeOp, Transition, WorkflowState,
};
use tokeira_types::{
    ExecutionStatus, NamespaceId, Payload, Payloads, QueueKey, RequestId, RunId, RunKey,
    ShardEpoch, ShardId, TaskKind, TaskQueueName, WorkflowId,
};

use crate::{
    BacklogEntry, BacklogPayload, DeleteRunRequest, DeleteRunResult, DeliveryOrder, RunRepository,
    memory::projection_accumulator_tests::{applied, following, fresh_transition, reset_history},
    write_budget::MAX_RESET_BATCH_BYTES,
};

/// Rows one run owns in each run-owned table, read directly from a store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnedRows {
    pub(crate) hot: usize,
    pub(crate) history: usize,
    pub(crate) request_dedupe: usize,
    pub(crate) timers: usize,
    pub(crate) activity_dispatch: usize,
    pub(crate) workflow_dispatch: usize,
    pub(crate) backlog: usize,
}

/// One stored history batch: how many events it holds, and its encoded size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StoredBatch {
    pub(crate) events: usize,
    pub(crate) bytes: usize,
}

#[async_trait]
pub(crate) trait Backend: Sync {
    fn repo(&self) -> &dyn RunRepository;
    /// The shard count the store places execution homes with.
    fn shard_count(&self) -> u32;
    async fn owned_rows(&self, run_key: RunKey) -> Result<OwnedRows>;
    async fn history_batches(&self, run_key: RunKey) -> Result<Vec<StoredBatch>>;
}

/// Bytes no store can shrink: a SplitMix64 stream, so a value's stored size is
/// its length whatever a store compresses.
pub(crate) fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed ^ 0x9E37_79B9_7F4A_7C15;
    let mut bytes = Vec::with_capacity(len + 8);
    while bytes.len() < len {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        bytes.extend_from_slice(&(z ^ (z >> 31)).to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

pub(crate) fn activity_queue(namespace_id: NamespaceId) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName("bulk".into()),
        task_kind: TaskKind::Activity,
        deployment: None,
        build_id: None,
    }
}

pub(crate) fn backlog_entry(
    run_key: RunKey,
    queue: &QueueKey,
    index: usize,
    input: usize,
) -> BacklogEntry {
    BacklogEntry {
        run_key,
        queue: queue.clone(),
        payload: BacklogPayload::Activity {
            activity_id: format!("activity-{index}"),
            input: Payloads(vec![Payload::new(noise(input, index as u64))]),
            schedule_event_id: 5,
            attempt: 1,
            dispatch_revision: 0,
            stamp: 0,
        },
        priority: None,
        scheduled_at: OffsetDateTime::UNIX_EPOCH + Duration::seconds(index as i64),
        order: DeliveryOrder {
            priority_key: 3,
            fair_pass: 0,
            insertion_tie: index as u64,
        },
    }
}

async fn spill(backend: &impl Backend, count: usize, input: usize) {
    let run_key = RunKey::new();
    let queue = activity_queue(NamespaceId::new());
    let entries = (0..count)
        .map(|index| backlog_entry(run_key, &queue, index, input))
        .collect::<Vec<_>>();
    backend
        .repo()
        .persist_to_backlog(entries)
        .await
        .expect("a spill is written in transactions DSQL accepts");
    assert_eq!(
        backend.owned_rows(run_key).await.unwrap().backlog,
        count,
        "every entry of the spill is stored"
    );
}

/// 3,001 small entries: more rows than DSQL allows in one transaction.
pub(crate) async fn spill_of_many_small_entries(backend: &impl Backend) {
    spill(backend, 3_001, 16).await;
}

/// Eleven entries of about 1 MB: more bytes than DSQL allows in one transaction.
pub(crate) async fn spill_of_large_entries(backend: &impl Backend) {
    spill(backend, 11, 1_000_000).await;
}

pub(crate) fn signal(index: usize, input: usize) -> HistoryEventKind {
    HistoryEventKind::WorkflowExecutionSignaled {
        signal_name: "bulk".into(),
        input: Payloads(vec![Payload::new(noise(input, index as u64))]),
        header: None,
        links: Vec::new(),
        request_id: format!("signal-{index}"),
        identity: None,
    }
}

pub(crate) fn timer(index: usize) -> HistoryEventKind {
    HistoryEventKind::TimerStarted {
        workflow_task_completed_event_id: 4,
        timer_id: format!("timer-{index}"),
        fire_at: OffsetDateTime::now_utc() + Duration::hours(1),
    }
}

/// The second workflow task of `reset_history`, renumbered to start at `first`.
pub(crate) fn workflow_task(state: &WorkflowState, first: i64) -> Vec<HistoryEventKind> {
    reset_history(state)[4..7]
        .iter()
        .map(|event| {
            let mut kind = event.kind.clone();
            match &mut kind {
                HistoryEventKind::WorkflowTaskStarted {
                    scheduled_event_id, ..
                } => *scheduled_event_id = first,
                HistoryEventKind::WorkflowTaskCompleted {
                    scheduled_event_id,
                    started_event_id,
                    ..
                } => {
                    *scheduled_event_id = first;
                    *started_event_id = first + 1;
                }
                _ => {}
            }
            kind
        })
        .collect()
}

pub(crate) fn with_events(
    mut transition: Transition,
    first: i64,
    kinds: Vec<HistoryEventKind>,
) -> Transition {
    let happened_at = transition.next_state.started_at;
    let events = kinds
        .into_iter()
        .enumerate()
        .map(|(offset, kind)| HistoryEvent {
            event_id: first + offset as i64,
            happened_at,
            kind,
        })
        .collect::<Vec<_>>();
    transition.next_state.last_event_id = events.last().map_or(first - 1, |event| event.event_id);
    transition.event_principals = vec![None; events.len()].into();
    transition.history_events = events.into();
    transition
}

/// Commit a closed base run whose history is the first workflow task, then each
/// chunk of `chunks` in a commit of its own, then a second workflow task, whose
/// completion is the returned fork point. The run's pointer names it.
pub(crate) async fn commit_base(
    backend: &impl Backend,
    chunks: Vec<Vec<HistoryEventKind>>,
) -> (WorkflowState, i64) {
    let start = fresh_transition(RunKey::new());
    let prefix = reset_history(&start.next_state)[..4]
        .iter()
        .map(|event| event.kind.clone())
        .collect();
    let mut state = applied(commit(backend, with_events(start, 1, prefix)).await);
    for chunk in chunks {
        let first = state.last_event_id + 1;
        state = applied(commit(backend, with_events(following(&state), first, chunk)).await);
    }
    let first = state.last_event_id + 1;
    let mut close = with_events(following(&state), first, workflow_task(&state, first));
    close.next_state.status = ExecutionStatus::Completed;
    close.next_state.closed_at = Some(close.next_state.started_at);
    close.next_state.pending_workflow_task = None;
    let state = applied(commit(backend, close).await);
    (state, first + 2)
}

pub(crate) async fn commit(backend: &impl Backend, transition: Transition) -> crate::CommitResult {
    let run_key = transition.next_state.run_key;
    backend
        .repo()
        .commit_transition(run_key, transition, ShardEpoch::ZERO)
        .await
        .expect("the fixture's commits fit DSQL's limits")
}

pub(crate) fn bundle(backend: &impl Backend, state: &WorkflowState) -> ShardId {
    tokeira_types::execution_home_bundle(
        state.namespace_id.0.as_bytes(),
        state.workflow_id.0.as_bytes(),
        backend.shard_count().max(1),
    )
}

/// The first transaction of a closed run's deletion, which leaves the run's
/// rows for a purge.
pub(crate) async fn delete_first(backend: &impl Backend, state: &WorkflowState) {
    let result = backend
        .repo()
        .delete_run_for_bundle(
            state.run_key,
            bundle(backend, state),
            DeleteRunRequest {
                expected_seq: state.transition_seq,
                deleted_at: OffsetDateTime::now_utc(),
            },
            ShardEpoch::ZERO,
        )
        .await
        .expect("a deletion's transactions fit DSQL's limits");
    assert!(
        matches!(result, DeleteRunResult::Deleted { .. }),
        "unexpected deletion result: {result:?}"
    );
}

/// Delete a closed run, and purge what its first transaction leaves.
pub(crate) async fn delete_and_purge(backend: &impl Backend, state: &WorkflowState) {
    delete_first(backend, state).await;
    backend
        .repo()
        .purge_run(state.run_key)
        .await
        .expect("a purge's transactions fit DSQL's limits");
}

/// A closed run owning 3,600 request records and four history batches.
pub(crate) async fn large_closed_run(backend: &impl Backend) -> WorkflowState {
    let mut state = applied(commit(backend, fresh_transition(RunKey::new())).await);
    for chunk in 0..4 {
        let first = state.last_event_id + 1;
        let mut transition = with_events(following(&state), first, vec![signal(chunk, 16)]);
        transition.request_dedupe_ops = (0..900)
            .map(|index| RequestDedupeOp {
                request_id: RequestId(format!("request-{chunk}-{index}")),
            })
            .collect();
        state = applied(commit(backend, transition).await);
    }
    let mut close = following(&state);
    close.next_state.status = ExecutionStatus::Completed;
    close.next_state.closed_at = Some(close.next_state.started_at);
    close.next_state.pending_workflow_task = None;
    let state = applied(commit(backend, close).await);
    let before = backend.owned_rows(state.run_key).await.unwrap();
    assert!(before.request_dedupe + before.history > 3_000);
    state
}

/// The deletion of a closed run owning more rows than DSQL lets one transaction
/// change.
pub(crate) async fn deletion_of_a_large_run(backend: &impl Backend) {
    let state = large_closed_run(backend).await;
    delete_and_purge(backend, &state).await;

    assert_eq!(
        backend.owned_rows(state.run_key).await.unwrap(),
        OwnedRows::default()
    );
    assert!(matches!(
        backend.repo().load_run(state.run_key).await.unwrap(),
        LoadedRun::Absent
    ));
    assert_eq!(
        backend
            .repo()
            .find_latest_run(state.namespace_id, &state.workflow_id)
            .await
            .unwrap(),
        None
    );
}

/// Materialize a reset successor of `base`, replacing `expected` as current.
pub(crate) async fn materialize(
    backend: &impl Backend,
    base: &WorkflowState,
    fork_event_id: i64,
    successor_run_id: RunId,
    expected: Option<RunKey>,
) -> Result<()> {
    backend
        .repo()
        .materialize_reset_successor(base.run_key, fork_event_id, successor_run_id, expected)
        .await
}

/// Materialize a successor of `base`, which its pointer names, and check what it
/// holds: the base's history before the fork, in batches within the budget, and
/// a History Size that is the sum of those batches.
async fn materialize_and_check(backend: &impl Backend, base: &WorkflowState, fork: i64) -> RunKey {
    let successor_run_id = RunId::new();
    materialize(backend, base, fork, successor_run_id, Some(base.run_key))
        .await
        .expect("a materialization's transactions fit DSQL's limits");
    let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
    assert_eq!(
        backend
            .repo()
            .find_latest_run(base.namespace_id, &base.workflow_id)
            .await
            .unwrap(),
        Some(successor)
    );
    let copied = backend
        .repo()
        .read_history_to_end(successor, 0)
        .await
        .unwrap();
    let mut prefix = backend
        .repo()
        .read_history_to_end(base.run_key, 0)
        .await
        .unwrap();
    prefix.retain(|event| event.event_id < fork);
    assert_eq!(copied, prefix);
    let batches = backend.history_batches(successor).await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.events).sum::<usize>(),
        prefix.len()
    );
    assert!(
        batches
            .iter()
            .all(|batch| batch.events == 1 || batch.bytes <= MAX_RESET_BATCH_BYTES)
    );
    let (_, stats) = backend.repo().load_run_with_stats(successor).await.unwrap();
    assert_eq!(
        stats.history_size_bytes,
        batches.iter().map(|batch| batch.bytes as i64).sum::<i64>()
    );
    successor
}

/// A copied history of about 2.4 MiB, over DSQL's value limit as one batch.
pub(crate) async fn reset_over_one_mib(backend: &impl Backend) {
    let chunks = (0..4).map(|index| vec![signal(index, 600_000)]).collect();
    let (base, fork) = commit_base(backend, chunks).await;
    materialize_and_check(backend, &base, fork).await;
}

/// A copied history of about 11 MiB, over DSQL's transaction limit too.
pub(crate) async fn reset_over_ten_mib(backend: &impl Backend) {
    let chunks = (0..22).map(|index| vec![signal(index, 520_000)]).collect();
    let (base, fork) = commit_base(backend, chunks).await;
    materialize_and_check(backend, &base, fork).await;
}

/// A successor holding 3,500 pending timers, each a timer row.
pub(crate) async fn reset_with_many_timers(backend: &impl Backend) {
    let chunks = (0..4)
        .map(|chunk| (0..875).map(|index| timer(chunk * 875 + index)).collect())
        .collect();
    let (base, fork) = commit_base(backend, chunks).await;
    let successor = materialize_and_check(backend, &base, fork).await;
    assert_eq!(backend.owned_rows(successor).await.unwrap().timers, 3_500);
    let LoadedRun::Existing(state) = backend.repo().load_run(successor).await.unwrap() else {
        panic!("successor missing");
    };
    assert_eq!(state.timers.len(), 3_500);
}

/// A plain reset of a closed workflow: the pointer names the closed base.
pub(crate) async fn plain_reset_of_a_closed_workflow(backend: &impl Backend) {
    let (base, fork) = commit_base(backend, vec![vec![signal(0, 16)]]).await;
    materialize_and_check(backend, &base, fork).await;
}

/// A start of the same workflow id between the reset's commit on its base and the
/// materialization: the started run stays current, and the successor never
/// becomes visible.
pub(crate) async fn reset_with_a_start_between(backend: &impl Backend) {
    let (base, fork) = commit_base(backend, vec![vec![signal(0, 16)]]).await;
    let run_id = RunId::new();
    let mut start = fresh_transition(RunKey::derive(base.namespace_id, &base.workflow_id, run_id));
    start.next_state.namespace_id = base.namespace_id;
    start.next_state.workflow_id = WorkflowId(base.workflow_id.0.clone());
    start.next_state.run_id = run_id;
    let started = applied(commit(backend, start).await);
    let successor_run_id = RunId::new();
    let result = materialize(backend, &base, fork, successor_run_id, Some(base.run_key)).await;
    assert!(
        result.is_err(),
        "the successor must not replace a run that started after the reset was admitted"
    );
    assert_eq!(
        backend
            .repo()
            .find_latest_run(base.namespace_id, &base.workflow_id)
            .await
            .unwrap(),
        Some(started.run_key)
    );
    let successor = RunKey::derive(base.namespace_id, &base.workflow_id, successor_run_id);
    assert!(matches!(
        backend.repo().load_run(successor).await.unwrap(),
        LoadedRun::Absent
    ));
}
