//! Shard recovery helpers: one-time sweep and lease renewal.

use std::sync::Arc;

use anyhow::{Result, anyhow};
use time::OffsetDateTime;
use tokeira_kernel::Command;
use tokeira_storage::{LeaseOutcome, LeaseRepository, RunRepository};
use tokeira_types::{ShardEpoch, ShardId};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{
    activity_timeout::{ActivityTrackingEntry, ActivityTrackingState},
    broker::{InMemoryActivityBroker, InMemoryBroker},
    lane::LaneHandle,
    nexus::{NexusTimeoutEntry, NexusTimeoutTrackingState},
    scanner::pick_lane,
    timeout::{WorkflowTimeoutEntry, WorkflowTimeoutTrackingState},
    wft_timeout::{WftTimeoutEntry, WftTimeoutTrackingState},
};

/// Observability summary produced by a shard sweep.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepResult {
    pub workflow_tasks_republished: usize,
    pub activity_tasks_republished: usize,
    pub due_timers_injected: usize,
    pub workflow_timeout_entries_reconstructed: usize,
    pub wft_timeout_entries_reconstructed: usize,
    pub activity_tracking_entries_reconstructed: usize,
    pub nexus_timeout_entries_reconstructed: usize,
    pub expired_sticky_claims_cleared: usize,
}

/// Reconstruct volatile delivery state for a newly-owned shard.
pub async fn sweep_shard<R>(
    shard_id: ShardId,
    repo: &R,
    broker: &InMemoryBroker,
    activity_broker: &InMemoryActivityBroker,
    lanes: &[LaneHandle],
    lane_count: usize,
    workflow_timeout_tracking: &WorkflowTimeoutTrackingState,
    wft_timeout_tracking: &WftTimeoutTrackingState,
    activity_tracking: &ActivityTrackingState,
    nexus_timeout_tracking: &NexusTimeoutTrackingState,
) -> Result<SweepResult>
where
    R: RunRepository + ?Sized,
{
    let mut result = SweepResult::default();
    let now = OffsetDateTime::now_utc();

    for mut task in repo
        .list_dispatchable_workflow_tasks_for_shard(shard_id, usize::MAX)
        .await?
    {
        if task
            .sticky_preferred
            .as_ref()
            .is_some_and(|_| task.sticky_expires_at.is_some_and(|expiry| expiry <= now))
        {
            task.sticky_preferred = None;
            result.expired_sticky_claims_cleared += 1;
        }
        broker.publish_workflow_task(task, None).await;
        result.workflow_tasks_republished += 1;
    }

    for task in repo
        .list_dispatchable_activity_tasks_for_shard(shard_id, usize::MAX)
        .await?
    {
        activity_broker.publish_activity_task(task, None).await?;
        result.activity_tasks_republished += 1;
    }

    for due in repo
        .list_due_timers_for_shard(shard_id, now, usize::MAX)
        .await?
    {
        let lane = pick_lane(lanes, lane_count, shard_id).clone();
        lane.submit(
            due.run_key,
            Command::TimerDue(tokeira_kernel::TimerDueRequest {
                timer_id: due.timer_id,
                fired_at: now,
            }),
        )
        .await?;
        result.due_timers_injected += 1;
    }

    for entry in repo
        .list_runs_with_workflow_timeouts_for_shard(shard_id, usize::MAX)
        .await?
    {
        workflow_timeout_tracking.insert(WorkflowTimeoutEntry {
            run_key: entry.run_key,
            shard_id,
            workflow_execution_timeout: entry.workflow_execution_timeout,
            workflow_run_timeout: entry.workflow_run_timeout,
            started_at: entry.started_at,
            first_run_started_at: entry.first_run_started_at,
            has_retry_policy: entry.has_retry_policy,
        });
        result.workflow_timeout_entries_reconstructed += 1;
    }

    for entry in repo
        .list_started_workflow_tasks_for_shard(shard_id, usize::MAX)
        .await?
    {
        wft_timeout_tracking.insert(WftTimeoutEntry {
            run_key: entry.run_key,
            shard_id,
            logical_seq: entry.logical_seq,
            started_event_id: entry.started_event_id,
            started_at: entry.started_at,
            workflow_task_timeout: entry.workflow_task_timeout,
        });
        result.wft_timeout_entries_reconstructed += 1;
    }

    for entry in repo
        .list_open_activities_for_shard(shard_id, usize::MAX)
        .await?
    {
        activity_tracking.insert(ActivityTrackingEntry {
            run_key: entry.run_key,
            shard_id,
            activity_id: entry.activity_id,
            original_scheduled_at: entry.original_scheduled_at,
            last_dispatched_at: entry.original_scheduled_at,
            started_at: entry.started_at,
            last_heartbeat_at: None,
            cancel_requested: false,
        });
        result.activity_tracking_entries_reconstructed += 1;
    }

    for entry in repo
        .list_pending_nexus_operations_for_shard(shard_id, usize::MAX)
        .await?
    {
        nexus_timeout_tracking.insert(NexusTimeoutEntry {
            run_key: entry.run_key,
            shard_id,
            operation_id: entry.operation_id,
            scheduled_event_id: entry.scheduled_event_id,
            schedule_to_close_timeout: entry.schedule_to_close_timeout,
            scheduled_at: entry.scheduled_at,
        });
        result.nexus_timeout_entries_reconstructed += 1;
    }

    Ok(result)
}

/// Periodically renew a shard lease until cancelled or rejected.
pub async fn run_lease_renewer<R>(
    repo: Arc<R>,
    shard_id: ShardId,
    owner: String,
    epoch: ShardEpoch,
    interval: tokio::time::Duration,
    max_retries: u32,
    cancel: CancellationToken,
    on_lost: oneshot::Sender<()>,
) where
    R: LeaseRepository + 'static,
{
    let mut failures = 0u32;
    let mut on_lost = Some(on_lost);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(interval) => {}
        }

        match repo.renew_bundle(shard_id, owner.clone(), epoch).await {
            Ok(LeaseOutcome::Renewed { .. }) => {
                failures = 0;
            }
            Ok(LeaseOutcome::Rejected { .. }) => {
                if let Some(tx) = on_lost.take() {
                    let _ = tx.send(());
                }
                break;
            }
            Ok(LeaseOutcome::Acquired { .. }) => {
                failures = 0;
            }
            Err(error) => {
                failures += 1;
                tracing::warn!(
                    ?error,
                    shard_id = ?shard_id,
                    failures,
                    "lease renewer failed to renew shard lease"
                );
                if failures > max_retries {
                    if let Some(tx) = on_lost.take() {
                        let _ = tx.send(());
                    }
                    break;
                }
            }
        }
    }
}

pub(crate) fn lease_rejected_error(shard_id: ShardId) -> anyhow::Error {
    anyhow!("shard lease rejected for {:?}", shard_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        broker::{InMemoryActivityBroker, InMemoryBroker},
        lane::{DispatchPublisher, LaneConfig, LaneHandle, spawn_lane},
        shard::ShardOwner,
    };
    use proptest::prelude::*;
    use std::sync::RwLock;
    use time::Duration;
    use tokeira_kernel::{
        ActivityOp, ActivityState, BasicKernel, DispatchOp, PendingNexusOperation,
        PendingWorkflowTask, TimerOp, TimerState, Transition, WorkflowState,
    };
    use tokeira_storage::{CommitResult, InMemoryStore};
    use tokeira_types::{
        ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payloads, QueueKey, RunId, RunKey,
        SearchAttributes, ShardEpoch, ShardId, TaskKind, TaskQueueName, TransitionSeq,
        WorkerIdentity, WorkflowId, WorkflowType,
    };

    #[derive(Clone)]
    struct NoopPublisher;

    #[async_trait::async_trait]
    impl DispatchPublisher for NoopPublisher {
        async fn publish(&self, _run_key: RunKey, _ops: &[DispatchOp]) -> anyhow::Result<()> {
            Ok(())
        }
        async fn submit_to_run(
            &self,
            _run_key: RunKey,
            _command: tokeira_kernel::Command,
        ) -> anyhow::Result<CommitResult> {
            Ok(CommitResult::Applied {
                new_state: sample_state(RunKey::new()),
            })
        }
    }

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("wf".into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("q".into()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
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
            first_execution_run_id: None,
            original_execution_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_initiated_event_id: 0,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_override: None,
            completion_callbacks: Vec::new(),
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
        }
    }

    fn start_transition(run_key: RunKey) -> Transition {
        Transition {
            expected_seq: TransitionSeq::ZERO,
            next_state: sample_state(run_key),
            history_events: Default::default(),
            request_dedupe_ops: Default::default(),
            activity_ops: Default::default(),
            timer_ops: Default::default(),
            dispatch_ops: Default::default(),
            projection_ops: Default::default(),
        }
    }

    fn make_lanes(store: &InMemoryStore) -> (Vec<LaneHandle>, usize) {
        let shard_owner = Arc::new(RwLock::new(ShardOwner::new(1)));
        {
            let mut owner = shard_owner.write().unwrap();
            let _ = owner.record_acquired(ShardId(0), ShardEpoch::ZERO);
            owner.mark_active(ShardId(0));
        }
        let lane = spawn_lane(
            BasicKernel::default(),
            store.clone(),
            NoopPublisher,
            shard_owner,
            ActivityTrackingState::default(),
            WorkflowTimeoutTrackingState::default(),
            crate::wft_timeout::WftTimeoutTrackingState::default(),
            NexusTimeoutTrackingState::default(),
            crate::update::UpdateRegistry::new(),
            LaneConfig::default(),
        );
        (vec![lane], 1)
    }

    #[tokio::test]
    async fn sweep_shard_reconstructs_started_workflow_task_timeouts() {
        let shard_count = 1u32;
        let store = InMemoryStore::with_shard_count(shard_count);
        let shard_id = ShardId(0);
        let run_key = RunKey::new();
        let mut transition = start_transition(run_key);
        transition.next_state.workflow_task_timeout = Duration::seconds(15);
        transition.next_state.pending_workflow_task = Some(PendingWorkflowTask {
            logical_seq: LogicalTaskSeq(7),
            scheduled_event_id: 1,
            scheduled_at: fixed_now(),
            started_event_id: Some(9),
            started_at: Some(fixed_now() + Duration::seconds(2)),
            attempt: 2,
        });
        let result = store
            .commit_transition(run_key, transition, ShardEpoch::ZERO)
            .await
            .unwrap();
        assert!(matches!(result, CommitResult::Applied { .. }));

        let broker = InMemoryBroker::default();
        let activity_broker = InMemoryActivityBroker::default();
        let (lanes, lane_count) = make_lanes(&store);
        let wts = WorkflowTimeoutTrackingState::default();
        let wfts = WftTimeoutTrackingState::default();
        let ats = ActivityTrackingState::default();
        let nts = NexusTimeoutTrackingState::default();

        let result = sweep_shard(
            shard_id,
            &store,
            &broker,
            &activity_broker,
            &lanes,
            lane_count,
            &wts,
            &wfts,
            &ats,
            &nts,
        )
        .await
        .unwrap();

        assert_eq!(result.wft_timeout_entries_reconstructed, 1);
        let entries = wfts.snapshot();
        assert_eq!(entries.len(), 1);
        let entry = &entries[0];
        assert_eq!(entry.run_key, run_key);
        assert_eq!(entry.shard_id, shard_id);
        assert_eq!(entry.logical_seq, LogicalTaskSeq(7));
        assert_eq!(entry.started_event_id, 9);
        assert_eq!(entry.started_at, fixed_now() + Duration::seconds(2));
        assert_eq!(entry.workflow_task_timeout, Duration::seconds(15));
    }

    // ── Property 5: Workflow task sweep completeness ─
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 4.1, 4.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn workflow_task_sweep_completeness(
            run_count in 1usize..5,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 4u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let ns = NamespaceId::new();
                let mut expected_runs = Vec::new();

                for idx in 0..run_count {
                    let run_key = RunKey::new();
                    let computed_shard = ShardId(
                        (run_key.0.as_u128() as u32)
                            % shard_count,
                    );
                    let mut t = start_transition(run_key);
                    t.next_state.namespace_id = ns;
                    t.next_state.workflow_id =
                        WorkflowId(format!("wf-{idx}"));
                    let result = store
                        .commit_transition(
                            run_key,
                            t,
                            ShardEpoch::ZERO,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        result,
                        CommitResult::Applied { .. }
                    ));
                    if computed_shard == shard_id {
                        expected_runs.push(run_key);
                    }
                }

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let result = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                prop_assert_eq!(
                    result.workflow_tasks_republished,
                    expected_runs.len(),
                );

                let queue = QueueKey {
                    namespace_id: ns,
                    task_queue: TaskQueueName("q".into()),
                    task_kind: TaskKind::Workflow,
                    deployment: None,
                    build_id: None,
                };
                let worker =
                    WorkerIdentity("w".into());
                let mut polled = Vec::new();
                loop {
                    match broker
                        .poll_workflow_task(
                            &queue,
                            &worker,
                            std::time::Duration::from_millis(
                                1,
                            ),
                        )
                        .await
                        .unwrap()
                    {
                        Some((task, _)) => {
                            polled.push(task.run_key)
                        }
                        None => break,
                    }
                }
                polled.sort_by_key(|rk| rk.0);
                let mut expected_sorted =
                    expected_runs.clone();
                expected_sorted.sort_by_key(|rk| rk.0);
                prop_assert_eq!(polled, expected_sorted);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 6: Activity task sweep completeness ─
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 5.1, 5.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn activity_task_sweep_completeness(
            run_count in 1usize..5,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 4u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let ns = NamespaceId::new();
                let mut expected_runs = Vec::new();

                for idx in 0..run_count {
                    let run_key = RunKey::new();
                    let computed_shard = ShardId(
                        (run_key.0.as_u128() as u32)
                            % shard_count,
                    );
                    let mut t = start_transition(run_key);
                    t.next_state.namespace_id = ns;
                    t.next_state.workflow_id =
                        WorkflowId(format!("wf-{idx}"));
                    let act_id = format!("act-{idx}");
                    let queue = QueueKey {
                        namespace_id: ns,
                        task_queue: TaskQueueName(
                            "q".into(),
                        ),
                        task_kind: TaskKind::Activity,
                        deployment: None,
                        build_id: None,
                    };
                    t.dispatch_ops.push(
                        DispatchOp::EnqueueActivityTask {
                            queue,
                            activity_id: act_id.clone(),
                            input: Payloads::default(),
                            schedule_event_id: idx as i64,
                            attempt: 1,
                            schedule_to_close_timeout: None,
                            schedule_to_start_timeout: None,
                            start_to_close_timeout: None,
                            heartbeat_timeout: None,
                        },
                    );
                    let result = store
                        .commit_transition(
                            run_key,
                            t,
                            ShardEpoch::ZERO,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        result,
                        CommitResult::Applied { .. }
                    ));
                    if computed_shard == shard_id {
                        expected_runs.push(run_key);
                    }
                }

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let result = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                prop_assert_eq!(
                    result.activity_tasks_republished,
                    expected_runs.len(),
                );

                let queue = QueueKey {
                    namespace_id: ns,
                    task_queue: TaskQueueName("q".into()),
                    task_kind: TaskKind::Activity,
                    deployment: None,
                    build_id: None,
                };
                let mut polled = Vec::new();
                loop {
                    match activity_broker
                        .poll_activity_task(
                            &queue,
                            std::time::Duration::from_millis(
                                1,
                            ),
                        )
                        .await
                        .unwrap()
                    {
                        Some((task, _)) => {
                            polled.push(task.run_key)
                        }
                        None => break,
                    }
                }
                polled.sort_by_key(|rk| rk.0);
                let mut expected_sorted =
                    expected_runs.clone();
                expected_sorted.sort_by_key(|rk| rk.0);
                prop_assert_eq!(polled, expected_sorted);
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 7: Due timer sweep completeness ────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 6.1, 6.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn due_timer_sweep_completeness(
            run_count in 1usize..4,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 4u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let mut expected_count = 0usize;

                for idx in 0..run_count {
                    let run_key = RunKey::new();
                    let computed_shard = ShardId(
                        (run_key.0.as_u128() as u32)
                            % shard_count,
                    );
                    let mut t = start_transition(run_key);
                    t.next_state.workflow_id =
                        WorkflowId(format!("wf-{idx}"));
                    let timer_id = format!("tmr-{idx}");
                    let tmr = TimerState {
                        timer_id: timer_id.clone(),
                        started_event_id: 11,
                        fire_at: fixed_now(),
                    };
                    t.timer_ops.push(TimerOp::Upsert(
                        tmr.clone(),
                    ));
                    t.next_state
                        .timers
                        .insert(timer_id, tmr);
                    let result = store
                        .commit_transition(
                            run_key,
                            t,
                            ShardEpoch::ZERO,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        result,
                        CommitResult::Applied { .. }
                    ));
                    if computed_shard == shard_id {
                        expected_count += 1;
                    }
                }

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let result = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                prop_assert_eq!(
                    result.due_timers_injected,
                    expected_count,
                );
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 8: Expired sticky claims republished
    //    without sticky preference ───────────────────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 7.1, 7.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn expired_sticky_claims_republished_without_sticky(
            _seed in 0u32..100,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 1u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let ns = NamespaceId::new();
                let run_key = RunKey::new();

                let mut t = start_transition(run_key);
                t.next_state.namespace_id = ns;
                // Set sticky with a future expiry so the
                // store does NOT clear it on read — the
                // sweep itself must detect and clear it.
                let far_past =
                    fixed_now() - Duration::seconds(1);
                t.next_state.sticky = Some(
                    tokeira_types::StickyAffinity {
                        worker_identity: WorkerIdentity(
                            "old-worker".into(),
                        ),
                        expires_at: far_past,
                    },
                );
                let result = store
                    .commit_transition(
                        run_key,
                        t,
                        ShardEpoch::ZERO,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    result,
                    CommitResult::Applied { .. }
                ));

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let result = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                // The task was republished (1 workflow task
                // in the shard).
                prop_assert_eq!(
                    result.workflow_tasks_republished,
                    1,
                );

                // After sweep, the task in the broker should
                // have no sticky preference — either the
                // store cleared it on read or the sweep
                // cleared it.
                let queue = QueueKey {
                    namespace_id: ns,
                    task_queue: TaskQueueName("q".into()),
                    task_kind: TaskKind::Workflow,
                    deployment: None,
                    build_id: None,
                };
                let worker =
                    WorkerIdentity("any-worker".into());
                let polled = broker
                    .poll_workflow_task(
                        &queue,
                        &worker,
                        std::time::Duration::from_millis(
                            10,
                        ),
                    )
                    .await
                    .unwrap();
                prop_assert!(polled.is_some());
                let (task, _) = polled.unwrap();
                prop_assert_eq!(
                    task.sticky_preferred,
                    None,
                );
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 9: Activity tracking reconstruction
    //    fidelity ────────────────────────────────────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 8.1, 8.2, 8.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn activity_tracking_reconstruction_fidelity(
            _seed in 0u32..100,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 1u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let run_key = RunKey::new();

                let mut t = start_transition(run_key);
                let act = ActivityState {
                    activity_id: "act-1".into(),
                    activity_type: "activity-type".into(),
                    schedule_event_id: 7,
                    task_queue: TaskQueueName("q".into()),
                    deployment: None,
                    build_id: None,
                    input: Payloads::default(),
                    header: None,
                    attempt: 2,
                    retry_policy: None,
                    schedule_to_close_timeout: Some(
                        Duration::seconds(30),
                    ),
                    schedule_to_start_timeout: Some(
                        Duration::seconds(10),
                    ),
                    start_to_close_timeout: Some(
                        Duration::seconds(20),
                    ),
                    heartbeat_timeout: Some(
                        Duration::seconds(5),
                    ),
                    scheduled_at: fixed_now(),
                    started_at: Some(
                        fixed_now()
                            + Duration::seconds(2),
                    ),
                    started_event_id: None,
                    pause_info: None,
                    stamp: 0,
                };
                t.activity_ops.push(ActivityOp::Upsert(
                    act.clone(),
                ));
                t.next_state.activities.insert(
                    "act-1".into(),
                    act.clone(),
                );
                let result = store
                    .commit_transition(
                        run_key,
                        t,
                        ShardEpoch::ZERO,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    result,
                    CommitResult::Applied { .. }
                ));

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let _ = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                let entries = ats.snapshot();
                prop_assert_eq!(entries.len(), 1);
                let entry = &entries[0];
                prop_assert_eq!(
                    entry.original_scheduled_at,
                    act.scheduled_at,
                );
                prop_assert_eq!(
                    entry.started_at,
                    act.started_at,
                );
                prop_assert_eq!(
                    entry.last_heartbeat_at,
                    None,
                );
                prop_assert_eq!(
                    entry.cancel_requested,
                    false,
                );
                prop_assert_eq!(
                    entry.shard_id,
                    shard_id,
                );
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 10: Workflow timeout tracking
    //    reconstruction fidelity ─────────────────────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 9.1, 9.2, 9.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn workflow_timeout_tracking_reconstruction(
            _seed in 0u32..100,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 1u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let run_key = RunKey::new();

                let mut t = start_transition(run_key);
                t.next_state.workflow_execution_timeout =
                    Some(Duration::minutes(5));
                t.next_state.workflow_run_timeout =
                    Some(Duration::minutes(10));
                t.next_state.started_at = fixed_now();
                t.next_state.first_run_started_at =
                    Some(
                        fixed_now()
                            - Duration::minutes(1),
                    );
                let result = store
                    .commit_transition(
                        run_key,
                        t,
                        ShardEpoch::ZERO,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    result,
                    CommitResult::Applied { .. }
                ));

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let _ = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                let entries = wts.snapshot();
                prop_assert_eq!(entries.len(), 1);
                let entry = &entries[0];
                prop_assert_eq!(
                    entry.workflow_execution_timeout,
                    Some(Duration::minutes(5)),
                );
                prop_assert_eq!(
                    entry.workflow_run_timeout,
                    Some(Duration::minutes(10)),
                );
                prop_assert_eq!(
                    entry.started_at,
                    fixed_now(),
                );
                prop_assert_eq!(
                    entry.first_run_started_at,
                    Some(
                        fixed_now()
                            - Duration::minutes(1)
                    ),
                );
                prop_assert_eq!(
                    entry.shard_id,
                    shard_id,
                );
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 11: Nexus timeout tracking
    //    reconstruction fidelity ─────────────────────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 10.1, 10.2, 10.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn nexus_timeout_tracking_reconstruction(
            _seed in 0u32..100,
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 1u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let shard_id = ShardId(0);
                let run_key = RunKey::new();

                let mut t = start_transition(run_key);
                let nexus_op = PendingNexusOperation {
                    operation_id: "nop-1".into(),
                    scheduled_event_id: 42,
                    endpoint: "ep".into(),
                    service: "svc".into(),
                    operation: "op".into(),
                    schedule_to_close_timeout: Some(
                        Duration::minutes(3),
                    ),
                    scheduled_at: fixed_now(),
                    started: false,
                };
                t.next_state
                    .pending_nexus_operations
                    .insert(
                        "nop-1".into(),
                        nexus_op.clone(),
                    );
                let result = store
                    .commit_transition(
                        run_key,
                        t,
                        ShardEpoch::ZERO,
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    result,
                    CommitResult::Applied { .. }
                ));

                let broker = InMemoryBroker::default();
                let activity_broker =
                    InMemoryActivityBroker::default();
                let (lanes, lane_count) =
                    make_lanes(&store);
                let wts =
                    WorkflowTimeoutTrackingState::default();
                let wfts =
                    WftTimeoutTrackingState::default();
                let ats = ActivityTrackingState::default();
                let nts =
                    NexusTimeoutTrackingState::default();

                let _ = sweep_shard(
                    shard_id,
                    &store,
                    &broker,
                    &activity_broker,
                    &lanes,
                    lane_count,
                    &wts,
                    &wfts,
                    &ats,
                    &nts,
                )
                .await
                .unwrap();

                let entries = nts.snapshot();
                prop_assert_eq!(entries.len(), 1);
                let entry = &entries[0];
                prop_assert_eq!(
                    &entry.operation_id,
                    "nop-1",
                );
                prop_assert_eq!(
                    entry.schedule_to_close_timeout,
                    Duration::minutes(3),
                );
                prop_assert_eq!(
                    entry.scheduled_at,
                    fixed_now(),
                );
                prop_assert_eq!(
                    entry.shard_id,
                    shard_id,
                );
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }

    // ── Property 18: Timer scanner shard scoping ────
    // Feature: runtime-sweeper-recovery
    // **Validates: Requirements 12.1**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn timer_scanner_shard_scoping(
            run_count in 2usize..6,
        ) {
            use crate::scanner::{
                TimerScannerConfig,
                scan_due_timers_once_for_shard,
            };
            use std::sync::Mutex;

            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let shard_count = 4u32;
                let store =
                    InMemoryStore::with_shard_count(shard_count);
                let target_shard = ShardId(0);
                let mut expected_count = 0usize;

                for idx in 0..run_count {
                    let run_key = RunKey::new();
                    let computed_shard = ShardId(
                        (run_key.0.as_u128() as u32)
                            % shard_count,
                    );
                    let mut t = start_transition(run_key);
                    t.next_state.workflow_id =
                        WorkflowId(format!("wf-{idx}"));
                    let timer_id =
                        format!("tmr-{idx}");
                    let tmr = TimerState {
                        timer_id: timer_id.clone(),
                        started_event_id: 11,
                        fire_at: fixed_now(),
                    };
                    t.timer_ops.push(TimerOp::Upsert(
                        tmr.clone(),
                    ));
                    t.next_state
                        .timers
                        .insert(timer_id, tmr);
                    let result = store
                        .commit_transition(
                            run_key,
                            t,
                            ShardEpoch::ZERO,
                        )
                        .await
                        .unwrap();
                    assert!(matches!(
                        result,
                        CommitResult::Applied { .. }
                    ));
                    if computed_shard == target_shard {
                        expected_count += 1;
                    }
                }

                let submitted = Arc::new(
                    Mutex::new(Vec::new()),
                );
                let submitted_clone =
                    submitted.clone();
                let config = TimerScannerConfig {
                    scan_interval:
                        std::time::Duration::from_millis(
                            100,
                        ),
                    max_timers_per_scan: 100,
                };

                scan_due_timers_once_for_shard(
                    &store,
                    target_shard,
                    &config,
                    |due, _fired_at| {
                        let submitted =
                            submitted_clone.clone();
                        async move {
                            submitted
                                .lock()
                                .unwrap()
                                .push(due.run_key);
                            Ok(())
                        }
                    },
                )
                .await;

                let submitted =
                    submitted.lock().unwrap();
                prop_assert_eq!(
                    submitted.len(),
                    expected_count,
                );
                Ok::<(), proptest::test_runner::TestCaseError>(())
            })?;
        }
    }
}
