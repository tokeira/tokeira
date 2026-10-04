//! The recovery index: which runs hold work the recovery sweep rebuilds after a
//! shard takeover, and what it rebuilds from each.
//!
//! Every `workflow_hot` write stores [`recovery_needed`] of the state it writes
//! (the nullable `recovery_needed` column, V072), and an index on
//! `(shard_id, recovery_needed, run_key)` (V073) lets a store page through a
//! shard's candidates by run key. The sweep and the worker-compute sampler then
//! read only those runs and derive everything from one decode of each state
//! with [`recovery_entries`], instead of decoding every run of the shard once
//! per kind of work (recovery-index spec).
//!
//! Rows written before V072 hold NULL. A store serves them first, as
//! candidates, so recovery stays complete without a backfill: a write always
//! sets the flag, so a legacy row written during a listing moves into the
//! flagged phase, which has not run yet.

use std::future::Future;

use tokeira_kernel::{CallbackState, WorkflowState};
use tokeira_types::RunKey;

use crate::api::{
    ActivitySweepEntry, CompletionCallbackSweepEntry, DispatchableWorkflowTask, NexusSweepEntry,
    WftTimeoutSweepEntry, WorkflowTimeoutSweepEntry, dispatchable_workflow_task,
};

/// Whether `state` holds work the recovery sweep or the worker-compute sampler
/// rebuilds after a takeover: a pending workflow task; in an open run, a
/// workflow execution or run timeout, an activity, or a pending Nexus
/// operation; or, in a run of any status, a completion callback awaiting
/// delivery.
///
/// It must cover everything [`recovery_entries`] and
/// [`reconstructible_nexus_deliveries`](crate::reconstructible_nexus_deliveries)
/// derive, or recovery would miss that work; a property test holds the two
/// together. It may cover more: a covered run that yields nothing is skipped.
#[must_use]
pub fn recovery_needed(state: &WorkflowState) -> bool {
    let open = state.status.is_open();
    state.pending_workflow_task.is_some()
        || (open
            && (state.workflow_execution_timeout.is_some()
                || state.workflow_run_timeout.is_some()
                || !state.activities.is_empty()
                || !state.pending_nexus_operations.is_empty()))
        || state
            .completion_callbacks
            .iter()
            .any(|callback| callback_awaits_delivery(&callback.state))
}

/// A callback that has fired but not been delivered: `Scheduled` (fired, not
/// yet attempted) or `BackingOff` (an attempt failed and a retry is due). Both
/// must be re-watched, so a `Scheduled` callback whose first attempt was lost
/// to a crash is still delivered.
fn callback_awaits_delivery(state: &CallbackState) -> bool {
    matches!(state, CallbackState::Scheduled | CallbackState::BackingOff)
}

/// Everything the recovery sweep rebuilds from one run's state.
#[derive(Clone, Debug, Default)]
pub struct RecoveryEntries {
    /// A scheduled, unstarted workflow task of a running run, to republish.
    pub dispatchable_workflow_task: Option<DispatchableWorkflowTask>,
    /// An open run's workflow execution or run timeout, to track.
    pub workflow_timeout: Option<WorkflowTimeoutSweepEntry>,
    /// A started workflow task, whose start-to-close timeout to track.
    pub started_workflow_task: Option<WftTimeoutSweepEntry>,
    /// An open run's activities, whose timeouts to track.
    pub activities: Vec<ActivitySweepEntry>,
    /// An open run's Nexus operations that have a timeout, to track.
    pub nexus_timeouts: Vec<NexusSweepEntry>,
    /// Completion callbacks awaiting delivery, to re-watch.
    pub completion_callbacks: Vec<CompletionCallbackSweepEntry>,
}

impl RecoveryEntries {
    /// Whether the run yields nothing to rebuild.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dispatchable_workflow_task.is_none()
            && self.workflow_timeout.is_none()
            && self.started_workflow_task.is_none()
            && self.activities.is_empty()
            && self.nexus_timeouts.is_empty()
            && self.completion_callbacks.is_empty()
    }
}

/// Derive everything the recovery sweep rebuilds from one run's state, with the
/// rules the per-kind shard listings used.
///
/// Activities come from the run's state, and only for an open run: a closed run
/// keeps its pending activities for Describe, but the runtime refuses work on
/// them, so there are no timeouts to track.
#[must_use]
pub fn recovery_entries(state: &WorkflowState) -> RecoveryEntries {
    let open = state.status.is_open();
    let run_key = state.run_key;
    RecoveryEntries {
        dispatchable_workflow_task: dispatchable_workflow_task(state),
        workflow_timeout: (open
            && (state.workflow_execution_timeout.is_some()
                || state.workflow_run_timeout.is_some()))
        .then(|| WorkflowTimeoutSweepEntry {
            run_key,
            workflow_execution_timeout: state.workflow_execution_timeout,
            workflow_run_timeout: state.workflow_run_timeout,
            started_at: state.started_at,
            workflow_start_delay: state.workflow_start_delay,
            first_run_started_at: state.first_run_started_at,
            has_retry_policy: state.retry_policy.is_some(),
        }),
        started_workflow_task: state.pending_workflow_task.as_ref().and_then(|task| {
            let (Some(started_event_id), Some(started_at)) =
                (task.started_event_id, task.started_at)
            else {
                return None;
            };
            Some(WftTimeoutSweepEntry {
                run_key,
                logical_seq: task.logical_seq,
                started_event_id,
                started_at,
                workflow_task_timeout: state.workflow_task_timeout,
            })
        }),
        activities: if open {
            state
                .activities
                .values()
                .map(|activity| ActivitySweepEntry {
                    run_key,
                    activity_id: activity.activity_id.clone(),
                    schedule_event_id: activity.schedule_event_id,
                    attempt: activity.attempt,
                    original_scheduled_at: activity.scheduled_at,
                    current_attempt_scheduled_at: activity.current_attempt_scheduled_at,
                    started_at: activity.started_at,
                    schedule_to_close_timeout: activity.schedule_to_close_timeout,
                    schedule_to_start_timeout: activity.schedule_to_start_timeout,
                    start_to_close_timeout: activity.start_to_close_timeout,
                    heartbeat_timeout: activity.heartbeat_timeout,
                })
                .collect()
        } else {
            Vec::new()
        },
        nexus_timeouts: if open {
            state
                .pending_nexus_operations
                .values()
                // An operation with no timeout has nothing to track.
                .filter(|operation| {
                    operation.schedule_to_close_timeout.is_some()
                        || operation.schedule_to_start_timeout.is_some()
                        || operation.start_to_close_timeout.is_some()
                })
                .map(|operation| NexusSweepEntry {
                    run_key,
                    operation_id: operation.operation_id.clone(),
                    scheduled_event_id: operation.scheduled_event_id,
                    scheduled_at: operation.scheduled_at,
                })
                .collect()
        } else {
            Vec::new()
        },
        completion_callbacks: state
            .completion_callbacks
            .iter()
            .enumerate()
            .filter(|(_, callback)| callback_awaits_delivery(&callback.state))
            .map(|(callback_index, _)| CompletionCallbackSweepEntry {
                run_key,
                callback_index,
            })
            .collect(),
    }
}

/// Which rows a candidate listing is reading.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryPhase {
    /// Rows written before the flag existed (NULL), served first.
    Legacy,
    /// Rows whose flag is true.
    Flagged,
}

/// Where a recovery-candidate listing resumes. Only a store reads its fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCursor {
    pub(crate) phase: RecoveryPhase,
    pub(crate) after: Option<RunKey>,
}

/// One page of a shard's recovery candidates.
#[derive(Clone, Debug, PartialEq)]
pub struct RecoveryPage {
    /// The candidates' decoded states, in run-key order within a phase.
    pub states: Vec<WorkflowState>,
    /// The cursor of the next page, or `None` once the listing is complete.
    pub next: Option<RecoveryCursor>,
}

/// Read one page of a listing that serves legacy rows before flagged rows.
///
/// `fetch(phase, after, limit)` reads up to `limit` rows of one phase in
/// run-key order, strictly after `after`. A full page ends with a cursor at its
/// last row; a short page ends its phase, moving the cursor to the flagged
/// phase or, after the flagged phase, to `None`. An empty legacy page goes
/// straight on to the flagged phase rather than returning nothing.
pub(crate) async fn read_candidate_page<T, F, Fut>(
    cursor: Option<&RecoveryCursor>,
    limit: usize,
    mut fetch: F,
) -> anyhow::Result<(Vec<(RunKey, T)>, Option<RecoveryCursor>)>
where
    F: FnMut(RecoveryPhase, Option<RunKey>, usize) -> Fut,
    Fut: Future<Output = anyhow::Result<Vec<(RunKey, T)>>>,
{
    let limit = limit.max(1);
    let (mut phase, mut after) = cursor.map_or((RecoveryPhase::Legacy, None), |cursor| {
        (cursor.phase, cursor.after)
    });
    loop {
        let rows = fetch(phase, after, limit).await?;
        if rows.len() >= limit {
            let next = rows.last().map(|(run_key, _)| RecoveryCursor {
                phase,
                after: Some(*run_key),
            });
            return Ok((rows, next));
        }
        match phase {
            RecoveryPhase::Legacy => {
                phase = RecoveryPhase::Flagged;
                after = None;
                if !rows.is_empty() {
                    let next = RecoveryCursor { phase, after };
                    return Ok((rows, Some(next)));
                }
            }
            RecoveryPhase::Flagged => return Ok((rows, None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::{Duration, OffsetDateTime};
    use tokeira_kernel::{
        ActivityState, CallbackSpec, CallbackState, CallbackTrigger, CompletionCallback,
        NexusOperationCancellation, NexusOperationCancellationState, PendingNexusOperation,
        PendingWorkflowTask, VersioningBehavior, WorkerDeploymentVersionRef, WorkflowState,
        WorkflowVersioningInfo,
    };
    use tokeira_types::{
        ExecutionStatus, LogicalTaskSeq, Memo, NamespaceId, Payloads, RunId, RunKey,
        SearchAttributes, TaskQueueName, TransitionSeq, WorkflowId, WorkflowType,
    };

    use super::*;
    use crate::reconstructible_nexus_deliveries;

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_state(run_key: RunKey) -> WorkflowState {
        WorkflowState {
            completed_update_count: 0,
            run_key,
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("workflow".into()),
            run_id: RunId::new(),
            workflow_type: WorkflowType("wf".into()),
            task_queue: TaskQueueName("queue".into()),
            deployment: None,
            build_id: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 0,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: Some(PendingWorkflowTask {
                advice: Default::default(),
                task_type: tokeira_kernel::WorkflowTaskType::Normal,
                schedule_to_start_deadline: None,
                logical_seq: LogicalTaskSeq(1),
                scheduled_event_id: 1,
                scheduled_at: fixed_now(),
                started_event_id: None,
                started_at: None,
                attempt: 1,
                target_worker_deployment_version_changed: false,
                target_version_changed_enabled: false,
                target_deployment_version: None,
            }),
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
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
            reset_run_id: None,
            parent_run_key: None,
            parent_workflow_id: None,
            parent_run_id: None,
            parent_namespace_id: None,
            parent_namespace_name: None,
            parent_initiated_event_id: 0,
            root_workflow_id: None,
            root_run_id: None,
            last_completion_result: None,
            activities: Default::default(),
            timers: Default::default(),
            children: Default::default(),
            pending_external_signals: Default::default(),
            pending_external_cancels: Default::default(),
            pending_updates: Default::default(),
            admitted_updates: Default::default(),
            pending_nexus_operations: Default::default(),
            versioning_info: None,
            worker_deployment_name: None,
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: fixed_now(),
            first_run_started_at: None,
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
            auto_reset_points: Vec::new(),
        }
    }

    /// A run with nothing to rebuild: open, no workflow task, no timeout, no
    /// activity, no Nexus operation and no callback.
    fn idle_state() -> WorkflowState {
        let mut state = sample_state(RunKey::new());
        state.pending_workflow_task = None;
        state
    }

    fn closed(mut state: WorkflowState) -> WorkflowState {
        state.status = ExecutionStatus::Completed;
        state.closed_at = Some(fixed_now());
        state.pending_workflow_task = None;
        state
    }

    fn activity(activity_id: &str) -> ActivityState {
        ActivityState {
            last_attempt_complete_time: None,
            cancel_requested: false,
            activity_reset: false,
            reset_heartbeats: false,
            started_identity: None,
            retry_last_worker_identity: None,
            activity_id: activity_id.to_owned(),
            activity_type: "activity-type".to_owned(),
            schedule_event_id: 5,
            task_queue: TaskQueueName("activity-queue".to_owned()),
            deployment: None,
            build_id: None,
            priority: None,
            input: Payloads::default(),
            header: None,
            last_failure: None,
            heartbeat_details: None,
            attempt: 2,
            retry_policy: None,
            schedule_to_close_timeout: Some(Duration::seconds(30)),
            schedule_to_start_timeout: Some(Duration::seconds(10)),
            start_to_close_timeout: Some(Duration::seconds(20)),
            heartbeat_timeout: Some(Duration::seconds(5)),
            scheduled_at: fixed_now(),
            current_attempt_scheduled_at: Some(fixed_now() + Duration::seconds(3)),
            started_at: Some(fixed_now() + Duration::seconds(4)),
            started_event_id: Some(6),
            pause_info: None,
            stamp: 0,
        }
    }

    fn nexus_operation(operation_id: &str, with_timeout: bool) -> PendingNexusOperation {
        PendingNexusOperation {
            operation_id: operation_id.to_owned(),
            scheduled_event_id: 42,
            endpoint: "endpoint".to_owned(),
            service: "service".to_owned(),
            operation: "operation".to_owned(),
            schedule_to_close_timeout: with_timeout.then_some(Duration::seconds(30)),
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            scheduled_at: fixed_now(),
            started: false,
            started_at: None,
            attempt: 0,
            last_attempt_failure: None,
            next_attempt_at: None,
            operation_token: String::new(),
            input: Payloads::default(),
            cancellation: None,
        }
    }

    fn callback(state: CallbackState) -> CompletionCallback {
        CompletionCallback {
            spec: CallbackSpec::Nexus {
                url: "temporal://system".into(),
                header: BTreeMap::new(),
            },
            links: Vec::new(),
            trigger: CallbackTrigger::WorkflowClosed,
            registration_time: None,
            state,
            attempt: 0,
            last_attempt_failure: None,
            last_attempt_complete_time: None,
            next_attempt_at: None,
        }
    }

    fn pinned(state: &mut WorkflowState) {
        state.versioning_info = Some(WorkflowVersioningInfo {
            behavior: VersioningBehavior::Pinned,
            deployment_version: Some(WorkerDeploymentVersionRef {
                deployment_name: "deployment".to_owned(),
                build_id: "build".to_owned(),
            }),
            ..WorkflowVersioningInfo::default()
        });
    }

    #[test]
    fn an_idle_open_run_needs_no_recovery() {
        let state = idle_state();
        assert!(!recovery_needed(&state));
        assert!(recovery_entries(&state).is_empty());
    }

    #[test]
    fn a_running_run_republishes_its_unstarted_workflow_task() {
        let state = sample_state(RunKey::new());
        let entries = recovery_entries(&state);
        let task = entries.dispatchable_workflow_task.expect("scheduled task");
        assert_eq!(task.run_key, state.run_key);
        assert!(entries.started_workflow_task.is_none());
        assert!(recovery_needed(&state));

        let mut paused = sample_state(RunKey::new());
        paused.status = ExecutionStatus::Paused;
        assert!(
            recovery_entries(&paused)
                .dispatchable_workflow_task
                .is_none()
        );
    }

    #[test]
    fn a_started_workflow_task_is_tracked_not_republished() {
        let mut state = sample_state(RunKey::new());
        let task = state.pending_workflow_task.as_mut().unwrap();
        task.started_event_id = Some(10);
        task.started_at = Some(fixed_now());

        let entries = recovery_entries(&state);

        assert!(entries.dispatchable_workflow_task.is_none());
        let started = entries.started_workflow_task.expect("started task");
        assert_eq!(started.started_event_id, 10);
        assert_eq!(started.workflow_task_timeout, state.workflow_task_timeout);
    }

    #[test]
    fn a_workflow_timeout_is_tracked_only_while_the_run_is_open() {
        let mut open = idle_state();
        open.workflow_run_timeout = Some(Duration::seconds(60));
        let entry = recovery_entries(&open).workflow_timeout.expect("timeout");
        assert_eq!(entry.workflow_run_timeout, Some(Duration::seconds(60)));
        assert!(recovery_needed(&open));

        let finished = closed(open);
        assert!(recovery_entries(&finished).workflow_timeout.is_none());
        assert!(!recovery_needed(&finished));
    }

    #[test]
    fn activities_are_tracked_only_while_the_run_is_open() {
        let mut open = idle_state();
        let kept = activity("activity-1");
        open.activities
            .insert(kept.activity_id.clone(), kept.clone());

        let entries = recovery_entries(&open);
        assert_eq!(entries.activities.len(), 1);
        let entry = &entries.activities[0];
        assert_eq!(entry.activity_id, kept.activity_id);
        assert_eq!(entry.schedule_event_id, kept.schedule_event_id);
        assert_eq!(entry.attempt, kept.attempt);
        assert_eq!(entry.original_scheduled_at, kept.scheduled_at);
        assert_eq!(
            entry.current_attempt_scheduled_at,
            kept.current_attempt_scheduled_at
        );
        assert_eq!(entry.started_at, kept.started_at);
        assert_eq!(entry.heartbeat_timeout, kept.heartbeat_timeout);
        assert!(recovery_needed(&open));

        // A closed run keeps its pending activities for Describe, but there is
        // nothing to time out.
        let finished = closed(open);
        assert!(recovery_entries(&finished).activities.is_empty());
        assert!(!recovery_needed(&finished));
    }

    #[test]
    fn only_nexus_operations_with_a_timeout_are_tracked() {
        let mut state = idle_state();
        for (operation_id, with_timeout) in [("timed", true), ("untimed", false)] {
            state.pending_nexus_operations.insert(
                operation_id.to_owned(),
                nexus_operation(operation_id, with_timeout),
            );
        }

        let entries = recovery_entries(&state);

        assert_eq!(entries.nexus_timeouts.len(), 1);
        assert_eq!(entries.nexus_timeouts[0].operation_id, "timed");
        assert!(recovery_needed(&state));
    }

    #[test]
    fn callbacks_awaiting_delivery_are_rewatched_in_a_closed_run() {
        let mut state = closed(idle_state());
        state.completion_callbacks = vec![
            callback(CallbackState::Scheduled),
            callback(CallbackState::BackingOff),
            callback(CallbackState::Succeeded),
        ];

        let entries = recovery_entries(&state);

        let indices: Vec<usize> = entries
            .completion_callbacks
            .iter()
            .map(|entry| entry.callback_index)
            .collect();
        assert_eq!(indices, vec![0, 1]);
        assert!(recovery_needed(&state));

        state.completion_callbacks = vec![callback(CallbackState::Succeeded)];
        assert!(!recovery_needed(&state));
    }

    #[test]
    fn reconstructible_deliveries_use_the_committed_version_and_due_state() {
        let mut state = idle_state();
        pinned(&mut state);
        state
            .pending_nexus_operations
            .insert("ready".to_owned(), nexus_operation("ready", false));
        let mut later = nexus_operation("later", false);
        later.next_attempt_at = Some(fixed_now() + Duration::minutes(1));
        state
            .pending_nexus_operations
            .insert("later".to_owned(), later);

        let deliveries = reconstructible_nexus_deliveries(&state, fixed_now());

        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].operation_id, "ready");
        assert_eq!(deliveries[0].version.deployment_name, "deployment");
        assert!(recovery_needed(&state));
    }

    fn arb_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Running),
            Just(ExecutionStatus::Paused),
            Just(ExecutionStatus::Completed),
            Just(ExecutionStatus::Failed),
            Just(ExecutionStatus::Terminated),
            Just(ExecutionStatus::TimedOut),
        ]
    }

    fn arb_callback_state() -> impl Strategy<Value = CallbackState> {
        prop_oneof![
            Just(CallbackState::Standby),
            Just(CallbackState::Scheduled),
            Just(CallbackState::BackingOff),
            Just(CallbackState::Succeeded),
            Just(CallbackState::Failed),
        ]
    }

    fn arb_cancellation() -> impl Strategy<Value = Option<NexusOperationCancellation>> {
        proptest::option::of(
            (
                prop_oneof![
                    Just(NexusOperationCancellationState::Unspecified),
                    Just(NexusOperationCancellationState::Scheduled),
                    Just(NexusOperationCancellationState::BackingOff),
                    Just(NexusOperationCancellationState::Succeeded),
                    Just(NexusOperationCancellationState::Failed),
                ],
                any::<bool>(),
            )
                .prop_map(|(state, due)| NexusOperationCancellation {
                    requested_event_id: 7,
                    requested_time: Some(fixed_now()),
                    state,
                    attempt: 1,
                    last_attempt_complete_time: None,
                    last_attempt_failure: None,
                    next_attempt_at: due.then_some(fixed_now()),
                }),
        )
    }

    /// States that combine every kind of recovery work with every status.
    fn arb_state() -> impl Strategy<Value = WorkflowState> {
        (
            arb_status(),
            0u8..3,
            (any::<bool>(), any::<bool>()),
            0usize..3,
            proptest::collection::vec(
                (
                    any::<bool>(),
                    any::<bool>(),
                    any::<bool>(),
                    arb_cancellation(),
                ),
                0..3,
            ),
            proptest::collection::vec(arb_callback_state(), 0..3),
            any::<bool>(),
        )
            .prop_map(
                |(
                    status,
                    task,
                    (execution_timeout, run_timeout),
                    activities,
                    operations,
                    callbacks,
                    versioned,
                )| {
                    let mut state = sample_state(RunKey::new());
                    state.status = status;
                    match task {
                        0 => state.pending_workflow_task = None,
                        1 => {}
                        _ => {
                            let pending = state.pending_workflow_task.as_mut().unwrap();
                            pending.started_event_id = Some(10);
                            pending.started_at = Some(fixed_now());
                        }
                    }
                    state.workflow_execution_timeout =
                        execution_timeout.then_some(Duration::hours(1));
                    state.workflow_run_timeout = run_timeout.then_some(Duration::minutes(30));
                    for index in 0..activities {
                        let entry = activity(&format!("activity-{index}"));
                        state.activities.insert(entry.activity_id.clone(), entry);
                    }
                    for (index, (with_timeout, started, due, cancellation)) in
                        operations.into_iter().enumerate()
                    {
                        let mut operation = nexus_operation(&format!("op-{index}"), with_timeout);
                        operation.started = started;
                        operation.next_attempt_at =
                            (!due).then_some(fixed_now() + Duration::minutes(1));
                        operation.cancellation = cancellation;
                        state
                            .pending_nexus_operations
                            .insert(operation.operation_id.clone(), operation);
                    }
                    state.completion_callbacks = callbacks.into_iter().map(callback).collect();
                    if versioned {
                        pinned(&mut state);
                    }
                    state
                },
            )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: recovery-index, Property 1: The predicate covers every derived entry
        #[test]
        fn property_predicate_covers_every_derived_entry(
            state in arb_state(),
            later in 0i64..120,
        ) {
            let now = fixed_now() + Duration::seconds(later);
            let derives_work = !recovery_entries(&state).is_empty()
                || !reconstructible_nexus_deliveries(&state, now).is_empty();
            prop_assert!(!derives_work || recovery_needed(&state));
        }

        // Feature: recovery-index, Property 3: Phases end in order and resume strictly after the cursor
        #[test]
        fn property_phases_end_in_order_and_resume_after_the_cursor(
            legacy in proptest::collection::btree_set(any::<u128>(), 0..12),
            flagged in proptest::collection::btree_set(any::<u128>(), 0..12),
            limit in 1usize..6,
        ) {
            let legacy: Vec<RunKey> =
                legacy.into_iter().map(|key| RunKey(uuid::Uuid::from_u128(key))).collect();
            let flagged: Vec<RunKey> =
                flagged.into_iter().map(|key| RunKey(uuid::Uuid::from_u128(key))).collect();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let mut read = Vec::new();
            let mut cursor: Option<RecoveryCursor> = None;
            let mut pages = 0;
            loop {
                let (rows, next) = runtime
                    .block_on(read_candidate_page(cursor.as_ref(), limit, |phase, after, limit| {
                        let source = match phase {
                            RecoveryPhase::Legacy => &legacy,
                            RecoveryPhase::Flagged => &flagged,
                        };
                        let rows: Vec<(RunKey, ())> = source
                            .iter()
                            .filter(|key| after.is_none_or(|after| **key > after))
                            .take(limit)
                            .map(|key| (*key, ()))
                            .collect();
                        std::future::ready(Ok(rows))
                    }))
                    .unwrap();
                prop_assert!(rows.len() <= limit);
                read.extend(rows.into_iter().map(|(key, ())| key));
                pages += 1;
                prop_assert!(pages <= legacy.len() + flagged.len() + 2);
                match next {
                    Some(next) => cursor = Some(next),
                    None => break,
                }
            }
            let expected: Vec<RunKey> = legacy.iter().chain(flagged.iter()).copied().collect();
            prop_assert_eq!(read, expected);
        }
    }
}
