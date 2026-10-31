//! Immutable successor inputs prepared before a close enters its execution lane.
//! Original metadata comes from start history; the closing transition supplies
//! the actual outcome. Frozen evaluation and start clocks survive OCC and fallback.

use anyhow::{Context as _, Result, anyhow, ensure};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    HistoryEvent, HistoryEventKind, Link, StartRequest, Transition, WorkflowState,
    WorkflowVersioningInfo, continue_as_new_min_backoff,
};
use tokeira_storage::RunRepository;
use tokeira_types::{Headers, Memo, Payloads, RunKey, SearchAttributes};

use crate::runtime::workflow_task::{
    build_cron_successor_start, build_retry_successor_start, workflow_run_timeout_failure,
};

/// History-owned inputs, never substituted with later mutable state.
/// `SetupNewWorkflowForRetryOrCron` copies these from the started event
/// (`service/history/workflow/retry.go:278-345 @ v1.31.0`).
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(Default))]
pub(crate) struct SuccessorStartedEvent {
    pub input: Payloads,
    pub header: Option<Headers>,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub links: Vec<Link>,
    pub cron_schedule: Option<String>,
    pub last_completion_result: Option<Payloads>,
    pub versioning_info: Option<WorkflowVersioningInfo>,
}

/// Both internal encodings implement the same public started-event contract.
pub(crate) fn started_event_inputs(event: &HistoryEvent) -> Result<SuccessorStartedEvent> {
    match &event.kind {
        HistoryEventKind::WorkflowExecutionStarted {
            input,
            header,
            memo,
            search_attributes,
            links,
            cron_schedule,
            last_completion_result,
            versioning_info,
            ..
        }
        | HistoryEventKind::WorkflowExecutionStartedV2 {
            input,
            header,
            memo,
            search_attributes,
            links,
            cron_schedule,
            last_completion_result,
            versioning_info,
            ..
        } => Ok(SuccessorStartedEvent {
            input: input.clone(),
            header: header.clone(),
            memo: memo.clone(),
            search_attributes: search_attributes.clone(),
            links: links.clone(),
            cron_schedule: cron_schedule.clone(),
            last_completion_result: last_completion_result.clone(),
            versioning_info: versioning_info.clone(),
        }),
        _ => Err(anyhow!(
            "required successor history is not a workflow started event"
        )),
    }
}

/// The repository point read hydrates split history values just like its page read.
/// A missing/corrupt start must fail before the close can commit, never fabricate metadata.
pub(crate) async fn load_started_event_inputs<R: RunRepository>(
    repo: &R,
    run_key: RunKey,
) -> Result<SuccessorStartedEvent> {
    let event = repo
        .read_history_event(run_key, 1)
        .await
        .with_context(|| format!("read successor start history for run {}", run_key.0))?
        .ok_or_else(|| anyhow!("successor start history is missing for run {}", run_key.0))?;
    started_event_inputs(&event)
        .with_context(|| format!("invalid successor start history for run {}", run_key.0))
}

/// A selected delay and evaluation instant are immutable for one lane submission.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SuccessorTiming {
    pub now: OffsetDateTime,
    pub backoff: Duration,
}

impl SuccessorTiming {
    /// Apply the shared minimum after selection, anchored on execution rather than wall-clock start.
    pub(crate) fn minimum_backoff(self, state: &WorkflowState) -> Option<Duration> {
        let execution_time =
            state.started_at + state.workflow_start_delay.unwrap_or(Duration::ZERO);
        continue_as_new_min_backoff(Some(self.backoff), self.now - execution_time)
    }
}

/// Timeout and workflow-task successors deliberately carry different prior results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SuccessorCause {
    WorkflowTask,
    RunTimeout,
}

/// Prepared reads and decisions; boxing keeps payload metadata out of every lane future's stack.
#[derive(Clone, Debug)]
pub(crate) enum SuccessorPlan {
    ContinueAsNew {
        links: Vec<Link>,
    },
    Retry {
        started: Box<SuccessorStartedEvent>,
        timing: SuccessorTiming,
        cause: SuccessorCause,
    },
    Cron {
        started: Box<SuccessorStartedEvent>,
        timing: SuccessorTiming,
        cause: SuccessorCause,
    },
}

/// Build from authoritative close history and immutable prepared inputs.
/// The caller reuses this start for the fenced pair and explicit size fallback.
pub(crate) fn build_successor_start(
    transition: &Transition,
    plan: Option<&SuccessorPlan>,
    successor_now: OffsetDateTime,
) -> Result<Option<StartRequest>> {
    let state = &transition.next_state;
    if transition.history_events.iter().any(|e| {
        matches!(
            e.kind,
            HistoryEventKind::WorkflowExecutionContinuedAsNew { .. }
        )
    }) {
        let Some(SuccessorPlan::ContinueAsNew { links }) = plan else {
            return Err(anyhow!(
                "continue-as-new close is missing its matching runtime plan"
            ));
        };
        return Ok(build_continue_as_new_start(
            state,
            &transition.history_events,
            successor_now,
            links.clone(),
        ));
    }
    let close = transition
        .history_events
        .iter()
        .find_map(|event| match &event.kind {
            HistoryEventKind::WorkflowExecutionFailed {
                new_execution_run_id: Some(id),
                failure,
                ..
            } => Some((
                *id,
                SuccessorCause::WorkflowTask,
                Some(failure.clone()),
                None,
            )),
            HistoryEventKind::WorkflowExecutionCompleted {
                new_execution_run_id: Some(id),
                result,
                ..
            } => Some((
                *id,
                SuccessorCause::WorkflowTask,
                None,
                Some(result.clone()),
            )),
            HistoryEventKind::WorkflowExecutionTimedOut {
                new_execution_run_id: Some(id),
                ..
            } => Some((
                *id,
                SuccessorCause::RunTimeout,
                Some(workflow_run_timeout_failure()),
                None,
            )),
            _ => None,
        });
    let Some((id, actual_cause, failure, result)) = close else {
        return Ok(None);
    };
    let mut start = match plan
        .ok_or_else(|| anyhow!("successor close is missing its runtime plan"))?
    {
        SuccessorPlan::Retry {
            started,
            timing,
            cause,
        } => {
            ensure!(
                *cause == actual_cause && failure.is_some(),
                "retry plan does not match its closing event"
            );
            let policy = state
                .retry_policy
                .as_ref()
                .ok_or_else(|| anyhow!("retry successor has no retry policy"))?;
            // handleRetry supplies no result, while run-timeout retry carries the
            // started result (service/history/api/respondworkflowtaskcompleted/
            // workflow_task_completed_handler.go:1354-1364 and
            // service/history/timer_queue_active_task_executor.go:743-754 @ v1.31.0).
            let last_result = (*cause == SuccessorCause::RunTimeout)
                .then(|| started.last_completion_result.clone())
                .flatten();
            build_retry_successor_start(state, started, policy, id, *timing, failure, last_result)
        }
        SuccessorPlan::Cron {
            started,
            timing,
            cause,
        } => {
            ensure!(
                *cause == actual_cause,
                "cron plan does not match its closing event"
            );
            let schedule = started
                .cron_schedule
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("cron successor has no original schedule"))?;
            // A successful empty result is still this run's result. Only failure/
            // timeout inherits the started result (handleCron @ v1.31.0).
            let last_result = if failure.is_some() {
                started.last_completion_result.clone()
            } else {
                result
            };
            build_cron_successor_start(state, schedule, started, id, *timing, failure, last_result)
        }
        SuccessorPlan::ContinueAsNew { .. } => {
            return Err(anyhow!(
                "successor close has a mismatched continue-as-new plan"
            ));
        }
    };
    // Delay evaluation and successor creation have separate clocks in
    // handleRetry/handleCron (service/history/api/respondworkflowtaskcompleted/
    // workflow_task_completed_handler.go @ v1.31.0). Preserve that separation:
    // reusing the close timestamp would regress our timestamp-derived TaskIds
    // when the new run's event ids restart at one. The lane freezes this clock
    // before its OCC loop, so retries and size fallback retain the same start.
    start.now = successor_now;
    start.request.received_at = successor_now;
    Ok(Some(start))
}

fn build_continue_as_new_start(
    new_state: &WorkflowState,
    history_events: &[HistoryEvent],
    now: OffsetDateTime,
    links: Vec<Link>,
) -> Option<StartRequest> {
    let successor_event = history_events.iter().find_map(|event| match &event.kind {
        HistoryEventKind::WorkflowExecutionContinuedAsNew {
            new_run_id,
            workflow_type,
            task_queue,
            input,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            backoff_start_interval,
            cron_schedule,
            header,
            initiator,
            last_completion_result,
            successor_versioning_info,
            ..
        } => Some((
            *new_run_id,
            workflow_type.clone(),
            task_queue.clone(),
            input.clone(),
            memo.clone(),
            search_attributes.clone(),
            *workflow_execution_timeout,
            *workflow_run_timeout,
            *workflow_task_timeout,
            retry_policy.clone(),
            *backoff_start_interval,
            cron_schedule.clone(),
            header.clone(),
            *initiator,
            last_completion_result.clone(),
            successor_versioning_info.clone(),
        )),
        _ => None,
    });
    if let Some((
        successor_run_id,
        workflow_type,
        task_queue,
        input,
        memo,
        search_attributes,
        workflow_execution_timeout,
        workflow_run_timeout,
        workflow_task_timeout,
        retry_policy,
        backoff_start_interval,
        cron_schedule,
        header,
        initiator,
        successor_last_completion_result,
        successor_versioning_info,
    )) = successor_event
    {
        // Carry the chain's origin forward: the
        // successor inherits the first run's id and
        // start time so execution-level timeouts and
        // lineage queries span the whole
        // continue-as-new chain, not just this hop.
        // `unwrap_or(self)` seeds these on the first
        // run, which has no predecessor to inherit
        // from.
        let first_execution_run_id =
            Some(new_state.first_execution_run_id.unwrap_or(new_state.run_id));
        let first_run_started_at = Some(
            new_state
                .first_run_started_at
                .unwrap_or(new_state.started_at),
        );
        let successor_run_key = RunKey::derive(
            new_state.namespace_id,
            &new_state.workflow_id,
            successor_run_id,
        );
        // Root identity only propagates within a
        // child lineage; a top-level run is its own
        // root, so the successor carries no root ref.
        let (root_workflow_id, root_run_id) = if new_state.parent_run_key.is_some() {
            (new_state.root_workflow_id.clone(), new_state.root_run_id)
        } else {
            (None, None)
        };
        let start_request = StartRequest {
            run_key: successor_run_key,
            advice_policy: crate::runtime::continue_as_new_advice_policy(),
            namespace_id: new_state.namespace_id,
            workflow_id: new_state.workflow_id.clone(),
            run_id: successor_run_id,
            workflow_type,
            // Empty task_queue means "reuse the predecessor's queue"
            task_queue: if task_queue.0.is_empty() {
                new_state.task_queue.clone()
            } else {
                task_queue
            },
            deployment: new_state.deployment.clone(),
            build_id: new_state.build_id.clone(),
            // Runtime already resolved whether an
            // override is compatible with the
            // successor queue and committed it in
            // `successor_versioning_info`.
            versioning_override: None,
            workflow_start_delay: backoff_start_interval,
            // Temporal carries completion callbacks
            // and priority into continue-as-new start
            // requests so run-chain completion and
            // scheduling intent survive the hop
            // (`service/history/workflow/mutable_state_impl.go:2480,2573 @ v1.31.0`).
            completion_callbacks: new_state.completion_callbacks.clone(),
            user_metadata: new_state.user_metadata.clone(),
            links,
            on_conflict_options: None,
            priority: new_state.priority.clone(),
            input,
            header,
            memo,
            search_attributes,
            workflow_execution_timeout,
            workflow_run_timeout,
            workflow_task_timeout,
            retry_policy,
            conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
            reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
            // The successor's Initiator is carried from
            // its close event: WORKFLOW for an explicit
            // continue-as-new, CRON_SCHEDULE for a cron
            // restart.
            initiator: Some(initiator),
            attempt: 1,
            continued_execution_run_id: Some(new_state.run_id),
            first_execution_run_id,
            // A child that retries / continues-as-new /
            // cron-restarts is still a child of the same
            // parent: propagate the parent linkage so the
            // successor's own WorkflowExecutionStarted event
            // authors ParentWorkflowNamespace/Execution and
            // the parent is notified only once the whole
            // chain ends (NewExecutionRunId==""). A top-level
            // run carries None here and stays parentless.
            parent_run_key: new_state.parent_run_key,
            parent_workflow_id: new_state.parent_workflow_id.clone(),
            parent_run_id: new_state.parent_run_id,
            parent_namespace_id: new_state.parent_namespace_id,
            parent_namespace_name: new_state.parent_namespace_name.clone(),
            parent_initiated_event_id: new_state.parent_initiated_event_id,
            root_workflow_id,
            root_run_id,
            original_execution_run_id: Some(
                new_state
                    .original_execution_run_id
                    .unwrap_or(new_state.run_id),
            ),
            continued_failure: new_state.close_failure.clone(),
            // Cron carries the last SUCCESSFUL result
            // forward even across a failed run; the
            // authoritative value is on the CaN event
            // (this run's result on complete, the
            // carried-forward result on failure), not
            // `close_result` (None on failure).
            last_completion_result: successor_last_completion_result,
            first_run_started_at,
            request: tokeira_types::RequestContext {
                // Deterministic request id keyed on
                // (predecessor, successor) so a
                // replayed continue-as-new dedupes
                // to the same successor start instead
                // of forking a duplicate run.
                request_id: tokeira_types::RequestId(format!(
                    "continue-as-new:{}:{}",
                    new_state.run_id.0, successor_run_id.0
                )),
                caller_identity: None,
                principal: None,
                received_at: now,
            },
            now,
            client_cron_schedule: None,
            cron_schedule,
            eager_execution_accepted: false,
            reserved_poller_identity: None,
            inherited_versioning_info: successor_versioning_info,
        };
        Some(start_request)
    } else {
        None
    }
}

#[cfg(test)]
#[path = "successor_legacy_tests.rs"]
pub(crate) mod legacy_tests;
