//! Frozen pre-pair continue-as-new builder, used only as a preservation oracle.
//! Keep this independent of the runtime builder so moving field reads to the
//! closing transition cannot silently change inheritance.

use time::OffsetDateTime;
use tokeira_kernel::{HistoryEvent, HistoryEventKind, StartRequest, WorkflowState};
use tokeira_types::RunKey;

pub(crate) fn continue_as_new_start(
    new_state: &WorkflowState,
    history_events: &[HistoryEvent],
    now: OffsetDateTime,
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
            links: Vec::new(),
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
