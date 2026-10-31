//! Pure construction of the complete reset branch. The runtime supplies the
//! replayed prefix, attributed reapply events, clocks, identities and failures;
//! this module orders the new events before storage publishes any participant.

use super::*;

/// Frozen inputs for a reset fork, verified against the captured base history.
/// No field is recomputed during storage retries. Failure payloads are built by
/// the compatibility runtime so the kernel remains independent of protobuf.
#[derive(Clone, Debug)]
pub struct ResetFork {
    /// Base execution whose history prefix was replayed.
    pub base_run_id: RunId,
    /// Accepted reset identity, options and clock.
    pub request: ResetRequest,
    /// Eligible events in lineage order, retaining original attribution.
    pub reapply: Vec<(HistoryEventKind, Option<EventPrincipal>)>,
    /// ResetWorkflow failure used for the fork task.
    pub workflow_task_failure: tokeira_types::Payload,
    /// ResetWorkflow failures with the corresponding last heartbeat details.
    /// Must contain exactly the started activities in the replayed state.
    pub activity_failures: BTreeMap<String, tokeira_types::Payload>,
    /// Encoded size of the copied prefix, for synthesized-start advice.
    pub history_size_bytes: i64,
    /// Explicit deployment policy used for synthesized-start advice.
    pub advice_policy: crate::command::ContinueAsNewAdvicePolicy,
}

impl ResetFork {
    pub(super) fn fail_inflight_activities(
        &self,
        builder: &mut TransitionBuilder,
    ) -> Result<(), Reject> {
        // Temporal fails started activities and refreshes both schedule anchors
        // for unstarted ones (ndc/workflow_resetter.go @ v1.31.0). Iterating the
        // ordered state map makes construction deterministic across retries.
        let activities = builder
            .state
            .activities
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for mut activity in activities {
            if let Some(started_event_id) = activity.started_event_id {
                let failure = self
                    .activity_failures
                    .get(&activity.activity_id)
                    .ok_or_else(|| Reject::ResetConstraintViolation {
                        reason: format!(
                            "missing reset failure for started activity {}",
                            activity.activity_id
                        ),
                    })?;
                builder.emit(HistoryEventKind::ActivityTaskFailed {
                    activity_id: activity.activity_id.clone(),
                    scheduled_event_id: activity.schedule_event_id,
                    started_event_id,
                    identity: activity.started_identity,
                    retry_state: RetryState::NonRetryableFailure,
                    failure: failure.clone(),
                });
                builder.state.activities.remove(&activity.activity_id);
                builder.activity_ops.push(ActivityOp::Delete {
                    activity_id: activity.activity_id,
                });
            } else {
                activity.scheduled_at = self.request.now;
                activity.current_attempt_scheduled_at = Some(self.request.now);
                builder
                    .state
                    .activities
                    .insert(activity.activity_id.clone(), activity.clone());
                builder.activity_ops.push(ActivityOp::Upsert(activity));
            }
        }
        Ok(())
    }
}

impl BasicKernel {
    /// Build a reset suffix in Temporal order: fork failure, activity failures,
    /// eligible reapply, post-reset options, then the fresh workflow task.
    /// The returned transition must be combined with the copied prefix and
    /// published atomically with base/current mutations; it is not a lane commit.
    pub fn prepare_reset_fork(
        &self,
        mut state: WorkflowState,
        fork: ResetFork,
    ) -> Result<Transition, Reject> {
        let pending = state.pending_workflow_task.clone().ok_or_else(|| {
            Reject::ResetConstraintViolation {
                reason: "reset prefix must end with a scheduled or started workflow task".into(),
            }
        })?;
        state.started_at = fork.request.now;
        state.first_run_started_at = Some(fork.request.now);
        state
            .outbound
            .get_or_insert_with(Default::default)
            .reset_request_id = Some(fork.request.request.request_id.clone());
        state.reset_run_id = None;
        state.sticky = None;
        let request = WorkflowTaskFailedRequest {
            logical_seq: pending.logical_seq,
            started_event_id: pending.started_event_id.unwrap_or(0),
            failure_cause: WorkflowTaskFailedCause::ResetWorkflow,
            failure_details: Some(fork.workflow_task_failure.clone()),
            worker_identity: WorkerIdentity("history-service".into()),
            request: fork.request.request.clone(),
            now: fork.request.now,
            reset_reapply: fork
                .reapply
                .iter()
                .map(|(event, _)| event.clone())
                .collect(),
            history_size_bytes: fork.history_size_bytes,
            advice_policy: fork.advice_policy,
            terminate_reason: None,
        };
        let mut transition = self.apply_workflow_task_failed_for_reset(
            LoadedRun::Existing(state),
            request,
            Some(&fork),
        )?;
        for (index, versioning_override) in fork
            .request
            .post_reset_versioning_overrides
            .iter()
            .cloned()
            .enumerate()
        {
            let mut request = fork.request.request.clone();
            request.request_id = tokeira_types::RequestId(format!(
                "post-reset-options:{}:{index}",
                request.request_id.0
            ));
            request.caller_identity = None;
            let options = self.apply_update_execution_options(
                LoadedRun::Existing(transition.next_state.clone()),
                UpdateExecutionOptionsRequest {
                    versioning_override,
                    completion_callbacks: FieldChange::Unchanged,
                    priority: FieldChange::Unchanged,
                    attached_completion_callbacks: Vec::new(),
                    attached_links: Vec::new(),
                    attached_request_id: None,
                    request,
                    now: fork.request.now,
                    completion_callback_limit: None,
                },
            )?;
            append_transition(&mut transition, options);
        }
        let mut final_task = TransitionBuilder::new(
            transition.next_state.clone(),
            fork.request.now,
            fork.request.request.principal,
        );
        final_task.state.workflow_task_attempt = 1;
        final_task.schedule_workflow_task();
        append_transition(&mut transition, final_task.finish()?);
        transition.next_state.transition_seq = transition.expected_seq.next();
        Ok(transition)
    }
}

fn append_transition(target: &mut Transition, next: Transition) {
    target.next_state = next.next_state;
    target.history_events.extend(next.history_events);
    target.event_principals.extend(next.event_principals);
    target.request_dedupe_ops.extend(next.request_dedupe_ops);
    target.activity_ops.extend(next.activity_ops);
    target.timer_ops.extend(next.timer_ops);
    target.dispatch_ops.extend(next.dispatch_ops);
    target.events_numbered_at_close = next.events_numbered_at_close;
}
