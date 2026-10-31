//! Atomic reset coordinator: capture and construct all participants, stage their
//! immutable manifest, then publish under one original home acquisition. No lane
//! is awaited while the exclusive publication barrier is held.

use std::collections::BTreeSet;

use anyhow::ensure;
use tokeira_kernel::{Kernel, ReplayContext, ResetFork, ResetRequest};
use tokeira_proto::{
    conversions::common::{failure_to_payload, payloads_from_domain},
    public::temporal::api::failure::v1 as failure_proto,
};
use tokeira_storage::{
    AttributedHistoryEvent, PreparationToken, PublicationResult, RunPreparation,
};

use super::*;
use crate::{lane::DispatchPublisher, outbound::driver::Attempt};

impl<R: RunRepository + 'static> TokeiraRuntime<R> {
    /// Return only after the complete reset branch and every affected current/base
    /// mutation are durably published. A retry deduplicates against the current
    /// execution, matching resetworkflow/api.go @ v1.31.0.
    pub async fn reset_workflow(
        &self,
        execution: ExecutionRef,
        mut request: ResetRequest,
    ) -> Result<ResetWorkflowResult> {
        self.ready().await?;
        let base_key = self
            .repo
            .resolve_execution(&execution)
            .await?
            .ok_or(crate::KernelRejected(tokeira_kernel::Reject::MissingRun))?;
        let base = self.reset_state(base_key).await?;
        let home = self.execution_home(&base);
        let attempt = Attempt::capture(&self.shard_owner, home)
            .ok_or_else(|| NotShardOwner::local(home, ShardEpoch::ZERO))?;
        if self.runtime_drain.is_draining() {
            return Err(NotShardOwner::local(home, attempt.acquisition.epoch).into());
        }
        let current_key = self
            .repo
            .find_latest_run(execution.namespace_id, &execution.workflow_id)
            .await?;
        let current = match current_key {
            Some(key) if key == base_key => Some(base.clone()),
            Some(key) => Some(self.reset_state(key).await?),
            None => None,
        };
        if let Some(current) = &current
            && current
                .outbound
                .as_ref()
                .and_then(|metadata| metadata.reset_request_id.as_ref())
                == Some(&request.request.request_id)
        {
            return Ok(reset_result(current));
        }
        request.expected_current_run_key = current_key;
        // RunKey's canonical digest keeps arbitrary request strings within the
        // manifest key bound; current execution scopes the retry identity.
        let request_digest = RunKey::derive(
            execution.namespace_id,
            &tokeira_types::WorkflowId(request.request.request_id.0.clone()),
            RunId(uuid::Uuid::nil()),
        );
        let request_key = format!(
            "reset:{:?}:{}",
            current_key.map(|key| key.0),
            request_digest.0
        );
        if let Some(previous) = self.repo.find_run_preparation(&request_key).await? {
            request.new_run_id = previous.run_id;
            request.now = previous.started_at;
            // Interrupted pages may no longer be reconstructible from a base
            // that advanced. Retire the exact generation before rebuilding;
            // an old uploader/purger can never touch this new generation.
            if !self
                .repo
                .abandon_prepared_run(previous.clone(), attempt.fence.clone())
                .await?
            {
                if let LoadedRun::Existing(published) =
                    self.repo.load_run(previous.target_run).await?
                    && published
                        .outbound
                        .as_ref()
                        .and_then(|metadata| metadata.reset_request_id.as_ref())
                        == Some(&request.request.request_id)
                {
                    return Ok(reset_result(&published));
                }
                return Err(crate::ResetPublicationConflict.into());
            }
        }
        let base_history = self.reset_history(&base).await?;
        let fork = request.fork_event_id;
        let prefix_len = base_history
            .iter()
            .position(|event| event.event.event_id == fork)
            .ok_or_else(|| {
                crate::KernelRejected(tokeira_kernel::Reject::ResetConstraintViolation {
                    reason: format!("fork event {fork} is outside committed history"),
                })
            })?;
        let prefix_principals: SmallVec<[_; 8]> = base_history[..prefix_len]
            .iter()
            .map(|event| event.principal.clone())
            .collect();
        let prefix: Vec<_> = base_history[..prefix_len]
            .iter()
            .map(|event| event.event.clone())
            .collect();
        let successor_key =
            RunKey::derive(base.namespace_id, &base.workflow_id, request.new_run_id);
        let mut state = BasicKernel
            .replay_history_prefix(
                ReplayContext {
                    run_key: successor_key,
                    namespace_id: base.namespace_id,
                    workflow_id: base.workflow_id.clone(),
                    run_id: request.new_run_id,
                    deployment: base.deployment.clone(),
                    build_id: base.build_id.clone(),
                    parent_run_key: base.parent_run_key,
                    parent_workflow_id: base.parent_workflow_id.clone(),
                    first_run_started_at: base.first_run_started_at,
                },
                &prefix,
            )
            .map_err(crate::KernelRejected)?;
        state.original_execution_run_id = base.original_execution_run_id.or(Some(base.run_id));
        let mut participants = Vec::new();
        let mut base_mutation =
            reset_participant(base.clone(), &request, current_key == Some(base_key), true)?;
        base_mutation.next_state.reset_run_id = Some(request.new_run_id);
        participants.push(base_mutation);
        if let Some(current) = &current
            && current.run_key != base_key
        {
            participants.push(reset_participant(current.clone(), &request, true, false)?);
        }
        let mut reapply = Vec::new();
        let mut history = base_history;
        let mut seen = BTreeSet::new();
        let mut run = base.clone();
        let mut after = fork;
        loop {
            ensure!(
                seen.insert(run.run_key) && seen.len() <= 100,
                "reset lineage is cyclic or exceeds 100 runs"
            );
            if let Some(mutation) = participants
                .iter()
                .find(|part| part.next_state.run_key == run.run_key)
            {
                history.extend(
                    mutation
                        .history_events
                        .iter()
                        .cloned()
                        .zip(mutation.event_principals.iter().cloned())
                        .map(|(event, principal)| AttributedHistoryEvent { event, principal }),
                );
            }
            for attributed in &history {
                let kinds = crate::lane::collect_reset_reapply(
                    std::slice::from_ref(&attributed.event),
                    after,
                    request.reapply_exclude_signal,
                    request.reapply_exclude_update,
                );
                reapply.extend(
                    kinds
                        .into_iter()
                        .map(|kind| (kind, attributed.principal.clone())),
                );
            }
            let next = history.iter().find_map(|entry| match entry.event.kind {
                HistoryEventKind::WorkflowExecutionContinuedAsNew { new_run_id, .. } => {
                    Some(new_run_id)
                }
                _ => None,
            });
            let Some(next) = next else {
                break;
            };
            let key = RunKey::derive(base.namespace_id, &base.workflow_id, next);
            run = if current.as_ref().is_some_and(|state| state.run_key == key) {
                current.as_ref().expect("checked current run").clone()
            } else {
                self.reset_state(key).await?
            };
            history = self.reset_history(&run).await?;
            after = 0;
        }
        let activity_failures = state
            .activities
            .values()
            .filter(|activity| activity.started_event_id.is_some())
            .map(|activity| {
                (
                    activity.activity_id.clone(),
                    reset_failure(&request.reason, activity.heartbeat_details.as_ref()),
                )
            })
            .collect();
        let mut successor = BasicKernel
            .prepare_reset_fork(
                state,
                ResetFork {
                    base_run_id: base.run_id,
                    request: request.clone(),
                    reapply,
                    workflow_task_failure: reset_failure(&request.reason, None),
                    activity_failures,
                    history_size_bytes: i64::try_from(
                        tokeira_storage::codec::encode_history_events(&prefix)?.len(),
                    )
                    .unwrap_or(i64::MAX),
                    advice_policy: continue_as_new_advice_policy(),
                },
            )
            .map_err(crate::KernelRejected)?;
        let mut full_history: SmallVec<[_; 8]> = prefix.into_iter().collect();
        full_history.extend(successor.history_events);
        successor.history_events = full_history;
        let mut principals = prefix_principals;
        principals.extend(successor.event_principals);
        successor.event_principals = principals;
        self.outbound_publisher
            .prepare_reset_child_releases(&mut successor.next_state)
            .await?;
        successor.expected_seq = TransitionSeq::ZERO;
        let preparation = RunPreparation {
            request_key,
            expected_current: current_key,
            participants,
            new_run: Box::new(successor),
            successor_intent: None,
        };
        attempt.validate(&self.shard_owner)?;
        let token = self
            .repo
            .prepare_run_publication(Box::new(preparation), attempt.fence.clone())
            .await?;
        self.publish_reset(token, attempt).await
    }

    async fn reset_state(&self, key: RunKey) -> Result<WorkflowState> {
        match self.repo.load_run(key).await? {
            LoadedRun::Existing(state) => Ok(state),
            LoadedRun::Absent => {
                Err(crate::KernelRejected(tokeira_kernel::Reject::MissingRun).into())
            }
        }
    }

    async fn reset_history(&self, state: &WorkflowState) -> Result<Vec<AttributedHistoryEvent>> {
        let mut result = Vec::new();
        let mut cursor = 0;
        while cursor < state.last_event_id {
            let before = cursor;
            let page = self
                .repo
                .read_attributed_history(state.run_key, cursor, 1024)
                .await?;
            ensure!(
                !page.is_empty(),
                "reset history ended before captured state"
            );
            for entry in page
                .into_iter()
                .take_while(|entry| entry.event.event_id <= state.last_event_id)
            {
                ensure!(
                    entry.event.event_id == cursor + 1,
                    "reset history is not contiguous"
                );
                cursor = entry.event.event_id;
                result.push(entry);
            }
            ensure!(cursor > before, "reset history omitted the captured prefix");
        }
        Ok(result)
    }

    async fn publish_reset(
        &self,
        token: PreparationToken,
        attempt: Attempt,
    ) -> Result<ResetWorkflowResult> {
        let exclusive =
            crate::serving_gate::quiesce(&self.shard_owner, &attempt.acquisition).await?;
        let repo = self.repo.clone();
        let owner = self.shard_owner.clone();
        let publisher = self.outbound_publisher.clone();
        let workflow = self
            .workflow_timeout_tracking
            .for_acquisition(attempt.acquisition.clone());
        let wft = self
            .wft_timeout_tracking
            .for_acquisition(attempt.acquisition.clone());
        let updates = self.update_registry.clone();
        self.runtime_shutdown
            .spawn(async move {
                let exclusive = exclusive;
                attempt.validate(&owner)?;
                owner
                    .write()
                    .expect("shard owner lock poisoned")
                    .invalidate_published_states();
                let result = repo
                    .publish_prepared_run(token.clone(), attempt.fence.clone())
                    .await;
                owner
                    .write()
                    .expect("shard owner lock poisoned")
                    .invalidate_published_states();
                let (participants, state) = match result? {
                    PublicationResult::Applied {
                        participants,
                        new_run,
                    } => (participants, new_run),
                    PublicationResult::AlreadyPublished { new_run } => (Vec::new(), new_run),
                    PublicationResult::Conflict { .. } => {
                        drop(exclusive);
                        repo.abandon_prepared_run(token, attempt.fence).await?;
                        return Err(crate::ResetPublicationConflict.into());
                    }
                };
                attempt.validate(&owner)?;
                for participant in participants {
                    if !participant.is_open() {
                        workflow.remove(participant.run_key);
                        wft.remove(participant.run_key);
                        updates.drain_for_run(participant.run_key, false);
                    }
                    publisher.track_published_outbound(&participant, &attempt);
                }
                if let Some(entry) = tokeira_storage::recovery_entries(&state).workflow_timeout {
                    workflow.insert(crate::timeout::WorkflowTimeoutEntry {
                        run_key: entry.run_key,
                        shard_id: token.home,
                        workflow_execution_timeout: entry.workflow_execution_timeout,
                        workflow_run_timeout: entry.workflow_run_timeout,
                        started_at: entry.started_at,
                        workflow_start_delay: entry.workflow_start_delay,
                        first_run_started_at: entry.first_run_started_at,
                        has_retry_policy: entry.has_retry_policy,
                    });
                }
                crate::lane::track_committed_run(&state, &[], token.home, &workflow, &wft);
                publisher.track_published_outbound(&state, &attempt);
                drop(exclusive);
                repo.complete_run_preparation(token.clone(), attempt.fence)
                    .await?;
                publisher.publish(state.run_key, token.home, &[]).await?;
                Ok(reset_result(&state))
            })
            .await?
    }
}

fn reset_result(state: &WorkflowState) -> ResetWorkflowResult {
    ResetWorkflowResult {
        successor_run_key: state.run_key,
        successor_run_id: state.run_id,
    }
}

fn reset_participant(
    state: WorkflowState,
    request: &ResetRequest,
    terminate: bool,
    is_base: bool,
) -> Result<Transition> {
    if terminate && state.is_open() {
        // Temporal marks the terminating current run WorkflowWasReset and
        // suppresses close callbacks only when it also has a reset redirect
        // (mutable_state_impl.go:processCloseCallbacks @ v1.31.0). The base's
        // redirect is installed by this same publication; an unrelated current
        // without a redirect still owns its terminal callbacks.
        let callbacks = (is_base
            || state
                .reset_run_id
                .is_some_and(|redirect| redirect != state.run_id))
        .then(|| state.completion_callbacks.clone());
        let mut context = request.request.clone();
        context.request_id = RequestId(format!("reset-terminate:{}", request.new_run_id.0));
        let mut transition = BasicKernel
            .apply(
                LoadedRun::Existing(state),
                Command::Terminate(TerminateRequest {
                    reason: request.reason.clone(),
                    details: None,
                    identity: "history-resetter".into(),
                    links: Vec::new(),
                    request: context,
                    now: request.now,
                }),
            )
            .map_err(crate::KernelRejected)?;
        // Child close actions are suppressed for reset; the final publication
        // derives obligations from history and sees the resetter identity.
        transition.dispatch_ops.clear();
        if let Some(callbacks) = callbacks {
            transition.next_state.completion_callbacks = callbacks;
        }
        Ok(transition)
    } else {
        let expected_seq = state.transition_seq;
        let mut next_state = state;
        next_state.transition_seq = expected_seq.next();
        Ok(Transition {
            expected_seq,
            next_state,
            history_events: SmallVec::new(),
            event_principals: SmallVec::new(),
            request_dedupe_ops: SmallVec::new(),
            activity_ops: SmallVec::new(),
            timer_ops: SmallVec::new(),
            dispatch_ops: SmallVec::new(),
            events_numbered_at_close: 0,
            growth_limits: None,
        })
    }
}

fn reset_failure(reason: &str, heartbeat: Option<&Payloads>) -> Payload {
    failure_to_payload(&failure_proto::Failure {
        message: reason.into(),
        failure_info: Some(
            failure_proto::failure::FailureInfo::ResetWorkflowFailureInfo(
                failure_proto::ResetWorkflowFailureInfo {
                    last_heartbeat_details: heartbeat.map(payloads_from_domain),
                },
            ),
        ),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_kernel::{CallbackSpec, CallbackState, CallbackTrigger, CompletionCallback};

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: outbound-dispatch-recovery, Property 9: A reset redirect preserves standby callbacks.
        #[test]
        fn reset_termination_preserves_redirected_callbacks(is_base in any::<bool>(), redirect in 0u8..3, count in 1usize..4) {
            let mut start = super::super::tests::sample_start_request(None, None);
            start.completion_callbacks = (0..count).map(|index| CompletionCallback {
                spec: CallbackSpec::Nexus { url: format!("https://callback.example/{index}"), header: Default::default() },
                links: Vec::new(), trigger: CallbackTrigger::WorkflowClosed,
                registration_time: Some(start.now), state: CallbackState::Standby, attempt: 0,
                last_attempt_failure: None, last_attempt_complete_time: None, next_attempt_at: None,
            }).collect();
            let mut state = BasicKernel.apply(LoadedRun::Absent, Command::Start(start.clone())).unwrap().next_state;
            state.reset_run_id = match redirect { 1 => Some(state.run_id), 2 => Some(RunId::new()), _ => None };
            let reset = ResetRequest {
                fork_event_id: 3, new_run_id: RunId::new(), reapply_exclude_signal: false,
                reapply_exclude_update: false, post_reset_versioning_overrides: Vec::new(),
                expected_current_run_key: Some(state.run_key), reason: "reset".into(),
                request: RequestContext::unattributed(start.now), now: start.now,
            };
            let transition = reset_participant(state, &reset, true, is_base).unwrap();
            prop_assert!(!transition.next_state.is_open());
            prop_assert_eq!(transition.next_state.completion_callbacks.len(), count);
            for (mut actual, expected) in transition.next_state.completion_callbacks.into_iter().zip(start.completion_callbacks) {
                prop_assert_eq!(actual.state, if is_base || redirect == 2 { CallbackState::Standby } else { CallbackState::Scheduled });
                actual.state = CallbackState::Standby;
                prop_assert_eq!(actual, expected);
            }
        }
    }
}
