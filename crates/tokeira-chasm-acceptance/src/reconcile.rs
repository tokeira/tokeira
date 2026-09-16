//! Pure reconciliation transitions. The parent outbox fences outcome application;
//! a completed operation advances only the generation captured when it was staged.

use prost::Message;
use tokeira_chasm::{
    ChasmError, Context, MutableContext, PureTaskHandler, SideEffectTaskHandler, StartActivityTask,
    Task, TaskOutcome, TaskValidity, task_type_id_for_fqn,
};
use tokeira_proto::{
    common::{Payload, Payloads, RetryPolicy},
    failure::{
        CanceledFailureInfo, Failure, TerminatedFailureInfo, TimeoutFailureInfo,
        failure::FailureInfo,
    },
};

use crate::{
    Operation, OperationOutcome, ReconcileInput, Resource, ResourceState,
    tasks::{RetryTimer, backoff},
};

/// Stage one fresh activity for the latest desired input. The caller must have
/// no active operation; the executor attaches the internal completion address.
pub fn stage_start(
    state: &mut ResourceState,
    ctx: &mut dyn MutableContext,
) -> Result<(), ChasmError> {
    if state.active_operation.is_some() {
        return Err(ChasmError::Internal(
            "cannot stage a second active resource operation".into(),
        ));
    }
    let attempt = u64::from(state.retry_attempt) + 1;
    // A live-id conflict is a permanent start rejection (edge chasm_executors.rs,
    // start_rejection). Give each staging a fresh id; retries never collide with
    // their previous failed activity or require activity business-id reuse policy.
    let activity_id = format!(
        "{}/gen-{}/try-{attempt}",
        ctx.execution_key().business_id,
        state.desired_generation
    );
    let input = Payloads {
        payloads: vec![Payload {
            data: ReconcileInput {
                generation: state.desired_generation,
                digest: state.desired_digest.clone(),
            }
            .encode_to_vec(),
            ..Default::default()
        }],
    }
    .encode_to_vec();
    let task = StartActivityTask {
        activity_id: activity_id.clone(),
        activity_type: "acceptance.reconcile".into(),
        task_queue: state.task_queue.clone(),
        input,
        header: vec![],
        // The component must observe the first worker failure. Activity retries
        // would otherwise hide it from our own durable retry timer.
        retry_policy: RetryPolicy {
            maximum_attempts: 1,
            ..Default::default()
        }
        .encode_to_vec(),
        schedule_to_start_nanos: 0,
        schedule_to_close_nanos: 0,
        start_to_close_nanos: 30_000_000_000,
        heartbeat_nanos: 0,
        version_target: Some(state.target.clone()),
    };
    ctx.add_task(
        StartActivityTask::KIND,
        task_type_id_for_fqn(StartActivityTask::FQN),
        Task::encode(&task)?,
        None,
    )?;
    state.active_operation = Some(Operation {
        generation: state.desired_generation,
        activity_id,
        outcome: OperationOutcome::Pending as i32,
        started_at_nanos: ctx.now_unix_nanos(),
        ..Default::default()
    });
    Ok(())
}

/// Applies a standalone activity's terminal outcome to its owning resource.
#[derive(Debug)]
pub struct ReconcileHandler;

impl SideEffectTaskHandler for ReconcileHandler {
    type Component = Resource;
    type Task = StartActivityTask;

    fn validate(
        &self,
        component: &Resource,
        task: &StartActivityTask,
        _: &dyn Context,
    ) -> TaskValidity {
        if component
            .state()
            .ok()
            .and_then(|state| state.active_operation.as_ref())
            .is_some_and(|operation| operation.activity_id == task.activity_id)
        {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }

    fn on_outcome(
        &self,
        component: &mut Resource,
        _: &StartActivityTask,
        outcome: &TaskOutcome,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        let state = component.state_mut()?;
        let mut operation = state.active_operation.take().ok_or_else(|| {
            ChasmError::Internal("outcome without an active resource operation".into())
        })?;
        operation.finished_at_nanos = ctx.now_unix_nanos();
        if matches!(outcome, TaskOutcome::Completed { .. }) {
            // Updates may have advanced desired while this activity ran. Only
            // the captured generation was reconciled; stage the latest next.
            state.observed_generation = operation.generation;
            operation.outcome = OperationOutcome::Completed as i32;
            state.retry_attempt = 0;
            state.last_failure.clear();
            state.finish(operation);
            if state.desired_generation > state.observed_generation {
                stage_start(state, ctx)?;
            }
        } else {
            operation.outcome = OperationOutcome::Failed as i32;
            operation.failure = outcome_failure(outcome)?;
            state.last_failure.clone_from(&operation.failure);
            state.finish(operation);
            state.retry_attempt = state
                .retry_attempt
                .checked_add(1)
                .ok_or_else(|| ChasmError::Validation("resource retry attempt overflow".into()))?;
            // Retry the latest desired input, even if the failed operation was
            // for an older generation. Later updates invalidate this timer.
            let timer = RetryTimer {
                generation: state.desired_generation,
                attempt: state.retry_attempt,
                fire_at_nanos: ctx
                    .now_unix_nanos()
                    .saturating_add(backoff(state.retry_attempt)),
            };
            ctx.add_task(
                RetryTimer::KIND,
                task_type_id_for_fqn(RetryTimer::FQN),
                timer.encode()?,
                timer.fire_at(),
            )?;
        }
        Ok(())
    }
}

fn outcome_failure(outcome: &TaskOutcome) -> Result<Vec<u8>, ChasmError> {
    let (message, failure_info) = match outcome {
        TaskOutcome::Failed { failure, .. } => return Ok(failure.clone()),
        TaskOutcome::Canceled { details } => (
            "reconcile activity canceled",
            FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: if details.is_empty() {
                    None
                } else {
                    Some(Payloads::decode(details.as_slice()).map_err(|error| {
                        ChasmError::Validation(format!("decode cancellation details: {error}"))
                    })?)
                },
                ..Default::default()
            }),
        ),
        TaskOutcome::TimedOut { timeout_type } => (
            "reconcile activity timed out",
            FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: *timeout_type,
                ..Default::default()
            }),
        ),
        TaskOutcome::Terminated => (
            "reconcile activity terminated",
            FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo::default()),
        ),
        TaskOutcome::Completed { .. } => {
            return Err(ChasmError::Internal(
                "successful outcome has no failure".into(),
            ));
        }
    };
    Ok(Failure {
        message: message.into(),
        failure_info: Some(failure_info),
        ..Default::default()
    }
    .encode_to_vec())
}

/// Re-stages external work only while the timer still names the desired retry.
#[derive(Debug)]
pub struct RetryHandler;

impl PureTaskHandler for RetryHandler {
    type Component = Resource;
    type Task = RetryTimer;

    fn validate(&self, component: &Resource, timer: &RetryTimer, _: &dyn Context) -> TaskValidity {
        if component.state().is_ok_and(|state| {
            timer.generation == state.desired_generation
                && timer.attempt == state.retry_attempt
                && state.active_operation.is_none()
        }) {
            TaskValidity::Valid
        } else {
            TaskValidity::Drop
        }
    }

    fn execute(
        &self,
        component: &mut Resource,
        _: &RetryTimer,
        ctx: &mut dyn MutableContext,
    ) -> Result<(), ChasmError> {
        stage_start(component.state_mut()?, ctx)
    }
}
