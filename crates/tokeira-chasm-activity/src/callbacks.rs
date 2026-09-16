//! Pure callback attachment and delivery bookkeeping inside an activity root.
//! The state graph follows `chasm/lib/callback/statemachine.go @ v1.32.0`.
//! Executors own I/O and backoff calculation; these transitions persist their
//! supplied outcomes and stage the next fenced task, with no runtime dependency.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use tokeira_chasm::{ChasmError, MutableContext, Task, TaskOutcome};
use tokeira_proto::enums::CallbackState;

use crate::{
    state::{ActivityCallback, ActivityState, InternalTarget, NexusTarget, activity_callback},
    tasks::{
        CALLBACK_RETRY_TASK_ID, CallbackRetryTimer, DELIVER_CALLBACK_TASK_ID, DeliverCallback,
    },
};

/// Attachment input; identity and registration time are assigned by the component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallbackSpec {
    /// Delivery destination, already validated by the caller's admission path.
    pub target: CallbackTarget,
    /// One encoded `temporal.api.common.v1.Link` per element.
    pub links: Vec<Vec<u8>>,
}

/// Pure attachment destinations. This crate accepts Internal unconditionally:
/// only the edge can enforce that it came from an executor rather than the wire (D2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackTarget {
    /// HTTP Nexus completion, with URL/header validation performed at the edge.
    Nexus {
        /// Completion URL.
        url: String,
        /// Deterministically ordered HTTP headers.
        header: BTreeMap<String, String>,
    },
    /// In-process return address, authored only by the start executor.
    Internal {
        /// `ComponentRef::encode()` output.
        component_ref: Vec<u8>,
        /// Registered task type to receive the activity outcome.
        task_type_id: u32,
        /// Postcard-encoded `TaskId` held by the destination's outbox.
        task_id: Vec<u8>,
    },
}

/// Result of one callback delivery, with retry policy evaluated by its executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallbackAttemptOutcome {
    /// The destination accepted completion.
    Succeeded,
    /// Retry at the supplied deadline using the workflow plane's shared backoff.
    RetryableFailure {
        /// Encoded `temporal.api.failure.v1.Failure`.
        failure: Vec<u8>,
        /// Absolute deadline computed by the executor from the CHASM clock.
        next_attempt_time_nanos: i64,
    },
    /// Permanently stop delivery.
    NonRetryableFailure {
        /// Encoded `temporal.api.failure.v1.Failure`.
        failure: Vec<u8>,
    },
}

/// Postcard envelope in a retryable `TaskOutcome::Failed`. The failure itself
/// stays an opaque Temporal Failure proto; only the executor supplies the deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryableDeliveryFailure {
    /// Encoded `temporal.api.failure.v1.Failure`.
    pub failure: Vec<u8>,
    /// Next delivery deadline in Unix nanoseconds.
    pub next_attempt_time_nanos: i64,
}

/// Decode the delivery envelope shared with executors. Success has an empty
/// payload; malformed retry envelopes are validation errors, other outcomes unsupported.
pub fn callback_attempt_outcome(
    outcome: &TaskOutcome,
) -> Result<CallbackAttemptOutcome, ChasmError> {
    match outcome {
        TaskOutcome::Completed { payload } if payload.is_empty() => {
            Ok(CallbackAttemptOutcome::Succeeded)
        }
        TaskOutcome::Failed {
            failure,
            retryable: true,
        } => {
            let envelope: RetryableDeliveryFailure =
                postcard::from_bytes(failure).map_err(|error| {
                    ChasmError::Validation(format!("decode callback retry outcome: {error}"))
                })?;
            Ok(CallbackAttemptOutcome::RetryableFailure {
                failure: envelope.failure,
                next_attempt_time_nanos: envelope.next_attempt_time_nanos,
            })
        }
        TaskOutcome::Failed {
            failure,
            retryable: false,
        } => Ok(CallbackAttemptOutcome::NonRetryableFailure {
            failure: failure.clone(),
        }),
        _ => Err(ChasmError::Unsupported(
            "unsupported callback delivery outcome".to_owned(),
        )),
    }
}

/// Encode a retryable delivery result without losing its executor-supplied deadline.
pub fn retryable_delivery_failure(failure: Vec<u8>, next_attempt_time_nanos: i64) -> TaskOutcome {
    TaskOutcome::Failed {
        failure: postcard::to_allocvec(&RetryableDeliveryFailure {
            failure,
            next_attempt_time_nanos,
        })
        .expect("postcard can encode a byte vector and an i64 into an allocated vector"),
        retryable: true,
    }
}

/// Carry a terminal delivery failure as its original Temporal Failure bytes.
pub fn non_retryable_delivery_failure(failure: Vec<u8>) -> TaskOutcome {
    TaskOutcome::Failed {
        failure,
        retryable: false,
    }
}

pub(crate) fn attach(
    state: &mut ActivityState,
    request_id: &str,
    callbacks: &[CallbackSpec],
    max_callbacks: usize,
    now: i64,
) -> Result<(), ChasmError> {
    // Count before upsert, including replacements, exactly as addCompletionCallbacks
    // does (chasm/lib/activity/activity.go:443–473 @ v1.32.0).
    if callbacks.len().saturating_add(state.callbacks.len()) > max_callbacks {
        return Err(ChasmError::FailedPrecondition(format!(
            "cannot attach more than {max_callbacks} callbacks to an activity ({} callbacks already attached)",
            state.callbacks.len()
        )));
    }
    for (index, spec) in callbacks.iter().enumerate() {
        let target = match &spec.target {
            CallbackTarget::Nexus { url, header } => {
                activity_callback::Target::Nexus(NexusTarget {
                    url: url.clone(),
                    header: header.clone(),
                })
            }
            CallbackTarget::Internal {
                component_ref,
                task_type_id,
                task_id,
            } => activity_callback::Target::Internal(InternalTarget {
                component_ref: component_ref.clone(),
                task_type_id: *task_type_id,
                task_id: task_id.clone(),
            }),
        };
        let callback = ActivityCallback {
            id: format!("{request_id}-{index}"),
            registration_time_nanos: now,
            state: CallbackState::Standby as i32,
            links: spec.links.clone(),
            target: Some(target),
            ..Default::default()
        };
        if let Some(existing) = state
            .callbacks
            .iter_mut()
            .find(|existing| existing.id == callback.id)
        {
            *existing = callback;
        } else {
            state.callbacks.push(callback);
        }
    }
    Ok(())
}

pub(crate) fn schedule_standby(
    state: &mut ActivityState,
    ctx: &mut dyn MutableContext,
) -> Result<(), ChasmError> {
    // One terminal hook covers every activity outcome (activity.go:421–426 and
    // callback/component.go:172–183 @ v1.32.0). Already scheduled callbacks are inert.
    for callback in &mut state.callbacks {
        if callback.state() == CallbackState::Standby {
            callback.set_state(CallbackState::Scheduled);
            stage_delivery(callback, ctx)?;
        }
    }
    Ok(())
}

fn stage_delivery(
    callback: &ActivityCallback,
    ctx: &mut dyn MutableContext,
) -> Result<(), ChasmError> {
    let task = DeliverCallback {
        callback_id: callback.id.clone(),
        stamp: callback.attempt,
    };
    ctx.add_task(
        DeliverCallback::KIND,
        DELIVER_CALLBACK_TASK_ID,
        task.encode()?,
        task.fire_at(),
    )
}

fn callback_in_state<'a>(
    state: &'a mut ActivityState,
    id: &str,
    expected: CallbackState,
    event: &str,
) -> Result<&'a mut ActivityCallback, ChasmError> {
    let callback = state
        .callbacks
        .iter_mut()
        .find(|callback| callback.id == id)
        .ok_or_else(|| ChasmError::Internal(format!("callback {id:?} is missing")))?;
    // The durable task's validator fences state and attempt before delivery; an
    // illegal direct event is a protocol error, never another counted attempt.
    if callback.state() != expected {
        return Err(ChasmError::IllegalTransition {
            from: format!("callback:{}", callback.state().as_str_name()),
            event: event.to_owned(),
        });
    }
    Ok(callback)
}

pub(crate) fn record_attempt(
    state: &mut ActivityState,
    id: &str,
    outcome: &CallbackAttemptOutcome,
    ctx: &mut dyn MutableContext,
    now: i64,
) -> Result<(), ChasmError> {
    let callback = callback_in_state(state, id, CallbackState::Scheduled, "CallbackAttempted")?;
    // recordAttempt is shared by all three outcomes (callback/component.go:72–75
    // and callback/statemachine.go:53–124 @ v1.32.0).
    callback.attempt = callback.attempt.checked_add(1).ok_or_else(|| {
        ChasmError::Internal(format!("callback {id:?} attempt counter exhausted"))
    })?;
    callback.last_attempt_complete_time_nanos = now;
    match outcome {
        CallbackAttemptOutcome::Succeeded => {
            callback.set_state(CallbackState::Succeeded);
            callback.last_attempt_failure.clear();
        }
        CallbackAttemptOutcome::NonRetryableFailure { failure } => {
            callback.set_state(CallbackState::Failed);
            callback.last_attempt_failure = failure.clone();
        }
        CallbackAttemptOutcome::RetryableFailure {
            failure,
            next_attempt_time_nanos,
        } => {
            callback.set_state(CallbackState::BackingOff);
            callback.last_attempt_failure = failure.clone();
            callback.next_attempt_time_nanos = *next_attempt_time_nanos;
            let task = CallbackRetryTimer {
                callback_id: id.to_owned(),
                attempt: callback.attempt,
                fire_at_nanos: *next_attempt_time_nanos,
            };
            ctx.add_task(
                CallbackRetryTimer::KIND,
                CALLBACK_RETRY_TASK_ID,
                task.encode()?,
                task.fire_at(),
            )?;
        }
    }
    Ok(())
}

pub(crate) fn retry_due(
    state: &mut ActivityState,
    id: &str,
    ctx: &mut dyn MutableContext,
) -> Result<(), ChasmError> {
    let callback = callback_in_state(state, id, CallbackState::BackingOff, "CallbackRetryDue")?;
    // Preserve the completed-attempt count in the next invocation fence
    // (callback/statemachine.go:36–50 @ v1.32.0).
    callback.set_state(CallbackState::Scheduled);
    callback.next_attempt_time_nanos = 0;
    stage_delivery(callback, ctx)
}

#[cfg(test)]
mod tests;
