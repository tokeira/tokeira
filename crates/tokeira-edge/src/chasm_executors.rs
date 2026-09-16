//! Post-commit activity effects: matching dispatch, staged starts, and completion
//! delivery. Executors hold weak engine handles to avoid engine–sink ownership
//! cycles; durable outboxes remain the authority across retries and restarts.

#[cfg(test)]
mod tests;

use std::{
    hash::{DefaultHasher, Hash, Hasher},
    sync::{Arc, Weak},
};

use async_trait::async_trait;
use prost::Message as _;
use tokeira_chasm::{
    BusinessIdPolicy, ChasmError, ComponentRef, ExecutionKey, ScheduledTask, StartActivityTask,
    Task, TaskId, TaskOutcome, VersionedTransition, task_type_id_for_fqn,
};
use tokeira_chasm_activity::{
    ActivityCallback, ActivityConfig, ActivityState, ActivityStatus, CallbackSpec, CallbackTarget,
    DELIVER_CALLBACK_TASK_ID, DISPATCH_TASK_ID, DeliverCallback, DispatchTask, activity_callback,
    callbacks::{decode_task_id, encode_task_id},
    non_retryable_delivery_failure, retryable_delivery_failure,
    statemachine::terminal_outcome,
};
use tokeira_proto::{
    conversions::common::{failure_to_payload, payload_to_domain},
    enums::CallbackState,
    failure::{ApplicationFailureInfo, Failure, failure::FailureInfo},
};
use tokeira_runtime::{
    CompletionDeliveryOutcome, NexusCompletion, NexusCompletionClient, NexusCompletionFailureBody,
    NexusCompletionRuntimeConfig,
    chasm::{ChasmEngine, Engine, OutcomeApplied, SideEffectExecutor},
    invoke_nexus_callback, nexus_completion_backoff,
};

use crate::{
    chasm_activity::{
        ActivityDispatchQueue, DispatchEntry, StartActivity, defaulted_retry_policy,
        retry_policy_fields, start_activity,
    },
    errors::EdgeError,
    namespace_cache::NamespaceCache,
};

/// Advertises committed activity attempts to matching. The root supplies the
/// version target because adding it to the persisted postcard dispatch payload
/// would break decoding of outboxes created before this release.
#[derive(Debug)]
pub struct ActivityDispatchExecutor {
    engine: Weak<ChasmEngine>,
    queue: Arc<ActivityDispatchQueue>,
}

/// Delivers task type 6 through either Nexus or an Internal return address. Both
/// arms record their attempt through the held activity task's generic outcome fence.
pub struct DeliverCallbackExecutor {
    engine: Weak<ChasmEngine>,
    client: Arc<dyn NexusCompletionClient>,
    config: NexusCompletionRuntimeConfig,
    namespaces: Arc<dyn NamespaceCache>,
}

impl std::fmt::Debug for DeliverCallbackExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliverCallbackExecutor")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl DeliverCallbackExecutor {
    /// Use the workflow plane's client, retry policy and edge namespace cache.
    pub fn new(
        engine: Weak<ChasmEngine>,
        client: Arc<dyn NexusCompletionClient>,
        config: NexusCompletionRuntimeConfig,
        namespaces: Arc<dyn NamespaceCache>,
    ) -> Self {
        Self {
            engine,
            client,
            config,
            namespaces,
        }
    }

    fn retry_failure(
        &self,
        engine: &ChasmEngine,
        key: &ExecutionKey,
        callback: &ActivityCallback,
        detail: &str,
    ) -> TaskOutcome {
        let attempt = u32::try_from(callback.attempt)
            .unwrap_or_default()
            .saturating_add(1);
        if self.config.retry_max_attempts != 0 && attempt >= self.config.retry_max_attempts {
            non_retryable_delivery_failure(application_failure(detail, true))
        } else {
            let delay =
                nexus_completion_backoff(&self.config, attempt, callback_seed(key, &callback.id));
            let deadline = i128::from(engine.now())
                .saturating_add(delay.whole_nanoseconds())
                .clamp(i128::from(i64::MIN), i128::from(i64::MAX))
                as i64;
            retryable_delivery_failure(application_failure(detail, false), deadline)
        }
    }

    async fn nexus_delivery(
        &self,
        key: &ExecutionKey,
        state: &ActivityState,
        callback: &ActivityCallback,
        target: &tokeira_chasm_activity::NexusTarget,
    ) -> anyhow::Result<CompletionDeliveryOutcome> {
        let mut links = Vec::new();
        match self.namespaces.get_by_id(&key.namespace_id).await? {
            Some(namespace) if !namespace.deleted => links.push(tokeira_kernel::Link::Activity {
                namespace: namespace.name,
                activity_id: key.business_id.clone(),
                run_id: key.run_id.clone(),
            }),
            // Links are best-effort headers, not completion-resolution authority.
            // A missing/tombstoned namespace must not prevent delivering the result.
            _ => tracing::debug!(
                ?key,
                "activity completion omits unavailable namespace back-link"
            ),
        }
        for encoded in &callback.links {
            let link = tokeira_proto::common::Link::decode(encoded.as_slice())?;
            links.push(crate::translate::to_internal::link_to_kernel(
                crate::grpc::translate::link_to_edge(&link)?,
            ));
        }
        invoke_nexus_callback(
            self.client.as_ref(),
            &self.config,
            &target.url,
            &target.header.clone().into_iter().collect(),
            activity_completion(state)?,
            &links,
        )
        .await
    }

    async fn internal_delivery(
        &self,
        engine: &ChasmEngine,
        target: &tokeira_chasm_activity::InternalTarget,
        state: &ActivityState,
    ) -> Result<OutcomeApplied, ChasmError> {
        let reference = ComponentRef::decode(&target.component_ref)?;
        let task = decode_task_id(&target.task_id)?;
        let outcome = terminal_outcome(state).ok_or_else(|| {
            ChasmError::Validation("activity callback has no terminal outcome".into())
        })?;
        let result = engine
            .apply_side_effect_outcome(&reference.execution_key, target.task_type_id, task, outcome)
            .await?;
        if matches!(result, OutcomeApplied::ExecutionMissing) {
            return Err(ChasmError::Validation(format!(
                "callback target execution is missing: {:?}",
                reference.execution_key
            )));
        }
        Ok(result)
    }

    fn internal_outcome(
        &self,
        engine: &ChasmEngine,
        key: &ExecutionKey,
        callback: &ActivityCallback,
        result: Result<OutcomeApplied, ChasmError>,
    ) -> TaskOutcome {
        match result {
            // NotHeld includes the crash window after the parent commit and
            // before this activity's attempt commit: replay completes locally.
            Ok(OutcomeApplied::Applied(_) | OutcomeApplied::NotHeld) => TaskOutcome::Completed {
                payload: Vec::new(),
            },
            Err(error @ ChasmError::RetriesExhausted { .. }) => {
                self.retry_failure(engine, key, callback, &error.to_string())
            }
            Err(error) => {
                non_retryable_delivery_failure(application_failure(&error.to_string(), true))
            }
            Ok(OutcomeApplied::ExecutionMissing) => {
                unreachable!("internal delivery names missing targets in its error")
            }
        }
    }
}

fn callback_seed(key: &ExecutionKey, callback_id: &str) -> u64 {
    let mut seed = DefaultHasher::new();
    key.hash(&mut seed);
    callback_id.hash(&mut seed);
    seed.finish()
}

fn activity_completion(state: &ActivityState) -> anyhow::Result<NexusCompletion> {
    if state.status() == ActivityStatus::Completed {
        // Nexus activity completion carries only the first result payload
        // (`chasm/lib/activity/activity.go:549–556 @ v1.32.0`); the workflow
        // publisher deliberately forwards its entire result collection.
        let payloads = tokeira_proto::common::Payloads::decode(state.result.as_slice())?;
        return Ok(NexusCompletion::Succeeded(tokeira_types::Payloads(
            payloads
                .payloads
                .first()
                .map(payload_to_domain)
                .into_iter()
                .collect(),
        )));
    }
    anyhow::ensure!(
        state.status().is_terminal(),
        "activity callback has no terminal outcome"
    );
    let mut failure = if state.failure_payload.is_empty() {
        Failure {
            message: state.failure.clone(),
            ..Default::default()
        }
    } else {
        Failure::decode(state.failure_payload.as_slice())?
    };
    let canceled = state.status() == ActivityStatus::Canceled;
    if canceled && failure.failure_info.is_none() {
        failure.failure_info = Some(FailureInfo::CanceledFailureInfo(
            tokeira_proto::failure::CanceledFailureInfo {
                identity: state.cancel_identity.clone(),
                details: if state.canceled_details.is_empty() {
                    None
                } else {
                    Some(tokeira_proto::common::Payloads::decode(
                        state.canceled_details.as_slice(),
                    )?)
                },
            },
        ));
    }
    let body = NexusCompletionFailureBody {
        message: if canceled {
            "operation canceled"
        } else {
            "operation failed"
        }
        .into(),
        failure: failure_to_payload(&failure),
    }
    .encode();
    Ok(if canceled {
        NexusCompletion::Canceled(body)
    } else {
        NexusCompletion::Failed(body)
    })
}

#[async_trait]
impl SideEffectExecutor for DeliverCallbackExecutor {
    fn task_type_id(&self) -> u32 {
        DELIVER_CALLBACK_TASK_ID
    }

    async fn execute(&self, key: &ExecutionKey, task: &ScheduledTask) -> anyhow::Result<()> {
        let Some(engine) = self.engine.upgrade() else {
            tracing::debug!(?key, "callback delivery engine has stopped");
            return Ok(());
        };
        let delivery = DeliverCallback::decode(&task.payload)?;
        let snapshot = match engine.read_component(key).await {
            Ok(snapshot) => snapshot,
            Err(ChasmError::ExecutionNotFound) => {
                tracing::debug!(?key, "callback activity is gone");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let state = ActivityState::decode(
            snapshot
                .data
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("activity root has no data"))?,
        )?;
        let Some(callback) = state.callbacks.iter().find(|callback| {
            callback.id == delivery.callback_id
                && callback.state() == CallbackState::Scheduled
                && callback.attempt == delivery.stamp
        }) else {
            return Ok(());
        };
        let outcome = match &callback.target {
            Some(activity_callback::Target::Nexus(target)) => {
                match self.nexus_delivery(key, &state, callback, target).await {
                    Ok(CompletionDeliveryOutcome::Delivered) => TaskOutcome::Completed {
                        payload: Vec::new(),
                    },
                    Ok(CompletionDeliveryOutcome::NonRetryableError { detail }) => {
                        non_retryable_delivery_failure(application_failure(&detail, true))
                    }
                    Ok(CompletionDeliveryOutcome::RetryableError { detail }) => {
                        self.retry_failure(&engine, key, callback, &detail)
                    }
                    Err(error) => self.retry_failure(
                        &engine,
                        key,
                        callback,
                        &format!("nexus completion delivery error: {error}"),
                    ),
                }
            }
            Some(activity_callback::Target::Internal(target)) => {
                let result = self.internal_delivery(&engine, target, &state).await;
                self.internal_outcome(&engine, key, callback, result)
            }
            None => non_retryable_delivery_failure(application_failure(
                "callback target is missing",
                true,
            )),
        };
        match engine
            .apply_side_effect_outcome(key, DELIVER_CALLBACK_TASK_ID, task.id, outcome)
            .await?
        {
            OutcomeApplied::Applied(_) => {}
            OutcomeApplied::NotHeld | OutcomeApplied::ExecutionMissing => {
                tracing::debug!(?key, task_id = ?task.id, "callback attempt is no longer held")
            }
        }
        Ok(())
    }
}

impl ActivityDispatchExecutor {
    /// Bind matching to an engine without keeping that engine alive.
    pub fn new(engine: Weak<ChasmEngine>, queue: Arc<ActivityDispatchQueue>) -> Self {
        Self { engine, queue }
    }

    /// Matching queue shared with the activity bridge.
    pub fn queue(&self) -> &Arc<ActivityDispatchQueue> {
        &self.queue
    }
}

#[async_trait]
impl SideEffectExecutor for ActivityDispatchExecutor {
    fn task_type_id(&self) -> u32 {
        DISPATCH_TASK_ID
    }

    async fn execute(&self, key: &ExecutionKey, task: &ScheduledTask) -> anyhow::Result<()> {
        let Some(engine) = self.engine.upgrade() else {
            tracing::debug!(?key, "activity dispatch engine has stopped");
            return Ok(());
        };
        let dispatch = DispatchTask::decode(&task.payload)?;
        let snapshot = match engine.read_component(key).await {
            Ok(snapshot) => snapshot,
            Err(ChasmError::ExecutionNotFound) => {
                tracing::debug!(?key, "activity dispatch execution is gone");
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let state = ActivityState::decode(
            snapshot
                .data
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("activity root has no data"))?,
        )?;
        // The queue retains (execution, stamp) after pickup, so rebuilds cannot
        // serve an unchanged attempt twice. A terminal/deleted run is forgotten.
        self.queue.enqueue(
            dispatch.task_queue,
            DispatchEntry {
                key: key.clone(),
                stamp: dispatch.stamp,
                fire_at: task.fire_at_unix_nanos,
                target: state.version_target,
            },
        );
        Ok(())
    }
}

/// Starts activities staged by any registered component. The return address
/// names the parent's held task, making completion independent of process lifetime.
#[derive(Debug)]
pub struct StartActivityExecutor {
    engine: Weak<ChasmEngine>,
    config: ActivityConfig,
    max_id_length: usize,
}

impl StartActivityExecutor {
    /// Bind the shared start path and its validation limits.
    pub fn new(engine: Weak<ChasmEngine>, config: ActivityConfig, max_id_length: usize) -> Self {
        Self {
            engine,
            config,
            max_id_length,
        }
    }
}

// TaskId is unique within a node, not across parents. Include the complete
// execution identity with length prefixes, avoiding ambiguous separators and
// preserving the key across toolchain upgrades (unlike a Debug/hash rendering).
fn start_request_id(key: &ExecutionKey, id: TaskId) -> String {
    format!(
        "chasm.start_activity:{}:{}:{}:{}:{}:{}:{}:{}:{}",
        id.versioned_transition.namespace_failover_version,
        id.versioned_transition.transition_count,
        id.offset,
        key.namespace_id.len(),
        key.namespace_id,
        key.business_id.len(),
        key.business_id,
        key.run_id.len(),
        key.run_id
    )
}

fn application_failure(detail: &str, non_retryable: bool) -> Vec<u8> {
    Failure {
        message: detail.to_owned(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                non_retryable,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
    .encode_to_vec()
}

fn start_rejection(activity_id: &str, error: &EdgeError) -> Option<TaskOutcome> {
    let detail = match error {
        EdgeError::ActivityExecutionAlreadyStarted {
            message, run_id, ..
        } => format!("activity {activity_id} already has run {run_id}: {message}"),
        EdgeError::AlreadyExists(reason) | EdgeError::BadRequest(reason) => {
            format!("cannot start activity {activity_id}: {reason}")
        }
        _ => return None,
    };
    Some(TaskOutcome::Failed {
        failure: application_failure(&detail, true),
        retryable: false,
    })
}

#[async_trait]
impl SideEffectExecutor for StartActivityExecutor {
    fn task_type_id(&self) -> u32 {
        task_type_id_for_fqn(StartActivityTask::FQN)
    }

    async fn execute(&self, key: &ExecutionKey, task: &ScheduledTask) -> anyhow::Result<()> {
        let Some(engine) = self.engine.upgrade() else {
            tracing::debug!(?key, "activity start engine has stopped");
            return Ok(());
        };
        let request = <StartActivityTask as Task>::decode(&task.payload)?;
        let Some(root) = engine.root_node(key).await? else {
            tracing::debug!(?key, "activity start parent is gone");
            return Ok(());
        };
        let parent = ComponentRef::new(
            key.clone(),
            root.metadata.component_type_id,
            VersionedTransition::default(),
            Vec::new(),
            VersionedTransition::default(),
        )
        .encode()?;
        let retry = if request.retry_policy.is_empty() {
            None
        } else {
            Some(tokeira_proto::common::RetryPolicy::decode(
                request.retry_policy.as_slice(),
            )?)
        };
        let normalized = defaulted_retry_policy(retry.as_ref());
        let (initial, coefficient, maximum, attempts) = retry_policy_fields(&normalized);
        let activity_id = request.activity_id.clone();
        let result = start_activity(
            &engine,
            &self.config,
            self.max_id_length,
            StartActivity {
                namespace_id: key.namespace_id.clone(),
                activity_id: request.activity_id,
                run_id: uuid::Uuid::new_v4().to_string(),
                activity_type: request.activity_type,
                task_queue: request.task_queue,
                input: request.input,
                header: request.header,
                schedule_to_start_nanos: request.schedule_to_start_nanos,
                schedule_to_close_nanos: request.schedule_to_close_nanos,
                start_to_close_nanos: request.start_to_close_nanos,
                heartbeat_nanos: request.heartbeat_nanos,
                run_timeout_nanos: 0,
                request_id: Some(start_request_id(key, task.id)),
                policy: BusinessIdPolicy::default(),
                retry_policy: request.retry_policy,
                retry_initial_interval_nanos: initial,
                retry_backoff_coefficient: coefficient,
                retry_maximum_interval_nanos: maximum,
                maximum_attempts: attempts,
                priority: Vec::new(),
                search_attributes: Vec::new(),
                user_metadata: Vec::new(),
                version_target: request.version_target,
                callbacks: vec![CallbackSpec {
                    target: CallbackTarget::Internal {
                        component_ref: parent,
                        task_type_id: task.task_type_id,
                        task_id: encode_task_id(task.id)?,
                    },
                    links: Vec::new(),
                }],
            },
        )
        .await;
        match result {
            Ok(_) => Ok(()), // The child's Internal callback eventually resolves the parent task.
            Err(error) => match start_rejection(&activity_id, &error) {
                Some(outcome) => {
                    engine
                        .apply_side_effect_outcome(key, task.task_type_id, task.id, outcome)
                        .await?;
                    Ok(())
                }
                None => Err(error.into()), // Rebuild retries transient failures from the held outbox.
            },
        }
    }
}
