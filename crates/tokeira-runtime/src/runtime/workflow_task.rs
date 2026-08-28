//! Workflow-task poll and start path, including Worker Deployment routing.
//!
//! This `impl TokeiraRuntime` continuation owns the workflow-task half of
//! delivery: long-polling the broker, resolving the deployment version a polled
//! task should target, starting the version transition when a differing poller
//! arrives, and driving the kernel `WorkflowTaskStarted`/`Completed`
//! transitions. Routing decisions are derived effects of the durable deployment
//! registry plus per-run versioning state — no correctness weight rests on the
//! transient broker (per "history is authority").
//!
//! The versioning routing here mirrors Temporal server v1.31.0
//! (`service/history/api/recordworkflowtaskstarted` and
//! `service/history/workflow/util.go`); the inline comments cite the specific
//! source for each non-obvious decision.

use super::*;
use prost::Message as _;
use tokeira_kernel::{
    ContinueAsNewVersioningBehavior, RetryContinuation, RetryState, VersioningBehavior,
    VersioningOverride, WorkerDeploymentVersionRef, WorkflowTaskCompletionLimits,
    WorkflowVersioningInfo,
};
use tokeira_observability::OutcomeLabel;
use tokeira_proto::failure::{Failure, failure::FailureInfo};
use tokeira_storage::{
    DeploymentKey, DeploymentName, StoredRoutingConfig, WorkerDeploymentVersionKey,
};
use tokeira_types::{WorkerTaskClass, WorkerTaskOrigin, WorkflowId};
use tracing::Instrument as _;

use crate::timeout::WorkflowTimeoutEntry;

/// Workflow-task routing target computed from durable run state plus registry config.
///
/// The value is shared with activity-start transition checks because Temporal
/// decides whether an activity poller should transition the workflow by asking
/// where the workflow task would route now
/// (`service/history/api/recordactivitytaskstarted/api.go:283 @ v1.31.0`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedWorkflowTaskTarget {
    /// Deployment version selected for the workflow task, or `None` for unversioned routing.
    pub deployment_version: Option<WorkerDeploymentVersionRef>,
    /// Registry/run revision that produced `deployment_version`.
    pub revision_number: i64,
    /// Whether the target is pinned and therefore cannot initiate a transition.
    pub pinned: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PolledWorkflowTaskTarget {
    resolved: ResolvedWorkflowTaskTarget,
    deployment_transition: Option<WorkerDeploymentVersionRef>,
    /// Current/Ramping target offered for SDK notification, distinct from a
    /// pinned effective dispatch destination.
    routing_target: Option<WorkerDeploymentVersionRef>,
}

#[cfg(not(feature = "conformance"))]
fn target_version_changed_enabled() -> bool {
    true
}

#[cfg(feature = "conformance")]
fn target_version_changed_enabled() -> bool {
    crate::conformance::reads()
        .get_bool("system.enableSendTargetVersionChanged")
        .unwrap_or(true)
}

const DEFAULT_PENDING_COMMAND_LIMIT: usize = 2_000;

fn normalize_pending_command_limit(configured: Option<i64>) -> Option<usize> {
    let configured = configured.unwrap_or(DEFAULT_PENDING_COMMAND_LIMIT as i64);
    usize::try_from(configured).ok().filter(|limit| *limit > 0)
}

#[cfg(not(feature = "conformance"))]
fn pending_command_limit(_key: &str) -> Option<usize> {
    normalize_pending_command_limit(None)
}

#[cfg(feature = "conformance")]
fn pending_command_limit(key: &str) -> Option<usize> {
    normalize_pending_command_limit(crate::conformance::reads().get_i64(key))
}

fn workflow_task_completion_limits() -> WorkflowTaskCompletionLimits {
    WorkflowTaskCompletionLimits {
        pending_child_workflows: pending_command_limit("limit.numPendingChildExecutions.error"),
        pending_activities: pending_command_limit("limit.numPendingActivities.error"),
        pending_signals: pending_command_limit("limit.numPendingSignals.error"),
        pending_cancel_requests: pending_command_limit("limit.numPendingCancelRequests.error"),
    }
}

/// Resolve the deployment version a polled workflow task should target.
///
/// Precedence mirrors v1.31.0 `GetEffectiveDeployment`
/// (`service/history/workflow/util.go @ v1.31.0`):
/// version_transition > pinned override > auto-upgrade override > pinned
/// behavior > SDK behavior + routing config. A pinned target (override or
/// behavior) carries `pinned: true` so the caller never starts a transition
/// for it. AUTO_UPGRADE / unversioned runs follow the deployment's routing
/// config (current/ramping), so they pick up the deployment's revision number;
/// pinned/transition targets carry the run's own revision number because their
/// routing decision was already made and recorded on the run.
pub(crate) fn resolve_workflow_task_target_version(
    routing_config: &StoredRoutingConfig,
    state: &WorkflowState,
) -> ResolvedWorkflowTaskTarget {
    let Some(info) = state.versioning_info.as_ref() else {
        let (deployment_version, revision_number) =
            routing_config_target_with_revision(routing_config, &state.workflow_id);
        return ResolvedWorkflowTaskTarget {
            deployment_version,
            revision_number,
            pinned: false,
        };
    };

    if let Some(transition) = &info.version_transition {
        return ResolvedWorkflowTaskTarget {
            deployment_version: Some(transition.clone()),
            revision_number: info.revision_number,
            pinned: false,
        };
    }

    if info.versioning_override.is_none()
        && let Some(initial_target) = inherited_auto_upgrade_target(routing_config, state)
    {
        return initial_target;
    }

    match &info.versioning_override {
        Some(VersioningOverride::Pinned { version }) => ResolvedWorkflowTaskTarget {
            deployment_version: Some(version.clone()),
            revision_number: info.revision_number,
            pinned: true,
        },
        Some(VersioningOverride::AutoUpgrade) => {
            let (deployment_version, revision_number) =
                routing_config_target_with_revision(routing_config, &state.workflow_id);
            ResolvedWorkflowTaskTarget {
                deployment_version,
                revision_number,
                pinned: false,
            }
        }
        None if info.behavior == VersioningBehavior::Pinned => ResolvedWorkflowTaskTarget {
            deployment_version: info.deployment_version.clone(),
            revision_number: info.revision_number,
            pinned: true,
        },
        None => {
            let (deployment_version, revision_number) =
                routing_config_target_with_revision(routing_config, &state.workflow_id);
            ResolvedWorkflowTaskTarget {
                deployment_version,
                revision_number,
                pinned: false,
            }
        }
    }
}

/// Pick the routing target for AUTO_UPGRADE / unversioned traffic from a
/// deployment's routing config, splitting by ramp percentage.
///
/// A nil current version means the deployment routes this traffic to
/// unversioned workers (returns None). When a ramping version is set with a
/// non-zero percentage, a deterministic fraction of workflow ids route to it;
/// the rest stay on current. The split is keyed on workflow id (not random) so
/// the same run always resolves to the same version across polls, which is
/// required for replay determinism.
///
/// Bucketing: `deterministic_bucket` returns `[0, 9999]`, so the ramp
/// percentage P is scaled to `P * 100` buckets — e.g. P=50 sends ids with
/// bucket `< 5000` (half) to the ramping version. This gives 1/100-percent
/// resolution matching the `float` percentage field.
pub(crate) fn routing_config_target(
    routing_config: &StoredRoutingConfig,
    workflow_id: &WorkflowId,
) -> Option<WorkerDeploymentVersionRef> {
    routing_config_target_with_revision(routing_config, workflow_id).0
}

/// Return the selected routing target and the revision belonging to that target.
///
/// Temporal's task-queue data can observe Current and Ramping changes at different
/// revisions. Tokeira keeps one centralized routing record, so retaining the two
/// field revisions preserves the same no-bounce comparison without recreating
/// matching-local propagation state (`CalculateTaskQueueVersioningInfo` and
/// `chooseTargetQueueByFlag @ v1.31.0`). Zero-valued fields from pre-field records
/// fall back to the aggregate revision.
fn routing_config_target_with_revision(
    routing_config: &StoredRoutingConfig,
    workflow_id: &WorkflowId,
) -> (Option<WorkerDeploymentVersionRef>, i64) {
    let ramping_is_configured =
        routing_config.ramping_version.is_some() || routing_config.ramping_to_unversioned;
    if ramping_is_configured
        && routing_config.ramping_version_percentage > 0.0
        && deterministic_bucket(&workflow_id.0)
            < (f64::from(routing_config.ramping_version_percentage) * 100.0) as u64
    {
        return (
            routing_config
                .ramping_version
                .as_ref()
                .map(version_key_to_ref),
            routing_target_revision(
                routing_config.ramping_version_revision_number,
                routing_config.revision_number,
            ),
        );
    }
    (
        routing_config
            .current_version
            .as_ref()
            .map(version_key_to_ref),
        routing_target_revision(
            routing_config.current_version_revision_number,
            routing_config.revision_number,
        ),
    )
}

fn routing_target_revision(target_revision: i64, aggregate_revision: i64) -> i64 {
    if target_revision == 0 {
        aggregate_revision
    } else {
        target_revision
    }
}

/// Resolve the versioning state committed with a Continue-as-New successor.
///
/// All mutable membership observations are Boolean inputs supplied by runtime;
/// this function is pure and therefore suitable for reference-model testing.
/// It implements `mutable_state_impl.go:2485-2630 @ v1.31.0` without importing
/// Temporal's history/matching architecture.
pub(crate) fn resolve_continue_as_new_versioning(
    predecessor: &WorkflowState,
    successor_task_queue: &TaskQueueName,
    initial_behavior: ContinueAsNewVersioningBehavior,
    source_version_has_successor_queue: bool,
    pinned_override_has_successor_queue: bool,
) -> Option<WorkflowVersioningInfo> {
    let same_task_queue = predecessor.task_queue == *successor_task_queue;
    let source_compatible = same_task_queue || source_version_has_successor_queue;
    let override_compatible = same_task_queue || pinned_override_has_successor_queue;
    let effective_behavior = predecessor.effective_behavior();
    let effective_version = predecessor.effective_deployment().cloned();
    let revision_number = predecessor
        .versioning_info
        .as_ref()
        .map(|info| info.revision_number)
        .unwrap_or_default();
    let declined_target_version_upgrade = predecessor.versioning_info.as_ref().and_then(|info| {
        info.last_notified_target_version
            .clone()
            .or_else(|| info.declined_target_version_upgrade.clone())
    });
    let pinned_override = predecessor
        .versioning_override()
        .and_then(|override_| match override_ {
            VersioningOverride::Pinned { .. } if override_compatible => Some(override_.clone()),
            VersioningOverride::Pinned { .. } | VersioningOverride::AutoUpgrade => None,
        });

    let inherited_pinned = (effective_behavior == VersioningBehavior::Pinned
        && initial_behavior == ContinueAsNewVersioningBehavior::Unspecified
        && source_compatible)
        .then_some(effective_version.clone())
        .flatten();
    let requests_auto_upgrade = initial_behavior != ContinueAsNewVersioningBehavior::Unspecified;
    let inherited_auto_upgrade = ((effective_behavior == VersioningBehavior::AutoUpgrade
        || (effective_behavior == VersioningBehavior::Pinned && requests_auto_upgrade))
        && source_compatible
        && revision_number != 0)
        .then_some(effective_version)
        .flatten();

    if inherited_pinned.is_none()
        && inherited_auto_upgrade.is_none()
        && pinned_override.is_none()
        && declined_target_version_upgrade.is_none()
    {
        return None;
    }

    let mut info = WorkflowVersioningInfo {
        versioning_override: pinned_override,
        declined_target_version_upgrade,
        ..WorkflowVersioningInfo::default()
    };
    if let Some(version) = inherited_pinned {
        info.behavior = VersioningBehavior::Pinned;
        info.deployment_version = Some(version);
        info.revision_number = revision_number;
    } else if let Some(version) = inherited_auto_upgrade {
        info.behavior = VersioningBehavior::AutoUpgrade;
        info.deployment_version = Some(version);
        info.revision_number = revision_number;
        info.continue_as_new_initial_versioning_behavior = initial_behavior;
    }
    Some(info)
}

/// Resolve versioning state inherited by a child workflow start.
///
/// Child workflows inherit the parent's effective pinned Version or AutoUpgrade
/// source only when the child remains in the same namespace and its workflow task
/// queue belongs to that Version. A pinned override follows the same compatibility
/// rule. Unlike Continue-as-New, a child never inherits the parent's one-run
/// `USE_RAMPING_VERSION` instruction; its AutoUpgrade source always starts with an
/// unspecified initial behavior (`transfer_queue_active_task_executor.go:915-979 @
/// v1.31.0`). Mutable membership observations are supplied by the runtime caller so
/// this decision remains pure.
pub(crate) fn resolve_child_versioning(
    parent: &WorkflowState,
    child_task_queue: &TaskQueueName,
    same_namespace: bool,
    source_version_has_child_queue: bool,
    pinned_override_has_child_queue: bool,
) -> Option<WorkflowVersioningInfo> {
    if !same_namespace {
        return None;
    }

    let same_task_queue = parent.task_queue == *child_task_queue;
    let source_compatible = same_task_queue || source_version_has_child_queue;
    let override_compatible = same_task_queue || pinned_override_has_child_queue;
    let effective_behavior = parent.effective_behavior();
    let effective_version = parent.effective_deployment().cloned();
    let revision_number = parent
        .versioning_info
        .as_ref()
        .map(|info| info.revision_number)
        .unwrap_or_default();
    let pinned_override = parent
        .versioning_override()
        .and_then(|override_| match override_ {
            VersioningOverride::Pinned { .. } if override_compatible => Some(override_.clone()),
            VersioningOverride::Pinned { .. } | VersioningOverride::AutoUpgrade => None,
        });
    let inherited_pinned = (effective_behavior == VersioningBehavior::Pinned && source_compatible)
        .then_some(effective_version.clone())
        .flatten();
    let inherited_auto_upgrade = (effective_behavior == VersioningBehavior::AutoUpgrade
        && source_compatible
        && revision_number != 0)
        .then_some(effective_version)
        .flatten();

    if inherited_pinned.is_none() && inherited_auto_upgrade.is_none() && pinned_override.is_none() {
        return None;
    }

    let mut info = WorkflowVersioningInfo {
        versioning_override: pinned_override,
        ..WorkflowVersioningInfo::default()
    };
    if let Some(version) = inherited_pinned {
        info.behavior = VersioningBehavior::Pinned;
        info.deployment_version = Some(version);
        info.revision_number = revision_number;
    } else if let Some(version) = inherited_auto_upgrade {
        info.behavior = VersioningBehavior::AutoUpgrade;
        info.deployment_version = Some(version);
        info.revision_number = revision_number;
        info.continue_as_new_initial_versioning_behavior =
            ContinueAsNewVersioningBehavior::Unspecified;
    }
    Some(info)
}

/// Project the versioning state that command handling observes on this completion.
///
/// The clone is only a runtime decision operand; the kernel still performs the
/// authoritative mutation. v1.31.0 applies the completing worker's behavior and
/// Version in `afterAddWorkflowTaskCompletedEvent` before it evaluates a
/// Continue-as-New command, so resolving from the loaded pre-completion state would
/// lose a first-task `PINNED` report (`workflow_task_state_machine.go` and
/// `mutable_state_impl.go:2485-2630 @ v1.31.0`).
fn state_after_wft_completion_versioning(
    predecessor: &WorkflowState,
    behavior: VersioningBehavior,
    deployment_version: Option<WorkerDeploymentVersionRef>,
    worker_deployment_name: Option<String>,
) -> WorkflowState {
    let mut projected = predecessor.clone();
    projected.apply_wft_versioning(behavior, deployment_version, worker_deployment_name);
    projected
}

/// Resolve the first WFT target for inherited AutoUpgrade state.
///
/// Ordinary AutoUpgrade uses the routing target unless its revision is older
/// than the inherited source revision within the same Deployment; retaining the
/// source in that one case prevents bounce-back while task-queue routing
/// converges (`chooseTargetQueueByFlag`,
/// `task_queue_partition_manager.go:2061-2078 @ v1.31.0`).
/// `USE_RAMPING_VERSION` bypasses percentage bucketing only until the first
/// successful WFT (and therefore across its failed retries); afterward normal
/// Current/Ramping routing resumes (`GetShouldUseRampingVersion`,
/// `mutable_state_impl.go:9122-9141 @ v1.31.0`).
fn inherited_auto_upgrade_target(
    routing_config: &StoredRoutingConfig,
    state: &WorkflowState,
) -> Option<ResolvedWorkflowTaskTarget> {
    let info = state.versioning_info.as_ref()?;
    if state.previous_started_event_id != 0 || info.behavior != VersioningBehavior::AutoUpgrade {
        return None;
    }
    if info.continue_as_new_initial_versioning_behavior
        == ContinueAsNewVersioningBehavior::UseRampingVersion
    {
        let (target, revision_number) =
            if routing_config.ramping_version.is_some() || routing_config.ramping_to_unversioned {
                (
                    routing_config
                        .ramping_version
                        .as_ref()
                        .map(version_key_to_ref),
                    routing_target_revision(
                        routing_config.ramping_version_revision_number,
                        routing_config.revision_number,
                    ),
                )
            } else {
                (
                    routing_config
                        .current_version
                        .as_ref()
                        .map(version_key_to_ref),
                    routing_target_revision(
                        routing_config.current_version_revision_number,
                        routing_config.revision_number,
                    ),
                )
            };
        return Some(ResolvedWorkflowTaskTarget {
            deployment_version: target,
            revision_number,
            pinned: false,
        });
    }

    let (routing_target, routing_revision) =
        routing_config_target_with_revision(routing_config, &state.workflow_id);
    let Some(routing_target) = routing_target else {
        // With no Current/Ramping target, v1.31.0's matching path falls back to
        // the default unversioned queue rather than forcing the inherited
        // source (`getPhysicalQueuesForAdd`, task_queue_partition_manager.go
        // @ v1.31.0).
        return Some(ResolvedWorkflowTaskTarget {
            deployment_version: None,
            revision_number: routing_revision,
            pinned: false,
        });
    };
    let source = info.deployment_version.clone();
    let routing_target_wins = source.as_ref().is_none_or(|source| {
        routing_target.deployment_name != source.deployment_name
            || routing_revision >= info.revision_number
    });
    if routing_target_wins {
        Some(ResolvedWorkflowTaskTarget {
            deployment_version: Some(routing_target),
            revision_number: routing_revision,
            pinned: false,
        })
    } else {
        Some(ResolvedWorkflowTaskTarget {
            deployment_version: source,
            revision_number: info.revision_number,
            pinned: false,
        })
    }
}

/// Re-derive an activity task's disposable queue from authoritative run and
/// Worker Deployment state.
///
/// Pinned dependent activities remain on the workflow's pinned version. A
/// pinned activity whose queue has no membership in that version is independent
/// and follows the activity queue's own current/ramping routing, as do
/// AUTO_UPGRADE activities. Temporal makes this choice when the task is added
/// to matching, including when an activity retry timer fires
/// (`service/matching/task_queue_partition_manager.go:getPhysicalQueuesForAdd
/// @ v1.31.0`). Tokeira performs the equivalent derivation immediately before
/// broker publication so durable run state, rather than a stored broker target,
/// remains authoritative.
pub(crate) async fn route_activity_task_queue(
    registry: Option<&DeploymentRegistry>,
    state: &WorkflowState,
    mut queue: QueueKey,
    fallback_revision: i64,
) -> Result<(QueueKey, i64)> {
    let Some(registry) = registry else {
        return Ok((queue, fallback_revision));
    };

    let effective_deployment = state.effective_deployment();
    if state.effective_behavior() == VersioningBehavior::Pinned
        && let Some(version) = effective_deployment
        && registry
            .version_has_activity_task_queue(
                state.namespace_id,
                &queue.task_queue.0,
                &version.deployment_name,
                &version.build_id,
            )
            .await?
    {
        queue.deployment = Some(DeploymentId(version.deployment_name.clone()));
        queue.build_id = Some(BuildId(version.build_id.clone()));
        return Ok((queue, 0));
    }

    let preferred_deployment = effective_deployment
        .map(|version| version.deployment_name.as_str())
        .or(state.worker_deployment_name.as_deref());
    let routing = registry
        .activity_task_routing_config(
            state.namespace_id,
            &queue.task_queue.0,
            preferred_deployment,
        )
        .await?;
    match routing_config_target(&routing, &state.workflow_id) {
        Some(version) => {
            queue.deployment = Some(DeploymentId(version.deployment_name));
            queue.build_id = Some(BuildId(version.build_id));
        }
        None => {
            queue.deployment = None;
            queue.build_id = None;
        }
    }
    Ok((queue, routing.revision_number))
}

/// Stable per-workflow-id bucket in `[0, 9999]` for ramp splits.
fn deterministic_bucket(value: &str) -> u64 {
    // FNV-1a is stable across processes, unlike DefaultHasher's seeded state.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash % 10_000
}

fn version_key_to_ref(version: &WorkerDeploymentVersionKey) -> WorkerDeploymentVersionRef {
    WorkerDeploymentVersionRef {
        deployment_name: version.deployment_name.0.clone(),
        build_id: version.build_id.0.clone(),
    }
}

/// Reconstruct the worker deployment version from the Matching queue key.
///
/// Unversioned pollers intentionally return `None`; they may start work but
/// cannot prove a target deployment for a version transition.
pub(super) fn poller_deployment_version(queue: &QueueKey) -> Option<WorkerDeploymentVersionRef> {
    Some(WorkerDeploymentVersionRef {
        deployment_name: queue.deployment.as_ref()?.0.clone(),
        build_id: queue.build_id.as_ref()?.0.clone(),
    })
}

/// Decide whether a polled workflow task should start a deployment-version
/// transition toward the poller's version.
///
/// Mirrors v1.31.0 `recordworkflowtaskstarted/api.go @ v1.31.0`: a transition
/// starts whenever the polling worker's deployment differs from the workflow's
/// *effective* deployment (`!pollerDeployment.Equal(wfDeployment)`). We do NOT
/// additionally require the poller to equal the freshly-resolved routing target
/// — Matching already chose this poller when it dispatched the task, and
/// re-deriving the routing target here would suppress a legitimate transition
/// when the routing config advanced between dispatch and start (the staleness
/// window the dispatch revision number guards). Pinned runs never transition;
/// `start_version_transition` itself rejects them
/// (`ErrPinnedWorkflowCannotTransition`), and we also short-circuit on the
/// resolved-target pin so a pinned run is never offered a transition.
fn transition_for_polled_workflow_task(
    state: &WorkflowState,
    target: &ResolvedWorkflowTaskTarget,
    queue: &QueueKey,
    speculative: bool,
) -> Option<WorkerDeploymentVersionRef> {
    // A speculative start is transactionally a no-op in v1.31.0: its worker-
    // deployment transition may be computed, but is applied only if completion
    // later materializes the speculative task (`recordworkflowtaskstarted/api.go:
    // 178-197 @ v1.31.0`). Tokeira expresses that observable contract by
    // withholding the transition operand from the pure start transition.
    if speculative || target.pinned {
        return None;
    }
    let poller_version = poller_deployment_version(queue)?;
    (state.effective_deployment() != Some(&poller_version)).then_some(poller_version)
}

fn routing_deployment_name(state: &WorkflowState, queue: &QueueKey) -> Option<DeploymentName> {
    state
        .worker_deployment_name
        .as_ref()
        .cloned()
        .or_else(|| {
            state
                .versioning_info
                .as_ref()
                .and_then(|info| info.deployment_version.as_ref())
                .map(|version| version.deployment_name.clone())
        })
        .or_else(|| {
            queue
                .deployment
                .as_ref()
                .map(|deployment| deployment.0.clone())
        })
        .map(DeploymentName)
}

impl<R> TokeiraRuntime<R>
where
    R: RunRepository + 'static,
{
    pub(super) async fn try_reserve_start_poller(
        &self,
        request: &StartRequest,
    ) -> Option<ReservedPoller> {
        let queue = QueueKey {
            namespace_id: request.namespace_id,
            task_queue: request.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: request.deployment.clone(),
            build_id: request.build_id.clone(),
        };
        self.broker.try_reserve_poller(&queue).await
    }

    pub(super) async fn deliver_reserved_start_workflow_task(
        &self,
        new_state: &WorkflowState,
        reserved: ReservedPoller,
    ) -> Result<()> {
        let worker_identity = reserved.worker_identity().clone();
        // A start sync-match delivers on the run's NORMAL queue: not a sticky
        // match (the flag licenses partial-history attach, moot here anyway
        // since a fresh run has previous_started_event_id == 0).
        let task = self
            .started_workflow_task_from_state(new_state, false, worker_identity)
            .await?;
        if !reserved.deliver(task) {
            tracing::warn!(
                run_key = %new_state.run_key.0,
                "reserved workflow task poller disappeared after durable start commit; timeout scanner will recover"
            );
        }
        Ok(())
    }

    pub async fn poll_workflow_task(
        &self,
        queue: QueueKey,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<StartedWorkflowTask>> {
        let polled = match self
            .broker
            .poll_workflow_task(&queue, &worker_identity, timeout_after)
            .await?
        {
            Some(polled) => {
                self.delivery_metrics.record_poll_success(&queue);
                polled
            }
            None => {
                self.delivery_metrics.record_poll_timeout(&queue);
                return Ok(None);
            }
        };

        match polled {
            WorkflowPollResult::Queued(offered, entered_at) => {
                let started = self
                    .start_polled_workflow_task(offered, entered_at, worker_identity)
                    .await?;
                Ok(Some(started))
            }
            WorkflowPollResult::Started(started) => Ok(Some(started)),
            // The workflow-task-only API calls the WFT-only broker method, so
            // this arm is defensive. If a future refactor routes it through
            // the unified poll, do not consume direct query work here: callers
            // of this method are not prepared to register a query waiter.
            WorkflowPollResult::Query(_) => Ok(None),
        }
    }

    /// Poll the workflow task queue for either a workflow task or a direct query.
    ///
    /// This is the public-api path used by `PollWorkflowTaskQueue`: Temporal
    /// delivers legacy direct queries as matched workflow poll tasks rather
    /// than through a separate query-poll RPC
    /// (`service/matching/matching_engine.go:1084 @ v1.31.0`). The older
    /// [`Self::poll_workflow_task`] method remains workflow-task-only for
    /// internal eager-claim paths that cannot safely consume query work.
    pub async fn poll_workflow_activation(
        &self,
        queue: QueueKey,
        normal_queue: Option<QueueKey>,
        worker_identity: WorkerIdentity,
        timeout_after: tokio::time::Duration,
    ) -> Result<Option<WorkflowActivation>> {
        let deadline = tokio::time::Instant::now() + timeout_after;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let polled = match self
                .broker
                .poll_workflow_activation(
                    &queue,
                    normal_queue.as_ref(),
                    &worker_identity,
                    remaining,
                )
                .await?
            {
                Some(polled) => {
                    self.delivery_metrics.record_poll_success(&queue);
                    polled
                }
                None => {
                    self.delivery_metrics.record_poll_timeout(&queue);
                    return Ok(None);
                }
            };

            match polled {
                WorkflowPollResult::Queued(offered, entered_at) => {
                    match self
                        .start_polled_workflow_task(offered, entered_at, worker_identity.clone())
                        .await
                    {
                        Ok(started) => {
                            return Ok(Some(WorkflowActivation::WorkflowTask(started)));
                        }
                        // A broker entry that no longer matches the run's current
                        // pending task — superseded by a schedule-to-start /
                        // start-to-close timeout that reclaimed and rescheduled
                        // it, or already started — is discarded (the poll already
                        // removed it from the broker) and we re-poll for the fresh
                        // task rather than surfacing a stale error to the worker.
                        // This is the poll-side of Invariant I.1's "clear broker
                        // in-flight" for a superseded speculative task (spec
                        // speculative-wft R.2). A non-stale error propagates.
                        Err(error) if is_stale_workflow_task_start(&error) => {
                            tracing::debug!(
                                ?error,
                                "discarded superseded workflow task from broker; re-polling"
                            );
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                }
                WorkflowPollResult::Started(started) => {
                    return Ok(Some(WorkflowActivation::WorkflowTask(started)));
                }
                WorkflowPollResult::Query(query) => {
                    return Ok(Some(WorkflowActivation::QueryTask(query)));
                }
            }
        }
    }

    pub async fn try_claim_workflow_task(
        &self,
        queue: QueueKey,
        normal_queue: Option<QueueKey>,
        run_key: RunKey,
        worker_identity: WorkerIdentity,
    ) -> Result<Option<StartedWorkflowTask>> {
        let Some(offered) = self
            .broker
            .try_claim_workflow_task_for_worker_with_normal(
                &queue,
                normal_queue.as_ref(),
                run_key,
                Some(&worker_identity),
            )
            .await
        else {
            return Ok(None);
        };
        self.delivery_metrics.record_poll_success(&queue);
        match self
            .start_polled_workflow_task(offered.0, offered.1, worker_identity)
            .await
        {
            Ok(started) => Ok(Some(started)),
            Err(error) => {
                tracing::debug!(?error, "eager workflow task claim did not start");
                Ok(None)
            }
        }
    }

    async fn resolve_polled_workflow_task_target(
        &self,
        offered: &DispatchableWorkflowTask,
    ) -> Result<PolledWorkflowTaskTarget> {
        let LoadedRun::Existing(state) = self.repo.load_run(offered.run_key).await? else {
            return Ok(PolledWorkflowTaskTarget {
                resolved: ResolvedWorkflowTaskTarget {
                    deployment_version: None,
                    revision_number: 0,
                    pinned: false,
                },
                deployment_transition: None,
                routing_target: None,
            });
        };
        let routing_config = self
            .load_worker_deployment_routing_config(&state, &offered.queue)
            .await?;
        let resolved = resolve_workflow_task_target_version(&routing_config, &state);
        let routing_target = routing_config_target(&routing_config, &state.workflow_id);
        let speculative = state.pending_workflow_task.as_ref().is_some_and(|pending| {
            pending.logical_seq == offered.logical_seq
                && pending.task_type == tokeira_kernel::WorkflowTaskType::Speculative
        });
        let deployment_transition =
            transition_for_polled_workflow_task(&state, &resolved, &offered.queue, speculative);
        Ok(PolledWorkflowTaskTarget {
            resolved,
            deployment_transition,
            routing_target,
        })
    }

    pub(super) async fn load_worker_deployment_routing_config(
        &self,
        state: &WorkflowState,
        queue: &QueueKey,
    ) -> Result<StoredRoutingConfig> {
        let Some(repository) = &self.worker_deployment_repository else {
            return Ok(StoredRoutingConfig::default());
        };
        let Some(deployment_name) = routing_deployment_name(state, queue) else {
            return Ok(StoredRoutingConfig::default());
        };
        let key = DeploymentKey {
            namespace_id: state.namespace_id,
            deployment_name,
        };
        Ok(repository
            .load_deployment(&key)
            .await?
            .map(|record| record.routing_config)
            .unwrap_or_default())
    }

    async fn prepare_continue_as_new_versioning(
        &self,
        run_key: RunKey,
        completion_behavior: VersioningBehavior,
        completion_deployment_version: Option<WorkerDeploymentVersionRef>,
        completion_worker_deployment_name: Option<String>,
        commands: &mut [WorkflowCommand],
    ) -> Result<()> {
        let Some((command_index, requested_task_queue, initial_behavior)) =
            commands.iter().enumerate().find_map(|(index, command)| {
                let WorkflowCommand::ContinueAsNew {
                    task_queue,
                    initial_versioning_behavior,
                    ..
                } = command
                else {
                    return None;
                };
                Some((index, task_queue.clone(), *initial_versioning_behavior))
            })
        else {
            return Ok(());
        };
        let LoadedRun::Existing(predecessor) = self.repo.load_run(run_key).await? else {
            return Err(anyhow!(
                "predecessor run not found while preparing continue-as-new"
            ));
        };
        // The kernel applies these same concrete fields before processing the
        // command batch. Use an ephemeral clone for runtime-only membership
        // resolution so the pre-resolved successor decision observes that exact
        // ordering without giving this clone any authoritative role.
        let predecessor = state_after_wft_completion_versioning(
            &predecessor,
            completion_behavior,
            completion_deployment_version,
            completion_worker_deployment_name,
        );
        let successor_task_queue = if requested_task_queue.0.is_empty() {
            predecessor.task_queue.clone()
        } else {
            requested_task_queue
        };
        let cross_task_queue = successor_task_queue != predecessor.task_queue;
        let source_version = predecessor.effective_deployment().cloned();
        let pinned_override_version = predecessor.versioning_override().and_then(|override_| {
            let VersioningOverride::Pinned { version } = override_ else {
                return None;
            };
            Some(version.clone())
        });
        let registry = self.deployment_registry();
        let source_version_has_successor_queue = if !cross_task_queue {
            true
        } else if let (Some(registry), Some(version)) = (registry.as_ref(), source_version.as_ref())
        {
            registry
                .version_has_workflow_task_queue(
                    predecessor.namespace_id,
                    &successor_task_queue.0,
                    &version.deployment_name,
                    &version.build_id,
                )
                .await?
        } else {
            false
        };
        let pinned_override_has_successor_queue = if !cross_task_queue {
            true
        } else if pinned_override_version.as_ref() == source_version.as_ref() {
            source_version_has_successor_queue
        } else if let (Some(registry), Some(version)) =
            (registry.as_ref(), pinned_override_version.as_ref())
        {
            registry
                .version_has_workflow_task_queue(
                    predecessor.namespace_id,
                    &successor_task_queue.0,
                    &version.deployment_name,
                    &version.build_id,
                )
                .await?
        } else {
            false
        };
        let successor_versioning_info = resolve_continue_as_new_versioning(
            &predecessor,
            &successor_task_queue,
            initial_behavior,
            source_version_has_successor_queue,
            pinned_override_has_successor_queue,
        );
        if let WorkflowCommand::ContinueAsNew {
            successor_versioning_info: slot,
            ..
        } = &mut commands[command_index]
        {
            *slot = successor_versioning_info;
        }
        Ok(())
    }

    /// Record the completion of a workflow task and
    /// apply any resulting commands.
    ///
    /// After the kernel commits the completion, it checks whether events
    /// arrived between WFT-Started and now (buffered events, e.g. signals).
    /// If so, a new WFT is scheduled immediately so the worker replays those
    /// events. The transport layer also uses this commit point to release any
    /// buffered queries whose barrier has been satisfied.
    pub async fn complete_workflow_task(
        &self,
        req: WorkflowTaskCompletedRequest,
    ) -> Result<CommitResult> {
        if let Err(error) = self.validate_workflow_task_token(&req.token).await {
            runtime_metrics::record_workflow_task_completed(OutcomeLabel::Rejected);
            return Err(error);
        }
        let run_key = req.token.run_key;
        let completion_token = req.token.clone();
        let completion_identity = req.identity.clone();
        // A heartbeat completion (`ForceCreateNewWorkflowTask`) never re-sent
        // the already-sent updates, so it must not reject-unprocessed them
        // (Req 9 guard, workflow_task_completed_handler.go:229-231 @ v1.31.0).
        let is_heartbeat_completion = req.force_new_workflow_task;
        // Stamp the set of updates DELIVERED on this task (Sent, not yet
        // accepted) so the kernel can reject-unprocessed the ones the worker
        // ignored within the same completion transition, distinguishing them
        // from updates admitted while the task ran (which ride the follow-up
        // task — K7). Read now, before the commit processes the worker's
        // accept/reject commands (spec speculative-wft Req 9).
        let mut req = req;
        // Registry membership is mutable runtime state, so resolve it before
        // invoking the pure transition. The resulting decision is committed on
        // the predecessor close event and becomes the sole successor-start
        // source after crashes (`mutable_state_impl.go:2485-2630 @ v1.31.0`).
        self.prepare_continue_as_new_versioning(
            run_key,
            req.versioning_behavior,
            req.deployment_version.clone(),
            req.worker_deployment_name.clone(),
            &mut req.commands,
        )
        .await?;
        req.delivered_update_ids = self.update_registry.sent_update_ids(run_key);
        // Rejections write NO history event (v1.31.0's
        // RejectWorkflowExecutionUpdate is a documented no-op), so the lane's
        // post-commit event scan cannot resolve their waiters — capture them
        // before `req` moves and publish the rejection outcomes once the
        // completion durably commits.
        let rejected_updates: Vec<(String, tokeira_types::Payload)> = req
            .commands
            .iter()
            .filter_map(|command| match command {
                tokeira_kernel::WorkflowCommand::ProtocolMessage {
                    body: tokeira_kernel::UpdateProtocolBody::Rejected { update_id, failure },
                    ..
                }
                | tokeira_kernel::WorkflowCommand::UpdateRejected { update_id, failure } => {
                    Some((update_id.clone(), failure.clone()))
                }
                _ => None,
            })
            .collect();
        // Pre-claim each rejected update's waiters BEFORE the commit, so a close
        // command in the SAME completion cannot abort them first via the lane's
        // post-commit `drain_for_run`: v1.31.0 applies the rejection as an effect
        // and only the close-abort touches still-pending updates, so a
        // rejected-and-closed update reports its worker rejection, not
        // `AbortedByClosingWorkflow` (`TestCompleteWorkflow_AbortUpdates/`
        // `update_rejected_*`). The held waiters are resolved once the
        // completion durably commits.
        let held_rejections: Vec<(tokeira_types::Payload, Vec<crate::update::UpdateWaiter>)> =
            rejected_updates
                .into_iter()
                .map(|(update_id, failure)| {
                    let waiters = self.update_registry.take_entry_waiters(run_key, &update_id);
                    (failure, waiters)
                })
                .collect();
        let cron_continuation = self.cron_continuation_for_completion(run_key, &req).await?;
        // Retry is evaluated before cron (retry.go @ v1.31.0). The cron helper
        // already declines when a retry policy is present on a FailWorkflow, so
        // the two are mutually exclusive here; compute retry only when cron did
        // not claim the completion.
        let retry_continuation = if cron_continuation.is_none() {
            self.retry_continuation_for_completion(run_key, &req)
                .await?
        } else {
            None
        };
        // Capture the successor id before `req` moves into the command: a Retry
        // continuation means the committed WorkflowExecutionFailed will carry
        // this new_execution_run_id, and we must start that successor run after
        // the predecessor's close commits (Req 2.2).
        let retry_successor_run_id = match &retry_continuation {
            Some(RetryContinuation::Retry { new_run_id }) => Some(*new_run_id),
            _ => None,
        };
        // A cron completion/failure closes with its real outcome carrying this
        // successor id; the runtime starts the cron successor after the close
        // commits (the same derived-effect posture as the retry successor).
        let cron_successor = cron_continuation.as_ref().map(|cron| {
            (
                cron.new_run_id,
                cron.cron_schedule.clone(),
                cron.input.clone(),
            )
        });
        // Anchor the cron backoff on the completion time (`req.now`), captured before
        // `req` moves into the command below.
        let completion_now = req.now;
        // A server-decided failure of this completion is authored on behalf of
        // the same authenticated worker call. Preserve its edge-derived
        // principal before moving the request into the first kernel attempt.
        let failure_request = req.request.clone();
        // Mutable policy is resolved exactly once at the runtime boundary.
        // The pure kernel receives concrete transition input and never reads
        // the conformance registry or retains configuration.
        req.limits = workflow_task_completion_limits();
        let command = match (retry_continuation, cron_continuation) {
            (Some(retry_continuation), _) => Command::WorkflowTaskCompletedWithRetry {
                request: req,
                retry_continuation,
            },
            (None, Some(cron_continuation)) => Command::WorkflowTaskCompletedWithCron {
                request: req,
                cron_continuation,
            },
            (None, None) => Command::WorkflowTaskCompleted(req),
        };
        let result = self.submit_for_owned_shard(run_key, command).await;
        // v1.31.0's invalid-command contract: the completion is discarded, the
        // WORKFLOW TASK is failed with the command's cause (persisted — unless
        // a transient attempt keeps failing on a non-UnhandledCommand cause,
        // which is dropped to time out instead), and the completion call
        // errors with INVALID_ARGUMENT carrying the cause message
        // (respondworkflowtaskcompleted/api.go:455-485,739-742).
        if let Err(error) = &result
            && let Some(crate::lane::KernelRejected(
                tokeira_kernel::Reject::InvalidCommandAttributes { cause, message },
            )) = error.downcast_ref()
        {
            let cause = cause.clone();
            // The wire message mirrors `workflowTaskFailedCause.Message()`:
            // `"{cause}: {causeErr}"` when a cause error exists, the bare
            // cause name otherwise (workflow_task_completed_handler.go:
            // 1502-1510 @ v1.31.0). The same rendering is persisted as the
            // WFT-failed event's server failure
            // (`failure.NewServerFailure(wtFailedCause.Message(), false)`,
            // respondworkflowtaskcompleted/api.go:1049-1059 @ v1.31.0).
            let wire_message = match message {
                Some(cause_err) => format!("{}: {}", cause.as_str(), cause_err),
                None => cause.as_str().to_string(),
            };
            runtime_metrics::record_workflow_task_completed(OutcomeLabel::Rejected);
            // R.3: a server-decided WFT failure (invalid command) aborts Sent
            // update waiters too — v1.31.0's `AbortReasonWorkflowTaskFailed`
            // fires for every `wtFailedCause != nil` completion, not only bad
            // update messages (respondworkflowtaskcompleted/api.go:454-460 @
            // v1.31.0). Fired before the WFT-failed persist (see the bad-message
            // arm) so it reads the still-Sent state.
            self.update_registry.abort_sent_for_wft_failure(run_key);
            let is_unhandled_command =
                cause == tokeira_kernel::WorkflowTaskFailedCause::UnhandledCommand;
            let drop_without_failing = completion_token.attempt > 1 && !is_unhandled_command;
            if !drop_without_failing
                && let Err(error) = self
                    .submit_for_owned_shard(
                        run_key,
                        Command::WorkflowTaskFailed(tokeira_kernel::WorkflowTaskFailedRequest {
                            logical_seq: completion_token.logical_seq,
                            started_event_id: completion_token.started_event_id,
                            failure_cause: cause,
                            failure_details: Some(server_failure_payload(&wire_message)),
                            worker_identity: completion_identity,
                            request: failure_request.clone(),
                            now: OffsetDateTime::now_utc(),
                            reset_reapply: Vec::new(),
                        }),
                    )
                    .await
            {
                tracing::warn!(
                    ?error,
                    run_key = ?run_key,
                    "failed to persist WFT failure for invalid command"
                );
            }
            // The workflow tried to close and bounced off buffered events:
            // while the RETRY WFT is started, new signals are rejected with
            // WorkflowClosing. Set only after the WFT-failed persist so a
            // concurrent signal cannot be rejected while the OLD attempt
            // still shows as started — v1.31.0 sets `workflowCloseAttempted`
            // atomically with the UNHANDLED_COMMAND failure event
            // (workflow_task_state_machine.go:924-928).
            if is_unhandled_command {
                self.close_attempt_tracking
                    .lock()
                    .expect("close-attempt tracking lock")
                    .insert(run_key);
            }
            return Err(crate::errors::InvalidWorkflowCommand {
                message: wire_message,
            }
            .into());
        }
        // Sibling of the invalid-command seam for bad update protocol
        // messages (spec speculative-wft K5, Req 6.1/6.2): v1.31.0 routes
        // every such message through `failWorkflowTask(
        // BAD_UPDATE_WORKFLOW_EXECUTION_MESSAGE, causeErr)` — the WFT-failed
        // event persists (dropped on a still-failing transient attempt,
        // respondworkflowtaskcompleted/api.go:478-481) with the composed
        // `wtFailedCause.Message()` server failure
        // (`failure.NewServerFailure(wtFailedCause.Message(), false)`,
        // api.go:1049-1059 @ v1.31.0), and the completion call errors with
        // the causeErr: NotFound for the unknown-update case, InvalidArgument
        // for the wrong-state / bad-sequencing cases (Req 6.2).
        if let Err(error) = &result
            && let Some(crate::lane::KernelRejected(tokeira_kernel::Reject::BadUpdateMessage {
                message,
                not_found,
            })) = error.downcast_ref()
        {
            let message = message.clone();
            let not_found = *not_found;
            let cause = tokeira_kernel::WorkflowTaskFailedCause::BadUpdateWorkflowExecutionMessage;
            // Persisted rendering mirrors `workflowTaskFailedCause.Message()`
            // ("{cause}: {causeErr}",
            // workflow_task_completed_handler.go:1502-1510 @ v1.31.0).
            let persisted_message = format!("{}: {}", cause.as_str(), message);
            runtime_metrics::record_workflow_task_completed(OutcomeLabel::Rejected);
            // R.3 (spec speculative-wft Req 6.3): a SERVER-decided WFT failure
            // aborts every Sent update waiter with the non-retryable
            // WorkflowNotReady `workflowTaskFailErr` — distinct from an explicit
            // RespondWorkflowTaskFailed, which leaves the update admitted for
            // redelivery (`AbortReasonWorkflowTaskFailed`, abort_reason.go:86-103
            // @ v1.31.0; `TestValidateWorkerMessages`,
            // `TestSpeculativeWorkflowTask_Fail`). Fired before the WFT-failed
            // persist so the abort reads the still-Sent state — the persisted
            // event's `reset_sent_for_run` would otherwise clear it first.
            self.update_registry.abort_sent_for_wft_failure(run_key);
            // Same transient-attempt drop rule as the invalid-command arm:
            // attempt > 1 on a non-UnhandledCommand cause is dropped to time
            // out instead of persisting a failure event (api.go:478-481
            // @ v1.31.0).
            let drop_without_failing = completion_token.attempt > 1;
            if !drop_without_failing
                && let Err(error) = self
                    .submit_for_owned_shard(
                        run_key,
                        Command::WorkflowTaskFailed(tokeira_kernel::WorkflowTaskFailedRequest {
                            logical_seq: completion_token.logical_seq,
                            started_event_id: completion_token.started_event_id,
                            failure_cause: cause,
                            failure_details: Some(server_failure_payload(&persisted_message)),
                            worker_identity: completion_identity,
                            request: failure_request,
                            now: OffsetDateTime::now_utc(),
                            reset_reapply: Vec::new(),
                        }),
                    )
                    .await
            {
                tracing::warn!(
                    ?error,
                    run_key = ?run_key,
                    "failed to persist WFT failure for bad update message"
                );
            }
            if not_found {
                return Err(crate::errors::UpdateMessageNotFound { message }.into());
            }
            return Err(crate::errors::InvalidWorkflowCommand { message }.into());
        }
        match &result {
            Ok(CommitResult::Applied { .. } | CommitResult::Duplicate) => {
                // A successfully completed WFT means the workflow observed the
                // buffered events and moved on — a stale close-attempt must not
                // keep rejecting signals (v1.31.0's volatile bit dies with the
                // reloaded mutable state).
                self.close_attempt_tracking
                    .lock()
                    .expect("close-attempt tracking lock")
                    .remove(&run_key);
                // The consecutive-problem count and its derived search
                // attribute clear inside the kernel's completion transition
                // (`workflow_task_state_machine.go:838-846 @ v1.31.0`).
                // Publish rejection outcomes now that the completion is durable:
                // the rejected update left the kernel's admitted set with no
                // event, and its waiters were pre-claimed above so the close
                // drain could not abort them first.
                for (failure, waiters) in held_rejections {
                    self.update_registry.resolve_waiters(
                        waiters,
                        crate::update::UpdateResolution::Rejected { failure },
                    );
                }
                // R.4 (spec speculative-wft Req 9): after a non-heartbeat
                // completion that left the run open, updates the worker was
                // sent but neither accepted nor rejected are auto-rejected with
                // the server-authored `unprocessedUpdateFailure` and never
                // redelivered (`rejectUnprocessedUpdates`,
                // workflow_task_completed_handler.go:213-262 @ v1.31.0). Runs
                // after the acceptance/rejection notifies above so the
                // remaining Sent-not-accepted entries are exactly the ignored
                // set. The kernel pruned the same ids from its admitted set in
                // the completion transition (via `delivered_update_ids`) so the
                // follow-up task carries only the mid-task admissions.
                let run_still_open = match &result {
                    Ok(CommitResult::Applied { new_state }) => new_state.closed_at.is_none(),
                    _ => false,
                };
                if !is_heartbeat_completion && run_still_open {
                    self.update_registry.reject_unprocessed(run_key);
                }
                runtime_metrics::record_workflow_task_completed(OutcomeLabel::Success);
            }
            Ok(CommitResult::Conflict { .. } | CommitResult::CurrentExecutionConflict { .. })
            | Err(_) => {
                runtime_metrics::record_workflow_task_completed(OutcomeLabel::Failure);
            }
        }
        // Start the attempt-N+1 successor once the predecessor's Failed-with-retry
        // close is durable. The successor start is a derived effect of the
        // authoritative close: if it fails, the predecessor close still stands
        // (Req 2.2.3), mirroring the continue-as-new successor posture. The
        // successor run id is fixed by the committed new_execution_run_id, so a
        // re-drive is idempotent on the derived run key.
        if let (Ok(CommitResult::Applied { .. }), Some(new_run_id)) =
            (&result, retry_successor_run_id)
            && let Err(error) = self.start_retry_successor(run_key, new_run_id).await
        {
            tracing::error!(
                ?error,
                predecessor_run_key = ?run_key,
                "failed to start workflow retry successor",
            );
        }
        // Start the cron successor once the predecessor's real-outcome close is
        // durable — same derived-effect posture as retry.
        if let (Ok(CommitResult::Applied { .. }), Some((new_run_id, cron_schedule, input))) =
            (&result, cron_successor)
            && let Err(error) = self
                .start_cron_successor(run_key, new_run_id, cron_schedule, input, completion_now)
                .await
        {
            tracing::error!(
                ?error,
                predecessor_run_key = ?run_key,
                "failed to start workflow cron successor",
            );
        }
        result
    }

    /// `RespondWorkflowTaskFailed`: route on the reported cause.
    /// `GrpcMessageTooLarge` terminates the run through the kernel's
    /// force-close-then-terminate command instead of retrying the task
    /// (`respondworkflowtaskfailed/api.go:88 @ v1.31.0`); every other cause
    /// takes the WFT-failed retry path. The terminate reason is the cause's
    /// v1.31.0 `String()` rendering and the identity is the internal
    /// history-service identity (`consts.IdentityHistoryService`).
    pub async fn fail_workflow_task(
        &self,
        token: WorkflowTaskToken,
        failure_cause: tokeira_kernel::WorkflowTaskFailedCause,
        failure_details: Option<Payload>,
        worker_identity: WorkerIdentity,
        request: RequestContext,
        now: OffsetDateTime,
    ) -> Result<CommitResult> {
        self.validate_workflow_task_token(&token).await?;
        let run_key = token.run_key;
        let command = if matches!(
            failure_cause,
            tokeira_kernel::WorkflowTaskFailedCause::GrpcMessageTooLarge
        ) {
            Command::TerminateOnWorkflowTaskFailed(
                tokeira_kernel::TerminateOnWorkflowTaskFailedRequest {
                    logical_seq: token.logical_seq,
                    started_event_id: token.started_event_id,
                    reason: "GrpcMessageTooLarge".to_string(),
                    identity: "history-service".to_string(),
                    request,
                    now,
                },
            )
        } else {
            Command::WorkflowTaskFailed(tokeira_kernel::WorkflowTaskFailedRequest {
                logical_seq: token.logical_seq,
                started_event_id: token.started_event_id,
                failure_cause,
                failure_details,
                worker_identity,
                request,
                now,
                reset_reapply: Vec::new(),
            })
        };
        // The consecutive-problem accumulator advances inside the kernel's
        // failed/timed-out transitions themselves — RPC-level failures,
        // transient retries, and timeout-driven retries all count at the
        // transition that records them, mirroring v1.31.0's single
        // `failWorkflowTask` funnel
        // (`workflow_task_state_machine.go:1010-1027 @ v1.31.0`).
        self.submit_for_owned_shard(run_key, command).await
    }

    async fn cron_continuation_for_completion(
        &self,
        run_key: RunKey,
        req: &WorkflowTaskCompletedRequest,
    ) -> Result<Option<CronContinuation>> {
        let closes_for_cron = req.commands.iter().any(|command| {
            matches!(command, WorkflowCommand::CompleteWorkflow { .. })
                || matches!(command, WorkflowCommand::FailWorkflow { .. })
        });
        if !closes_for_cron {
            return Ok(None);
        }

        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Ok(None);
        };
        if state.retry_policy.is_some()
            && req
                .commands
                .iter()
                .any(|command| matches!(command, WorkflowCommand::FailWorkflow { .. }))
        {
            return Ok(None);
        }

        let start_event = self.repo.read_history(run_key, 0, 1).await?;
        let (input, cron_schedule) = match start_event.first().map(|event| &event.kind) {
            Some(HistoryEventKind::WorkflowExecutionStarted {
                input,
                cron_schedule: Some(cron_schedule),
                ..
            })
            | Some(HistoryEventKind::WorkflowExecutionStartedV2 {
                input,
                cron_schedule: Some(cron_schedule),
                ..
            }) => (input, cron_schedule),
            _ => return Ok(None),
        };
        if cron_schedule.is_empty() {
            return Ok(None);
        }

        // Temporal's cron close path creates the successor immediately but
        // records a first-WFT backoff on that successor
        // (`service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go:1383`,
        // `service/history/workflow/mutable_state_impl.go:2601 @ v1.31.0`).
        // Runtime owns the wall-clock cron calculation; kernel owns the
        // durable event that makes the successor replayable.
        Ok(Some(CronContinuation {
            new_run_id: RunId::new(),
            first_workflow_task_backoff: cron_initial_backoff(cron_schedule, req.now)?,
            input: input.clone(),
            cron_schedule: cron_schedule.clone(),
        }))
    }

    /// Evaluate the workflow retry decision for a WFT completion that fails the
    /// run, returning the [`RetryContinuation`] to record on
    /// `WorkflowExecutionFailed`, or `None` when no retry continuation applies (no
    /// `FailWorkflow` command, or no retry policy — the kernel maps those to the
    /// terminal `RetryPolicyNotSet`).
    ///
    /// Mirrors `service/history/workflow/retry.go:32-116` +
    /// `mutable_state_impl.go:1630 @ v1.31.0`: a run with a retry policy retries on
    /// failure unless the failure is non-retryable, the maximum attempts are
    /// reached, or the successor's first attempt would begin at/after the
    /// workflow-execution deadline. The evaluation (failure decoding, wall clock,
    /// backoff) is a runtime concern; the kernel only records the outcome (Req 2.1).
    async fn retry_continuation_for_completion(
        &self,
        run_key: RunKey,
        req: &WorkflowTaskCompletedRequest,
    ) -> Result<Option<RetryContinuation>> {
        let Some(failure) = req.commands.iter().find_map(|command| match command {
            WorkflowCommand::FailWorkflow { failure } => Some(failure.clone()),
            _ => None,
        }) else {
            return Ok(None);
        };
        let LoadedRun::Existing(state) = self.repo.load_run(run_key).await? else {
            return Ok(None);
        };
        let Some(policy) = state.retry_policy.clone() else {
            return Ok(None);
        };

        if !workflow_failure_is_retryable(&failure, &policy.non_retryable_error_types) {
            return Ok(Some(RetryContinuation::Terminal {
                retry_state: RetryState::NonRetryableFailure,
            }));
        }
        // `maximum_attempts == 0` means unlimited (v1.31.0 NoInterval semantics).
        if policy.maximum_attempts > 0 && state.attempt >= policy.maximum_attempts {
            return Ok(Some(RetryContinuation::Terminal {
                retry_state: RetryState::MaximumAttemptsReached,
            }));
        }
        // Execution-expiration cap: if the next attempt could not begin before the
        // workflow-execution deadline, the chain ends as Timeout (retry.go). The
        // deadline is anchored on the first run's start so it spans the whole chain.
        let backoff = retry_backoff(&policy, state.attempt);
        if let Some(execution_timeout) = state.workflow_execution_timeout {
            let anchor = state.first_run_started_at.unwrap_or(state.started_at);
            if req.now + backoff >= anchor + execution_timeout {
                return Ok(Some(RetryContinuation::Terminal {
                    retry_state: RetryState::Timeout,
                }));
            }
        }
        Ok(Some(RetryContinuation::Retry {
            new_run_id: RunId::new(),
        }))
    }

    /// Start the attempt-N+1 successor after a Failed-with-retry close commits.
    ///
    /// Mirrors the continue-as-new / cron successor start in `lane.rs`: the
    /// successor inherits the run's type/queue/input/policy/timeouts, chains its
    /// lineage (`continued_execution_run_id`, first-run identity), carries the
    /// predecessor's failure as `continued_failure`, and delays its first workflow
    /// task by the recomputed backoff. The original input re-runs on retry, read
    /// from the run's `WorkflowExecutionStarted` event (the same source cron uses).
    /// The successor start is submitted through the normal run-routing path so it
    /// lands on the owning lane; `Duplicate` is treated as success because a
    /// re-driven close mints the same derived run key (Req 2.2, 5.1.3).
    async fn start_retry_successor(
        &self,
        predecessor_run_key: RunKey,
        new_run_id: RunId,
    ) -> Result<()> {
        let LoadedRun::Existing(state) = self.repo.load_run(predecessor_run_key).await? else {
            return Err(anyhow!("predecessor run not found for retry successor"));
        };
        let Some(policy) = state.retry_policy.clone() else {
            return Err(anyhow!("retry successor requested without a retry policy"));
        };
        let start_event = self.repo.read_history(predecessor_run_key, 0, 1).await?;
        let (input, started_versioning_info) = match start_event.first().map(|event| &event.kind) {
            Some(HistoryEventKind::WorkflowExecutionStarted {
                input,
                versioning_info,
                ..
            })
            | Some(HistoryEventKind::WorkflowExecutionStartedV2 {
                input,
                versioning_info,
                ..
            }) => (input.clone(), versioning_info.as_ref()),
            _ => (Payloads::default(), None),
        };
        let start_request = build_retry_successor_start(
            &state,
            started_versioning_info,
            &policy,
            input,
            new_run_id,
        );
        let successor_run_key = start_request.run_key;
        let shard_id = self.shard_id_for(successor_run_key).await;
        match self
            .submit(successor_run_key, Command::Start(start_request))
            .await?
        {
            CommitResult::Applied { new_state } => {
                if new_state.workflow_execution_timeout.is_some()
                    || new_state.workflow_run_timeout.is_some()
                {
                    self.workflow_timeout_tracking.insert(WorkflowTimeoutEntry {
                        run_key: new_state.run_key,
                        shard_id,
                        workflow_execution_timeout: new_state.workflow_execution_timeout,
                        workflow_run_timeout: new_state.workflow_run_timeout,
                        started_at: new_state.started_at,
                        workflow_start_delay: new_state.workflow_start_delay,
                        first_run_started_at: new_state.first_run_started_at,
                        has_retry_policy: new_state.retry_policy.is_some(),
                    });
                }
                Ok(())
            }
            CommitResult::Duplicate => Ok(()),
            CommitResult::Conflict { reason } => {
                Err(anyhow!("retry successor start conflicted: {reason}"))
            }
            CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } => Err(anyhow!(
                "retry successor current-execution conflict: {existing_run_key:?}"
            )),
        }
    }

    /// Start the cron successor after a cron run's real-outcome close commits
    /// (complete / fail). The successor carries the predecessor's failure (if it
    /// failed) as `continued_failure` and the last SUCCESSFUL completion result:
    /// a completion sets `close_result`, so it becomes the successor's
    /// `last_completion_result`; a failure leaves `close_result` empty, so the
    /// previously-carried result flows through instead. Derived-effect posture:
    /// the close stands even if the successor start fails; `cron-successor:*` is
    /// a deterministic request id so a re-drive dedupes.
    async fn start_cron_successor(
        &self,
        predecessor_run_key: RunKey,
        new_run_id: RunId,
        cron_schedule: String,
        input: Payloads,
        now: OffsetDateTime,
    ) -> Result<()> {
        let LoadedRun::Existing(state) = self.repo.load_run(predecessor_run_key).await? else {
            return Err(anyhow!("predecessor run not found for cron successor"));
        };
        let continued_failure = state.close_failure.clone();
        let last_completion_result = state
            .close_result
            .clone()
            .or_else(|| state.last_completion_result.clone());
        let start_request = build_cron_successor_start(
            &state,
            cron_schedule,
            input,
            new_run_id,
            now,
            OffsetDateTime::now_utc(),
            continued_failure,
            last_completion_result,
        )
        .map_err(|error| anyhow!("invalid cron schedule on continuation: {error:?}"))?;
        let successor_run_key = start_request.run_key;
        let shard_id = self.shard_id_for(successor_run_key).await;
        match self
            .submit(successor_run_key, Command::Start(start_request))
            .await?
        {
            CommitResult::Applied { new_state } => {
                if new_state.workflow_execution_timeout.is_some()
                    || new_state.workflow_run_timeout.is_some()
                {
                    self.workflow_timeout_tracking.insert(WorkflowTimeoutEntry {
                        run_key: new_state.run_key,
                        shard_id,
                        workflow_execution_timeout: new_state.workflow_execution_timeout,
                        workflow_run_timeout: new_state.workflow_run_timeout,
                        started_at: new_state.started_at,
                        workflow_start_delay: new_state.workflow_start_delay,
                        first_run_started_at: new_state.first_run_started_at,
                        has_retry_policy: new_state.retry_policy.is_some(),
                    });
                }
                Ok(())
            }
            CommitResult::Duplicate => Ok(()),
            CommitResult::Conflict { reason } => {
                Err(anyhow!("cron successor start conflicted: {reason}"))
            }
            CommitResult::CurrentExecutionConflict {
                existing_run_key, ..
            } => Err(anyhow!(
                "cron successor current-execution conflict: {existing_run_key:?}"
            )),
        }
    }

    /// Atomically transition a polled workflow task into the Started state.
    async fn start_polled_workflow_task(
        &self,
        offered: DispatchableWorkflowTask,
        entered_at: tokio::time::Instant,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let span = tracing::info_span!(
            "workflow_task.process",
            tokeira.run_key = %offered.run_key.0,
            tokeira.workflow_id = tracing::field::Empty,
            tokeira.run_id = tracing::field::Empty,
            tokeira.task_queue = %offered.queue.task_queue.0,
            tokeira.workflow_task_sequence = offered.logical_seq.0,
            tokeira.attempt = tracing::field::Empty,
        );
        let result = self
            .start_polled_workflow_task_inner(offered, entered_at, worker_identity)
            .instrument(span.clone())
            .await;
        if let Ok(started) = &result {
            span.record("tokeira.workflow_id", started.workflow_id.0.as_str());
            span.record("tokeira.run_id", started.run_id.0.to_string());
            span.record("tokeira.attempt", i64::from(started.token.attempt));
        }
        result
    }

    async fn start_polled_workflow_task_inner(
        &self,
        offered: DispatchableWorkflowTask,
        entered_at: tokio::time::Instant,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let target = self.resolve_polled_workflow_task_target(&offered).await?;
        tracing::debug!(
            run_key = %offered.run_key.0,
            task_queue = %offered.queue.task_queue.0,
            target_deployment = target
                .resolved
                .deployment_version
                .as_ref()
                .map(|version| version.deployment_name.as_str()),
            target_build_id = target
                .resolved
                .deployment_version
                .as_ref()
                .map(|version| version.build_id.as_str()),
            transition_deployment = target
                .deployment_transition
                .as_ref()
                .map(|version| version.deployment_name.as_str()),
            transition_build_id = target
                .deployment_transition
                .as_ref()
                .map(|version| version.build_id.as_str()),
            revision_number = target.resolved.revision_number,
            pinned = target.resolved.pinned,
            "resolved worker deployment target for polled workflow task"
        );
        let now = OffsetDateTime::now_utc();
        let request = StartWorkflowTaskRequest {
            logical_seq: offered.logical_seq,
            worker_identity: worker_identity.clone(),
            request_id: uuid::Uuid::new_v4().to_string(),
            history_size_bytes: 0,
            suggest_continue_as_new: false,
            deployment_transition: target.deployment_transition.clone(),
            deployment_transition_revision_number: target
                .deployment_transition
                .as_ref()
                .map(|_| target.resolved.revision_number),
            target_version_changed_enabled: target_version_changed_enabled(),
            target_deployment_version: target.routing_target,
            polled_task_queue: offered.queue.task_queue.clone(),
            now,
        };
        let result = match self
            .submit(offered.run_key, Command::WorkflowTaskStarted(request))
            .await
        {
            Ok(result) => result,
            Err(error) => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(error);
            }
        };

        let new_state = match result {
            CommitResult::Applied { new_state } => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Success);
                new_state
            }
            CommitResult::Conflict { reason } => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(anyhow!(
                    "failed to start workflow task due to conflict: {reason}"
                ));
            }
            CommitResult::CurrentExecutionConflict { .. } => {
                // Unreachable for a workflow-task start (not a zero-seq start).
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(anyhow!(
                    "unexpected current-execution conflict while starting workflow task"
                ));
            }
            CommitResult::Duplicate => {
                runtime_metrics::record_workflow_task_started(OutcomeLabel::Failure);
                return Err(anyhow!("unexpected duplicate while starting workflow task"));
            }
        };

        let pending = new_state
            .pending_workflow_task
            .clone()
            .ok_or_else(|| anyhow!("workflow task missing after start"))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| anyhow!("workflow task started without started_event_id"))?;

        let token = WorkflowTaskToken {
            run_key: new_state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id,
            attempt: pending.attempt,
            shard_epoch: self.current_shard_epoch(new_state.run_key).await?,
        };
        let shard_id = self.shard_id_for(new_state.run_key).await;
        // A SPECULATIVE task's start-to-close is enforced by the precise
        // in-memory timer the lane armed on this same WorkflowTaskStarted commit
        // (spec speculative-wft R.2), so it is kept out of the coarse sweep.
        if pending.task_type != tokeira_kernel::WorkflowTaskType::Speculative {
            self.wft_timeout_tracking.insert(WftTimeoutEntry {
                kind: WftTimeoutKind::StartToClose,
                run_key: new_state.run_key,
                shard_id,
                logical_seq: pending.logical_seq,
                started_event_id,
                started_at: pending.started_at.unwrap_or(now),
                workflow_task_timeout: new_state.workflow_task_timeout,
            });
        }
        self.delivery_metrics
            .record_latency(&offered.queue, entered_at.elapsed());

        // Partial-history attach is licensed by STICKY-QUEUE dispatch, not by
        // worker-identity affinity: v1.31.0 trims history to
        // previous_started+1 only when the run's sticky queue is set
        // (`setHistoryForRecordWfTaskStartedResp`,
        // recordworkflowtaskstarted/api.go:272-278; `IsStickyTaskQueueSet`,
        // api.go:418 @ v1.31.0). Tokeira's degraded identity-only hint (empty
        // sticky queue, e.g. a transient retry preferring the failing worker)
        // dispatches on the NORMAL queue and must attach full history — the
        // dispatch queue differing from the run's queue is the sticky-dispatch
        // marker (`schedule_workflow_task` dispatches on the sticky queue
        // name; broker redirects rewrite only build ids, never queue names).
        let is_sticky_match = offered.queue.task_queue != new_state.task_queue
            && offered.sticky_preferred.as_ref() == Some(&worker_identity);
        let origin = WorkerTaskOrigin::from_queue_key(
            &offered.queue,
            new_state.task_queue.clone(),
            WorkerTaskClass::Workflow,
        );
        Ok(StartedWorkflowTask {
            run_key: new_state.run_key,
            run_id: new_state.run_id,
            workflow_id: new_state.workflow_id,
            task_queue: new_state.task_queue,
            previous_started_event_id: new_state.previous_started_event_id,
            is_sticky_match,
            scheduled_time: pending.scheduled_at,
            started_time: pending.started_at.unwrap_or(now),
            workflow_task_timeout: new_state.workflow_task_timeout,
            worker_identity,
            target_worker_deployment_version_changed: pending
                .target_worker_deployment_version_changed,
            token,
            origin,
        })
    }

    pub(super) async fn started_workflow_task_from_state(
        &self,
        state: &WorkflowState,
        is_sticky_match: bool,
        worker_identity: WorkerIdentity,
    ) -> Result<StartedWorkflowTask> {
        let pending = state
            .pending_workflow_task
            .clone()
            .ok_or_else(|| anyhow!("workflow task missing after direct start"))?;
        let started_event_id = pending
            .started_event_id
            .ok_or_else(|| anyhow!("direct-start workflow task missing started_event_id"))?;
        let started_at = pending
            .started_at
            .ok_or_else(|| anyhow!("direct-start workflow task missing started_at"))?;
        let token = WorkflowTaskToken {
            run_key: state.run_key,
            logical_seq: pending.logical_seq,
            started_event_id,
            attempt: pending.attempt,
            shard_epoch: self.current_shard_epoch(state.run_key).await?,
        };
        let shard_id = self.shard_id_for(state.run_key).await;
        // Speculative tasks use the precise in-memory timer (armed by the lane
        // on the start commit), not the coarse sweep (spec speculative-wft R.2).
        if pending.task_type != tokeira_kernel::WorkflowTaskType::Speculative {
            self.wft_timeout_tracking.insert(WftTimeoutEntry {
                kind: WftTimeoutKind::StartToClose,
                run_key: state.run_key,
                shard_id,
                logical_seq: pending.logical_seq,
                started_event_id,
                started_at,
                workflow_task_timeout: state.workflow_task_timeout,
            });
        }
        let queue = QueueKey {
            namespace_id: state.namespace_id,
            task_queue: state.task_queue.clone(),
            task_kind: TaskKind::Workflow,
            deployment: state.deployment.clone(),
            build_id: state.build_id.clone(),
        };
        Ok(StartedWorkflowTask {
            run_key: state.run_key,
            run_id: state.run_id,
            workflow_id: state.workflow_id.clone(),
            task_queue: state.task_queue.clone(),
            previous_started_event_id: state.previous_started_event_id,
            is_sticky_match,
            scheduled_time: pending.scheduled_at,
            started_time: started_at,
            workflow_task_timeout: state.workflow_task_timeout,
            worker_identity: worker_identity.clone(),
            target_worker_deployment_version_changed: pending
                .target_worker_deployment_version_changed,
            token,
            origin: WorkerTaskOrigin::from_queue_key(
                &queue,
                state.task_queue.clone(),
                WorkerTaskClass::Workflow,
            ),
        })
    }
}

/// Workflow-retry failure classification, mirroring `isRetryable`
/// (`service/history/workflow/retry.go:115 @ v1.31.0`) exactly:
///
/// - no / undecodable / info-less failure → **retryable** (nil → true and the
///   trailing default → true);
/// - Terminated / Canceled info → not retryable;
/// - Timeout info → retryable only for StartToClose / Heartbeat, and then only
///   when `"TemporalTimeout:" + type` is absent from the policy's
///   non-retryable types (`retrypolicy.TimeoutFailureTypePrefix`,
///   retry_policy.go:19);
/// - Server info → `!non_retryable` (the corpus drives workflow retry with
///   `failure.NewServerFailure`, tests/workflow_test.go:1543);
/// - Application info → `!non_retryable` and `type` not excluded.
///
/// Decoding the proto failure lives here because the retry decision is a
/// runtime concern (the kernel stays proto-free).
fn workflow_failure_is_retryable(failure: &Payload, non_retryable_types: &[String]) -> bool {
    use tokeira_proto::enums::TimeoutType;
    let Ok(decoded) = Failure::decode(failure.data.as_slice()) else {
        return true;
    };
    match decoded.failure_info {
        Some(FailureInfo::TerminatedFailureInfo(_)) | Some(FailureInfo::CanceledFailureInfo(_)) => {
            false
        }
        Some(FailureInfo::TimeoutFailureInfo(timeout)) => {
            let (retryable_kind, type_name) = match TimeoutType::try_from(timeout.timeout_type) {
                Ok(TimeoutType::StartToClose) => (true, "StartToClose"),
                Ok(TimeoutType::Heartbeat) => (true, "Heartbeat"),
                _ => (false, ""),
            };
            retryable_kind
                && !non_retryable_types
                    .iter()
                    .any(|excluded| excluded == &format!("TemporalTimeout:{type_name}"))
        }
        Some(FailureInfo::ServerFailureInfo(server)) => !server.non_retryable,
        Some(FailureInfo::ApplicationFailureInfo(app)) => {
            !app.non_retryable
                && !non_retryable_types
                    .iter()
                    .any(|excluded| excluded == &app.r#type)
        }
        _ => true,
    }
}

/// Exponential retry backoff for the attempt-N+1 successor:
/// `initial_interval × backoff_coefficient^(attempt-1)`, capped by
/// `maximum_interval` when set (`retry.go @ v1.31.0`). Wall-clock-free and
/// deterministic, so the retry decision and the successor start compute the same
/// delay without threading it through the kernel.
pub(crate) fn retry_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1) as f64;
    let seconds =
        policy.initial_interval.as_seconds_f64() * policy.backoff_coefficient.powf(exponent);
    let capped = match policy.maximum_interval {
        Some(max) if max.as_seconds_f64() > 0.0 => seconds.min(max.as_seconds_f64()),
        _ => seconds,
    };
    Duration::seconds_f64(capped.max(0.0))
}

/// The server failure persisted on an invalid-command `WorkflowTaskFailed`
/// event: message = the rendered cause message, `ServerFailureInfo`,
/// non-retryable false (`failure.NewServerFailure(wtFailedCause.Message(),
/// false)` in `failWorkflowTask`, respondworkflowtaskcompleted/api.go:1049-1059
/// @ v1.31.0).
fn server_failure_payload(message: &str) -> Payload {
    tokeira_proto::conversions::common::failure_to_payload(&Failure {
        message: message.to_string(),
        failure_info: Some(FailureInfo::ServerFailureInfo(
            tokeira_proto::failure::ServerFailureInfo {
                non_retryable: false,
            },
        )),
        ..Default::default()
    })
}

/// Whether a failed workflow-task START means the polled broker entry is stale —
/// the run's current pending task is a different one (superseded by a timeout
/// reschedule), already started, or gone. Such a task is discarded and the poll
/// retries rather than surfacing the error to the worker (Invariant I.1).
fn is_stale_workflow_task_start(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<crate::lane::KernelRejected>()
        .is_some_and(|rejected| {
            matches!(
                rejected.0,
                tokeira_kernel::Reject::WorkflowTaskSeqMismatch { .. }
                    | tokeira_kernel::Reject::WorkflowTaskAlreadyStarted { .. }
                    | tokeira_kernel::Reject::NoPendingWorkflowTask
            )
        })
}

/// Build the attempt-N+1 retry successor start from the CLOSED predecessor's
/// state — shared by the WFT-completion failure path and the workflow-timeout
/// scanner (a RUN timeout with attempts remaining also continues the retry
/// chain, timer_queue_active_task_executor.go:713-796 @ v1.31.0). The
/// successor inherits type/queue/input/policy/timeouts, chains its lineage
/// (`continued_execution_run_id`, first-run identity), and delays its first
/// workflow task by the recomputed backoff.
pub(crate) fn build_retry_successor_start(
    state: &tokeira_kernel::WorkflowState,
    started_versioning_info: Option<&WorkflowVersioningInfo>,
    policy: &tokeira_types::RetryPolicy,
    input: Payloads,
    new_run_id: RunId,
) -> StartRequest {
    let backoff = retry_backoff(policy, state.attempt);
    let successor_run_key = RunKey::derive(state.namespace_id, &state.workflow_id, new_run_id);
    // Chain origin propagates so execution-level timeouts and lineage queries
    // span the whole retry chain, not just this hop.
    let first_execution_run_id = Some(state.first_execution_run_id.unwrap_or(state.run_id));
    let first_run_started_at = Some(state.first_run_started_at.unwrap_or(state.started_at));
    // Root identity only propagates within a child lineage.
    let (root_workflow_id, root_run_id) = if state.parent_run_key.is_some() {
        (state.root_workflow_id.clone(), state.root_run_id)
    } else {
        (None, None)
    };
    let inherited_versioning_info = retry_successor_versioning_info(state, started_versioning_info);
    StartRequest {
        run_key: successor_run_key,
        namespace_id: state.namespace_id,
        workflow_id: state.workflow_id.clone(),
        run_id: new_run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        deployment: state.deployment.clone(),
        build_id: state.build_id.clone(),
        versioning_override: state.versioning_override().cloned(),
        workflow_start_delay: Some(backoff),
        completion_callbacks: state.completion_callbacks.clone(),
        user_metadata: state.user_metadata.clone(),
        links: Vec::new(),
        on_conflict_options: None,
        priority: state.priority.clone(),
        input,
        header: None,
        memo: state.memo.clone(),
        search_attributes: state.search_attributes.clone(),
        workflow_execution_timeout: state.workflow_execution_timeout,
        workflow_run_timeout: state.workflow_run_timeout,
        workflow_task_timeout: state.workflow_task_timeout,
        retry_policy: Some(policy.clone()),
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        // The attempt-N+1 run was created by the workflow-retry chain, so its
        // WorkflowExecutionStarted.Initiator is RETRY.
        initiator: Some(tokeira_kernel::ContinueAsNewInitiator::Retry),
        attempt: state.attempt + 1,
        continued_execution_run_id: Some(state.run_id),
        first_execution_run_id,
        // A retrying child stays a child of the same parent, so the attempt-N+1
        // run authors the parent linkage on its own WorkflowExecutionStarted
        // event. A top-level run carries None and stays parentless.
        parent_run_key: state.parent_run_key,
        parent_workflow_id: state.parent_workflow_id.clone(),
        parent_run_id: state.parent_run_id,
        parent_namespace_id: state.parent_namespace_id,
        parent_namespace_name: state.parent_namespace_name.clone(),
        parent_initiated_event_id: state.parent_initiated_event_id,
        root_workflow_id,
        root_run_id,
        original_execution_run_id: Some(state.original_execution_run_id.unwrap_or(state.run_id)),
        continued_failure: state.close_failure.clone(),
        last_completion_result: state.close_result.clone(),
        first_run_started_at,
        request: RequestContext {
            // Deterministic request id keyed on (predecessor, successor) so a
            // replayed close dedupes to the same successor start instead of
            // forking a duplicate run (mirrors the continue-as-new key).
            request_id: tokeira_types::RequestId(format!(
                "workflow-retry:{}:{}",
                state.run_id.0, new_run_id.0
            )),
            caller_identity: None,
            principal: None,
            received_at: OffsetDateTime::now_utc(),
        },
        now: OffsetDateTime::now_utc(),
        client_cron_schedule: None,
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info,
    }
}

fn retry_successor_versioning_info(
    state: &WorkflowState,
    started_versioning_info: Option<&WorkflowVersioningInfo>,
) -> Option<WorkflowVersioningInfo> {
    let started_decline =
        started_versioning_info.and_then(|info| info.declined_target_version_upgrade.clone());
    let pinned_override = state.versioning_override().and_then(|override_| {
        matches!(override_, VersioningOverride::Pinned { .. }).then(|| override_.clone())
    });
    let effective_behavior = state.effective_behavior();
    let effective_version = state.effective_deployment().cloned();
    let revision_number = state
        .versioning_info
        .as_ref()
        .map(|info| info.revision_number)
        .unwrap_or_default();
    let began_inherited_pinned =
        started_versioning_info.is_some_and(|info| info.behavior == VersioningBehavior::Pinned);

    let mut inherited = WorkflowVersioningInfo {
        versioning_override: pinned_override,
        declined_target_version_upgrade: started_decline,
        ..WorkflowVersioningInfo::default()
    };
    if effective_behavior == VersioningBehavior::Pinned && began_inherited_pinned {
        inherited.behavior = VersioningBehavior::Pinned;
        inherited.deployment_version = effective_version;
        inherited.revision_number = revision_number;
    } else if effective_behavior == VersioningBehavior::AutoUpgrade
        && effective_version.is_some()
        && revision_number != 0
    {
        inherited.behavior = VersioningBehavior::AutoUpgrade;
        inherited.deployment_version = effective_version;
        inherited.revision_number = revision_number;
        inherited.continue_as_new_initial_versioning_behavior = state
            .versioning_info
            .as_ref()
            .map(|info| info.continue_as_new_initial_versioning_behavior)
            .unwrap_or_default();
    }

    (inherited.has_execution_versioning_info()
        || inherited.declined_target_version_upgrade.is_some())
    .then_some(inherited)
}

/// The `Failure` payload a cron run's timeout hands to its successor, so the
/// SDK's `GetLastError` renders `"workflow timeout (type: StartToClose)"`. A
/// workflow run timeout surfaces to the workflow-failure layer as
/// `TIMEOUT_TYPE_START_TO_CLOSE` (retry.go / mutable_state_impl.go @ v1.31.0).
pub(crate) fn workflow_run_timeout_failure() -> tokeira_types::Payload {
    use tokeira_proto::failure::{Failure, TimeoutFailureInfo, failure::FailureInfo};
    tokeira_proto::conversions::common::failure_to_payload(&Failure {
        message: "workflow timeout".to_string(),
        failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: tokeira_proto::enums::TimeoutType::StartToClose as i32,
            ..Default::default()
        })),
        ..Default::default()
    })
}

/// Build the cron successor start after a cron run closes (complete / fail /
/// run-timeout). The successor is a SEPARATE run — v1.31.0 records the real
/// outcome event on the predecessor with `NewExecutionRunId` and starts the
/// successor via `handleCron` (workflow_task_completed_handler.go:730-738 +
/// SetupNewWorkflowForRetryOrCron @ v1.31.0), NOT a ContinueAsNew. Its first WFT
/// is delayed to the next cron trigger, its `Initiator` is `CRON_SCHEDULE`, it
/// inherits the chain deadline + parent linkage, and it carries the caller-
/// supplied `continued_failure` (the run's failure / timeout, so `GetLastError`
/// reports it) and `last_completion_result` (the last SUCCESSFUL result).
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_cron_successor_start(
    state: &tokeira_kernel::WorkflowState,
    cron_schedule: String,
    input: Payloads,
    new_run_id: RunId,
    now: OffsetDateTime,
    execution_started_at: OffsetDateTime,
    continued_failure: Option<tokeira_types::Payload>,
    last_completion_result: Option<Payloads>,
) -> Result<StartRequest, crate::schedule::ScheduleError> {
    // v1.31.0 anchors the next cron fire on the CLOSING run's scheduled (execution)
    // time and rounds up to the next whole second, so a run that outlived one or more
    // intervals still lands on the schedule's phase rather than `now + interval`
    // (`common/backoff/cron.go` `GetBackoffForNextSchedule`).
    let scheduled_time = state.started_at + state.workflow_start_delay.unwrap_or(Duration::ZERO);
    let backoff =
        crate::schedule::cron_backoff_for_next_schedule(&cron_schedule, scheduled_time, now)?;
    let successor_run_key = RunKey::derive(state.namespace_id, &state.workflow_id, new_run_id);
    let first_execution_run_id = Some(state.first_execution_run_id.unwrap_or(state.run_id));
    let first_run_started_at = Some(state.first_run_started_at.unwrap_or(state.started_at));
    let (root_workflow_id, root_run_id) = if state.parent_run_key.is_some() {
        (state.root_workflow_id.clone(), state.root_run_id)
    } else {
        (None, None)
    };
    Ok(StartRequest {
        run_key: successor_run_key,
        namespace_id: state.namespace_id,
        workflow_id: state.workflow_id.clone(),
        run_id: new_run_id,
        workflow_type: state.workflow_type.clone(),
        task_queue: state.task_queue.clone(),
        deployment: state.deployment.clone(),
        build_id: state.build_id.clone(),
        versioning_override: state.versioning_override().cloned(),
        workflow_start_delay: Some(backoff),
        completion_callbacks: state.completion_callbacks.clone(),
        user_metadata: state.user_metadata.clone(),
        links: Vec::new(),
        on_conflict_options: None,
        priority: state.priority.clone(),
        input,
        header: None,
        memo: state.memo.clone(),
        search_attributes: state.search_attributes.clone(),
        workflow_execution_timeout: state.workflow_execution_timeout,
        workflow_run_timeout: state.workflow_run_timeout,
        workflow_task_timeout: state.workflow_task_timeout,
        retry_policy: state.retry_policy.clone(),
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        initiator: Some(tokeira_kernel::ContinueAsNewInitiator::CronSchedule),
        attempt: 1,
        continued_execution_run_id: Some(state.run_id),
        first_execution_run_id,
        parent_run_key: state.parent_run_key,
        parent_workflow_id: state.parent_workflow_id.clone(),
        parent_run_id: state.parent_run_id,
        parent_namespace_id: state.parent_namespace_id,
        parent_namespace_name: state.parent_namespace_name.clone(),
        parent_initiated_event_id: state.parent_initiated_event_id,
        root_workflow_id,
        root_run_id,
        original_execution_run_id: Some(state.original_execution_run_id.unwrap_or(state.run_id)),
        continued_failure,
        last_completion_result,
        first_run_started_at,
        request: RequestContext {
            request_id: tokeira_types::RequestId(format!(
                "cron-successor:{}:{}",
                state.run_id.0, new_run_id.0
            )),
            caller_identity: None,
            principal: None,
            received_at: execution_started_at,
        },
        // The successor's `WorkflowExecutionStarted` time — and thus its start-delay
        // timer anchor — is the caller-provided execution start (wall-clock for a
        // completion successor; the precise timeout deadline for a timeout successor,
        // so the next fire lands on the schedule's phase free of scan jitter).
        now: execution_started_at,
        client_cron_schedule: None,
        cron_schedule: Some(cron_schedule),
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, HashSet};

    use proptest::prelude::*;
    use tokeira_kernel::{
        BasicKernel, CallbackSpec, CallbackState, CallbackTrigger, Command, CompletionCallback,
        ContinueAsNewVersioningBehavior, Kernel, LoadedRun, StartWorkflowTaskRequest,
        VersionTarget, WorkflowVersioningInfo,
    };
    use tokeira_storage::{BuildId as StorageBuildId, DeploymentName};
    use tokeira_types::{
        BuildId as RuntimeBuildId, DeploymentId, LogicalTaskSeq, Memo, SearchAttributes, TaskKind,
        WorkerIdentity, WorkflowType,
    };

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn completion_limit_resolution_matches_v1_31_policy(
            configured in prop::option::of(any::<i64>()),
        ) {
            // Feature: api-conformance-client-misc, Property 1: live completion-limit resolution
            let expected = match configured {
                None => Some(DEFAULT_PENDING_COMMAND_LIMIT),
                Some(value) if value <= 0 => None,
                Some(value) => usize::try_from(value).ok(),
            };

            prop_assert_eq!(normalize_pending_command_limit(configured), expected);
        }
    }

    fn app_failure(error_type: &str, non_retryable: bool) -> Payload {
        use prost::Message as _;
        use tokeira_proto::failure::{ApplicationFailureInfo, Failure, failure::FailureInfo};
        let failure = Failure {
            message: "boom".to_string(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: error_type.to_string(),
                    non_retryable,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };
        Payload::new(failure.encode_to_vec())
    }

    #[test]
    fn workflow_failure_retryable_classification() {
        // Feature: workflow-retry-chain — classification mirrors `isRetryable`
        // (service/history/workflow/retry.go:115 @ v1.31.0) across every
        // failure-info class, not just ApplicationFailureInfo.
        use prost::Message as _;
        use tokeira_proto::failure::{
            CanceledFailureInfo, Failure, ServerFailureInfo, TerminatedFailureInfo,
            TimeoutFailureInfo, failure::FailureInfo,
        };
        let encoded = |info: FailureInfo| {
            Payload::new(
                Failure {
                    message: "boom".to_string(),
                    failure_info: Some(info),
                    ..Default::default()
                }
                .encode_to_vec(),
            )
        };

        // Application failures: flag, then excluded-type check.
        assert!(workflow_failure_is_retryable(
            &app_failure("Boom", false),
            &[]
        ));
        assert!(!workflow_failure_is_retryable(
            &app_failure("Boom", true),
            &[]
        ));
        assert!(!workflow_failure_is_retryable(
            &app_failure("Fatal", false),
            &["Fatal".to_string()],
        ));

        // Server failures use only their non_retryable flag — the corpus drives
        // workflow retry with failure.NewServerFailure
        // (tests/workflow_test.go:1543 @ v1.31.0), and the excluded-type list
        // does NOT apply to them (retry.go:138-140).
        assert!(workflow_failure_is_retryable(
            &encoded(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                non_retryable: false,
            })),
            &["boom".to_string()],
        ));
        assert!(!workflow_failure_is_retryable(
            &encoded(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                non_retryable: true,
            })),
            &[],
        ));

        // Terminated / Canceled → never retryable (retry.go:120-122).
        assert!(!workflow_failure_is_retryable(
            &encoded(FailureInfo::TerminatedFailureInfo(
                TerminatedFailureInfo::default()
            )),
            &[],
        ));
        assert!(!workflow_failure_is_retryable(
            &encoded(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default()
            )),
            &[],
        ));

        // Timeouts: StartToClose/Heartbeat retry unless excluded via the
        // "TemporalTimeout:" prefix; other timeout kinds never retry
        // (retry.go:124-136; TimeoutFailureTypePrefix, retry_policy.go:19).
        let timeout = |timeout_type: tokeira_proto::enums::TimeoutType| {
            encoded(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: timeout_type as i32,
                ..Default::default()
            }))
        };
        assert!(workflow_failure_is_retryable(
            &timeout(tokeira_proto::enums::TimeoutType::StartToClose),
            &[],
        ));
        assert!(!workflow_failure_is_retryable(
            &timeout(tokeira_proto::enums::TimeoutType::StartToClose),
            &["TemporalTimeout:StartToClose".to_string()],
        ));
        assert!(!workflow_failure_is_retryable(
            &timeout(tokeira_proto::enums::TimeoutType::ScheduleToClose),
            &[],
        ));

        // No failure info / undecodable → retryable (nil → true and the
        // trailing default → true, retry.go:116-118,152).
        assert!(workflow_failure_is_retryable(
            &Payload::new(Vec::new()),
            &[]
        ));
        assert!(workflow_failure_is_retryable(
            &encoded(FailureInfo::ApplicationFailureInfo(Default::default())),
            &[]
        ));
    }

    #[test]
    fn retry_backoff_matches_exponential_formula() {
        // Feature: workflow-retry-chain — backoff = initial × coeff^(attempt-1),
        // capped by maximum_interval.
        let policy = RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::seconds(10)),
            maximum_attempts: 0,
            non_retryable_error_types: Vec::new(),
        };
        assert_eq!(retry_backoff(&policy, 1), Duration::seconds(1));
        assert_eq!(retry_backoff(&policy, 2), Duration::seconds(2));
        assert_eq!(retry_backoff(&policy, 3), Duration::seconds(4));
        assert_eq!(retry_backoff(&policy, 6), Duration::seconds(10));

        // TestWorkflowRetry uses coefficient 1.0 → a constant 1s backoff per attempt.
        let flat = RetryPolicy {
            initial_interval: Duration::seconds(1),
            backoff_coefficient: 1.0,
            maximum_interval: None,
            maximum_attempts: 3,
            non_retryable_error_types: Vec::new(),
        };
        assert_eq!(retry_backoff(&flat, 1), Duration::seconds(1));
        assert_eq!(retry_backoff(&flat, 3), Duration::seconds(1));
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: nexus-async-completion, Property 10: retry successors inherit
        // every completion callback without changing Standby lifecycle state.
        #[test]
        fn retry_successor_preserves_standby_completion_callbacks(callback_count in 0usize..6) {
            let mut state = open_state("retry-callbacks".to_string(), None);
            state.completion_callbacks = (0..callback_count)
                .map(|index| CompletionCallback {
                    spec: CallbackSpec::Nexus {
                        url: format!("https://callback.example/{index}"),
                        header: BTreeMap::new(),
                    },
                    links: Vec::new(),
                    trigger: CallbackTrigger::WorkflowClosed,
                    registration_time: Some(now()),
                    state: CallbackState::Standby,
                    attempt: 0,
                    last_attempt_failure: None,
                    last_attempt_complete_time: None,
                    next_attempt_at: None,
                })
                .collect();
            let policy = RetryPolicy {
                initial_interval: Duration::seconds(1),
                backoff_coefficient: 2.0,
                maximum_interval: Some(Duration::seconds(10)),
                maximum_attempts: 5,
                non_retryable_error_types: Vec::new(),
            };

            let successor = build_retry_successor_start(
                &state,
                None,
                &policy,
                Payloads::default(),
                RunId::new(),
            );
            prop_assert_eq!(&successor.completion_callbacks, &state.completion_callbacks);
            prop_assert!(successor
                .completion_callbacks
                .iter()
                .all(|callback| callback.state == CallbackState::Standby));
        }
    }

    #[derive(Clone, Debug)]
    enum VersioningCase {
        Transition,
        OverridePinned,
        OverrideAutoUpgrade,
        BehaviorPinned,
        BehaviorAutoUpgrade,
        Unversioned,
    }

    fn arb_versioning_case() -> impl Strategy<Value = VersioningCase> {
        prop_oneof![
            Just(VersioningCase::Transition),
            Just(VersioningCase::OverridePinned),
            Just(VersioningCase::OverrideAutoUpgrade),
            Just(VersioningCase::BehaviorPinned),
            Just(VersioningCase::BehaviorAutoUpgrade),
            Just(VersioningCase::Unversioned),
        ]
    }

    #[derive(Clone, Copy, Debug)]
    enum ContinueAsNewSourceCase {
        Unversioned,
        Pinned,
        AutoUpgrade,
        PinnedOverride,
        AutoUpgradeOverride,
    }

    fn arb_continue_as_new_source_case() -> impl Strategy<Value = ContinueAsNewSourceCase> {
        prop_oneof![
            Just(ContinueAsNewSourceCase::Unversioned),
            Just(ContinueAsNewSourceCase::Pinned),
            Just(ContinueAsNewSourceCase::AutoUpgrade),
            Just(ContinueAsNewSourceCase::PinnedOverride),
            Just(ContinueAsNewSourceCase::AutoUpgradeOverride),
        ]
    }

    fn arb_continue_as_new_initial_behavior()
    -> impl Strategy<Value = ContinueAsNewVersioningBehavior> {
        prop_oneof![
            Just(ContinueAsNewVersioningBehavior::Unspecified),
            Just(ContinueAsNewVersioningBehavior::AutoUpgrade),
            Just(ContinueAsNewVersioningBehavior::UseRampingVersion),
            (3i32..32).prop_map(ContinueAsNewVersioningBehavior::Unknown),
        ]
    }

    fn arb_versioning_behavior() -> impl Strategy<Value = VersioningBehavior> {
        prop_oneof![
            Just(VersioningBehavior::Unspecified),
            Just(VersioningBehavior::Pinned),
            Just(VersioningBehavior::AutoUpgrade),
        ]
    }

    fn arb_version_target_lineage() -> impl Strategy<Value = Option<VersionTarget>> {
        prop_oneof![
            Just(None),
            Just(Some(VersionTarget::Unversioned)),
            Just(Some(VersionTarget::Deployment(version_ref(
                "deployment",
                "lineage"
            )))),
        ]
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp")
    }

    fn version_ref(deployment_name: &str, build_id: &str) -> WorkerDeploymentVersionRef {
        WorkerDeploymentVersionRef {
            deployment_name: deployment_name.into(),
            build_id: build_id.into(),
        }
    }

    fn continue_as_new_source_info(
        case: ContinueAsNewSourceCase,
        revision_number: i64,
        last_notified: Option<VersionTarget>,
        declined: Option<VersionTarget>,
    ) -> Option<WorkflowVersioningInfo> {
        let source = version_ref("deployment", "source");
        let override_version = version_ref("deployment", "override");
        let (behavior, versioning_override) = match case {
            ContinueAsNewSourceCase::Unversioned => {
                if last_notified.is_none() && declined.is_none() {
                    return None;
                }
                return Some(WorkflowVersioningInfo {
                    last_notified_target_version: last_notified,
                    declined_target_version_upgrade: declined,
                    ..WorkflowVersioningInfo::default()
                });
            }
            ContinueAsNewSourceCase::Pinned => (VersioningBehavior::Pinned, None),
            ContinueAsNewSourceCase::AutoUpgrade => (VersioningBehavior::AutoUpgrade, None),
            ContinueAsNewSourceCase::PinnedOverride => (
                VersioningBehavior::AutoUpgrade,
                Some(VersioningOverride::Pinned {
                    version: override_version,
                }),
            ),
            ContinueAsNewSourceCase::AutoUpgradeOverride => (
                VersioningBehavior::Pinned,
                Some(VersioningOverride::AutoUpgrade),
            ),
        };
        Some(WorkflowVersioningInfo {
            behavior,
            deployment_version: Some(source),
            versioning_override,
            revision_number,
            last_notified_target_version: last_notified,
            declined_target_version_upgrade: declined,
            ..WorkflowVersioningInfo::default()
        })
    }

    fn reference_continue_as_new_versioning(
        predecessor: &WorkflowState,
        same_task_queue: bool,
        source_member: bool,
        override_member: bool,
        initial_behavior: ContinueAsNewVersioningBehavior,
    ) -> Option<WorkflowVersioningInfo> {
        let source_compatible = same_task_queue || source_member;
        let override_compatible = same_task_queue || override_member;
        let info = predecessor.versioning_info.as_ref();
        let (effective_behavior, effective_version) =
            match info.and_then(|info| info.versioning_override.as_ref()) {
                Some(VersioningOverride::Pinned { version }) => {
                    (VersioningBehavior::Pinned, Some(version.clone()))
                }
                Some(VersioningOverride::AutoUpgrade) => (
                    VersioningBehavior::AutoUpgrade,
                    info.and_then(|info| info.deployment_version.clone()),
                ),
                None => (
                    info.map(|info| info.behavior).unwrap_or_default(),
                    info.and_then(|info| info.deployment_version.clone()),
                ),
            };
        let revision_number = info.map(|info| info.revision_number).unwrap_or_default();
        let pinned_override = info
            .and_then(|info| info.versioning_override.as_ref())
            .and_then(|override_| match override_ {
                VersioningOverride::Pinned { version } if override_compatible => {
                    Some(VersioningOverride::Pinned {
                        version: version.clone(),
                    })
                }
                VersioningOverride::Pinned { .. } | VersioningOverride::AutoUpgrade => None,
            });
        let inherited_pinned = (effective_behavior == VersioningBehavior::Pinned
            && initial_behavior == ContinueAsNewVersioningBehavior::Unspecified
            && source_compatible)
            .then(|| effective_version.clone())
            .flatten();
        let inherited_auto_upgrade = ((effective_behavior == VersioningBehavior::AutoUpgrade
            || (effective_behavior == VersioningBehavior::Pinned
                && initial_behavior != ContinueAsNewVersioningBehavior::Unspecified))
            && source_compatible
            && revision_number != 0)
            .then_some(effective_version)
            .flatten();
        let declined_target_version_upgrade = info.and_then(|info| {
            info.last_notified_target_version
                .clone()
                .or_else(|| info.declined_target_version_upgrade.clone())
        });

        if inherited_pinned.is_none()
            && inherited_auto_upgrade.is_none()
            && pinned_override.is_none()
            && declined_target_version_upgrade.is_none()
        {
            return None;
        }

        let mut info = WorkflowVersioningInfo {
            versioning_override: pinned_override,
            declined_target_version_upgrade,
            ..WorkflowVersioningInfo::default()
        };
        if let Some(version) = inherited_pinned {
            info.behavior = VersioningBehavior::Pinned;
            info.deployment_version = Some(version);
            info.revision_number = revision_number;
        } else if let Some(version) = inherited_auto_upgrade {
            info.behavior = VersioningBehavior::AutoUpgrade;
            info.deployment_version = Some(version);
            info.revision_number = revision_number;
            info.continue_as_new_initial_versioning_behavior = initial_behavior;
        }
        Some(info)
    }

    #[test]
    fn continue_as_new_observes_behavior_reported_by_same_completion() {
        let mut loaded = open_state(
            "same-completion-source".into(),
            continue_as_new_source_info(ContinueAsNewSourceCase::AutoUpgrade, 1, None, None),
        );
        loaded.task_queue = TaskQueueName("source-queue".into());
        let reported_version = version_ref("deployment", "source");
        let projected = state_after_wft_completion_versioning(
            &loaded,
            VersioningBehavior::Pinned,
            Some(reported_version.clone()),
            Some("deployment".into()),
        );

        let successor = resolve_continue_as_new_versioning(
            &projected,
            &TaskQueueName("successor-queue".into()),
            ContinueAsNewVersioningBehavior::Unspecified,
            true,
            false,
        )
        .expect("the reported pinned version belongs to the successor queue");

        assert_eq!(successor.behavior, VersioningBehavior::Pinned);
        assert_eq!(successor.deployment_version, Some(reported_version));
        assert_eq!(
            successor.continue_as_new_initial_versioning_behavior,
            ContinueAsNewVersioningBehavior::Unspecified
        );
    }

    #[test]
    fn child_inherits_auto_upgrade_source_without_use_ramping_instruction() {
        let source = version_ref("deployment", "ramping");
        let mut parent = open_state(
            "child-parent".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(source.clone()),
                revision_number: 7,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::UseRampingVersion,
                ..WorkflowVersioningInfo::default()
            }),
        );
        parent.task_queue = TaskQueueName("shared-queue".into());

        let inherited = resolve_child_versioning(
            &parent,
            &TaskQueueName("shared-queue".into()),
            true,
            true,
            true,
        )
        .expect("same-namespace child inherits the parent's AutoUpgrade source");

        assert_eq!(inherited.behavior, VersioningBehavior::AutoUpgrade);
        assert_eq!(inherited.deployment_version, Some(source));
        assert_eq!(inherited.revision_number, 7);
        assert_eq!(
            inherited.continue_as_new_initial_versioning_behavior,
            ContinueAsNewVersioningBehavior::Unspecified
        );
    }

    #[test]
    fn child_versioning_inheritance_requires_namespace_and_queue_compatibility() {
        let mut parent = open_state(
            "child-compatibility-parent".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(version_ref("deployment", "source")),
                revision_number: 4,
                ..WorkflowVersioningInfo::default()
            }),
        );
        parent.task_queue = TaskQueueName("parent-queue".into());
        let child_queue = TaskQueueName("child-queue".into());

        assert_eq!(
            resolve_child_versioning(&parent, &child_queue, true, false, false),
            None
        );
        assert_eq!(
            resolve_child_versioning(&parent, &child_queue, false, true, true),
            None
        );
        assert!(resolve_child_versioning(&parent, &child_queue, true, true, false).is_some());
    }

    #[test]
    fn child_inherits_compatible_pinned_override() {
        let override_version = version_ref("deployment", "override");
        let mut parent = open_state(
            "child-override-parent".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(version_ref("deployment", "reported")),
                versioning_override: Some(VersioningOverride::Pinned {
                    version: override_version.clone(),
                }),
                revision_number: 9,
                ..WorkflowVersioningInfo::default()
            }),
        );
        parent.task_queue = TaskQueueName("parent-queue".into());

        let inherited = resolve_child_versioning(
            &parent,
            &TaskQueueName("child-queue".into()),
            true,
            true,
            true,
        )
        .expect("compatible pinned override is inherited");

        assert_eq!(inherited.behavior, VersioningBehavior::Pinned);
        assert_eq!(inherited.deployment_version, Some(override_version.clone()));
        assert_eq!(
            inherited.versioning_override,
            Some(VersioningOverride::Pinned {
                version: override_version
            })
        );
    }

    #[test]
    fn inherited_auto_upgrade_first_task_uses_revision_no_bounce_rule() {
        let source = version_ref("deployment", "source");
        let mut successor = open_state(
            "auto-upgrade-successor".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(source.clone()),
                revision_number: 5,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::AutoUpgrade,
                ..WorkflowVersioningInfo::default()
            }),
        );
        successor.previous_started_event_id = 0;
        let mut routing = routing_config(Some(version_key("deployment", "current")), None, 0.0);

        routing.revision_number = 6;
        routing.current_version_revision_number = 6;
        assert_eq!(
            resolve_workflow_task_target_version(&routing, &successor),
            ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "current")),
                revision_number: 6,
                pinned: false,
            }
        );

        // A later ramp update advances the aggregate config revision, but the
        // selected Current target still carries its older field revision. The
        // inherited source must not bounce backward merely because an unrelated
        // routing field changed.
        routing.revision_number = 7;
        routing.current_version_revision_number = 4;
        assert_eq!(
            resolve_workflow_task_target_version(&routing, &successor),
            ResolvedWorkflowTaskTarget {
                deployment_version: Some(source),
                revision_number: 5,
                pinned: false,
            }
        );
    }

    fn version_key(deployment_name: &str, build_id: &str) -> WorkerDeploymentVersionKey {
        WorkerDeploymentVersionKey {
            deployment_name: DeploymentName(deployment_name.into()),
            build_id: StorageBuildId(build_id.into()),
        }
    }

    fn routing_config(
        current: Option<WorkerDeploymentVersionKey>,
        ramping: Option<WorkerDeploymentVersionKey>,
        ramping_percentage: f32,
    ) -> StoredRoutingConfig {
        StoredRoutingConfig {
            current_version: current,
            ramping_version: ramping,
            ramping_version_percentage: ramping_percentage,
            ramping_to_unversioned: false,
            current_version_changed_time: None,
            ramping_version_changed_time: None,
            ramping_version_percentage_changed_time: None,
            revision_number: 100,
            current_version_revision_number: 100,
            ramping_version_revision_number: 100,
        }
    }

    pub(crate) fn open_state(
        workflow_id: String,
        info: Option<WorkflowVersioningInfo>,
    ) -> WorkflowState {
        WorkflowState {
            run_key: RunKey::new(),
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId(workflow_id),
            run_id: RunId::new(),
            workflow_type: WorkflowType("workflow-type".into()),
            task_queue: TaskQueueName("workflow-task-queue".into()),
            deployment: None,
            build_id: None,
            versioning_info: info,
            worker_deployment_name: None,
            status: ExecutionStatus::Running,
            transition_seq: TransitionSeq(1),
            last_event_id: 1,
            external_payload_count: 0,
            external_payload_size_bytes: 0,
            next_workflow_task_seq: LogicalTaskSeq(1),
            pending_workflow_task: None,
            previous_started_event_id: 0,
            workflow_task_attempt: 1,
            workflow_task_attempts_since_last_success: 0,
            last_workflow_task_problem: None,
            sticky: None,
            pause_info: None,
            cancel_requested: false,
            wft_stamp: 0,
            memo: Memo(BTreeMap::new()),
            search_attributes: SearchAttributes(BTreeMap::new()),
            workflow_execution_timeout: Some(Duration::minutes(10)),
            workflow_run_timeout: Some(Duration::minutes(5)),
            workflow_task_timeout: Duration::seconds(10),
            retry_policy: None,
            attempt: 1,
            first_execution_run_id: Some(RunId::new()),
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
            activities: BTreeMap::new(),
            timers: BTreeMap::new(),
            children: BTreeMap::new(),
            pending_external_signals: BTreeMap::new(),
            pending_external_cancels: BTreeMap::new(),
            pending_updates: BTreeMap::new(),
            admitted_updates: HashSet::new(),
            pending_nexus_operations: BTreeMap::new(),
            completion_callbacks: Vec::new(),
            user_metadata: None,
            links: Vec::new(),
            workflow_start_delay: None,
            priority: None,
            started_at: now(),
            first_run_started_at: Some(now()),
            closed_at: None,
            close_result: None,
            close_failure: None,
            request_id_infos: std::collections::BTreeMap::new(),
            buffered_events: Vec::new(),
            auto_reset_points: Vec::new(),
        }
    }

    fn info_for_case(case: &VersioningCase) -> Option<WorkflowVersioningInfo> {
        let behavior_version = version_ref("deployment", "behavior");
        let override_version = version_ref("deployment", "override");
        let transition_version = version_ref("deployment", "transition");
        match case {
            VersioningCase::Transition => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(behavior_version),
                versioning_override: Some(VersioningOverride::Pinned {
                    version: override_version,
                }),
                version_transition: Some(transition_version),
                revision_number: 7,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
            VersioningCase::OverridePinned => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(behavior_version),
                versioning_override: Some(VersioningOverride::Pinned {
                    version: override_version,
                }),
                version_transition: None,
                revision_number: 8,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
            VersioningCase::OverrideAutoUpgrade => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::Pinned,
                deployment_version: Some(behavior_version),
                versioning_override: Some(VersioningOverride::AutoUpgrade),
                version_transition: None,
                revision_number: 9,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
            VersioningCase::BehaviorPinned => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::Pinned,
                deployment_version: Some(behavior_version),
                versioning_override: None,
                version_transition: None,
                revision_number: 10,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
            VersioningCase::BehaviorAutoUpgrade => Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(behavior_version),
                versioning_override: None,
                version_transition: None,
                revision_number: 11,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
            VersioningCase::Unversioned => None,
        }
    }

    fn workflow_queue(deployment_name: Option<&str>, build_id: Option<&str>) -> QueueKey {
        QueueKey {
            namespace_id: NamespaceId::new(),
            task_queue: TaskQueueName("workflow-task-queue".into()),
            task_kind: TaskKind::Workflow,
            deployment: deployment_name.map(|name| DeploymentId(name.to_string())),
            build_id: build_id.map(|id| RuntimeBuildId(id.to_string())),
        }
    }

    fn expected_for_case(
        case: &VersioningCase,
        routing_config: &StoredRoutingConfig,
        workflow_id: &WorkflowId,
    ) -> ResolvedWorkflowTaskTarget {
        match case {
            VersioningCase::Transition => ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "transition")),
                revision_number: 7,
                pinned: false,
            },
            VersioningCase::OverridePinned => ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "override")),
                revision_number: 8,
                pinned: true,
            },
            VersioningCase::BehaviorPinned => ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "behavior")),
                revision_number: 10,
                pinned: true,
            },
            VersioningCase::OverrideAutoUpgrade
            | VersioningCase::BehaviorAutoUpgrade
            | VersioningCase::Unversioned => {
                let (deployment_version, revision_number) =
                    routing_config_target_with_revision(routing_config, workflow_id);
                ResolvedWorkflowTaskTarget {
                    deployment_version,
                    revision_number,
                    pinned: false,
                }
            }
        }
    }

    #[test]
    fn transition_for_polled_workflow_task_starts_when_poller_differs_from_effective() {
        let routing = routing_config(Some(version_key("deployment", "current")), None, 0.0);
        let state = open_state("workflow".into(), None);
        let target = resolve_workflow_task_target_version(&routing, &state);

        // Unversioned run (effective deployment None): any versioned poller
        // differs from the effective deployment and starts a transition toward
        // the poller's version — matching v1.31.0's `!pollerDeployment.Equal(
        // wfDeployment)`. This holds even when the poller is NOT the current
        // routing target ("stale"): Matching already dispatched the task here,
        // and re-checking against the routing config would suppress a
        // legitimate transition.
        assert_eq!(
            transition_for_polled_workflow_task(
                &state,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
                false,
            ),
            Some(version_ref("deployment", "current"))
        );
        assert_eq!(
            transition_for_polled_workflow_task(
                &state,
                &target,
                &workflow_queue(Some("deployment"), Some("stale")),
                false,
            ),
            Some(version_ref("deployment", "stale"))
        );
        // An unversioned poller (no deployment/build_id) cannot start a
        // transition.
        assert_eq!(
            transition_for_polled_workflow_task(
                &state,
                &target,
                &workflow_queue(None, None),
                false,
            ),
            None
        );
        assert_eq!(
            transition_for_polled_workflow_task(
                &state,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
                true,
            ),
            None,
            "a speculative start must not persist its computed transition"
        );

        let already_effective = open_state(
            "workflow".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::AutoUpgrade,
                deployment_version: Some(version_ref("deployment", "current")),
                versioning_override: None,
                version_transition: None,
                revision_number: 100,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
        );
        let target = resolve_workflow_task_target_version(&routing, &already_effective);
        // Poller already equals the run's effective deployment: no transition.
        assert_eq!(
            transition_for_polled_workflow_task(
                &already_effective,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
                false,
            ),
            None
        );

        let pinned = open_state(
            "workflow".into(),
            Some(WorkflowVersioningInfo {
                behavior: VersioningBehavior::Pinned,
                deployment_version: Some(version_ref("deployment", "current")),
                versioning_override: None,
                version_transition: None,
                revision_number: 100,
                continue_as_new_initial_versioning_behavior:
                    ContinueAsNewVersioningBehavior::Unspecified,
                ..WorkflowVersioningInfo::default()
            }),
        );
        let target = resolve_workflow_task_target_version(&routing, &pinned);
        // Pinned runs never transition, even when a differing poller arrives.
        assert_eq!(
            transition_for_polled_workflow_task(
                &pinned,
                &target,
                &workflow_queue(Some("deployment"), Some("current")),
                false,
            ),
            None
        );
    }

    #[test]
    fn partial_ramp_from_unversioned_can_select_the_ramping_version() {
        let routing = routing_config(None, Some(version_key("deployment", "ramping")), 25.0);
        let state = open_state("workflow-0".into(), None);

        // A nil Current target is the unversioned side of a ramp, not a reason
        // to bypass the ramp. v1.31.0 explicitly covers partial ramping from
        // unversioned to a Deployment Version
        // (`TestFindDeploymentVersionForWorkflowID_PartialRamp`,
        // `common/worker_versioning/worker_versioning_test.go @ v1.31.0`).
        assert_eq!(
            resolve_workflow_task_target_version(&routing, &state),
            ResolvedWorkflowTaskTarget {
                deployment_version: Some(version_ref("deployment", "ramping")),
                revision_number: 100,
                pinned: false,
            }
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        #[test]
        fn property_routing_determinism_and_effective_version_precedence(
            case in arb_versioning_case(),
            workflow_suffix in 0u32..10_000,
            current_present in any::<bool>(),
            ramping_present in any::<bool>(),
            ramping_percentage in prop_oneof![Just(0.0f32), Just(1.0), Just(25.0), Just(50.0), Just(75.0), Just(100.0)],
        ) {
            let routing = routing_config(
                current_present.then(|| version_key("deployment", "current")),
                ramping_present.then(|| version_key("deployment", "ramping")),
                ramping_percentage,
            );
            let mut state = open_state(format!("workflow-{workflow_suffix}"), info_for_case(&case));
            if matches!(case, VersioningCase::BehaviorAutoUpgrade) {
                // This older property models ordinary routing after an SDK has
                // completed a WFT as AUTO_UPGRADE. A source-bearing run with no
                // successful WFT instead means inherited first-task placement,
                // which Property 23 exercises independently.
                state.previous_started_event_id = 1;
            }

            let resolved = resolve_workflow_task_target_version(&routing, &state);
            let resolved_again = resolve_workflow_task_target_version(&routing, &state);
            let expected = expected_for_case(&case, &routing, &state.workflow_id);

            prop_assert_eq!(&resolved, &resolved_again);
            prop_assert_eq!(&resolved, &expected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: worker-deployments, Property 23: Continue-as-New versioning decision
        #[test]
        fn continue_as_new_versioning_matches_v1_31_reference(
            case in arb_continue_as_new_source_case(),
            same_task_queue in any::<bool>(),
            source_member in any::<bool>(),
            override_member in any::<bool>(),
            revision_number in 0i64..4,
            initial_behavior in arb_continue_as_new_initial_behavior(),
            last_notified in arb_version_target_lineage(),
            declined in arb_version_target_lineage(),
            completion_behavior in arb_versioning_behavior(),
            completion_version_present in any::<bool>(),
            current_present in any::<bool>(),
            ramping_present in any::<bool>(),
            routing_revision in 0i64..4,
            routing_same_deployment in any::<bool>(),
        ) {
            let mut predecessor = open_state(
                "continue-as-new-source".into(),
                continue_as_new_source_info(
                    case,
                    revision_number,
                    last_notified.clone(),
                    declined.clone(),
                ),
            );
            predecessor.task_queue = TaskQueueName("source-queue".into());
            let completion_deployment_version = (completion_behavior
                != VersioningBehavior::Unspecified
                && completion_version_present)
                .then(|| version_ref("deployment", "completion"));
            let projected_predecessor = state_after_wft_completion_versioning(
                &predecessor,
                completion_behavior,
                completion_deployment_version,
                (completion_behavior != VersioningBehavior::Unspecified)
                    .then(|| "deployment".to_string()),
            );
            let successor_task_queue = if same_task_queue {
                projected_predecessor.task_queue.clone()
            } else {
                TaskQueueName("successor-queue".into())
            };
            let expected = reference_continue_as_new_versioning(
                &projected_predecessor,
                same_task_queue,
                source_member,
                override_member,
                initial_behavior,
            );

            let actual = resolve_continue_as_new_versioning(
                &projected_predecessor,
                &successor_task_queue,
                initial_behavior,
                source_member,
                override_member,
            );
            let repeated = resolve_continue_as_new_versioning(
                &projected_predecessor,
                &successor_task_queue,
                initial_behavior,
                source_member,
                override_member,
            );
            prop_assert_eq!(&actual, &expected);
            prop_assert_eq!(&actual, &repeated);

            if let Some(successor_info) = actual.clone() {
                let routing_deployment = if routing_same_deployment {
                    "deployment"
                } else {
                    "other-deployment"
                };
                let mut routing = routing_config(
                    current_present.then(|| version_key(routing_deployment, "current")),
                    ramping_present.then(|| version_key(routing_deployment, "ramping")),
                    50.0,
                );
                routing.revision_number = routing_revision;
                routing.current_version_revision_number = routing_revision;
                routing.ramping_version_revision_number = routing_revision;
                let mut successor = open_state("continue-as-new-successor".into(), Some(successor_info.clone()));
                successor.task_queue = successor_task_queue.clone();
                if successor_info.versioning_override.is_none()
                    && successor_info.behavior == VersioningBehavior::AutoUpgrade
                {
                    let initial_target = resolve_workflow_task_target_version(&routing, &successor);
                    if initial_behavior == ContinueAsNewVersioningBehavior::UseRampingVersion {
                        let expected_target = routing
                            .ramping_version
                            .as_ref()
                            .map(version_key_to_ref)
                            .or_else(|| routing.current_version.as_ref().map(version_key_to_ref));
                        prop_assert_eq!(initial_target.deployment_version, expected_target);
                        prop_assert_eq!(initial_target.revision_number, routing.revision_number);
                    } else {
                        let routing_target = routing_config_target(&routing, &successor.workflow_id);
                        let routing_target_wins = routing_target.as_ref().is_none_or(|target| {
                            successor_info.deployment_version.as_ref().is_none_or(|source| {
                                target.deployment_name != source.deployment_name
                                    || routing.revision_number >= successor_info.revision_number
                            })
                        });
                        let (expected_target, expected_revision) = if routing_target_wins {
                            (routing_target, routing.revision_number)
                        } else {
                            (
                                successor_info.deployment_version.clone(),
                                successor_info.revision_number,
                            )
                        };
                        prop_assert_eq!(
                            initial_target.deployment_version,
                            expected_target
                        );
                        prop_assert_eq!(initial_target.revision_number, expected_revision);
                    }

                    successor.previous_started_event_id = 1;
                    let later_target = resolve_workflow_task_target_version(&routing, &successor);
                    prop_assert_eq!(
                        later_target.deployment_version,
                        routing_config_target(&routing, &successor.workflow_id)
                    );
                }

                let next_hop = resolve_continue_as_new_versioning(
                    &successor,
                    &successor_task_queue,
                    ContinueAsNewVersioningBehavior::Unspecified,
                    true,
                    true,
                );
                if let Some(next_hop) = next_hop
                    && next_hop.behavior == VersioningBehavior::AutoUpgrade
                {
                    prop_assert_eq!(
                        next_hop.continue_as_new_initial_versioning_behavior,
                        ContinueAsNewVersioningBehavior::Unspecified
                    );
                }
            }

            let started_decline = declined.or(last_notified);
            let started_info = WorkflowVersioningInfo {
                behavior: if matches!(case, ContinueAsNewSourceCase::Pinned | ContinueAsNewSourceCase::PinnedOverride) {
                    VersioningBehavior::Pinned
                } else {
                    VersioningBehavior::Unspecified
                },
                declined_target_version_upgrade: started_decline.clone(),
                ..WorkflowVersioningInfo::default()
            };
            let retry = retry_successor_versioning_info(&predecessor, Some(&started_info));
            if predecessor.effective_behavior() == VersioningBehavior::Pinned
                && started_info.behavior != VersioningBehavior::Pinned
            {
                prop_assert!(retry.as_ref().is_none_or(|info| info.behavior != VersioningBehavior::Pinned));
            }
            if let Some(retry) = retry {
                prop_assert_eq!(retry.declined_target_version_upgrade, started_decline);
                if retry.behavior == VersioningBehavior::AutoUpgrade {
                    prop_assert_eq!(
                        retry.continue_as_new_initial_versioning_behavior,
                        predecessor
                            .versioning_info
                            .as_ref()
                            .map(|info| info.continue_as_new_initial_versioning_behavior)
                            .unwrap_or_default()
                    );
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: worker-deployments, Property 25: runtime-resolved boundary determinism
        #[test]
        fn runtime_resolves_mutable_inputs_before_pure_kernel_evaluation(
            case in arb_continue_as_new_source_case(),
            same_task_queue in any::<bool>(),
            source_member in any::<bool>(),
            override_member in any::<bool>(),
            revision_number in 0i64..4,
            initial_behavior in arb_continue_as_new_initial_behavior(),
            notification_enabled in any::<bool>(),
            current_present in any::<bool>(),
            ramping_present in any::<bool>(),
            ramping_percentage in prop_oneof![Just(0.0f32), Just(50.0), Just(100.0)],
        ) {
            let now = now();
            let mut predecessor = open_state(
                "boundary-source".into(),
                continue_as_new_source_info(case, revision_number, None, None),
            );
            predecessor.task_queue = TaskQueueName("source-queue".into());
            let successor_task_queue = if same_task_queue {
                predecessor.task_queue.clone()
            } else {
                TaskQueueName("successor-queue".into())
            };
            let routing = routing_config(
                current_present.then(|| version_key("deployment", "current")),
                ramping_present.then(|| version_key("deployment", "ramping")),
                ramping_percentage,
            );

            let successor_info = resolve_continue_as_new_versioning(
                &predecessor,
                &successor_task_queue,
                initial_behavior,
                source_member,
                override_member,
            );
            let dispatch_target = resolve_workflow_task_target_version(&routing, &predecessor);
            let repeated_dispatch_target =
                resolve_workflow_task_target_version(&routing, &predecessor);
            let notification_target = routing_config_target(&routing, &predecessor.workflow_id);
            prop_assert_eq!(dispatch_target, repeated_dispatch_target);

            let policy = RetryPolicy {
                initial_interval: Duration::seconds(1),
                backoff_coefficient: 1.0,
                maximum_interval: None,
                maximum_attempts: 2,
                non_retryable_error_types: Vec::new(),
            };
            let mut start = build_retry_successor_start(
                &predecessor,
                None,
                &policy,
                Payloads::default(),
                RunId::new(),
            );
            start.task_queue = successor_task_queue;
            start.versioning_override = None;
            start.workflow_start_delay = None;
            start.inherited_versioning_info = successor_info;
            start.now = now;
            start.request.received_at = now;

            let first_start = BasicKernel
                .apply(LoadedRun::Absent, Command::Start(start.clone()))
                .unwrap();
            let second_start = BasicKernel
                .apply(LoadedRun::Absent, Command::Start(start))
                .unwrap();
            prop_assert_eq!(&first_start, &second_start);

            let pending = first_start
                .next_state
                .pending_workflow_task
                .as_ref()
                .expect("a non-delayed successor schedules its first workflow task");
            let start_wft = Command::WorkflowTaskStarted(StartWorkflowTaskRequest {
                logical_seq: pending.logical_seq,
                worker_identity: WorkerIdentity("boundary-worker".into()),
                request_id: "boundary-wft-start".into(),
                history_size_bytes: 0,
                suggest_continue_as_new: false,
                deployment_transition: None,
                deployment_transition_revision_number: None,
                target_version_changed_enabled: notification_enabled,
                target_deployment_version: notification_target,
                polled_task_queue: TaskQueueName("queue".into()),
                now,
            });
            let first_wft = BasicKernel
                .apply(
                    LoadedRun::Existing(first_start.next_state.clone()),
                    start_wft.clone(),
                )
                .unwrap();
            let second_wft = BasicKernel
                .apply(LoadedRun::Existing(first_start.next_state), start_wft)
                .unwrap();
            prop_assert_eq!(first_wft, second_wft);
        }
    }
}
