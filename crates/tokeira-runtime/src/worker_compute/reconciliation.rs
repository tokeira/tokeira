//! Deployment catalog reconciliation and fenced controller decisions.
//!
//! Worker Deployment storage remains configuration authority. This module derives
//! restart-safe advisory controller records, claims each evaluation briefly, and
//! commits an immutable provider action atomically with scaler state. It never
//! participates in task publication, workflow transitions, or kernel execution.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::{Result, anyhow};
use time::OffsetDateTime;
use tokeira_storage::{
    DeploymentKey, DeploymentTaskQueueType, StoredVersion, WorkerComputeControllerAdmission,
    WorkerComputeControllerClaim, WorkerComputeControllerCommitResult,
    WorkerComputeControllerRecord, WorkerComputeQueueSample, WorkerComputeRepository,
    WorkerComputeScalingGroupState, WorkerDeploymentRepository,
};
use tokeira_types::{
    BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, IncarnationId,
    ScalingGroupId, WorkerComputeControllerLifecycle, WorkerComputeFailureCategory,
    WorkerComputeGroupEligibility, WorkerComputeHealth, WorkerComputeInvokeReason,
    WorkerComputeProviderActionStatus, WorkerComputeTaskQueueBinding, WorkerComputeTaskType,
};
use uuid::Uuid;

use super::{
    CONTROLLER_CLAIM_LEASE, EffectiveScalingGroup, NoSyncState, ObservationBatch,
    ProviderActionInput, QUEUE_SAMPLE_TTL, ScalerDecision, UnsupportedScalingGroup,
    UnsupportedScalingGroupReason, ValidatedComputeConfig, ValidatedScalingGroup,
    WorkerComputeNamespace, WorkerComputeNamespaceCatalog, aggregate_queue_samples,
    build_provider_action, evaluate_metrics, evaluate_task_add, metrics_for_group,
    validate_compute_config,
};
use crate::metrics as runtime_metrics;

/// Durable reconciler for Worker Deployment Version controller instances.
#[derive(Clone)]
pub struct WorkerComputeReconciler {
    deployment_repository: Arc<dyn WorkerDeploymentRepository>,
    controller_repository: Arc<dyn WorkerComputeRepository>,
    owner: IncarnationId,
}

impl std::fmt::Debug for WorkerComputeReconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerComputeReconciler")
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl WorkerComputeReconciler {
    /// Construct one process-local reconciler over provider-neutral repositories.
    #[must_use]
    pub fn new(
        deployment_repository: Arc<dyn WorkerDeploymentRepository>,
        controller_repository: Arc<dyn WorkerComputeRepository>,
        owner: IncarnationId,
    ) -> Self {
        Self {
            deployment_repository,
            controller_repository,
            owner,
        }
    }

    /// Run one complete active-namespace catalog sweep.
    ///
    /// Startup calls this once before processing advisory hints. Supervision later
    /// repeats the same idempotent operation at the fixed catalog interval.
    pub async fn reconcile_catalog(
        &self,
        catalog: &dyn WorkerComputeNamespaceCatalog,
        now: OffsetDateTime,
    ) -> Result<()> {
        for namespace in catalog.list_active().await? {
            self.reconcile_namespace(&namespace, now).await?;
        }
        Ok(())
    }

    /// Reconcile every Deployment Version in one active namespace.
    pub async fn reconcile_namespace(
        &self,
        namespace: &WorkerComputeNamespace,
        now: OffsetDateTime,
    ) -> Result<()> {
        let deployments = self
            .deployment_repository
            .list_all_for_namespace(namespace.namespace_id)
            .await?;
        let existing = self
            .controller_repository
            .list_controllers(namespace.namespace_id)
            .await?;
        let mut desired = BTreeSet::new();
        let mut candidates = Vec::new();

        for deployment in deployments {
            for version in deployment.versions.values() {
                let Ok(validated) = validate_compute_config(&version.compute_config) else {
                    continue;
                };
                if !has_eligible_group(&validated) {
                    continue;
                }
                let key = ControllerInstanceKey {
                    namespace_id: namespace.namespace_id,
                    deployment_name: DeploymentId(deployment.name.0.clone()),
                    build_id: BuildId(version.build_id.0.clone()),
                };
                desired.insert(key.clone());
                candidates.push((key, version.clone(), validated));
            }
        }

        // Release obsolete slots before admission so a waiting version can be
        // promoted in this same bounded sweep rather than one interval later.
        for record in existing {
            if !desired.contains(&record.key) {
                self.controller_repository
                    .inactivate_controller(&record.key, now)
                    .await?;
            }
        }
        for (key, version, validated) in candidates {
            self.reconcile_version(namespace, key, &version, validated, now)
                .await?;
        }
        Ok(())
    }

    async fn reconcile_version(
        &self,
        namespace: &WorkerComputeNamespace,
        key: ControllerInstanceKey,
        version: &StoredVersion,
        validated: ValidatedComputeConfig,
        now: OffsetDateTime,
    ) -> Result<()> {
        let candidate = candidate_record(namespace, key.clone(), &validated, now);
        let admission = self
            .controller_repository
            .admit_controller(
                candidate,
                super::MAX_CONTROLLER_INSTANCES_PER_NAMESPACE,
                now,
            )
            .await?;
        if matches!(
            admission,
            WorkerComputeControllerAdmission::CapacityLimited(_)
        ) {
            return Ok(());
        }

        let lease_until = now
            + time::Duration::try_from(CONTROLLER_CLAIM_LEASE)
                .expect("controller claim lease fits time::Duration");
        let Some(claimed) = self
            .controller_repository
            .claim_controller(&key, self.owner, now, lease_until)
            .await?
        else {
            return Ok(());
        };
        let claim = claimed.claim;
        let mut current = claimed.record;
        let next_groups = reconcile_group_states(&current.groups, &validated);
        if current.namespace_name != namespace.name || current.groups != next_groups {
            let mut next = current.clone();
            next.namespace_name.clone_from(&namespace.name);
            next.groups = next_groups;
            next.reconciled_at = now;
            next.next_metrics_poll_at = next_metrics_poll_at(&validated, now);
            next.revision = current.revision.saturating_add(1);
            match self
                .controller_repository
                .commit_decision(&claim, current.revision, next.clone(), None)
                .await?
            {
                WorkerComputeControllerCommitResult::Applied => current = next,
                WorkerComputeControllerCommitResult::Conflict
                | WorkerComputeControllerCommitResult::Fenced
                | WorkerComputeControllerCommitResult::NotFound => return Ok(()),
            }
        }

        let queues = version_queue_bindings(version);
        for group in validated.groups.values() {
            let ValidatedScalingGroup::Eligible(group) = group else {
                continue;
            };
            let needs_activation = current
                .groups
                .get(&group.id)
                .is_some_and(|state| state.activation_fingerprint != Some(group.fingerprint));
            if !needs_activation {
                continue;
            }
            let action_id = Uuid::new_v4();
            let action = build_provider_action(ProviderActionInput {
                action_id,
                controller_key: key.clone(),
                namespace_name: namespace.name.clone(),
                scaling_group: group.id.clone(),
                fingerprint: group.fingerprint,
                provider: group.provider.clone(),
                reason: WorkerComputeInvokeReason::ConfigurationActivation,
                task_queues: queues
                    .iter()
                    .filter(|binding| group.task_types.contains(&binding.task_type))
                    .cloned()
                    .collect(),
                now,
            });
            let mut next = current.clone();
            next.revision = current.revision.saturating_add(1);
            next.reconciled_at = now;
            let state = next
                .groups
                .get_mut(&group.id)
                .expect("validated eligible group was synchronized into controller state");
            state.activation_fingerprint = Some(group.fingerprint);
            let action = match action {
                Ok(action) => {
                    state.activation_status = Some(WorkerComputeProviderActionStatus::Pending);
                    state.last_action_id = Some(action_id);
                    state.last_failure_category = None;
                    Some(action)
                }
                Err(_) => {
                    state.activation_status =
                        Some(WorkerComputeProviderActionStatus::TerminalFailed);
                    state.health = WorkerComputeHealth::ProviderRequestTooLarge;
                    state.last_failure_category =
                        Some(WorkerComputeFailureCategory::RequestTooLarge);
                    None
                }
            };
            let action_committed = action.is_some();
            match self
                .controller_repository
                .commit_decision(&claim, current.revision, next.clone(), action)
                .await?
            {
                WorkerComputeControllerCommitResult::Applied => {
                    if action_committed {
                        runtime_metrics::record_worker_compute_decision(
                            group.task_types.iter().copied(),
                            WorkerComputeInvokeReason::ConfigurationActivation,
                        );
                    }
                    current = next;
                }
                WorkerComputeControllerCommitResult::Conflict
                | WorkerComputeControllerCommitResult::Fenced
                | WorkerComputeControllerCommitResult::NotFound => return Ok(()),
            }
        }
        Ok(())
    }

    /// Reconcile one exact version promptly after a successful registry mutation.
    pub async fn reconcile_key(
        &self,
        namespace: &WorkerComputeNamespace,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<()> {
        let deployment = self
            .deployment_repository
            .load_deployment(&DeploymentKey {
                namespace_id: key.namespace_id,
                deployment_name: tokeira_storage::DeploymentName(key.deployment_name.0.clone()),
            })
            .await?;
        let Some(version) = deployment.as_ref().and_then(|record| {
            record
                .versions
                .get(&tokeira_storage::BuildId(key.build_id.0.clone()))
        }) else {
            self.controller_repository
                .inactivate_controller(key, now)
                .await?;
            return Ok(());
        };
        let validated = validate_compute_config(&version.compute_config)
            .map_err(|error| anyhow!(error.to_string()))?;
        if has_eligible_group(&validated) {
            self.reconcile_version(namespace, key.clone(), version, validated, now)
                .await
        } else {
            self.controller_repository
                .inactivate_controller(key, now)
                .await?;
            Ok(())
        }
    }

    /// Retain a removed namespace/version controller as inactive audit state.
    pub async fn inactivate_key(
        &self,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<()> {
        self.controller_repository
            .inactivate_controller(key, now)
            .await?;
        Ok(())
    }

    /// Evaluate one due exact-version observation batch under a short fenced claim.
    ///
    /// The batch is advisory and may be lost; periodic samples independently recover
    /// durable backlog demand. Current deployment bytes are reloaded before every
    /// decision so an old hint cannot create an action for superseded configuration.
    pub async fn evaluate_observation_batch(
        &self,
        namespace: &WorkerComputeNamespace,
        key: &ControllerInstanceKey,
        batch: &ObservationBatch,
        now: OffsetDateTime,
    ) -> Result<()> {
        let Some((version, validated)) = self.load_current_eligible_version(key).await? else {
            self.controller_repository
                .inactivate_controller(key, now)
                .await?;
            return Ok(());
        };
        self.reconcile_version(namespace, key.clone(), &version, validated.clone(), now)
            .await?;
        let Some((claim, mut current)) = self.claim(key, now).await? else {
            return Ok(());
        };
        let version_queues = version_queue_bindings(&version);

        for group in validated.groups.values() {
            let ValidatedScalingGroup::Eligible(group) = group else {
                continue;
            };
            let routed = observation_batch_for_group(batch, &group.task_types);
            if routed.task_types.is_empty() {
                continue;
            }
            let Some(state) = current.groups.get(&group.id) else {
                continue;
            };
            let decision = evaluate_task_add(&group.scaler, &scaler_state(state), &routed, now);
            let queues = union_queue_bindings(
                &version_queues,
                routed.task_queues.iter().cloned(),
                &group.task_types,
            );
            let Some(next) = self
                .commit_group_decision(
                    &claim, current, namespace, group, decision, queues, None, now,
                )
                .await?
            else {
                return Ok(());
            };
            current = next;
        }
        Ok(())
    }

    /// Evaluate one exact-version periodic metrics snapshot under a short claim.
    ///
    /// Only non-expired queue-home samples enter policy. Missing samples deliberately
    /// form a zero snapshot, allowing the durable schedule to progress after restart.
    pub async fn evaluate_metrics_snapshot(
        &self,
        namespace: &WorkerComputeNamespace,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<()> {
        let Some((version, validated)) = self.load_current_eligible_version(key).await? else {
            self.controller_repository
                .inactivate_controller(key, now)
                .await?;
            return Ok(());
        };
        self.reconcile_version(namespace, key.clone(), &version, validated.clone(), now)
            .await?;
        let not_before = now
            - time::Duration::try_from(QUEUE_SAMPLE_TTL)
                .expect("fixed queue sample TTL fits time::Duration");
        let samples = self
            .controller_repository
            .list_queue_samples(key, not_before)
            .await?;
        let aggregate = aggregate_queue_samples(key, &samples, not_before);
        let sample_queues = sample_queue_bindings(&samples);
        let version_queues = version_queue_bindings(&version);
        let Some((claim, mut current)) = self.claim(key, now).await? else {
            return Ok(());
        };
        let scheduled_metrics_poll_at = next_metrics_poll_at(&validated, now);

        for group in validated.groups.values() {
            let ValidatedScalingGroup::Eligible(group) = group else {
                continue;
            };
            let Some(state) = current.groups.get(&group.id) else {
                continue;
            };
            let decision = evaluate_metrics(
                &group.scaler,
                &scaler_state(state),
                &metrics_for_group(&aggregate, &group.task_types),
                &group.task_types,
                now,
            );
            let queues = union_queue_bindings(
                &version_queues,
                sample_queues.iter().cloned(),
                &group.task_types,
            );
            let Some(next) = self
                .commit_group_decision(
                    &claim,
                    current,
                    namespace,
                    group,
                    decision,
                    queues,
                    scheduled_metrics_poll_at,
                    now,
                )
                .await?
            else {
                return Ok(());
            };
            current = next;
        }
        Ok(())
    }

    async fn load_current_eligible_version(
        &self,
        key: &ControllerInstanceKey,
    ) -> Result<Option<(StoredVersion, ValidatedComputeConfig)>> {
        let deployment = self
            .deployment_repository
            .load_deployment(&DeploymentKey {
                namespace_id: key.namespace_id,
                deployment_name: tokeira_storage::DeploymentName(key.deployment_name.0.clone()),
            })
            .await?;
        let Some(version) = deployment.and_then(|record| {
            record
                .versions
                .get(&tokeira_storage::BuildId(key.build_id.0.clone()))
                .cloned()
        }) else {
            return Ok(None);
        };
        let validated = validate_compute_config(&version.compute_config)
            .map_err(|error| anyhow!(error.to_string()))?;
        Ok(has_eligible_group(&validated).then_some((version, validated)))
    }

    async fn claim(
        &self,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<Option<(WorkerComputeControllerClaim, WorkerComputeControllerRecord)>> {
        let lease_until = now
            + time::Duration::try_from(CONTROLLER_CLAIM_LEASE)
                .expect("controller claim lease fits time::Duration");
        Ok(self
            .controller_repository
            .claim_controller(key, self.owner, now, lease_until)
            .await?
            .map(|claimed| (claimed.claim, claimed.record)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_group_decision(
        &self,
        claim: &WorkerComputeControllerClaim,
        current: WorkerComputeControllerRecord,
        namespace: &WorkerComputeNamespace,
        group: &EffectiveScalingGroup,
        decision: ScalerDecision,
        task_queues: Vec<WorkerComputeTaskQueueBinding>,
        scheduled_metrics_poll_at: Option<OffsetDateTime>,
        now: OffsetDateTime,
    ) -> Result<Option<WorkerComputeControllerRecord>> {
        let decision_reason = decision.action.map(|action| action.reason);
        let suppressions = decision.suppressions.clone();
        let mut next = current.clone();
        next.revision = current.revision.saturating_add(1);
        next.next_metrics_poll_at = scheduled_metrics_poll_at.or(current.next_metrics_poll_at);
        let state = next
            .groups
            .get_mut(&group.id)
            .expect("validated eligible group is present after reconciliation");
        state.last_scale_up_at = decision.next_state.last_scale_up_at;
        state.prior_dispatch_rates = decision.next_state.prior_dispatch_rates;

        let action = decision.action.map(|scale| {
            let action_id = Uuid::new_v4();
            build_provider_action(ProviderActionInput {
                action_id,
                controller_key: current.key.clone(),
                namespace_name: namespace.name.clone(),
                scaling_group: group.id.clone(),
                fingerprint: group.fingerprint,
                provider: group.provider.clone(),
                reason: scale.reason,
                task_queues,
                now,
            })
            .map(|action| (action_id, action))
        });
        let action = match action {
            Some(Ok((action_id, action))) => {
                state.health = WorkerComputeHealth::Active;
                state.last_action_id = Some(action_id);
                state.last_failure_category = None;
                Some(action)
            }
            Some(Err(_)) => {
                state.health = WorkerComputeHealth::ProviderRequestTooLarge;
                state.last_failure_category = Some(WorkerComputeFailureCategory::RequestTooLarge);
                None
            }
            None => None,
        };
        match self
            .controller_repository
            .commit_decision(claim, current.revision, next.clone(), action)
            .await?
        {
            WorkerComputeControllerCommitResult::Applied => {
                if let Some(reason) = decision_reason {
                    runtime_metrics::record_worker_compute_decision(
                        group.task_types.iter().copied(),
                        reason,
                    );
                }
                for (task_type, suppression) in suppressions {
                    runtime_metrics::record_worker_compute_suppression(task_type, suppression);
                }
                Ok(Some(next))
            }
            WorkerComputeControllerCommitResult::Conflict
            | WorkerComputeControllerCommitResult::Fenced
            | WorkerComputeControllerCommitResult::NotFound => Ok(None),
        }
    }
}

fn has_eligible_group(config: &ValidatedComputeConfig) -> bool {
    config
        .groups
        .values()
        .any(|group| matches!(group, ValidatedScalingGroup::Eligible(_)))
}

fn candidate_record(
    namespace: &WorkerComputeNamespace,
    key: ControllerInstanceKey,
    config: &ValidatedComputeConfig,
    now: OffsetDateTime,
) -> WorkerComputeControllerRecord {
    WorkerComputeControllerRecord {
        format_version: tokeira_storage::WORKER_COMPUTE_RECORD_FORMAT_VERSION,
        key,
        namespace_name: namespace.name.clone(),
        revision: 0,
        lifecycle: WorkerComputeControllerLifecycle::Active,
        slot: None,
        owner: None,
        owner_epoch: 0,
        lease_until: None,
        groups: reconcile_group_states(&BTreeMap::new(), config),
        next_metrics_poll_at: next_metrics_poll_at(config, now),
        reconciled_at: now,
    }
}

fn reconcile_group_states(
    current: &BTreeMap<ScalingGroupId, WorkerComputeScalingGroupState>,
    config: &ValidatedComputeConfig,
) -> BTreeMap<ScalingGroupId, WorkerComputeScalingGroupState> {
    config
        .groups
        .iter()
        .map(|(group_id, group)| {
            let state = match group {
                ValidatedScalingGroup::Eligible(group) => {
                    eligible_group_state(current.get(group_id), group)
                }
                ValidatedScalingGroup::Unsupported(group) => unsupported_group_state(group),
            };
            (group_id.clone(), state)
        })
        .collect()
}

fn eligible_group_state(
    current: Option<&WorkerComputeScalingGroupState>,
    group: &EffectiveScalingGroup,
) -> WorkerComputeScalingGroupState {
    let mut state = current
        .filter(|state| state.eligibility == WorkerComputeGroupEligibility::Eligible)
        .cloned()
        .unwrap_or_else(|| WorkerComputeScalingGroupState {
            fingerprint: group.fingerprint,
            effective_task_types: group.task_types.clone(),
            eligibility: WorkerComputeGroupEligibility::Eligible,
            health: WorkerComputeHealth::Active,
            activation_fingerprint: None,
            activation_status: None,
            last_scale_up_at: None,
            prior_dispatch_rates: BTreeMap::new(),
            last_action_id: None,
            last_failure_category: None,
        });
    if state.fingerprint != group.fingerprint {
        state.activation_fingerprint = None;
        state.activation_status = None;
        state.last_action_id = None;
        state.last_failure_category = None;
        state.health = WorkerComputeHealth::Active;
    }
    state.fingerprint = group.fingerprint;
    state.effective_task_types.clone_from(&group.task_types);
    state.eligibility = WorkerComputeGroupEligibility::Eligible;
    state
}

fn scaler_state(state: &WorkerComputeScalingGroupState) -> NoSyncState {
    NoSyncState {
        last_scale_up_at: state.last_scale_up_at,
        prior_dispatch_rates: state.prior_dispatch_rates.clone(),
    }
}

fn observation_batch_for_group(
    batch: &ObservationBatch,
    task_types: &BTreeSet<WorkerComputeTaskType>,
) -> ObservationBatch {
    let counts_by_task_type = batch
        .counts_by_task_type
        .iter()
        .filter(|(task_type, _)| task_types.contains(task_type))
        .map(|(task_type, counts)| (*task_type, *counts))
        .collect::<BTreeMap<_, _>>();
    let (sync_count, no_sync_count) =
        counts_by_task_type
            .values()
            .fold((0_u64, 0_u64), |(sync, no_sync), counts| {
                (
                    sync.saturating_add(counts.sync_count),
                    no_sync.saturating_add(counts.no_sync_count),
                )
            });
    ObservationBatch {
        first_observed_at: batch.first_observed_at,
        first_no_sync_at: (no_sync_count > 0)
            .then_some(batch.first_no_sync_at.unwrap_or(batch.first_observed_at)),
        sync_count,
        no_sync_count,
        task_types: counts_by_task_type.keys().copied().collect(),
        counts_by_task_type,
        task_queues: batch
            .task_queues
            .iter()
            .filter(|binding| task_types.contains(&binding.task_type))
            .cloned()
            .collect(),
    }
}

fn sample_queue_bindings(
    samples: &[WorkerComputeQueueSample],
) -> BTreeSet<WorkerComputeTaskQueueBinding> {
    samples
        .iter()
        .map(|sample| WorkerComputeTaskQueueBinding {
            name: sample.key.task_queue.clone(),
            task_type: sample.key.task_type,
        })
        .collect()
}

fn union_queue_bindings(
    version: &[WorkerComputeTaskQueueBinding],
    additional: impl IntoIterator<Item = WorkerComputeTaskQueueBinding>,
    task_types: &BTreeSet<WorkerComputeTaskType>,
) -> Vec<WorkerComputeTaskQueueBinding> {
    version
        .iter()
        .cloned()
        .chain(additional)
        .filter(|binding| task_types.contains(&binding.task_type))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn unsupported_group_state(group: &UnsupportedScalingGroup) -> WorkerComputeScalingGroupState {
    let (eligibility, health) = match group.reason {
        UnsupportedScalingGroupReason::DirectProvider => (
            WorkerComputeGroupEligibility::UnsupportedProvider,
            WorkerComputeHealth::UnsupportedProvider,
        ),
        UnsupportedScalingGroupReason::RateBasedScaler => (
            WorkerComputeGroupEligibility::UnsupportedScaler,
            WorkerComputeHealth::UnsupportedScaler,
        ),
    };
    WorkerComputeScalingGroupState {
        fingerprint: ConfigurationFingerprint::from_bytes([0; 32]),
        effective_task_types: group.task_types.clone(),
        eligibility,
        health,
        activation_fingerprint: None,
        activation_status: None,
        last_scale_up_at: None,
        prior_dispatch_rates: BTreeMap::new(),
        last_action_id: None,
        last_failure_category: None,
    }
}

fn version_queue_bindings(version: &StoredVersion) -> Vec<WorkerComputeTaskQueueBinding> {
    version
        .polled_task_queues
        .iter()
        .filter_map(|queue| {
            let task_type = match queue.task_queue_type {
                DeploymentTaskQueueType::Workflow => WorkerComputeTaskType::Workflow,
                DeploymentTaskQueueType::Activity => WorkerComputeTaskType::Activity,
                DeploymentTaskQueueType::Nexus => WorkerComputeTaskType::Nexus,
                DeploymentTaskQueueType::Unspecified => return None,
            };
            Some(WorkerComputeTaskQueueBinding {
                name: tokeira_types::TaskQueueName(queue.name.clone()),
                task_type,
            })
        })
        .collect()
}

fn next_metrics_poll_at(
    config: &ValidatedComputeConfig,
    now: OffsetDateTime,
) -> Option<OffsetDateTime> {
    config
        .groups
        .values()
        .filter_map(|group| match group {
            ValidatedScalingGroup::Eligible(group) => Some(group.scaler.metrics_poll_interval_ms),
            ValidatedScalingGroup::Unsupported(_) => None,
        })
        .min()
        .map(|milliseconds| now + time::Duration::milliseconds(milliseconds))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, sync::Arc};

    use proptest::prelude::*;
    use prost::Message;
    use time::Duration;
    use tokeira_proto::compute::v1::InvokeWorkerRequest;
    use tokeira_storage::{
        BuildId as StoredBuildId, ComputeConfig, ComputeConfigScalingGroup, ComputeProvider,
        ComputeScaler, ConflictToken, DeploymentCasResult, DeploymentName, InMemoryStore,
        InMemoryWorkerComputeRepository, RoutingConfigUpdateState, StoredRoutingConfig,
        StoredWorkerDeployment, VersionMetadata, WorkerDeploymentRepository,
        WorkerDeploymentVersionStatus,
    };
    use tokeira_types::{NamespaceId, Payload, TaskQueueName};

    use super::{super::TaskTypeObservationCounts, *};

    fn remote_group(
        marker: &str,
        task_types: Vec<DeploymentTaskQueueType>,
    ) -> ComputeConfigScalingGroup {
        ComputeConfigScalingGroup {
            task_queue_types: task_types,
            provider: Some(ComputeProvider {
                provider_type: "test-remote".to_owned(),
                details: Some(Payload::new(marker)),
                nexus_endpoint: "worker-compute-endpoint".to_owned(),
            }),
            scaler: Some(ComputeScaler {
                scaler_type: "no-sync".to_owned(),
                details: None,
            }),
        }
    }

    fn version(marker: &str) -> StoredVersion {
        StoredVersion {
            build_id: StoredBuildId("build-a".to_owned()),
            status: WorkerDeploymentVersionStatus::Inactive,
            create_time: OffsetDateTime::UNIX_EPOCH,
            routing_changed_time: None,
            current_since_time: None,
            ramping_since_time: None,
            first_activation_time: None,
            last_current_time: None,
            last_deactivation_time: None,
            ramp_percentage: 0.0,
            drainage_info: None,
            metadata: VersionMetadata::default(),
            compute_config: ComputeConfig {
                scaling_groups: BTreeMap::from([
                    (
                        "activity".to_owned(),
                        remote_group(marker, vec![DeploymentTaskQueueType::Activity]),
                    ),
                    (
                        "workflow".to_owned(),
                        remote_group(marker, vec![DeploymentTaskQueueType::Workflow]),
                    ),
                ]),
            },
            last_modifier_identity: "test".to_owned(),
            polled_task_queues: BTreeSet::new(),
            create_request_ids: BTreeSet::new(),
            compute_config_request_ids: BTreeSet::new(),
        }
    }

    fn deployment(namespace_id: NamespaceId, marker: &str) -> StoredWorkerDeployment {
        let version = version(marker);
        StoredWorkerDeployment {
            namespace_id,
            name: DeploymentName("deployment-a".to_owned()),
            create_time: OffsetDateTime::UNIX_EPOCH,
            routing_config: StoredRoutingConfig::default(),
            last_modifier_identity: "test".to_owned(),
            manager_identity: None,
            routing_config_update_state: RoutingConfigUpdateState::Completed,
            versions: BTreeMap::from([(version.build_id.clone(), version)]),
            conflict_token: ConflictToken::default(),
            create_request_ids: BTreeSet::new(),
        }
    }

    fn controller_key(namespace_id: NamespaceId) -> ControllerInstanceKey {
        ControllerInstanceKey {
            namespace_id,
            deployment_name: DeploymentId("deployment-a".to_owned()),
            build_id: BuildId("build-a".to_owned()),
        }
    }

    fn group_state<'a>(
        record: &'a WorkerComputeControllerRecord,
        group: &str,
    ) -> &'a WorkerComputeScalingGroupState {
        record
            .groups
            .get(&ScalingGroupId(group.to_owned()))
            .expect("expected scaling group")
    }

    async fn fixture() -> (
        WorkerComputeNamespace,
        ControllerInstanceKey,
        Arc<InMemoryStore>,
        Arc<InMemoryWorkerComputeRepository>,
        WorkerComputeReconciler,
    ) {
        let namespace_id = NamespaceId::new();
        let namespace = WorkerComputeNamespace {
            namespace_id,
            name: "payments".to_owned(),
        };
        let key = controller_key(namespace_id);
        let deployments = Arc::new(InMemoryStore::default());
        assert!(matches!(
            deployments
                .put_deployment(deployment(namespace_id, "v1"), None)
                .await
                .expect("deployment insert"),
            DeploymentCasResult::Applied { .. }
        ));
        let controllers = Arc::new(InMemoryWorkerComputeRepository::default());
        let reconciler = WorkerComputeReconciler::new(
            deployments.clone(),
            controllers.clone(),
            IncarnationId::new(),
        );
        (namespace, key, deployments, controllers, reconciler)
    }

    #[test]
    fn mixed_batches_route_no_sync_only_to_its_effective_group() {
        let now = OffsetDateTime::UNIX_EPOCH;
        let batch = ObservationBatch {
            first_observed_at: now,
            first_no_sync_at: Some(now),
            sync_count: 1,
            no_sync_count: 1,
            task_types: BTreeSet::from([
                WorkerComputeTaskType::Workflow,
                WorkerComputeTaskType::Activity,
            ]),
            counts_by_task_type: BTreeMap::from([
                (
                    WorkerComputeTaskType::Workflow,
                    TaskTypeObservationCounts {
                        sync_count: 1,
                        no_sync_count: 0,
                    },
                ),
                (
                    WorkerComputeTaskType::Activity,
                    TaskTypeObservationCounts {
                        sync_count: 0,
                        no_sync_count: 1,
                    },
                ),
            ]),
            task_queues: BTreeSet::from([
                WorkerComputeTaskQueueBinding {
                    name: TaskQueueName("workflow".to_owned()),
                    task_type: WorkerComputeTaskType::Workflow,
                },
                WorkerComputeTaskQueueBinding {
                    name: TaskQueueName("activity".to_owned()),
                    task_type: WorkerComputeTaskType::Activity,
                },
            ]),
        };

        let workflow =
            observation_batch_for_group(&batch, &BTreeSet::from([WorkerComputeTaskType::Workflow]));
        let activity =
            observation_batch_for_group(&batch, &BTreeSet::from([WorkerComputeTaskType::Activity]));
        assert_eq!((workflow.sync_count, workflow.no_sync_count), (1, 0));
        assert_eq!((activity.sync_count, activity.no_sync_count), (0, 1));
    }

    #[tokio::test]
    async fn startup_reconcile_activates_once_and_preserves_scaler_state_on_change() {
        let (namespace, key, deployments, controllers, reconciler) = fixture().await;
        let now = OffsetDateTime::now_utc();
        reconciler
            .reconcile_namespace(&namespace, now)
            .await
            .expect("startup reconcile");
        let initial = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller admitted");
        assert_eq!(
            group_state(&initial, "workflow").activation_status,
            Some(WorkerComputeProviderActionStatus::Pending)
        );
        assert!(group_state(&initial, "workflow").last_scale_up_at.is_none());
        let initial_action = group_state(&initial, "workflow")
            .last_action_id
            .expect("activation action");

        reconciler
            .reconcile_key(&namespace, &key, now + Duration::seconds(1))
            .await
            .expect("duplicate reconcile");
        let unchanged = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        assert_eq!(
            group_state(&unchanged, "workflow").last_action_id,
            Some(initial_action)
        );

        let batch = ObservationBatch {
            first_observed_at: now,
            first_no_sync_at: Some(now),
            sync_count: 0,
            no_sync_count: 1,
            task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
            counts_by_task_type: BTreeMap::from([(
                WorkerComputeTaskType::Workflow,
                TaskTypeObservationCounts {
                    sync_count: 0,
                    no_sync_count: 1,
                },
            )]),
            task_queues: BTreeSet::from([WorkerComputeTaskQueueBinding {
                name: TaskQueueName("workflow".to_owned()),
                task_type: WorkerComputeTaskType::Workflow,
            }]),
        };
        reconciler
            .evaluate_observation_batch(&namespace, &key, &batch, now + Duration::seconds(2))
            .await
            .expect("observation evaluation");
        let scaled = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        let last_scale_up_at = group_state(&scaled, "workflow").last_scale_up_at;
        assert_eq!(last_scale_up_at, Some(now + Duration::seconds(2)));

        let deployment_key = DeploymentKey {
            namespace_id: namespace.namespace_id,
            deployment_name: DeploymentName("deployment-a".to_owned()),
        };
        let mut changed = deployments
            .load_deployment(&deployment_key)
            .await
            .expect("deployment load")
            .expect("deployment exists");
        let expected = changed.conflict_token;
        changed
            .versions
            .get_mut(&StoredBuildId("build-a".to_owned()))
            .expect("version exists")
            .compute_config = version("v2").compute_config;
        assert!(matches!(
            deployments
                .put_deployment(changed, Some(expected))
                .await
                .expect("deployment update"),
            DeploymentCasResult::Applied { .. }
        ));
        reconciler
            .reconcile_key(&namespace, &key, now + Duration::seconds(3))
            .await
            .expect("changed reconcile");
        let changed = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        assert_ne!(
            group_state(&changed, "workflow").last_action_id,
            Some(initial_action)
        );
        assert_eq!(
            group_state(&changed, "workflow").last_scale_up_at,
            last_scale_up_at
        );
    }

    #[tokio::test]
    async fn activation_before_membership_has_an_empty_canonical_queue_list() {
        let (namespace, _key, _deployments, controllers, reconciler) = fixture().await;
        let now = OffsetDateTime::now_utc();
        reconciler
            .reconcile_namespace(&namespace, now)
            .await
            .expect("startup reconcile");
        let claimed = controllers
            .claim_due_actions(
                namespace.namespace_id,
                IncarnationId::new(),
                now,
                now + Duration::seconds(30),
                10,
            )
            .await
            .expect("claim activation actions");
        assert_eq!(claimed.len(), 2);
        for action in claimed {
            let request = InvokeWorkerRequest::decode(action.action.request_data.as_slice())
                .expect("canonical request");
            assert!(request.task_queues.is_empty());
            assert_eq!(request.count, 1);
        }
    }

    #[tokio::test]
    async fn periodic_metrics_recover_demand_and_restart_retains_cooloff() {
        let (namespace, key, deployments, controllers, reconciler) = fixture().await;
        let now = OffsetDateTime::now_utc();
        reconciler
            .reconcile_namespace(&namespace, now)
            .await
            .expect("startup reconcile");
        controllers
            .put_queue_sample(WorkerComputeQueueSample {
                key: tokeira_types::WorkerComputeQueueKey {
                    namespace_id: namespace.namespace_id,
                    deployment_name: key.deployment_name.clone(),
                    build_id: key.build_id.clone(),
                    task_type: WorkerComputeTaskType::Workflow,
                    task_queue: TaskQueueName("workflow".to_owned()),
                },
                writer_id: IncarnationId::new(),
                writer_sequence: 1,
                backlog_count: 3,
                add_rate: 4.0,
                dispatch_rate: 2.5,
                sampled_at: now,
            })
            .await
            .expect("queue sample");
        let decision_at = now + Duration::seconds(1);
        reconciler
            .evaluate_metrics_snapshot(&namespace, &key, decision_at)
            .await
            .expect("metrics evaluation");
        let after_scale = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        let workflow = group_state(&after_scale, "workflow");
        assert_eq!(workflow.last_scale_up_at, Some(decision_at));
        assert_eq!(
            workflow
                .prior_dispatch_rates
                .get(&WorkerComputeTaskType::Workflow),
            Some(&2.5)
        );
        let scale_action = workflow.last_action_id.expect("backlog action");

        let restarted =
            WorkerComputeReconciler::new(deployments, controllers.clone(), IncarnationId::new());
        restarted
            .evaluate_metrics_snapshot(&namespace, &key, decision_at + Duration::milliseconds(50))
            .await
            .expect("restart evaluation inside cooloff");
        let inside_cooloff = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        assert_eq!(
            group_state(&inside_cooloff, "workflow").last_action_id,
            Some(scale_action)
        );
        assert_eq!(
            group_state(&inside_cooloff, "workflow").last_scale_up_at,
            Some(decision_at)
        );
    }

    #[tokio::test]
    async fn terminal_delivery_health_does_not_block_a_later_scaler_decision() {
        let (namespace, key, _deployments, controllers, reconciler) = fixture().await;
        let now = OffsetDateTime::now_utc();
        reconciler
            .reconcile_namespace(&namespace, now)
            .await
            .expect("startup reconcile");
        let claimed = controllers
            .claim_due_actions(
                namespace.namespace_id,
                IncarnationId::new(),
                now,
                now + Duration::seconds(30),
                10,
            )
            .await
            .expect("claim activation actions");
        let workflow_action = claimed
            .into_iter()
            .find(|action| action.action.scaling_group.0 == "workflow")
            .expect("workflow activation action");
        controllers
            .finalize_action(
                &workflow_action.claim,
                tokeira_storage::WorkerComputeActionFinalization::TerminalFailure {
                    category: WorkerComputeFailureCategory::NonRetryableHandler,
                    completed_at: now + Duration::seconds(1),
                },
            )
            .await
            .expect("terminal finalization");
        let failed = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        assert_eq!(
            group_state(&failed, "workflow").health,
            WorkerComputeHealth::DeliveryTerminalFailure
        );

        let batch = ObservationBatch {
            first_observed_at: now,
            first_no_sync_at: Some(now),
            sync_count: 0,
            no_sync_count: 1,
            task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
            counts_by_task_type: BTreeMap::from([(
                WorkerComputeTaskType::Workflow,
                TaskTypeObservationCounts {
                    sync_count: 0,
                    no_sync_count: 1,
                },
            )]),
            task_queues: BTreeSet::new(),
        };
        reconciler
            .evaluate_observation_batch(&namespace, &key, &batch, now + Duration::seconds(2))
            .await
            .expect("later scaler decision");
        let recovered = controllers
            .list_controllers(namespace.namespace_id)
            .await
            .expect("controllers")
            .pop()
            .expect("controller retained");
        let group = group_state(&recovered, "workflow");
        assert_eq!(group.health, WorkerComputeHealth::Active);
        assert_eq!(group.last_failure_category, None);
        assert_ne!(group.last_action_id, Some(workflow_action.action.action_id));
    }

    #[tokio::test]
    async fn catalog_sweep_releases_obsolete_slot_before_capacity_promotion() {
        let namespace_id = NamespaceId::new();
        let namespace = WorkerComputeNamespace {
            namespace_id,
            name: "capacity".to_owned(),
        };
        let deployments = Arc::new(InMemoryStore::default());
        let mut record = deployment(namespace_id, "capacity");
        record.versions.clear();
        for index in 0..=100 {
            let mut candidate = version("capacity");
            candidate.build_id = StoredBuildId(format!("build-{index:03}"));
            candidate.compute_config.scaling_groups.remove("activity");
            record
                .versions
                .insert(candidate.build_id.clone(), candidate);
        }
        assert!(matches!(
            deployments
                .put_deployment(record, None)
                .await
                .expect("deployment insert"),
            DeploymentCasResult::Applied { .. }
        ));
        let controllers = Arc::new(InMemoryWorkerComputeRepository::default());
        let reconciler = WorkerComputeReconciler::new(
            deployments.clone(),
            controllers.clone(),
            IncarnationId::new(),
        );
        let now = OffsetDateTime::now_utc();
        reconciler
            .reconcile_namespace(&namespace, now)
            .await
            .expect("capacity reconcile");
        let before = controllers
            .list_controllers(namespace_id)
            .await
            .expect("controllers");
        assert_eq!(
            before
                .iter()
                .filter(|record| { record.lifecycle == WorkerComputeControllerLifecycle::Active })
                .count(),
            100
        );
        assert_eq!(
            before
                .iter()
                .filter(|record| {
                    record.lifecycle == WorkerComputeControllerLifecycle::CapacityLimited
                })
                .count(),
            1
        );

        let deployment_key = DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName("deployment-a".to_owned()),
        };
        let mut changed = deployments
            .load_deployment(&deployment_key)
            .await
            .expect("deployment load")
            .expect("deployment exists");
        let expected = changed.conflict_token;
        changed
            .versions
            .get_mut(&StoredBuildId("build-000".to_owned()))
            .expect("first version")
            .compute_config = ComputeConfig::default();
        assert!(matches!(
            deployments
                .put_deployment(changed, Some(expected))
                .await
                .expect("deployment update"),
            DeploymentCasResult::Applied { .. }
        ));

        reconciler
            .reconcile_namespace(&namespace, now + Duration::seconds(60))
            .await
            .expect("promotion reconcile");
        let after = controllers
            .list_controllers(namespace_id)
            .await
            .expect("controllers");
        assert_eq!(
            after
                .iter()
                .filter(|record| { record.lifecycle == WorkerComputeControllerLifecycle::Active })
                .count(),
            100
        );
        assert_eq!(
            after
                .iter()
                .filter(|record| {
                    record.lifecycle == WorkerComputeControllerLifecycle::CapacityLimited
                })
                .count(),
            0
        );
        let promoted = after
            .iter()
            .find(|record| record.key.build_id.0 == "build-100")
            .expect("waiting controller retained");
        assert_eq!(promoted.lifecycle, WorkerComputeControllerLifecycle::Active);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: worker-compute-controller, Property 4: one activation per group fingerprint
        #[test]
        fn property_one_activation_per_group_fingerprint(
            initial in any::<[u8; 32]>(),
            changed in any::<[u8; 32]>(),
            duplicate_hints in 1usize..20,
        ) {
            let group = |fingerprint| EffectiveScalingGroup {
                id: ScalingGroupId("group".to_owned()),
                task_types: BTreeSet::from([WorkerComputeTaskType::Workflow]),
                provider: super::super::RemoteNexusProvider {
                    provider_type: "remote".to_owned(),
                    details: None,
                    nexus_endpoint: "endpoint".to_owned(),
                },
                scaler: super::super::NoSyncConfig::default(),
                scaler_details: None,
                fingerprint: ConfigurationFingerprint::from_bytes(fingerprint),
            };
            let mut state = eligible_group_state(None, &group(initial));
            let mut activations = 0usize;
            for _ in 0..duplicate_hints {
                if state.activation_fingerprint != Some(state.fingerprint) {
                    state.activation_fingerprint = Some(state.fingerprint);
                    activations += 1;
                }
                state = eligible_group_state(Some(&state), &group(initial));
            }
            prop_assert_eq!(activations, 1);

            let changed_group = group(changed);
            state = eligible_group_state(Some(&state), &changed_group);
            for _ in 0..duplicate_hints {
                if state.activation_fingerprint != Some(state.fingerprint) {
                    state.activation_fingerprint = Some(state.fingerprint);
                    activations += 1;
                }
                state = eligible_group_state(Some(&state), &changed_group);
            }
            let expected = if initial == changed { 1 } else { 2 };
            prop_assert_eq!(activations, expected);
        }
    }
}
