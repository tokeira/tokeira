//! Aurora DSQL persistence for Worker Compute Controller coordination.
//!
//! Namespace capacity is represented by 100 contender rows rather than a hot
//! aggregate counter. Controller decisions use owner-epoch plus revision fencing,
//! and provider actions are inserted in the same transaction as scaler state.
//! Action delivery scans one namespace and one UUID-derived bucket at a time.

use std::sync::Arc;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use sqlx::{Connection, Row, postgres::PgRow};
use time::OffsetDateTime;
use tokeira_types::{
    BuildId, ConfigurationFingerprint, ControllerInstanceKey, DeploymentId, IncarnationId,
    NamespaceId, ScalingGroupId, TaskQueueName, WorkerComputeControllerLifecycle,
    WorkerComputeFailureCategory, WorkerComputeHealth, WorkerComputeInvokeReason,
    WorkerComputeProviderActionStatus, WorkerComputeQueueKey, WorkerComputeTaskType,
};
use uuid::Uuid;

use crate::{
    ClaimedWorkerComputeController, ClaimedWorkerComputeProviderAction, DbClass,
    WORKER_COMPUTE_ACTION_BUCKETS, WORKER_COMPUTE_ACTION_CLAIM_LIMIT,
    WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT, WorkerComputeActionAttemptStart, WorkerComputeActionClaim,
    WorkerComputeActionFinalization, WorkerComputeActionFinalizeResult,
    WorkerComputeControllerAdmission, WorkerComputeControllerClaim,
    WorkerComputeControllerCommitResult, WorkerComputeControllerHealthView,
    WorkerComputeControllerRecord, WorkerComputeHealthFilter, WorkerComputeProviderAction,
    WorkerComputeQueueSample, WorkerComputeRepository,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, DsqlRunRepository, codec};

const ACTION_COLUMNS: &str = "
    action_id, due_bucket, namespace_id, deployment_name, build_id, scaling_group,
    configuration_fingerprint, reason, status, next_attempt_at, claim_owner,
    claim_epoch, claim_until, attempts, attempt_started_at, endpoint_name,
    request_data, last_error_category, superseded_at, created_at, updated_at";
const INSERT_ACTION_SQL: &str = "
    INSERT INTO worker_compute_action
    (action_id, due_bucket, namespace_id, deployment_name, build_id,
     scaling_group, configuration_fingerprint, reason, status,
     next_attempt_at, claim_owner, claim_epoch, claim_until, attempts,
     attempt_started_at, endpoint_name, request_data, last_error_category,
     superseded_at, created_at, updated_at)
    VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
     $15, $16, $17, $18, $19, $20, $21)
    ON CONFLICT (action_id) DO NOTHING";

/// DSQL-backed Worker Compute Controller repository.
#[derive(Debug)]
pub struct DsqlWorkerComputeRepository {
    director: Arc<dyn DsqlConnectionAcquirer>,
}

impl DsqlWorkerComputeRepository {
    /// Construct a repository over the process-wide DSQL connection director.
    #[must_use]
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
        }
    }

    /// Read stable, redacted controller health through a caller-owned connection.
    ///
    /// Operator diagnostics use this query-only entry point so they do not start the
    /// runtime connection reservoir or its DynamoDB coordination machinery.
    pub async fn list_health_with_connection(
        connection: &mut sqlx::PgConnection,
        namespace_id: NamespaceId,
        filter: WorkerComputeHealthFilter,
    ) -> Result<Vec<WorkerComputeControllerHealthView>> {
        let rows = sqlx::query(
            "SELECT revision, lease_epoch, record_data
             FROM worker_compute_controller
             WHERE namespace_id = $1
             ORDER BY deployment_name ASC, build_id ASC",
        )
        .bind(namespace_id.0)
        .fetch_all(connection)
        .await?;
        let mut views = Vec::new();
        for row in &rows {
            let controller = Self::decode_controller_row(row)?;
            if filter
                .deployment_name
                .as_ref()
                .is_some_and(|value| controller.key.deployment_name.0 != *value)
                || filter
                    .build_id
                    .as_ref()
                    .is_some_and(|value| controller.key.build_id.0 != *value)
            {
                continue;
            }
            for (group_id, group) in &controller.groups {
                if filter
                    .scaling_group
                    .as_ref()
                    .is_some_and(|value| group_id.0 != *value)
                {
                    continue;
                }
                views.push(WorkerComputeControllerHealthView {
                    namespace_name: controller.namespace_name.clone(),
                    controller_key: controller.key.clone(),
                    scaling_group: group_id.clone(),
                    fingerprint: group.fingerprint,
                    health: group.health,
                    last_action_id: group.last_action_id,
                    last_failure_category: group.last_failure_category,
                    next_metrics_poll_at: controller.next_metrics_poll_at,
                });
            }
        }
        Ok(views)
    }

    fn i64(value: u64, field: &str) -> Result<i64> {
        i64::try_from(value).map_err(|_| anyhow::anyhow!("{field} exceeds DSQL BIGINT"))
    }

    fn u64(value: i64, field: &str) -> Result<u64> {
        u64::try_from(value).map_err(|_| anyhow::anyhow!("{field} is negative"))
    }

    fn encode_task_type(task_type: WorkerComputeTaskType) -> i16 {
        match task_type {
            WorkerComputeTaskType::Workflow => 1,
            WorkerComputeTaskType::Activity => 2,
            WorkerComputeTaskType::Nexus => 3,
        }
    }

    fn decode_task_type(value: i16) -> Result<WorkerComputeTaskType> {
        match value {
            1 => Ok(WorkerComputeTaskType::Workflow),
            2 => Ok(WorkerComputeTaskType::Activity),
            3 => Ok(WorkerComputeTaskType::Nexus),
            _ => anyhow::bail!("unknown worker-compute task type {value}"),
        }
    }

    fn encode_status(status: WorkerComputeProviderActionStatus) -> i16 {
        match status {
            WorkerComputeProviderActionStatus::Pending => 0,
            WorkerComputeProviderActionStatus::Claimed => 1,
            WorkerComputeProviderActionStatus::Delivered => 2,
            WorkerComputeProviderActionStatus::TerminalFailed => 3,
            WorkerComputeProviderActionStatus::Superseded => 4,
        }
    }

    fn decode_status(value: i16) -> Result<WorkerComputeProviderActionStatus> {
        match value {
            0 => Ok(WorkerComputeProviderActionStatus::Pending),
            1 => Ok(WorkerComputeProviderActionStatus::Claimed),
            2 => Ok(WorkerComputeProviderActionStatus::Delivered),
            3 => Ok(WorkerComputeProviderActionStatus::TerminalFailed),
            4 => Ok(WorkerComputeProviderActionStatus::Superseded),
            _ => anyhow::bail!("unknown worker-compute action status {value}"),
        }
    }

    fn encode_reason(reason: WorkerComputeInvokeReason) -> i16 {
        match reason {
            WorkerComputeInvokeReason::ConfigurationActivation => 0,
            WorkerComputeInvokeReason::NoSyncMatch => 1,
            WorkerComputeInvokeReason::Backlog => 2,
            WorkerComputeInvokeReason::WorkerRefresh => 3,
        }
    }

    fn decode_reason(value: i16) -> Result<WorkerComputeInvokeReason> {
        match value {
            0 => Ok(WorkerComputeInvokeReason::ConfigurationActivation),
            1 => Ok(WorkerComputeInvokeReason::NoSyncMatch),
            2 => Ok(WorkerComputeInvokeReason::Backlog),
            3 => Ok(WorkerComputeInvokeReason::WorkerRefresh),
            _ => anyhow::bail!("unknown worker-compute action reason {value}"),
        }
    }

    fn encode_failure_category(category: WorkerComputeFailureCategory) -> &'static str {
        match category {
            WorkerComputeFailureCategory::NamespaceUnresolved => "namespace_unresolved",
            WorkerComputeFailureCategory::EndpointNotFound => "endpoint_not_found",
            WorkerComputeFailureCategory::Transport => "transport",
            WorkerComputeFailureCategory::RetryableHandler => "retryable_handler",
            WorkerComputeFailureCategory::NonRetryableHandler => "non_retryable_handler",
            WorkerComputeFailureCategory::OperationUnsuccessful => "operation_unsuccessful",
            WorkerComputeFailureCategory::AsyncResponse => "async_response",
            WorkerComputeFailureCategory::RequestTooLarge => "request_too_large",
            WorkerComputeFailureCategory::InvalidResponsePayload => "invalid_response_payload",
            WorkerComputeFailureCategory::ResponseIdMismatch => "response_id_mismatch",
            WorkerComputeFailureCategory::Storage => "storage",
        }
    }

    fn decode_failure_category(value: &str) -> Result<WorkerComputeFailureCategory> {
        match value {
            "namespace_unresolved" => Ok(WorkerComputeFailureCategory::NamespaceUnresolved),
            "endpoint_not_found" => Ok(WorkerComputeFailureCategory::EndpointNotFound),
            "transport" => Ok(WorkerComputeFailureCategory::Transport),
            "retryable_handler" => Ok(WorkerComputeFailureCategory::RetryableHandler),
            "non_retryable_handler" => Ok(WorkerComputeFailureCategory::NonRetryableHandler),
            "operation_unsuccessful" => Ok(WorkerComputeFailureCategory::OperationUnsuccessful),
            "async_response" => Ok(WorkerComputeFailureCategory::AsyncResponse),
            "request_too_large" => Ok(WorkerComputeFailureCategory::RequestTooLarge),
            "invalid_response_payload" => Ok(WorkerComputeFailureCategory::InvalidResponsePayload),
            "response_id_mismatch" => Ok(WorkerComputeFailureCategory::ResponseIdMismatch),
            "storage" => Ok(WorkerComputeFailureCategory::Storage),
            _ => anyhow::bail!("unknown worker-compute failure category {value:?}"),
        }
    }

    fn fingerprint(bytes: Vec<u8>) -> Result<ConfigurationFingerprint> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            anyhow::anyhow!(
                "worker-compute configuration fingerprint has {} bytes, expected 32",
                bytes.len()
            )
        })?;
        Ok(ConfigurationFingerprint::from_bytes(bytes))
    }

    fn decode_controller_row(row: &PgRow) -> Result<WorkerComputeControllerRecord> {
        let revision = Self::u64(row.try_get("revision")?, "controller revision")?;
        let epoch = Self::u64(row.try_get("lease_epoch")?, "controller lease epoch")?;
        let record_data: Vec<u8> = row.try_get("record_data")?;
        let record = codec::decode_worker_compute_controller(&record_data)?;
        ensure!(
            record.revision == revision,
            "worker_compute_controller revision column {revision} disagrees with record {}",
            record.revision
        );
        ensure!(
            record.owner_epoch == epoch,
            "worker_compute_controller lease_epoch column {epoch} disagrees with record {}",
            record.owner_epoch
        );
        Ok(record)
    }

    fn decode_action_row(row: &PgRow) -> Result<WorkerComputeProviderAction> {
        let namespace_id = NamespaceId(row.try_get("namespace_id")?);
        let claim_owner = row
            .try_get::<Option<Uuid>, _>("claim_owner")?
            .map(IncarnationId);
        let claim_epoch = Self::u64(row.try_get("claim_epoch")?, "action claim epoch")?;
        let claim_until = row.try_get::<Option<OffsetDateTime>, _>("claim_until")?;
        let action_id = row.try_get("action_id")?;
        let claim = match (claim_owner, claim_until) {
            (Some(owner), Some(claim_until)) => Some(WorkerComputeActionClaim {
                action_id,
                owner,
                claim_epoch,
                claim_until,
            }),
            (None, None) => None,
            _ => anyhow::bail!(
                "worker_compute_action {action_id} has inconsistent claim owner/deadline"
            ),
        };
        let last_error_category = row
            .try_get::<Option<String>, _>("last_error_category")?
            .as_deref()
            .map(Self::decode_failure_category)
            .transpose()?;
        let due_bucket: i16 = row.try_get("due_bucket")?;
        let due_bucket = u8::try_from(due_bucket)
            .map_err(|_| anyhow::anyhow!("invalid action due bucket {due_bucket}"))?;
        ensure!(
            due_bucket < WORKER_COMPUTE_ACTION_BUCKETS,
            "action due bucket {due_bucket} exceeds fixed bucket count"
        );

        Ok(WorkerComputeProviderAction {
            action_id,
            due_bucket,
            controller_key: ControllerInstanceKey {
                namespace_id,
                deployment_name: DeploymentId(row.try_get("deployment_name")?),
                build_id: BuildId(row.try_get("build_id")?),
            },
            scaling_group: ScalingGroupId(row.try_get("scaling_group")?),
            configuration_fingerprint: Self::fingerprint(
                row.try_get("configuration_fingerprint")?,
            )?,
            endpoint_name: row.try_get("endpoint_name")?,
            reason: Self::decode_reason(row.try_get("reason")?)?,
            request_data: row.try_get("request_data")?,
            status: Self::decode_status(row.try_get("status")?)?,
            attempts: Self::u64(row.try_get("attempts")?, "action attempts")?,
            attempt_started_at: row.try_get("attempt_started_at")?,
            claim_epoch,
            next_attempt_at: row.try_get("next_attempt_at")?,
            claim,
            superseded_at: row.try_get("superseded_at")?,
            last_error_category,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    async fn insert_action(
        tx: &mut sqlx::PgConnection,
        action: &WorkerComputeProviderAction,
    ) -> Result<u64> {
        let last_error_category = action
            .last_error_category
            .map(Self::encode_failure_category);
        let result = sqlx::query(INSERT_ACTION_SQL)
            .bind(action.action_id)
            .bind(i16::from(action.due_bucket))
            .bind(action.controller_key.namespace_id.0)
            .bind(&action.controller_key.deployment_name.0)
            .bind(&action.controller_key.build_id.0)
            .bind(&action.scaling_group.0)
            .bind(action.configuration_fingerprint.as_bytes().as_slice())
            .bind(Self::encode_reason(action.reason))
            .bind(Self::encode_status(action.status))
            .bind(action.next_attempt_at)
            .bind(action.claim.as_ref().map(|claim| claim.owner.0))
            .bind(Self::i64(action.claim_epoch, "action claim epoch")?)
            .bind(action.claim.as_ref().map(|claim| claim.claim_until))
            .bind(Self::i64(action.attempts, "action attempts")?)
            .bind(action.attempt_started_at)
            .bind(&action.endpoint_name)
            .bind(&action.request_data)
            .bind(last_error_category)
            .bind(action.superseded_at)
            .bind(action.created_at)
            .bind(action.updated_at)
            .execute(&mut *tx)
            .await?;
        Ok(result.rows_affected())
    }

    async fn current_action_fingerprint(
        tx: &mut sqlx::PgConnection,
        action: &WorkerComputeProviderAction,
    ) -> Result<(Option<WorkerComputeControllerRecord>, bool)> {
        let row = sqlx::query(
            "SELECT revision, lease_epoch, record_data
             FROM worker_compute_controller
             WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3",
        )
        .bind(action.controller_key.namespace_id.0)
        .bind(&action.controller_key.deployment_name.0)
        .bind(&action.controller_key.build_id.0)
        .fetch_optional(&mut *tx)
        .await?;
        let controller = row.as_ref().map(Self::decode_controller_row).transpose()?;
        let current = controller
            .as_ref()
            .and_then(|controller| controller.groups.get(&action.scaling_group))
            .is_some_and(|group| group.fingerprint == action.configuration_fingerprint);
        Ok((controller, current))
    }

    async fn update_action(
        tx: &mut sqlx::PgConnection,
        action: &WorkerComputeProviderAction,
        expected_claim: &WorkerComputeActionClaim,
    ) -> Result<u64> {
        let result = sqlx::query(
            "UPDATE worker_compute_action
             SET status = $4, next_attempt_at = $5, claim_owner = $6,
                 claim_epoch = $7, claim_until = $8, attempts = $9,
                 attempt_started_at = $10, last_error_category = $11,
                 superseded_at = $12, updated_at = $13
             WHERE action_id = $1 AND claim_owner = $2 AND claim_epoch = $3",
        )
        .bind(action.action_id)
        .bind(expected_claim.owner.0)
        .bind(Self::i64(
            expected_claim.claim_epoch,
            "expected claim epoch",
        )?)
        .bind(Self::encode_status(action.status))
        .bind(action.next_attempt_at)
        .bind(action.claim.as_ref().map(|claim| claim.owner.0))
        .bind(Self::i64(action.claim_epoch, "action claim epoch")?)
        .bind(action.claim.as_ref().map(|claim| claim.claim_until))
        .bind(Self::i64(action.attempts, "action attempts")?)
        .bind(action.attempt_started_at)
        .bind(
            action
                .last_error_category
                .map(Self::encode_failure_category),
        )
        .bind(action.superseded_at)
        .bind(action.updated_at)
        .execute(&mut *tx)
        .await?;
        Ok(result.rows_affected())
    }
}

#[async_trait]
impl WorkerComputeRepository for DsqlWorkerComputeRepository {
    async fn list_controllers(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<WorkerComputeControllerRecord>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT revision, lease_epoch, record_data
             FROM worker_compute_controller
             WHERE namespace_id = $1
             ORDER BY deployment_name ASC, build_id ASC",
        )
        .bind(namespace_id.0)
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter().map(Self::decode_controller_row).collect()
    }

    async fn inactivate_controller(
        &self,
        key: &ControllerInstanceKey,
        now: OffsetDateTime,
    ) -> Result<Option<WorkerComputeControllerRecord>> {
        for _ in 0..32 {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            let row = sqlx::query(
                "SELECT revision, lease_epoch, record_data
                 FROM worker_compute_controller
                 WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3",
            )
            .bind(key.namespace_id.0)
            .bind(&key.deployment_name.0)
            .bind(&key.build_id.0)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(mut record) = row.as_ref().map(Self::decode_controller_row).transpose()?
            else {
                tx.rollback().await?;
                return Ok(None);
            };
            if record.lifecycle == WorkerComputeControllerLifecycle::Inactive {
                tx.rollback().await?;
                return Ok(Some(record));
            }
            let previous_revision = record.revision;
            if let Some(slot) = record.slot {
                sqlx::query(
                    "DELETE FROM worker_compute_controller_slot
                     WHERE namespace_id = $1 AND slot = $2
                       AND deployment_name = $3 AND build_id = $4",
                )
                .bind(key.namespace_id.0)
                .bind(i16::from(slot))
                .bind(&key.deployment_name.0)
                .bind(&key.build_id.0)
                .execute(&mut *tx)
                .await?;
            }
            record.lifecycle = WorkerComputeControllerLifecycle::Inactive;
            record.slot = None;
            record.owner = None;
            record.lease_until = None;
            record.next_metrics_poll_at = None;
            record.revision = record.revision.saturating_add(1);
            record.reconciled_at = now;
            for group in record.groups.values_mut() {
                group.health = WorkerComputeHealth::Inactive;
            }
            let data = codec::encode_worker_compute_controller(&record)?;
            let update = sqlx::query(
                "UPDATE worker_compute_controller
                 SET revision = $4, active = FALSE, slot = NULL,
                     next_metrics_poll_at = NULL, lease_owner = NULL,
                     lease_until = NULL, record_data = $5, updated_at = $6
                 WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3
                   AND revision = $7",
            )
            .bind(key.namespace_id.0)
            .bind(&key.deployment_name.0)
            .bind(&key.build_id.0)
            .bind(Self::i64(record.revision, "controller revision")?)
            .bind(data)
            .bind(now)
            .bind(Self::i64(
                previous_revision,
                "previous controller revision",
            )?)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() == 1 {
                tx.commit().await?;
                return Ok(Some(record));
            }
            tx.rollback().await?;
        }
        anyhow::bail!("worker-compute controller inactivation exhausted optimistic retries")
    }

    async fn admit_controller(
        &self,
        mut candidate: WorkerComputeControllerRecord,
        namespace_limit: usize,
        now: OffsetDateTime,
    ) -> Result<WorkerComputeControllerAdmission> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let existing = sqlx::query(
            "SELECT revision, lease_epoch, record_data
             FROM worker_compute_controller
             WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3",
        )
        .bind(candidate.key.namespace_id.0)
        .bind(&candidate.key.deployment_name.0)
        .bind(&candidate.key.build_id.0)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(existing) = existing
            .as_ref()
            .map(Self::decode_controller_row)
            .transpose()?
        {
            if existing.lifecycle == WorkerComputeControllerLifecycle::Active {
                tx.rollback().await?;
                return Ok(WorkerComputeControllerAdmission::Existing(existing));
            }
            candidate.revision = existing.revision.saturating_add(1);
            candidate.owner_epoch = existing.owner_epoch;
        }

        let limit = namespace_limit.min(WORKER_COMPUTE_NAMESPACE_SLOT_LIMIT);
        let mut assigned = None;
        for slot in 0..limit {
            let slot = u8::try_from(slot).expect("namespace slot bound is at most 100");
            let inserted = sqlx::query(
                "INSERT INTO worker_compute_controller_slot
                 (namespace_id, slot, deployment_name, build_id, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (namespace_id, slot) DO NOTHING",
            )
            .bind(candidate.key.namespace_id.0)
            .bind(i16::from(slot))
            .bind(&candidate.key.deployment_name.0)
            .bind(&candidate.key.build_id.0)
            .bind(now)
            .execute(&mut *tx)
            .await?;
            if inserted.rows_affected() == 1 {
                assigned = Some(slot);
                break;
            }
        }

        candidate.reconciled_at = now;
        candidate.owner = None;
        candidate.lease_until = None;
        candidate.slot = assigned;
        candidate.lifecycle = if assigned.is_some() {
            WorkerComputeControllerLifecycle::Active
        } else {
            for group in candidate.groups.values_mut() {
                if group.eligibility == tokeira_types::WorkerComputeGroupEligibility::Eligible {
                    group.health = WorkerComputeHealth::CapacityLimited;
                }
            }
            WorkerComputeControllerLifecycle::CapacityLimited
        };
        let record_data = codec::encode_worker_compute_controller(&candidate)?;
        sqlx::query(
            "INSERT INTO worker_compute_controller
             (namespace_id, deployment_name, build_id, revision, active, slot,
              next_metrics_poll_at, lease_owner, lease_epoch, lease_until,
              record_data, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, NULL, $9, $10)
             ON CONFLICT (namespace_id, deployment_name, build_id) DO UPDATE SET
                 revision = EXCLUDED.revision,
                 active = EXCLUDED.active,
                 slot = EXCLUDED.slot,
                 next_metrics_poll_at = EXCLUDED.next_metrics_poll_at,
                 lease_owner = NULL,
                 lease_epoch = EXCLUDED.lease_epoch,
                 lease_until = NULL,
                 record_data = EXCLUDED.record_data,
                 updated_at = EXCLUDED.updated_at",
        )
        .bind(candidate.key.namespace_id.0)
        .bind(&candidate.key.deployment_name.0)
        .bind(&candidate.key.build_id.0)
        .bind(Self::i64(candidate.revision, "controller revision")?)
        .bind(assigned.is_some())
        .bind(assigned.map(i16::from))
        .bind(candidate.next_metrics_poll_at)
        .bind(Self::i64(candidate.owner_epoch, "controller owner epoch")?)
        .bind(record_data)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(if assigned.is_some() {
            WorkerComputeControllerAdmission::Admitted(candidate)
        } else {
            WorkerComputeControllerAdmission::CapacityLimited(candidate)
        })
    }

    async fn claim_controller(
        &self,
        key: &ControllerInstanceKey,
        owner: IncarnationId,
        now: OffsetDateTime,
        lease_until: OffsetDateTime,
    ) -> Result<Option<ClaimedWorkerComputeController>> {
        if lease_until <= now {
            return Ok(None);
        }
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let row = sqlx::query(
            "SELECT revision, lease_epoch, record_data
             FROM worker_compute_controller
             WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3
               AND active = TRUE
               AND (lease_until IS NULL OR lease_until <= $4 OR lease_owner = $5)",
        )
        .bind(key.namespace_id.0)
        .bind(&key.deployment_name.0)
        .bind(&key.build_id.0)
        .bind(now)
        .bind(owner.0)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(mut record) = row.as_ref().map(Self::decode_controller_row).transpose()? else {
            tx.rollback().await?;
            return Ok(None);
        };
        let previous_epoch = record.owner_epoch;
        record.owner_epoch = record.owner_epoch.saturating_add(1);
        record.owner = Some(owner);
        record.lease_until = Some(lease_until);
        let record_data = codec::encode_worker_compute_controller(&record)?;
        let update = sqlx::query(
            "UPDATE worker_compute_controller
             SET lease_owner = $4, lease_epoch = $5, lease_until = $6,
                 record_data = $7, updated_at = $8
             WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3
               AND lease_epoch = $9
               AND (lease_until IS NULL OR lease_until <= $8 OR lease_owner = $4)",
        )
        .bind(key.namespace_id.0)
        .bind(&key.deployment_name.0)
        .bind(&key.build_id.0)
        .bind(owner.0)
        .bind(Self::i64(record.owner_epoch, "controller owner epoch")?)
        .bind(lease_until)
        .bind(record_data)
        .bind(now)
        .bind(Self::i64(
            previous_epoch,
            "previous controller owner epoch",
        )?)
        .execute(&mut *tx)
        .await?;
        if update.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(None);
        }
        match tx.commit().await {
            Ok(()) => {
                let claim = WorkerComputeControllerClaim {
                    key: key.clone(),
                    owner,
                    owner_epoch: record.owner_epoch,
                    lease_until,
                };
                Ok(Some(ClaimedWorkerComputeController { claim, record }))
            }
            Err(error) if DsqlRunRepository::is_serialization_failure(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn commit_decision(
        &self,
        claim: &WorkerComputeControllerClaim,
        expected_revision: u64,
        next: WorkerComputeControllerRecord,
        action: Option<WorkerComputeProviderAction>,
    ) -> Result<WorkerComputeControllerCommitResult> {
        if next.key != claim.key || next.revision != expected_revision.saturating_add(1) {
            return Ok(WorkerComputeControllerCommitResult::Conflict);
        }
        if let Some(action) = action.as_ref()
            && (action.controller_key != claim.key
                || action.due_bucket >= WORKER_COMPUTE_ACTION_BUCKETS
                || action.due_bucket != WorkerComputeProviderAction::due_bucket(action.action_id))
        {
            return Ok(WorkerComputeControllerCommitResult::Conflict);
        }
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        if let Some(action) = action.as_ref()
            && Self::insert_action(&mut tx, action).await? != 1
        {
            tx.rollback().await?;
            return Ok(WorkerComputeControllerCommitResult::Conflict);
        }

        let record_data = codec::encode_worker_compute_controller(&next)?;
        let update = sqlx::query(
            "UPDATE worker_compute_controller
             SET revision = $7, active = $8, slot = $9,
                 next_metrics_poll_at = $10, lease_owner = $11,
                 lease_epoch = $12, lease_until = $13, record_data = $14,
                 updated_at = now()
             WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3
               AND revision = $4 AND lease_owner = $5 AND lease_epoch = $6
               AND lease_until > now()",
        )
        .bind(claim.key.namespace_id.0)
        .bind(&claim.key.deployment_name.0)
        .bind(&claim.key.build_id.0)
        .bind(Self::i64(
            expected_revision,
            "expected controller revision",
        )?)
        .bind(claim.owner.0)
        .bind(Self::i64(claim.owner_epoch, "controller owner epoch")?)
        .bind(Self::i64(next.revision, "controller revision")?)
        .bind(next.lifecycle == WorkerComputeControllerLifecycle::Active)
        .bind(next.slot.map(i16::from))
        .bind(next.next_metrics_poll_at)
        .bind(next.owner.map(|owner| owner.0))
        .bind(Self::i64(next.owner_epoch, "next controller owner epoch")?)
        .bind(next.lease_until)
        .bind(record_data)
        .execute(&mut *tx)
        .await?;
        if update.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(WorkerComputeControllerCommitResult::Fenced);
        }
        if next.lifecycle != WorkerComputeControllerLifecycle::Active {
            sqlx::query(
                "DELETE FROM worker_compute_controller_slot
                 WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3",
            )
            .bind(next.key.namespace_id.0)
            .bind(&next.key.deployment_name.0)
            .bind(&next.key.build_id.0)
            .execute(&mut *tx)
            .await?;
        }
        match tx.commit().await {
            Ok(()) => Ok(WorkerComputeControllerCommitResult::Applied),
            Err(error) if DsqlRunRepository::is_serialization_failure(&error) => {
                Ok(WorkerComputeControllerCommitResult::Conflict)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn put_queue_sample(&self, sample: WorkerComputeQueueSample) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        sqlx::query(
            "INSERT INTO worker_compute_queue_sample
             (namespace_id, deployment_name, build_id, task_type, task_queue,
              writer_id, writer_sequence, backlog_count, add_rate, dispatch_rate, sampled_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
             ON CONFLICT (namespace_id, deployment_name, build_id, task_type, task_queue)
             DO UPDATE SET writer_id = EXCLUDED.writer_id,
                 writer_sequence = EXCLUDED.writer_sequence,
                 backlog_count = EXCLUDED.backlog_count,
                 add_rate = EXCLUDED.add_rate,
                 dispatch_rate = EXCLUDED.dispatch_rate,
                 sampled_at = EXCLUDED.sampled_at
             WHERE
                 (worker_compute_queue_sample.writer_id = EXCLUDED.writer_id
                  AND worker_compute_queue_sample.writer_sequence < EXCLUDED.writer_sequence)
                 OR
                 (worker_compute_queue_sample.writer_id <> EXCLUDED.writer_id
                  AND worker_compute_queue_sample.sampled_at <= EXCLUDED.sampled_at)",
        )
        .bind(sample.key.namespace_id.0)
        .bind(&sample.key.deployment_name.0)
        .bind(&sample.key.build_id.0)
        .bind(Self::encode_task_type(sample.key.task_type))
        .bind(&sample.key.task_queue.0)
        .bind(sample.writer_id.0)
        .bind(Self::i64(
            sample.writer_sequence,
            "queue sample writer sequence",
        )?)
        .bind(Self::i64(
            sample.backlog_count,
            "queue sample backlog count",
        )?)
        .bind(sample.add_rate)
        .bind(sample.dispatch_rate)
        .bind(sample.sampled_at)
        .execute(permit.connection()?)
        .await?;
        Ok(())
    }

    async fn list_queue_samples(
        &self,
        key: &ControllerInstanceKey,
        not_before: OffsetDateTime,
    ) -> Result<Vec<WorkerComputeQueueSample>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT task_type, task_queue, writer_id, writer_sequence, backlog_count,
                    add_rate, dispatch_rate, sampled_at
             FROM worker_compute_queue_sample
             WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3
               AND sampled_at >= $4
             ORDER BY task_type ASC, task_queue ASC",
        )
        .bind(key.namespace_id.0)
        .bind(&key.deployment_name.0)
        .bind(&key.build_id.0)
        .bind(not_before)
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter()
            .map(|row| {
                Ok(WorkerComputeQueueSample {
                    key: WorkerComputeQueueKey {
                        namespace_id: key.namespace_id,
                        deployment_name: key.deployment_name.clone(),
                        build_id: key.build_id.clone(),
                        task_type: Self::decode_task_type(row.try_get("task_type")?)?,
                        task_queue: TaskQueueName(row.try_get("task_queue")?),
                    },
                    writer_id: IncarnationId(row.try_get("writer_id")?),
                    writer_sequence: Self::u64(
                        row.try_get("writer_sequence")?,
                        "queue sample writer sequence",
                    )?,
                    backlog_count: Self::u64(
                        row.try_get("backlog_count")?,
                        "queue sample backlog count",
                    )?,
                    add_rate: row.try_get("add_rate")?,
                    dispatch_rate: row.try_get("dispatch_rate")?,
                    sampled_at: row.try_get("sampled_at")?,
                })
            })
            .collect()
    }

    async fn claim_due_actions(
        &self,
        namespace_id: NamespaceId,
        owner: IncarnationId,
        now: OffsetDateTime,
        claim_until: OffsetDateTime,
        limit: usize,
    ) -> Result<Vec<ClaimedWorkerComputeProviderAction>> {
        let limit = limit.min(WORKER_COMPUTE_ACTION_CLAIM_LIMIT);
        if claim_until <= now || limit == 0 {
            return Ok(Vec::new());
        }
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let start_bucket = owner.0.as_bytes()[0] >> 2;
        let mut claimed = Vec::with_capacity(limit.min(WORKER_COMPUTE_ACTION_BUCKETS as usize));

        for offset in 0..WORKER_COMPUTE_ACTION_BUCKETS {
            if claimed.len() == limit {
                break;
            }
            let bucket = start_bucket.wrapping_add(offset) & (WORKER_COMPUTE_ACTION_BUCKETS - 1);
            let remaining = i64::try_from(limit - claimed.len())
                .map_err(|_| anyhow::anyhow!("action claim limit exceeds i64"))?;
            let query = format!(
                "SELECT {ACTION_COLUMNS}
                 FROM worker_compute_action
                 WHERE namespace_id = $1 AND due_bucket = $2
                   AND next_attempt_at <= $3
                   AND (status = $4 OR (status = $5 AND claim_until <= $3))
                 ORDER BY next_attempt_at ASC, action_id ASC
                 LIMIT $6"
            );
            let rows = sqlx::query(&query)
                .bind(namespace_id.0)
                .bind(i16::from(bucket))
                .bind(now)
                .bind(Self::encode_status(
                    WorkerComputeProviderActionStatus::Pending,
                ))
                .bind(Self::encode_status(
                    WorkerComputeProviderActionStatus::Claimed,
                ))
                .bind(remaining)
                .fetch_all(&mut *tx)
                .await?;
            for row in rows {
                let mut action = Self::decode_action_row(&row)?;
                let previous_epoch = action.claim_epoch;
                let claim = WorkerComputeActionClaim {
                    action_id: action.action_id,
                    owner,
                    claim_epoch: previous_epoch.saturating_add(1),
                    claim_until,
                };
                let update = sqlx::query(
                    "UPDATE worker_compute_action
                     SET status = $4, claim_owner = $5, claim_epoch = $6,
                         claim_until = $7, updated_at = $8
                     WHERE action_id = $1 AND status = $2 AND claim_epoch = $3",
                )
                .bind(action.action_id)
                .bind(Self::encode_status(action.status))
                .bind(Self::i64(previous_epoch, "previous action claim epoch")?)
                .bind(Self::encode_status(
                    WorkerComputeProviderActionStatus::Claimed,
                ))
                .bind(owner.0)
                .bind(Self::i64(claim.claim_epoch, "action claim epoch")?)
                .bind(claim_until)
                .bind(now)
                .execute(&mut *tx)
                .await?;
                if update.rows_affected() == 1 {
                    action.status = WorkerComputeProviderActionStatus::Claimed;
                    action.claim_epoch = claim.claim_epoch;
                    action.claim = Some(claim.clone());
                    action.updated_at = now;
                    claimed.push(ClaimedWorkerComputeProviderAction { claim, action });
                }
            }
        }
        match tx.commit().await {
            Ok(()) => Ok(claimed),
            Err(error) if DsqlRunRepository::is_serialization_failure(&error) => Ok(Vec::new()),
            Err(error) => Err(error.into()),
        }
    }

    async fn begin_action_attempt(
        &self,
        claim: &WorkerComputeActionClaim,
        now: OffsetDateTime,
    ) -> Result<WorkerComputeActionAttemptStart> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let query =
            format!("SELECT {ACTION_COLUMNS} FROM worker_compute_action WHERE action_id = $1");
        let row = sqlx::query(&query)
            .bind(claim.action_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(mut action) = row.as_ref().map(Self::decode_action_row).transpose()? else {
            tx.rollback().await?;
            return Ok(WorkerComputeActionAttemptStart::NotFound);
        };
        if action.claim.as_ref() != Some(claim)
            || action.status != WorkerComputeProviderActionStatus::Claimed
            || claim.claim_until <= now
        {
            tx.rollback().await?;
            return Ok(WorkerComputeActionAttemptStart::StaleClaim);
        }
        let (_, fingerprint_current) = Self::current_action_fingerprint(&mut tx, &action).await?;
        if !fingerprint_current {
            action.status = WorkerComputeProviderActionStatus::Superseded;
            action.superseded_at = Some(now);
            action.claim = None;
            action.updated_at = now;
            if Self::update_action(&mut tx, &action, claim).await? != 1 {
                tx.rollback().await?;
                return Ok(WorkerComputeActionAttemptStart::StaleClaim);
            }
            tx.commit().await?;
            return Ok(WorkerComputeActionAttemptStart::Superseded);
        }
        action.attempts = action.attempts.saturating_add(1);
        action.attempt_started_at = Some(now);
        action.updated_at = now;
        if Self::update_action(&mut tx, &action, claim).await? != 1 {
            tx.rollback().await?;
            return Ok(WorkerComputeActionAttemptStart::StaleClaim);
        }
        tx.commit().await?;
        Ok(WorkerComputeActionAttemptStart::Started(action))
    }

    async fn finalize_action(
        &self,
        claim: &WorkerComputeActionClaim,
        result: WorkerComputeActionFinalization,
    ) -> Result<WorkerComputeActionFinalizeResult> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let query =
            format!("SELECT {ACTION_COLUMNS} FROM worker_compute_action WHERE action_id = $1");
        let row = sqlx::query(&query)
            .bind(claim.action_id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(mut action) = row.as_ref().map(Self::decode_action_row).transpose()? else {
            tx.rollback().await?;
            return Ok(WorkerComputeActionFinalizeResult::NotFound);
        };
        let completed_at = match &result {
            WorkerComputeActionFinalization::Delivered { completed_at }
            | WorkerComputeActionFinalization::RetryableFailure { completed_at, .. }
            | WorkerComputeActionFinalization::TerminalFailure { completed_at, .. } => {
                *completed_at
            }
            WorkerComputeActionFinalization::Superseded { superseded_at } => *superseded_at,
        };
        if action.claim.as_ref() != Some(claim)
            || action.status != WorkerComputeProviderActionStatus::Claimed
            || claim.claim_until <= completed_at
        {
            tx.rollback().await?;
            return Ok(WorkerComputeActionFinalizeResult::StaleClaim);
        }
        let (mut controller, fingerprint_current) =
            Self::current_action_fingerprint(&mut tx, &action).await?;
        let (status, category, next_attempt_at, superseded_at, completed_at) = match result {
            WorkerComputeActionFinalization::Delivered { completed_at } => (
                WorkerComputeProviderActionStatus::Delivered,
                None,
                action.next_attempt_at,
                None,
                completed_at,
            ),
            WorkerComputeActionFinalization::RetryableFailure {
                category,
                next_attempt_at,
                completed_at,
            } if fingerprint_current => (
                WorkerComputeProviderActionStatus::Pending,
                Some(category),
                next_attempt_at,
                None,
                completed_at,
            ),
            WorkerComputeActionFinalization::RetryableFailure { completed_at, .. }
            | WorkerComputeActionFinalization::TerminalFailure { completed_at, .. }
                if !fingerprint_current =>
            {
                (
                    WorkerComputeProviderActionStatus::Superseded,
                    None,
                    action.next_attempt_at,
                    Some(completed_at),
                    completed_at,
                )
            }
            WorkerComputeActionFinalization::TerminalFailure {
                category,
                completed_at,
            } => (
                WorkerComputeProviderActionStatus::TerminalFailed,
                Some(category),
                action.next_attempt_at,
                None,
                completed_at,
            ),
            WorkerComputeActionFinalization::Superseded { superseded_at } => (
                WorkerComputeProviderActionStatus::Superseded,
                None,
                action.next_attempt_at,
                Some(superseded_at),
                superseded_at,
            ),
            WorkerComputeActionFinalization::RetryableFailure { .. } => {
                unreachable!("stale retryable failures were matched above")
            }
        };
        action.status = status;
        action.last_error_category = category;
        action.next_attempt_at = next_attempt_at;
        action.superseded_at = superseded_at;
        action.claim = None;
        action.updated_at = completed_at;
        if Self::update_action(&mut tx, &action, claim).await? != 1 {
            tx.rollback().await?;
            return Ok(WorkerComputeActionFinalizeResult::StaleClaim);
        }

        if fingerprint_current
            && let Some(controller) = controller.as_mut()
            && let Some(group) = controller.groups.get_mut(&action.scaling_group)
        {
            group.last_action_id = Some(action.action_id);
            group.last_failure_category = category;
            group.health = match status {
                WorkerComputeProviderActionStatus::Pending => WorkerComputeHealth::DeliveryRetrying,
                WorkerComputeProviderActionStatus::TerminalFailed => {
                    WorkerComputeHealth::DeliveryTerminalFailure
                }
                _ => WorkerComputeHealth::Active,
            };
            if action.reason == WorkerComputeInvokeReason::ConfigurationActivation {
                group.activation_status = Some(status);
            }
            let previous_revision = controller.revision;
            controller.revision = controller.revision.saturating_add(1);
            let record_data = codec::encode_worker_compute_controller(controller)?;
            let update = sqlx::query(
                "UPDATE worker_compute_controller
                 SET revision = $4, record_data = $5, updated_at = $6
                 WHERE namespace_id = $1 AND deployment_name = $2 AND build_id = $3
                   AND revision = $7",
            )
            .bind(controller.key.namespace_id.0)
            .bind(&controller.key.deployment_name.0)
            .bind(&controller.key.build_id.0)
            .bind(Self::i64(controller.revision, "controller revision")?)
            .bind(record_data)
            .bind(completed_at)
            .bind(Self::i64(
                previous_revision,
                "previous controller revision",
            )?)
            .execute(&mut *tx)
            .await?;
            if update.rows_affected() != 1 {
                tx.rollback().await?;
                return Ok(WorkerComputeActionFinalizeResult::StaleClaim);
            }
        }
        match tx.commit().await {
            Ok(()) => Ok(WorkerComputeActionFinalizeResult::Applied { status }),
            Err(error) if DsqlRunRepository::is_serialization_failure(&error) => {
                Ok(WorkerComputeActionFinalizeResult::StaleClaim)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list_health(
        &self,
        namespace_id: NamespaceId,
        filter: WorkerComputeHealthFilter,
    ) -> Result<Vec<WorkerComputeControllerHealthView>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        Self::list_health_with_connection(permit.connection()?, namespace_id, filter).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn action_sql_is_namespace_scoped_and_uses_all_twenty_one_binds() {
        assert!(ACTION_COLUMNS.contains("namespace_id"));
        assert!(ACTION_COLUMNS.contains("configuration_fingerprint"));
        assert!(ACTION_COLUMNS.contains("reason"));
        assert!(INSERT_ACTION_SQL.contains("ON CONFLICT (action_id) DO NOTHING"));
        for bind in 1..=21 {
            assert!(
                INSERT_ACTION_SQL.contains(&format!("${bind}")),
                "missing action bind ${bind}"
            );
        }
    }

    #[test]
    fn enum_database_encodings_round_trip() {
        for value in [
            WorkerComputeProviderActionStatus::Pending,
            WorkerComputeProviderActionStatus::Claimed,
            WorkerComputeProviderActionStatus::Delivered,
            WorkerComputeProviderActionStatus::TerminalFailed,
            WorkerComputeProviderActionStatus::Superseded,
        ] {
            assert_eq!(
                DsqlWorkerComputeRepository::decode_status(
                    DsqlWorkerComputeRepository::encode_status(value)
                )
                .unwrap(),
                value
            );
        }
        for value in [
            WorkerComputeInvokeReason::ConfigurationActivation,
            WorkerComputeInvokeReason::NoSyncMatch,
            WorkerComputeInvokeReason::Backlog,
            WorkerComputeInvokeReason::WorkerRefresh,
        ] {
            assert_eq!(
                DsqlWorkerComputeRepository::decode_reason(
                    DsqlWorkerComputeRepository::encode_reason(value)
                )
                .unwrap(),
                value
            );
        }
    }

    #[test]
    fn controller_document_codec_is_stable_across_reload() {
        let record = WorkerComputeControllerRecord {
            format_version: crate::WORKER_COMPUTE_RECORD_FORMAT_VERSION,
            key: ControllerInstanceKey {
                namespace_id: NamespaceId::new(),
                deployment_name: DeploymentId("payments".to_owned()),
                build_id: BuildId("build-a".to_owned()),
            },
            namespace_name: "payments-prod".to_owned(),
            revision: 7,
            lifecycle: WorkerComputeControllerLifecycle::Active,
            slot: Some(9),
            owner: Some(IncarnationId::new()),
            owner_epoch: 4,
            lease_until: Some(OffsetDateTime::UNIX_EPOCH),
            groups: BTreeMap::new(),
            next_metrics_poll_at: None,
            reconciled_at: OffsetDateTime::UNIX_EPOCH,
        };
        let first = codec::encode_worker_compute_controller(&record).unwrap();
        let decoded = codec::decode_worker_compute_controller(&first).unwrap();
        let second = codec::encode_worker_compute_controller(&decoded).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(second, first);
    }
}
