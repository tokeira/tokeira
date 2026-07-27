//! Aurora DSQL persistence for public task-queue delivery policy.
//!
//! Each write is one row-level compare-and-swap keyed by namespace, queue name,
//! and task kind. The data is low-volume control-plane state, independent from
//! run transitions and reconstructible runtime delivery caches.

use std::sync::Arc;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use sqlx::Row;

use crate::{
    DbClass, StoredTaskQueueConfig, StoredTaskQueueConfigKey, StoredTaskQueueConfigKind,
    TaskQueueConfigCasResult, TaskQueueConfigRepository,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, DsqlRunRepository, codec};

/// DSQL-backed task-queue policy repository.
#[derive(Debug)]
pub struct DsqlTaskQueueConfigRepository {
    director: Arc<dyn DsqlConnectionAcquirer>,
}

impl DsqlTaskQueueConfigRepository {
    /// Construct a repository over the process-wide connection director.
    #[must_use]
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
        }
    }

    fn encode_kind(kind: StoredTaskQueueConfigKind) -> i16 {
        match kind {
            StoredTaskQueueConfigKind::Workflow => 1,
            StoredTaskQueueConfigKind::Activity => 2,
            StoredTaskQueueConfigKind::Nexus => 3,
        }
    }

    fn decode_row(row: &sqlx::postgres::PgRow) -> Result<StoredTaskQueueConfig> {
        let revision: i64 = row.try_get("revision")?;
        let revision = u64::try_from(revision)
            .map_err(|_| anyhow::anyhow!("negative task_queue_config revision {revision}"))?;
        let record_data: Vec<u8> = row.try_get("record_data")?;
        let record = codec::decode_task_queue_config(&record_data)?;
        ensure!(
            record.revision == revision,
            "task_queue_config revision column {} disagrees with encoded record {}",
            revision,
            record.revision
        );
        Ok(record)
    }

    async fn execute_cas(
        &self,
        record: &StoredTaskQueueConfig,
        expected_revision: Option<u64>,
    ) -> Result<u64> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let record_data = codec::encode_task_queue_config(record)?;
        let revision = i64::try_from(record.revision)
            .map_err(|_| anyhow::anyhow!("task-queue configuration revision exceeds i64"))?;
        let result = if let Some(expected_revision) = expected_revision {
            let expected_revision = i64::try_from(expected_revision)
                .map_err(|_| anyhow::anyhow!("expected task-queue revision exceeds i64"))?;
            sqlx::query(
                "UPDATE task_queue_config
                 SET revision = $4, record_data = $5, updated_at = now()
                 WHERE namespace_id = $1 AND task_queue = $2 AND task_kind = $3
                   AND revision = $6",
            )
            .bind(record.namespace_id.0)
            .bind(&record.task_queue.0)
            .bind(Self::encode_kind(record.kind))
            .bind(revision)
            .bind(record_data)
            .bind(expected_revision)
            .execute(permit.connection()?)
            .await
        } else {
            sqlx::query(
                "INSERT INTO task_queue_config
                 (namespace_id, task_queue, task_kind, revision, record_data, updated_at)
                 VALUES ($1, $2, $3, $4, $5, now())
                 ON CONFLICT (namespace_id, task_queue, task_kind) DO NOTHING",
            )
            .bind(record.namespace_id.0)
            .bind(&record.task_queue.0)
            .bind(Self::encode_kind(record.kind))
            .bind(revision)
            .bind(record_data)
            .execute(permit.connection()?)
            .await
        };
        match result {
            Ok(result) => Ok(result.rows_affected()),
            Err(error) if DsqlRunRepository::is_serialization_failure(&error) => Ok(0),
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl TaskQueueConfigRepository for DsqlTaskQueueConfigRepository {
    async fn load_task_queue_config(
        &self,
        key: &StoredTaskQueueConfigKey,
    ) -> Result<Option<StoredTaskQueueConfig>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let row = sqlx::query(
            "SELECT revision, record_data
             FROM task_queue_config
             WHERE namespace_id = $1 AND task_queue = $2 AND task_kind = $3",
        )
        .bind(key.namespace_id.0)
        .bind(&key.task_queue.0)
        .bind(Self::encode_kind(key.kind))
        .fetch_optional(permit.connection()?)
        .await?;
        row.as_ref().map(Self::decode_row).transpose()
    }

    async fn compare_and_swap_task_queue_config(
        &self,
        mut record: StoredTaskQueueConfig,
        expected_revision: Option<u64>,
    ) -> Result<TaskQueueConfigCasResult> {
        let revision = expected_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("task-queue configuration revision overflow"))?;
        record.revision = revision;
        if self.execute_cas(&record, expected_revision).await? == 1 {
            Ok(TaskQueueConfigCasResult::Applied { revision })
        } else {
            Ok(TaskQueueConfigCasResult::Conflict)
        }
    }

    async fn list_all_task_queue_configs(&self) -> Result<Vec<StoredTaskQueueConfig>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT revision, record_data
             FROM task_queue_config
             ORDER BY namespace_id ASC, task_queue ASC, task_kind ASC",
        )
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter().map(Self::decode_row).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use time::{Duration, OffsetDateTime};
    use tokeira_types::{NamespaceId, TaskQueueName};

    use super::*;
    use crate::{
        StoredTaskQueueConfigMetadata,
        dsql::{DsqlPoolConfig, DsqlStore, MigrationRunner, ReservoirConfig},
    };

    fn test_pool_config() -> DsqlPoolConfig {
        DsqlPoolConfig {
            reservoir: ReservoirConfig {
                target_ready: 5,
                inflight_limit: 1,
                base_lifetime: Duration::minutes(5),
                lifetime_jitter: Duration::ZERO,
                guard_window: Duration::seconds(45),
                scan_interval: Duration::seconds(60),
            },
            ..DsqlPoolConfig::default()
        }
    }

    fn sample_record(namespace_id: NamespaceId) -> StoredTaskQueueConfig {
        StoredTaskQueueConfig {
            namespace_id,
            task_queue: TaskQueueName("restart-policy".to_owned()),
            kind: StoredTaskQueueConfigKind::Workflow,
            revision: 0,
            queue_rate_limit: Some(7.5),
            queue_rate_limit_metadata: Some(StoredTaskQueueConfigMetadata {
                reason: "verify DSQL restart recovery".to_owned(),
                update_identity: "configuration-policy-test".to_owned(),
                update_time: OffsetDateTime::UNIX_EPOCH,
            }),
            fairness_key_rate_limit_default: Some(2.5),
            fairness_key_rate_limit_metadata: None,
            fairness_weight_overrides: BTreeMap::from([
                ("bronze".to_owned(), 1.0),
                ("gold".to_owned(), 3.0),
            ]),
        }
    }

    #[tokio::test]
    async fn dsql_task_queue_policy_survives_repository_recreation() -> anyhow::Result<()> {
        let Ok(database_url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = sqlx::PgPool::connect(&database_url).await?;
        MigrationRunner::embedded().apply(&pool).await?;
        pool.close().await;

        let namespace_id = NamespaceId::new();
        let record = sample_record(namespace_id);
        let key = record.key();

        let first =
            DsqlStore::from_database_url_for_tests(&database_url, test_pool_config()).await?;
        let first_repository = first.task_queue_config_repository();
        assert_eq!(
            first_repository
                .compare_and_swap_task_queue_config(record.clone(), None)
                .await?,
            TaskQueueConfigCasResult::Applied { revision: 1 }
        );
        drop(first_repository);
        first.shutdown().await?;

        // Reconstructing the DSQL foundation is the storage boundary crossed by a
        // process replacement. The runtime's Property 11 independently proves that
        // startup hydration exposes this recovered record to edge and broker reads.
        let restarted =
            DsqlStore::from_database_url_for_tests(&database_url, test_pool_config()).await?;
        let restarted_repository = restarted.task_queue_config_repository();
        let mut expected = record;
        expected.revision = 1;
        assert_eq!(
            restarted_repository.load_task_queue_config(&key).await?,
            Some(expected)
        );
        drop(restarted_repository);
        restarted.shutdown().await?;

        let cleanup_pool = sqlx::PgPool::connect(&database_url).await?;
        sqlx::query("DELETE FROM task_queue_config WHERE namespace_id = $1")
            .bind(namespace_id.0)
            .execute(&cleanup_pool)
            .await?;
        cleanup_pool.close().await;
        Ok(())
    }
}
