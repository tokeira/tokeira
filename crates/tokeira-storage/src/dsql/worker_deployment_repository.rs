//! DSQL-backed Worker Deployment registry repository.
//!
//! The registry is stored as one document per `(namespace_id, deployment_name)`
//! so routing-config changes and version-state changes commit under one CAS
//! token. Secondary projections can be derived later; correctness is fenced on
//! this single row.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Connection, Row};
use tokeira_types::NamespaceId;

use crate::{
    ConflictToken, DbClass, DeploymentCasResult, DeploymentKey, DeploymentName,
    StoredWorkerDeployment, WorkerDeploymentRepository,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, DsqlRunRepository, codec};

/// Production Worker Deployment registry backed by Aurora DSQL.
#[derive(Debug)]
pub struct DsqlWorkerDeploymentRepository {
    director: Arc<dyn DsqlConnectionAcquirer>,
}

impl DsqlWorkerDeploymentRepository {
    /// Build a repository using the production DSQL connection director.
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
        }
    }

    fn next_token(current: Option<ConflictToken>) -> ConflictToken {
        ConflictToken::from_generation(current.map_or(1, |token| token.generation() + 1))
    }

    fn token_from_bytes(bytes: Vec<u8>) -> Result<ConflictToken> {
        let bytes: [u8; crate::CONFLICT_TOKEN_BYTES] =
            bytes.try_into().map_err(|bytes: Vec<u8>| {
                anyhow::anyhow!(
                    "worker_deployments.conflict_token length {} is invalid",
                    bytes.len()
                )
            })?;
        Ok(ConflictToken(bytes))
    }
}

#[async_trait]
impl WorkerDeploymentRepository for DsqlWorkerDeploymentRepository {
    async fn load_deployment(&self, key: &DeploymentKey) -> Result<Option<StoredWorkerDeployment>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let row = sqlx::query(
            "SELECT record_data
             FROM worker_deployments
             WHERE namespace_id = $1 AND deployment_name = $2",
        )
        .bind(key.namespace_id.0)
        .bind(&key.deployment_name.0)
        .fetch_optional(permit.connection()?)
        .await?;

        row.map(|row| {
            codec::decode_worker_deployment(row.try_get::<Vec<u8>, _>("record_data")?.as_slice())
        })
        .transpose()
    }

    async fn put_deployment(
        &self,
        mut record: StoredWorkerDeployment,
        expected: Option<ConflictToken>,
    ) -> Result<DeploymentCasResult> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let row = sqlx::query(
            "SELECT conflict_token
             FROM worker_deployments
             WHERE namespace_id = $1 AND deployment_name = $2",
        )
        .bind(record.namespace_id.0)
        .bind(&record.name.0)
        .fetch_optional(&mut *tx)
        .await?;

        let current_token = row
            .map(|row| row.try_get::<Vec<u8>, _>("conflict_token"))
            .transpose()?
            .map(Self::token_from_bytes)
            .transpose()?;

        match (current_token, expected) {
            (Some(_), None) => {
                tx.rollback().await?;
                Ok(DeploymentCasResult::AlreadyExists)
            }
            (None, Some(_)) => {
                tx.rollback().await?;
                Ok(DeploymentCasResult::NotFound)
            }
            (Some(current), Some(expected)) if current != expected => {
                tx.rollback().await?;
                Ok(DeploymentCasResult::Conflict)
            }
            (current, _) => {
                let token = Self::next_token(current);
                record.conflict_token = token;
                let record_data = codec::encode_worker_deployment(&record)?;
                let write = sqlx::query(
                    "INSERT INTO worker_deployments
                     (namespace_id, deployment_name, conflict_token, record_data, updated_at)
                     VALUES ($1, $2, $3, $4, now())
                     ON CONFLICT (namespace_id, deployment_name) DO UPDATE SET
                         conflict_token = EXCLUDED.conflict_token,
                         record_data = EXCLUDED.record_data,
                         updated_at = EXCLUDED.updated_at",
                )
                .bind(record.namespace_id.0)
                .bind(&record.name.0)
                .bind(token.0.as_slice())
                .bind(record_data)
                .execute(&mut *tx)
                .await;
                // Aurora DSQL may reject a concurrent writer at write or commit with
                // SQLSTATE 40001. The registry's load -> validate -> CAS loop already
                // knows how to reload and retry `Conflict`, so normalize it here
                // (matching `run_repository::commit` and `leases`).
                if let Err(err) = write {
                    if DsqlRunRepository::is_serialization_failure(&err) {
                        tx.rollback().await?;
                        return Ok(DeploymentCasResult::Conflict);
                    }
                    return Err(err.into());
                }
                match tx.commit().await {
                    Ok(()) => Ok(DeploymentCasResult::Applied { token }),
                    Err(err) if DsqlRunRepository::is_serialization_failure(&err) => {
                        Ok(DeploymentCasResult::Conflict)
                    }
                    Err(err) => Err(err.into()),
                }
            }
        }
    }

    async fn delete_deployment(
        &self,
        key: &DeploymentKey,
        expected: ConflictToken,
    ) -> Result<DeploymentCasResult> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        let row = sqlx::query(
            "SELECT conflict_token
             FROM worker_deployments
             WHERE namespace_id = $1 AND deployment_name = $2",
        )
        .bind(key.namespace_id.0)
        .bind(&key.deployment_name.0)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(DeploymentCasResult::NotFound);
        };
        let current = Self::token_from_bytes(row.try_get::<Vec<u8>, _>("conflict_token")?)?;
        if current != expected {
            tx.rollback().await?;
            return Ok(DeploymentCasResult::Conflict);
        }

        let token = Self::next_token(Some(current));
        let write = sqlx::query(
            "DELETE FROM worker_deployments
             WHERE namespace_id = $1 AND deployment_name = $2",
        )
        .bind(key.namespace_id.0)
        .bind(&key.deployment_name.0)
        .execute(&mut *tx)
        .await;
        if let Err(err) = write {
            if DsqlRunRepository::is_serialization_failure(&err) {
                tx.rollback().await?;
                return Ok(DeploymentCasResult::Conflict);
            }
            return Err(err.into());
        }
        match tx.commit().await {
            Ok(()) => Ok(DeploymentCasResult::Applied { token }),
            Err(err) if DsqlRunRepository::is_serialization_failure(&err) => {
                Ok(DeploymentCasResult::Conflict)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn list_deployments(
        &self,
        namespace_id: NamespaceId,
        after: Option<&DeploymentName>,
        limit: usize,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let limit = i64::try_from(limit).map_err(|_| {
            anyhow::anyhow!("worker deployment list limit {limit} exceeds i64 range")
        })?;
        let rows = if let Some(after) = after {
            sqlx::query(
                "SELECT record_data
                 FROM worker_deployments
                 WHERE namespace_id = $1 AND deployment_name > $2
                 ORDER BY deployment_name ASC
                 LIMIT $3",
            )
            .bind(namespace_id.0)
            .bind(&after.0)
            .bind(limit)
            .fetch_all(permit.connection()?)
            .await?
        } else {
            sqlx::query(
                "SELECT record_data
                 FROM worker_deployments
                 WHERE namespace_id = $1
                 ORDER BY deployment_name ASC
                 LIMIT $2",
            )
            .bind(namespace_id.0)
            .bind(limit)
            .fetch_all(permit.connection()?)
            .await?
        };

        rows.into_iter()
            .map(|row| {
                codec::decode_worker_deployment(
                    row.try_get::<Vec<u8>, _>("record_data")?.as_slice(),
                )
            })
            .collect()
    }

    async fn list_all_for_namespace(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<StoredWorkerDeployment>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT record_data
             FROM worker_deployments
             WHERE namespace_id = $1
             ORDER BY deployment_name ASC",
        )
        .bind(namespace_id.0)
        .fetch_all(permit.connection()?)
        .await?;

        rows.into_iter()
            .map(|row| {
                codec::decode_worker_deployment(
                    row.try_get::<Vec<u8>, _>("record_data")?.as_slice(),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use time::{Duration, OffsetDateTime};
    use tokeira_types::NamespaceId;

    use crate::{
        ConflictToken, DeploymentCasResult, DeploymentKey, DeploymentName, StoredRoutingConfig,
        StoredWorkerDeployment, WorkerDeploymentRepository,
        dsql::{DsqlPoolConfig, DsqlStore, ReservoirConfig},
    };

    fn fixed_now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    fn sample_deployment(namespace_id: NamespaceId, name: &str) -> StoredWorkerDeployment {
        StoredWorkerDeployment {
            namespace_id,
            name: DeploymentName(name.to_owned()),
            create_time: fixed_now(),
            routing_config: StoredRoutingConfig::default(),
            last_modifier_identity: "tester".to_owned(),
            manager_identity: None,
            routing_config_update_state: Default::default(),
            versions: BTreeMap::new(),
            conflict_token: ConflictToken::default(),
            create_request_ids: BTreeSet::from([format!("create-{name}")]),
        }
    }

    fn deployment_key(namespace_id: NamespaceId, name: &str) -> DeploymentKey {
        DeploymentKey {
            namespace_id,
            deployment_name: DeploymentName(name.to_owned()),
        }
    }

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

    async fn ensure_worker_deployments_table(database_url: &str) -> anyhow::Result<()> {
        let pool = sqlx::PgPool::connect(database_url).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS worker_deployments (
                namespace_id    UUID        NOT NULL,
                deployment_name TEXT        NOT NULL,
                conflict_token  BYTEA       NOT NULL,
                record_data     BYTEA       NOT NULL,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (namespace_id, deployment_name)
            )",
        )
        .execute(&pool)
        .await?;
        pool.close().await;
        Ok(())
    }

    async fn dsql_store_from_env() -> anyhow::Result<Option<DsqlStore>> {
        let Ok(database_url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(None);
        };
        ensure_worker_deployments_table(&database_url).await?;
        DsqlStore::from_database_url_for_tests(database_url, test_pool_config())
            .await
            .map(Some)
    }

    #[tokio::test]
    async fn dsql_worker_deployment_repository_cas_and_pagination_match_contract()
    -> anyhow::Result<()> {
        let Some(store) = dsql_store_from_env().await? else {
            return Ok(());
        };
        let repository = store.worker_deployment_repository();
        let namespace_id = NamespaceId::new();
        let key = deployment_key(namespace_id, "alpha");

        let created = repository
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await?;
        let DeploymentCasResult::Applied {
            token: create_token,
        } = created
        else {
            panic!("fresh create should apply, got {created:?}");
        };
        let duplicate = repository
            .put_deployment(sample_deployment(namespace_id, "alpha"), None)
            .await?;
        assert_eq!(duplicate, DeploymentCasResult::AlreadyExists);

        let mut current_update = sample_deployment(namespace_id, "alpha");
        current_update.last_modifier_identity = "current-writer".to_owned();
        let updated = repository
            .put_deployment(current_update.clone(), Some(create_token))
            .await?;
        let DeploymentCasResult::Applied {
            token: update_token,
        } = updated
        else {
            panic!("current-token update should apply, got {updated:?}");
        };
        assert_eq!(update_token.generation(), create_token.generation() + 1);
        current_update.conflict_token = update_token;

        let mut stale_update = sample_deployment(namespace_id, "alpha");
        stale_update.last_modifier_identity = "stale-writer".to_owned();
        let stale = repository
            .put_deployment(stale_update, Some(create_token))
            .await?;
        assert_eq!(stale, DeploymentCasResult::Conflict);
        assert_eq!(
            repository.load_deployment(&key).await?,
            Some(current_update)
        );

        for name in ["gamma", "epsilon", "beta", "delta"] {
            let result = repository
                .put_deployment(sample_deployment(namespace_id, name), None)
                .await?;
            assert!(matches!(result, DeploymentCasResult::Applied { .. }));
        }
        let other_namespace_id = NamespaceId::new();
        let result = repository
            .put_deployment(sample_deployment(other_namespace_id, "alpha"), None)
            .await?;
        assert!(matches!(result, DeploymentCasResult::Applied { .. }));

        let mut after = None;
        let mut seen = Vec::new();
        loop {
            let page = repository
                .list_deployments(namespace_id, after.as_ref(), 2)
                .await?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|record| record.name.clone());
            seen.extend(page.into_iter().map(|record| record.name.0));
        }

        assert_eq!(seen, ["alpha", "beta", "delta", "epsilon", "gamma"]);
        assert_eq!(
            repository
                .list_deployments(namespace_id, after.as_ref(), 2)
                .await?,
            Vec::<StoredWorkerDeployment>::new()
        );

        store.shutdown().await?;
        Ok(())
    }
}
