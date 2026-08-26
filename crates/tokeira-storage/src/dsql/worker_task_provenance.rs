//! DSQL persistence for scoped Worker task-token authorization evidence.
//!
//! The table is intentionally separate from authoritative run state. Its rows
//! can only gate an edge response; they can never start, complete, or dispatch
//! work without the runtime's existing token fence or correlation.

use std::sync::Arc;

use async_trait::async_trait;
use sqlx::Row;
use time::OffsetDateTime;
use tokeira_types::{
    BuildId, DeploymentId, NamespaceId, TaskQueueName, WorkerTaskClass, WorkerTaskOrigin,
};

use crate::{
    DbClass, ProvenancePut, WorkerTaskProvenance, WorkerTaskProvenanceError,
    WorkerTaskProvenanceStore,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, key_codec};

/// DSQL-backed Worker task-provenance repository.
#[derive(Debug)]
pub struct DsqlWorkerTaskProvenanceStore {
    director: Arc<dyn DsqlConnectionAcquirer>,
}

impl DsqlWorkerTaskProvenanceStore {
    /// Construct a repository over the process-wide connection director.
    #[must_use]
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
        }
    }

    fn decode_row(
        token_digest: [u8; 32],
        row: &sqlx::postgres::PgRow,
    ) -> Result<WorkerTaskProvenance, WorkerTaskProvenanceError> {
        let task_class: i16 = row
            .try_get("task_class")
            .map_err(|error| corrupt(error.to_string()))?;
        let task_class =
            WorkerTaskClass::try_from(task_class).map_err(|error| corrupt(error.to_string()))?;
        Ok(WorkerTaskProvenance {
            token_digest,
            origin: WorkerTaskOrigin {
                namespace_id: NamespaceId(
                    row.try_get("namespace_id")
                        .map_err(|error| corrupt(error.to_string()))?,
                ),
                normal_task_queue: TaskQueueName(
                    row.try_get("normal_task_queue")
                        .map_err(|error| corrupt(error.to_string()))?,
                ),
                task_class,
                deployment: DeploymentId(
                    row.try_get("deployment_name")
                        .map_err(|error| corrupt(error.to_string()))?,
                ),
                build_id: BuildId(
                    row.try_get("build_id")
                        .map_err(|error| corrupt(error.to_string()))?,
                ),
            },
            expires_at: row
                .try_get("expires_at")
                .map_err(|error| corrupt(error.to_string()))?,
            created_at: row
                .try_get("created_at")
                .map_err(|error| corrupt(error.to_string()))?,
        })
    }

    async fn load_any(
        &self,
        token_digest: [u8; 32],
    ) -> Result<Option<WorkerTaskProvenance>, WorkerTaskProvenanceError> {
        let token_digest_key = key_codec::encode(&token_digest);
        let mut permit = self
            .director
            .acquire(DbClass::Read)
            .await
            .map_err(unavailable)?;
        let row = sqlx::query(
            "SELECT namespace_id, normal_task_queue, task_class, deployment_name, build_id,
                    expires_at, created_at
             FROM worker_task_provenance
             WHERE token_digest = $1",
        )
        .bind(token_digest_key)
        .fetch_optional(permit.connection().map_err(unavailable)?)
        .await
        .map_err(unavailable)?;
        row.as_ref()
            .map(|row| Self::decode_row(token_digest, row))
            .transpose()
    }
}

#[async_trait]
impl WorkerTaskProvenanceStore for DsqlWorkerTaskProvenanceStore {
    async fn put(
        &self,
        record: WorkerTaskProvenance,
    ) -> Result<ProvenancePut, WorkerTaskProvenanceError> {
        let token_digest_key = key_codec::encode(&record.token_digest);
        let mut permit = self
            .director
            .acquire(DbClass::Commit)
            .await
            .map_err(unavailable)?;
        let result = sqlx::query(
            "INSERT INTO worker_task_provenance
             (token_digest, namespace_id, normal_task_queue, task_class, deployment_name,
              build_id, expires_at, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (token_digest) DO NOTHING",
        )
        .bind(token_digest_key)
        .bind(record.origin.namespace_id.0)
        .bind(&record.origin.normal_task_queue.0)
        .bind(record.origin.task_class.to_db_smallint())
        .bind(&record.origin.deployment.0)
        .bind(&record.origin.build_id.0)
        .bind(record.expires_at)
        .bind(record.created_at)
        .execute(permit.connection().map_err(unavailable)?)
        .await
        .map_err(unavailable)?;
        drop(permit);
        if result.rows_affected() == 1 {
            return Ok(ProvenancePut::Inserted);
        }
        match self.load_any(record.token_digest).await? {
            Some(existing) if existing == record => Ok(ProvenancePut::AlreadyPresent),
            Some(_) | None => Err(WorkerTaskProvenanceError::DigestConflict),
        }
    }

    async fn get(
        &self,
        token_digest: [u8; 32],
    ) -> Result<Option<WorkerTaskProvenance>, WorkerTaskProvenanceError> {
        Ok(self
            .load_any(token_digest)
            .await?
            .filter(|record| record.expires_at > OffsetDateTime::now_utc()))
    }

    async fn delete(&self, token_digest: [u8; 32]) -> Result<(), WorkerTaskProvenanceError> {
        let token_digest_key = key_codec::encode(&token_digest);
        let mut permit = self
            .director
            .acquire(DbClass::Commit)
            .await
            .map_err(unavailable)?;
        sqlx::query("DELETE FROM worker_task_provenance WHERE token_digest = $1")
            .bind(token_digest_key)
            .execute(permit.connection().map_err(unavailable)?)
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    async fn delete_expired(
        &self,
        now: OffsetDateTime,
        limit: usize,
    ) -> Result<usize, WorkerTaskProvenanceError> {
        let limit = i64::try_from(limit).map_err(|_| corrupt("expiry delete limit exceeds i64"))?;
        let mut permit = self
            .director
            .acquire(DbClass::Maintenance)
            .await
            .map_err(unavailable)?;
        let result = sqlx::query(
            "DELETE FROM worker_task_provenance
             WHERE token_digest IN (
                 SELECT token_digest
                 FROM worker_task_provenance
                 WHERE expires_at <= $1
                 ORDER BY expires_at ASC, token_digest ASC
                 LIMIT $2
             )",
        )
        .bind(now)
        .bind(limit)
        .execute(permit.connection().map_err(unavailable)?)
        .await
        .map_err(unavailable)?;
        usize::try_from(result.rows_affected())
            .map_err(|_| corrupt("deleted provenance row count exceeds usize"))
    }
}

fn unavailable(error: impl std::fmt::Display) -> WorkerTaskProvenanceError {
    WorkerTaskProvenanceError::Unavailable {
        message: error.to_string(),
    }
}

fn corrupt(message: impl Into<String>) -> WorkerTaskProvenanceError {
    WorkerTaskProvenanceError::Corrupt {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use sha2::Digest as _;

    use crate::worker_task_token_digest;

    #[test]
    fn token_digest_is_domain_separated_and_exact() {
        let raw_digest: [u8; 32] = sha2::Sha256::digest(b"task-token").into();
        assert_ne!(worker_task_token_digest(b"task-token"), raw_digest);
        assert_ne!(
            worker_task_token_digest(b"task-token"),
            worker_task_token_digest(b"task-token\0")
        );
    }

    #[test]
    fn provenance_schema_contains_only_bounded_origin_evidence() {
        let table = include_str!("../../migrations/V064__worker_task_provenance.sql");
        let index = include_str!("../../migrations/V065__idx_worker_task_provenance_expiry.sql");
        for forbidden in [
            "raw_token",
            "task_token",
            "bearer",
            "subject",
            "payload",
            "workflow_id",
            "activity_id",
            "run_id",
            "role",
        ] {
            assert!(
                !table.contains(forbidden),
                "provenance schema must not persist {forbidden}"
            );
            assert!(
                !index.contains(forbidden),
                "provenance index must not persist {forbidden}"
            );
        }
        assert!(table.contains("token_digest TEXT NOT NULL"));
        assert!(index.contains("CREATE INDEX ASYNC"));
    }
}
