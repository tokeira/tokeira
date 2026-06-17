//! DSQL-backed CHASM node store (Requirement 9).
//!
//! The production backend for [`ChasmNodeRepository`](crate::ChasmNodeRepository),
//! persisting one row per node in the `chasm_node` table (migration `V049`). It is
//! the DSQL counterpart of the [`InMemoryChasmNodeStore`](crate::InMemoryChasmNodeStore):
//! identical semantics (write-only-dirty nodes, all-or-nothing CAS fencing on each
//! node's prior VersionedTransition, encoded-path prefix range scans), realized
//! against Aurora DSQL through the shared connection director.
//!
//! ## Row layout and the metadata blob
//!
//! The node's [`VersionedTransition`] stamps and `archetype_id` are denormalized
//! into typed columns so the CAS fence is a cheap column comparison and future
//! secondary indexes are possible; the full [`NodeMetadata`](tokeira_chasm::NodeMetadata)
//! — including the task outboxes — is also stored as a postcard blob in `metadata`,
//! which is authoritative on load. Persist writes both from the same node, so the
//! columns and the blob never diverge. The node `data` payload is the nullable
//! `data` column.
//!
//! ## The CAS-fenced, all-or-nothing batch
//!
//! [`persist_dirty`](ChasmNodeRepository::persist_dirty) runs the whole batch in one
//! DSQL transaction: it first checks every node's [`ExpectedVersion`] against the
//! stored row, rolling back to [`NodePersistOutcome::Conflict`] with no write if any
//! fence fails (Requirement 9.5, 9.6). DSQL's commit-time optimistic concurrency
//! (SQLSTATE 40001) covers a concurrent writer that slips between the check and the
//! commit — normalized to the same `Conflict` so the runtime reloads and re-runs.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{Connection, Row};
use tokeira_chasm::{ChasmNode, ExecutionKey, LifecycleState, VersionedTransition};
use uuid::Uuid;

use crate::{
    ChasmNodeRepository, CurrentRun, DbClass, ExpectedVersion, NodePersistOutcome, NodeWrite,
};

use super::{DsqlConnectionAcquirer, DsqlConnectionDirector, DsqlRunRepository, codec};

/// Production CHASM node store backed by Aurora DSQL.
#[derive(Debug)]
pub struct DsqlChasmNodeRepository {
    director: Arc<dyn DsqlConnectionAcquirer>,
}

impl DsqlChasmNodeRepository {
    /// Build a repository over the shared DSQL connection director.
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
        }
    }

    /// Parse one of the execution key's UUID components, erroring with context if
    /// it is not a UUID. The `business_id` is free text; `namespace_id`/`run_id`
    /// are UUID columns.
    fn parse_uuid(field: &str, value: &str) -> Result<Uuid> {
        Uuid::parse_str(value)
            .map_err(|e| anyhow::anyhow!("chasm node {field} `{value}` is not a UUID: {e}"))
    }

    /// The `(namespace_id, run_id)` UUID pair plus `business_id` for an execution.
    fn key_parts(key: &ExecutionKey) -> Result<(Uuid, &str, Uuid)> {
        Ok((
            Self::parse_uuid("namespace_id", &key.namespace_id)?,
            key.business_id.as_str(),
            Self::parse_uuid("run_id", &key.run_id)?,
        ))
    }

    /// Reconstruct a [`ChasmNode`] from a result row (`metadata` blob is
    /// authoritative; `data` is the nullable column).
    fn node_from_row(row: &sqlx::postgres::PgRow) -> Result<(Vec<u8>, ChasmNode)> {
        let encoded_path: Vec<u8> = row.try_get("encoded_path")?;
        let metadata_blob: Vec<u8> = row.try_get("metadata")?;
        let data: Option<Vec<u8>> = row.try_get("data")?;
        let metadata = codec::decode(&metadata_blob)?;
        Ok((encoded_path, ChasmNode { metadata, data }))
    }

    /// Encode a [`LifecycleState`] as the `chasm_current_run.status` SMALLINT
    /// (0=Running, 1=Completed, 2=Failed). Stable on-disk encoding — extend, never
    /// renumber.
    fn encode_status(status: LifecycleState) -> i16 {
        match status {
            LifecycleState::Running => 0,
            LifecycleState::Completed => 1,
            LifecycleState::Failed => 2,
        }
    }

    /// Decode a `chasm_current_run.status` SMALLINT back to a [`LifecycleState`].
    fn decode_status(value: i16) -> Result<LifecycleState> {
        match value {
            0 => Ok(LifecycleState::Running),
            1 => Ok(LifecycleState::Completed),
            2 => Ok(LifecycleState::Failed),
            other => Err(anyhow::anyhow!(
                "chasm_current_run.status `{other}` is not a known LifecycleState"
            )),
        }
    }
}

#[async_trait]
impl ChasmNodeRepository for DsqlChasmNodeRepository {
    async fn persist_dirty(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
    ) -> Result<NodePersistOutcome> {
        let (namespace_id, business_id, run_id) = Self::key_parts(key)?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;

        // Phase 1 — check every fence before mutating anything (all-or-nothing).
        for write in &batch {
            let stored = sqlx::query(
                "SELECT failover_version, transition_count
                 FROM chasm_node
                 WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3
                   AND encoded_path = $4",
            )
            .bind(namespace_id)
            .bind(business_id)
            .bind(run_id)
            .bind(write.encoded_path.as_slice())
            .fetch_optional(&mut *tx)
            .await?;

            let conflict_reason = match (&write.expected, stored) {
                (ExpectedVersion::Absent, Some(_)) => Some(format!(
                    "node at {:?} expected absent but already exists",
                    write.encoded_path
                )),
                (ExpectedVersion::Absent, None) => None,
                (ExpectedVersion::Vt(expected), Some(row)) => {
                    let failover: i64 = row.try_get("failover_version")?;
                    let count: i64 = row.try_get("transition_count")?;
                    let stored_vt = VersionedTransition::new(failover, count);
                    if &stored_vt == expected {
                        None
                    } else {
                        Some(format!(
                            "node at {:?} VT {stored_vt:?} does not match expected {expected:?}",
                            write.encoded_path
                        ))
                    }
                }
                (ExpectedVersion::Vt(expected), None) => Some(format!(
                    "node at {:?} expected VT {expected:?} but is absent",
                    write.encoded_path
                )),
            };
            if let Some(reason) = conflict_reason {
                tx.rollback().await?;
                return Ok(NodePersistOutcome::Conflict { reason });
            }
        }

        // Phase 2 — every fence held; upsert the whole batch.
        for write in &batch {
            let metadata_blob = codec::encode(&write.node.metadata)?;
            let vt = write.node.metadata.versioned_transition;
            let initial_vt = write.node.metadata.initial_versioned_transition;
            let result = sqlx::query(
                "INSERT INTO chasm_node
                   (namespace_id, business_id, run_id, encoded_path, archetype_id,
                    failover_version, transition_count,
                    initial_failover_version, initial_transition_count,
                    metadata, data, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
                 ON CONFLICT (namespace_id, business_id, run_id, encoded_path) DO UPDATE SET
                    archetype_id = EXCLUDED.archetype_id,
                    failover_version = EXCLUDED.failover_version,
                    transition_count = EXCLUDED.transition_count,
                    initial_failover_version = EXCLUDED.initial_failover_version,
                    initial_transition_count = EXCLUDED.initial_transition_count,
                    metadata = EXCLUDED.metadata,
                    data = EXCLUDED.data,
                    updated_at = EXCLUDED.updated_at",
            )
            .bind(namespace_id)
            .bind(business_id)
            .bind(run_id)
            .bind(write.encoded_path.as_slice())
            .bind(i64::from(write.node.metadata.component_type_id))
            .bind(vt.namespace_failover_version)
            .bind(vt.transition_count)
            .bind(initial_vt.namespace_failover_version)
            .bind(initial_vt.transition_count)
            .bind(metadata_blob)
            .bind(write.node.data.clone())
            .execute(&mut *tx)
            .await;
            if let Err(err) = result {
                if DsqlRunRepository::is_serialization_failure(&err) {
                    tx.rollback().await?;
                    return Ok(NodePersistOutcome::Conflict {
                        reason: "dsql serialization failure during node write".to_owned(),
                    });
                }
                return Err(err.into());
            }
        }

        match tx.commit().await {
            Ok(()) => Ok(NodePersistOutcome::Applied),
            Err(err) if DsqlRunRepository::is_serialization_failure(&err) => {
                Ok(NodePersistOutcome::Conflict {
                    reason: "dsql serialization failure at commit".to_owned(),
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn persist_new_execution(
        &self,
        key: &ExecutionKey,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
    ) -> Result<NodePersistOutcome> {
        let (namespace_id, business_id, run_id) = Self::key_parts(key)?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;

        // Phase 1 — node fences (all-or-nothing), identical to `persist_dirty`.
        for write in &batch {
            let stored = sqlx::query(
                "SELECT failover_version, transition_count
                 FROM chasm_node
                 WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3
                   AND encoded_path = $4",
            )
            .bind(namespace_id)
            .bind(business_id)
            .bind(run_id)
            .bind(write.encoded_path.as_slice())
            .fetch_optional(&mut *tx)
            .await?;
            let conflict_reason = match (&write.expected, stored) {
                (ExpectedVersion::Absent, Some(_)) => Some(format!(
                    "node at {:?} expected absent but already exists",
                    write.encoded_path
                )),
                (ExpectedVersion::Absent, None) => None,
                (ExpectedVersion::Vt(expected), Some(row)) => {
                    let failover: i64 = row.try_get("failover_version")?;
                    let count: i64 = row.try_get("transition_count")?;
                    let stored_vt = VersionedTransition::new(failover, count);
                    if &stored_vt == expected {
                        None
                    } else {
                        Some(format!(
                            "node at {:?} VT {stored_vt:?} does not match expected {expected:?}",
                            write.encoded_path
                        ))
                    }
                }
                (ExpectedVersion::Vt(expected), None) => Some(format!(
                    "node at {:?} expected VT {expected:?} but is absent",
                    write.encoded_path
                )),
            };
            if let Some(reason) = conflict_reason {
                tx.rollback().await?;
                return Ok(NodePersistOutcome::Conflict { reason });
            }
        }

        // Phase 2 — upsert the node batch.
        for write in &batch {
            let metadata_blob = codec::encode(&write.node.metadata)?;
            let vt = write.node.metadata.versioned_transition;
            let initial_vt = write.node.metadata.initial_versioned_transition;
            let result = sqlx::query(
                "INSERT INTO chasm_node
                   (namespace_id, business_id, run_id, encoded_path, archetype_id,
                    failover_version, transition_count,
                    initial_failover_version, initial_transition_count,
                    metadata, data, updated_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
                 ON CONFLICT (namespace_id, business_id, run_id, encoded_path) DO UPDATE SET
                    archetype_id = EXCLUDED.archetype_id,
                    failover_version = EXCLUDED.failover_version,
                    transition_count = EXCLUDED.transition_count,
                    initial_failover_version = EXCLUDED.initial_failover_version,
                    initial_transition_count = EXCLUDED.initial_transition_count,
                    metadata = EXCLUDED.metadata,
                    data = EXCLUDED.data,
                    updated_at = EXCLUDED.updated_at",
            )
            .bind(namespace_id)
            .bind(business_id)
            .bind(run_id)
            .bind(write.encoded_path.as_slice())
            .bind(i64::from(write.node.metadata.component_type_id))
            .bind(vt.namespace_failover_version)
            .bind(vt.transition_count)
            .bind(initial_vt.namespace_failover_version)
            .bind(initial_vt.transition_count)
            .bind(metadata_blob)
            .bind(write.node.data.clone())
            .execute(&mut *tx)
            .await;
            if let Err(err) = result {
                if DsqlRunRepository::is_serialization_failure(&err) {
                    tx.rollback().await?;
                    return Ok(NodePersistOutcome::Conflict {
                        reason: "dsql serialization failure during node write".to_owned(),
                    });
                }
                return Err(err.into());
            }
        }

        // Phase 3 — advance the current-run pointer in the SAME transaction (the
        // analog of v1.31.0's current_executions write inside the entity-create tx),
        // so the run's root node and its current-run pointer never tear.
        let current_run_id = Self::parse_uuid("current run_id", &current.run_id)?;
        let pointer = sqlx::query(
            "INSERT INTO chasm_current_run
               (namespace_id, business_id, run_id, status,
                failover_version, transition_count, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, now())
             ON CONFLICT (namespace_id, business_id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                status = EXCLUDED.status,
                failover_version = EXCLUDED.failover_version,
                transition_count = EXCLUDED.transition_count,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(namespace_id)
        .bind(business_id)
        .bind(current_run_id)
        .bind(Self::encode_status(current.status))
        .bind(current.vt_epoch.namespace_failover_version)
        .bind(current.vt_epoch.transition_count)
        .execute(&mut *tx)
        .await;
        if let Err(err) = pointer {
            if DsqlRunRepository::is_serialization_failure(&err) {
                tx.rollback().await?;
                return Ok(NodePersistOutcome::Conflict {
                    reason: "dsql serialization failure during current-run write".to_owned(),
                });
            }
            return Err(err.into());
        }

        match tx.commit().await {
            Ok(()) => Ok(NodePersistOutcome::Applied),
            Err(err) if DsqlRunRepository::is_serialization_failure(&err) => {
                Ok(NodePersistOutcome::Conflict {
                    reason: "dsql serialization failure at commit".to_owned(),
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn current_run(
        &self,
        namespace_id: &str,
        business_id: &str,
    ) -> Result<Option<CurrentRun>> {
        let namespace_uuid = Self::parse_uuid("namespace_id", namespace_id)?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let row = sqlx::query(
            "SELECT run_id, status, failover_version, transition_count
             FROM chasm_current_run
             WHERE namespace_id = $1 AND business_id = $2",
        )
        .bind(namespace_uuid)
        .bind(business_id)
        .fetch_optional(permit.connection()?)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let run_id: Uuid = row.try_get("run_id")?;
        let status: i16 = row.try_get("status")?;
        let failover: i64 = row.try_get("failover_version")?;
        let count: i64 = row.try_get("transition_count")?;
        Ok(Some(CurrentRun {
            run_id: run_id.to_string(),
            status: Self::decode_status(status)?,
            vt_epoch: VersionedTransition::new(failover, count),
        }))
    }

    async fn load_execution(&self, key: &ExecutionKey) -> Result<Vec<(Vec<u8>, ChasmNode)>> {
        let (namespace_id, business_id, run_id) = Self::key_parts(key)?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT encoded_path, metadata, data
             FROM chasm_node
             WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3
             ORDER BY encoded_path ASC",
        )
        .bind(namespace_id)
        .bind(business_id)
        .bind(run_id)
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter().map(Self::node_from_row).collect()
    }

    async fn load_subtree(
        &self,
        key: &ExecutionKey,
        encoded_prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, ChasmNode)>> {
        let (namespace_id, business_id, run_id) = Self::key_parts(key)?;
        let end = tokeira_chasm::path::subtree_range_end(encoded_prefix);
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT encoded_path, metadata, data
             FROM chasm_node
             WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3
               AND encoded_path >= $4 AND encoded_path < $5
             ORDER BY encoded_path ASC",
        )
        .bind(namespace_id)
        .bind(business_id)
        .bind(run_id)
        .bind(encoded_prefix)
        .bind(end.as_slice())
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter().map(Self::node_from_row).collect()
    }

    async fn delete_execution(&self, key: &ExecutionKey) -> Result<()> {
        let (namespace_id, business_id, run_id) = Self::key_parts(key)?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        sqlx::query(
            "DELETE FROM chasm_node
             WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3",
        )
        .bind(namespace_id)
        .bind(business_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        // Clear the current-run pointer iff it still points at the deleted run
        // (read-your-write; a superseded run leaves a newer pointer intact).
        sqlx::query(
            "DELETE FROM chasm_current_run
             WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3",
        )
        .bind(namespace_id)
        .bind(business_id)
        .bind(run_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn scan_executions(&self) -> Result<Vec<(ExecutionKey, ChasmNode)>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        // Every execution's root component node (encoded_path = ROOT_PATH, b""),
        // ordered deterministically for the repair scanner (Req 10.11; AGENTS
        // determinism). The empty-bytea predicate hits the PK prefix.
        let rows = sqlx::query(
            "SELECT namespace_id, business_id, run_id, metadata, data
             FROM chasm_node
             WHERE encoded_path = $1
             ORDER BY namespace_id ASC, business_id ASC, run_id ASC",
        )
        .bind(b"".as_slice())
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter()
            .map(|row| {
                let namespace_id: Uuid = row.try_get("namespace_id")?;
                let business_id: String = row.try_get("business_id")?;
                let run_id: Uuid = row.try_get("run_id")?;
                let metadata_blob: Vec<u8> = row.try_get("metadata")?;
                let data: Option<Vec<u8>> = row.try_get("data")?;
                let metadata = codec::decode(&metadata_blob)?;
                let key =
                    ExecutionKey::new(namespace_id.to_string(), business_id, run_id.to_string());
                Ok((key, ChasmNode { metadata, data }))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use time::Duration;
    use tokeira_chasm::{LifecycleState, NodeMetadata, NodeTree, RetainAllValidator};

    use crate::{
        ChasmNodeRepository, ExpectedVersion, NodePersistOutcome, NodeWrite,
        dsql::{DsqlPoolConfig, DsqlStore, ReservoirConfig},
    };
    use tokeira_chasm::{ChasmNode, ExecutionKey, VersionedTransition};

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

    async fn ensure_chasm_node_table(database_url: &str) -> anyhow::Result<()> {
        let pool = sqlx::PgPool::connect(database_url).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS chasm_node (
                namespace_id                UUID        NOT NULL,
                business_id                 TEXT        NOT NULL,
                run_id                      UUID        NOT NULL,
                encoded_path                BYTEA       NOT NULL,
                archetype_id                BIGINT      NOT NULL,
                failover_version            BIGINT      NOT NULL,
                transition_count            BIGINT      NOT NULL,
                initial_failover_version    BIGINT      NOT NULL,
                initial_transition_count    BIGINT      NOT NULL,
                metadata                    BYTEA       NOT NULL,
                data                        BYTEA,
                updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (namespace_id, business_id, run_id, encoded_path)
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
        ensure_chasm_node_table(&database_url).await?;
        DsqlStore::from_database_url_for_tests(database_url, test_pool_config())
            .await
            .map(Some)
    }

    fn key() -> ExecutionKey {
        ExecutionKey::new(
            uuid::Uuid::new_v4().to_string(),
            "wf-1",
            uuid::Uuid::new_v4().to_string(),
        )
    }

    fn vt(failover: i64, count: i64) -> VersionedTransition {
        VersionedTransition::new(failover, count)
    }

    #[tokio::test]
    async fn dsql_chasm_node_store_round_trips_and_fences() -> anyhow::Result<()> {
        let Some(store) = dsql_store_from_env().await? else {
            return Ok(());
        };
        let repo = store.chasm_node_repository();
        let key = key();

        // First commit: create the root via the pure tree, then persist.
        let mut tree = NodeTree::new();
        tree.create_node(
            b"".to_vec(),
            7,
            Some(LifecycleState::Running),
            Some(vec![1]),
        )
        .expect("create");
        let result = tree
            .close_transaction(vt(1, 1), &RetainAllValidator)
            .expect("close");
        let batch: Vec<NodeWrite> = result
            .dirty_nodes
            .into_iter()
            .map(|(encoded_path, node)| NodeWrite {
                encoded_path,
                node,
                expected: ExpectedVersion::Absent,
            })
            .collect();
        assert_eq!(
            repo.persist_dirty(&key, batch).await?,
            NodePersistOutcome::Applied
        );

        let loaded = repo.load_execution(&key).await?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1.metadata.versioned_transition, vt(1, 1));
        assert_eq!(loaded[0].1.data, Some(vec![1]));

        // Stale CAS update is rejected with no write.
        let stale = vec![NodeWrite {
            encoded_path: b"".to_vec(),
            node: ChasmNode {
                metadata: NodeMetadata::new(7, Some(LifecycleState::Running), vt(1, 2)),
                data: Some(vec![9]),
            },
            expected: ExpectedVersion::Vt(vt(9, 9)),
        }];
        assert!(matches!(
            repo.persist_dirty(&key, stale).await?,
            NodePersistOutcome::Conflict { .. }
        ));
        assert_eq!(repo.load_execution(&key).await?[0].1.data, Some(vec![1]));

        repo.delete_execution(&key).await?;
        assert!(repo.load_execution(&key).await?.is_empty());

        store.shutdown().await?;
        Ok(())
    }
}
