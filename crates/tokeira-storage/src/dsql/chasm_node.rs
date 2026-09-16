//! DSQL-backed CHASM node store (Requirement 9).
//!
//! The production backend for [`ChasmNodeRepository`],
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
use sqlx::{Connection, PgConnection, Row};
use tokeira_chasm::{ChasmNode, ExecutionKey, LifecycleState, VersionedTransition};
use uuid::Uuid;

use crate::{
    CHASM_CURRENT_EXECUTION_BACKFILL_MARKER, ChasmNodeRepository, CurrentExecution,
    CurrentExecutionCursor, CurrentRun, DbClass, ExpectedVersion, NodePersistOutcome, NodeWrite,
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

    /// Borrow the canonical path bytes as the DSQL `TEXT` key.
    ///
    /// Temporal's v1.31.0 path encoder emits a string
    /// (`chasm/path_encoder.go:25-75 @ v1.31.0`), and Tokeira's equivalent
    /// encoder appends only UTF-8 segment text plus ASCII separators and
    /// escapes. Keeping that representation one-for-one avoids the key-size
    /// expansion of a binary-to-text wrapper under DSQL's 1 KiB combined-key
    /// limit.
    fn path_key(encoded_path: &[u8]) -> Result<&str> {
        std::str::from_utf8(encoded_path)
            .map_err(|e| anyhow::anyhow!("CHASM encoded path is not valid UTF-8: {e}"))
    }

    /// Reconstruct a [`ChasmNode`] from a result row (`metadata` blob is
    /// authoritative; `data` is the nullable column).
    fn node_from_row(row: &sqlx::postgres::PgRow) -> Result<(Vec<u8>, ChasmNode)> {
        let encoded_path: String = row.try_get("encoded_path")?;
        let metadata_blob: Vec<u8> = row.try_get("metadata")?;
        let data: Option<Vec<u8>> = row.try_get("data")?;
        let metadata = codec::decode(&metadata_blob)?;
        Ok((encoded_path.into_bytes(), ChasmNode { metadata, data }))
    }

    fn current_from_row(row: &sqlx::postgres::PgRow) -> Result<CurrentRun> {
        let run_id: Uuid = row.try_get("run_id")?;
        Ok(CurrentRun {
            run_id: run_id.to_string(),
            request_id: row.try_get("request_id")?,
            status: Self::decode_status(row.try_get("status")?)?,
            vt_epoch: VersionedTransition::new(
                row.try_get("failover_version")?,
                row.try_get("transition_count")?,
            ),
        })
    }

    async fn read_current(
        connection: &mut PgConnection,
        namespace_id: Uuid,
        archetype_id: u32,
        business_id: &str,
    ) -> Result<Option<CurrentRun>> {
        // One statement gives the new pointer, marker and legacy fallback the same
        // snapshot: a concurrent backfill cannot hide a row between those reads.
        // The legacy key lacks an archetype; its root prevents cross-archetype
        // aliasing and resurrection of deleted runs without writing the old table.
        let row = sqlx::query(
            "SELECT run_id, request_id, status, failover_version, transition_count
             FROM chasm_current_execution
             WHERE namespace_id = $1 AND archetype_id = $2 AND business_id = $3
             UNION ALL
             SELECT old.run_id, old.request_id, old.status, old.failover_version, old.transition_count
             FROM chasm_current_run AS old
             JOIN chasm_node AS root ON root.namespace_id = old.namespace_id
               AND root.business_id = old.business_id AND root.run_id = old.run_id
               AND root.encoded_path = '' AND root.archetype_id = $2
             WHERE old.namespace_id = $1 AND old.business_id = $3
               AND NOT EXISTS (SELECT 1 FROM chasm_backfill_marker WHERE marker_name = $4)
               AND NOT EXISTS (SELECT 1 FROM chasm_current_execution
                 WHERE namespace_id = $1 AND archetype_id = $2 AND business_id = $3)",
        )
        .bind(namespace_id)
        .bind(i64::from(archetype_id))
        .bind(business_id)
        .bind(CHASM_CURRENT_EXECUTION_BACKFILL_MARKER)
        .fetch_optional(connection)
        .await?;
        row.as_ref().map(Self::current_from_row).transpose()
    }

    /// Encode a [`LifecycleState`] as the `CHASM pointer status` SMALLINT
    /// (0=Running, 1=Completed, 2=Failed). Stable on-disk encoding — extend, never
    /// renumber.
    fn encode_status(status: LifecycleState) -> i16 {
        match status {
            LifecycleState::Running => 0,
            LifecycleState::Completed => 1,
            LifecycleState::Failed => 2,
        }
    }

    /// Decode a `CHASM pointer status` SMALLINT back to a [`LifecycleState`].
    fn decode_status(value: i16) -> Result<LifecycleState> {
        match value {
            0 => Ok(LifecycleState::Running),
            1 => Ok(LifecycleState::Completed),
            2 => Ok(LifecycleState::Failed),
            other => Err(anyhow::anyhow!(
                "CHASM pointer status `{other}` is not a known LifecycleState"
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
            let encoded_path = Self::path_key(&write.encoded_path)?;
            let stored = sqlx::query(
                "SELECT failover_version, transition_count
                 FROM chasm_node
                 WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3
                   AND encoded_path = $4",
            )
            .bind(namespace_id)
            .bind(business_id)
            .bind(run_id)
            .bind(encoded_path)
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
            let encoded_path = Self::path_key(&write.encoded_path)?;
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
            .bind(encoded_path)
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
        archetype_id: u32,
        batch: Vec<NodeWrite>,
        current: CurrentRun,
        expected_current: Option<CurrentRun>,
    ) -> Result<NodePersistOutcome> {
        let (namespace_id, business_id, run_id) = Self::key_parts(key)?;
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;

        // Admission may have observed a legacy pointer before backfill. Use the
        // same effective lookup here; a new scoped pointer must not erase it just
        // because the scoped table is still empty. The conditional write below
        // also fences writers racing after this snapshot was read.
        if Self::read_current(&mut tx, namespace_id, archetype_id, business_id).await?
            != expected_current
        {
            tx.rollback().await?;
            return Ok(NodePersistOutcome::Conflict {
                reason: "current-run pointer changed during start admission".into(),
            });
        }
        let expected_run_id = expected_current
            .as_ref()
            .map(|current| Self::parse_uuid("expected current run_id", &current.run_id))
            .transpose()?;

        // Phase 1 — node fences (all-or-nothing), identical to `persist_dirty`.
        for write in &batch {
            let encoded_path = Self::path_key(&write.encoded_path)?;
            let stored = sqlx::query(
                "SELECT failover_version, transition_count
                 FROM chasm_node
                 WHERE namespace_id = $1 AND business_id = $2 AND run_id = $3
                   AND encoded_path = $4",
            )
            .bind(namespace_id)
            .bind(business_id)
            .bind(run_id)
            .bind(encoded_path)
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
            let encoded_path = Self::path_key(&write.encoded_path)?;
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
            .bind(encoded_path)
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

        // Phase 3 — conditionally advance the pointer in the SAME transaction (the
        // analog of v1.31.0's current_executions write inside the entity-create tx),
        // so the run's root node and its current-run pointer never tear. Archetype
        // is part of the key upstream too (schema/postgresql/v12/temporal/versioned/
        // v1.19/current_chasm_executions.sql @ v1.31.0). A NULL expected run permits
        // insertion only; a superseding start must replace the exact observed run
        // and epoch. A miss rolls back node writes, while DSQL OCC rejects a pair
        // of concurrent writes even if both transactions saw the expected value.
        let current_run_id = Self::parse_uuid("current run_id", &current.run_id)?;
        let pointer = sqlx::query(
            "INSERT INTO chasm_current_execution
               (namespace_id, business_id, run_id, request_id, status,
                failover_version, transition_count, archetype_id, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
             ON CONFLICT (namespace_id, archetype_id, business_id) DO UPDATE SET
                run_id = EXCLUDED.run_id,
                request_id = EXCLUDED.request_id,
                status = EXCLUDED.status,
                failover_version = EXCLUDED.failover_version,
                transition_count = EXCLUDED.transition_count,
                updated_at = EXCLUDED.updated_at
             WHERE chasm_current_execution.run_id = $9
               AND chasm_current_execution.failover_version = $10
               AND chasm_current_execution.transition_count = $11",
        )
        .bind(namespace_id)
        .bind(business_id)
        .bind(current_run_id)
        .bind(current.request_id.as_str())
        .bind(Self::encode_status(current.status))
        .bind(current.vt_epoch.namespace_failover_version)
        .bind(current.vt_epoch.transition_count)
        .bind(i64::from(archetype_id))
        .bind(expected_run_id)
        .bind(
            expected_current
                .as_ref()
                .map(|current| current.vt_epoch.namespace_failover_version),
        )
        .bind(
            expected_current
                .as_ref()
                .map(|current| current.vt_epoch.transition_count),
        )
        .execute(&mut *tx)
        .await;
        match pointer {
            Ok(result) if result.rows_affected() == 0 => {
                tx.rollback().await?;
                return Ok(NodePersistOutcome::Conflict {
                    reason: "current-run pointer changed during start admission".into(),
                });
            }
            Ok(_) => {}
            Err(err) if DsqlRunRepository::is_serialization_failure(&err) => {
                tx.rollback().await?;
                return Ok(NodePersistOutcome::Conflict {
                    reason: "dsql serialization failure during current-run write".to_owned(),
                });
            }
            Err(err) => return Err(err.into()),
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
        archetype_id: u32,
        business_id: &str,
    ) -> Result<Option<CurrentRun>> {
        let namespace_uuid = Self::parse_uuid("namespace_id", namespace_id)?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        Self::read_current(
            permit.connection()?,
            namespace_uuid,
            archetype_id,
            business_id,
        )
        .await
    }

    async fn scan_current_executions(
        &self,
        status: LifecycleState,
        after: Option<CurrentExecutionCursor>,
        limit: usize,
    ) -> Result<Vec<CurrentExecution>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let namespace = after
            .as_ref()
            .map(|key| Self::parse_uuid("cursor namespace_id", &key.namespace_id))
            .transpose()?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT namespace_id, archetype_id, business_id, run_id, request_id,
                    status, failover_version, transition_count
             FROM chasm_current_execution
             WHERE status = $1 AND ($2::uuid IS NULL OR
                 (namespace_id, archetype_id, business_id) > ($2, $3, $4))
             ORDER BY namespace_id, archetype_id, business_id LIMIT $5",
        )
        .bind(Self::encode_status(status))
        .bind(namespace)
        .bind(after.as_ref().map(|key| i64::from(key.archetype_id)))
        .bind(after.as_ref().map(|key| key.business_id.as_str()))
        .bind(i64::try_from(limit)?)
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter()
            .map(|row| {
                let current = Self::current_from_row(row)?;
                let namespace: Uuid = row.try_get("namespace_id")?;
                let business_id: String = row.try_get("business_id")?;
                let archetype: i64 = row.try_get("archetype_id")?;
                Ok(CurrentExecution {
                    key: ExecutionKey::new(namespace.to_string(), business_id, &current.run_id),
                    archetype_id: u32::try_from(archetype)?,
                    current,
                })
            })
            .collect()
    }

    async fn backfill_current_executions(&self, archetype_id: u32, batch: usize) -> Result<usize> {
        anyhow::ensure!(batch > 0, "CHASM backfill batch must be nonzero");
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        // Copied keys are durable progress: exclude them before LIMIT, otherwise a
        // full first page would make the next call return zero prematurely. This
        // ordered remaining-key scan resumes after crashes without an in-memory
        // cursor, and never overwrites a newer pointer. Bootstrap runs before
        // request admission; errors (including OCC) leave the marker unset.
        let inserted = sqlx::query(
            "INSERT INTO chasm_current_execution
               (namespace_id, archetype_id, business_id, run_id, request_id, status,
                failover_version, transition_count, updated_at)
             SELECT old.namespace_id, $1, old.business_id, old.run_id, old.request_id,
                    old.status, old.failover_version, old.transition_count, old.updated_at
             FROM chasm_current_run AS old
             JOIN chasm_node AS root ON root.namespace_id = old.namespace_id
               AND root.business_id = old.business_id AND root.run_id = old.run_id
               AND root.encoded_path = '' AND root.archetype_id = $1
             WHERE NOT EXISTS (SELECT 1 FROM chasm_current_execution AS current
               WHERE current.namespace_id = old.namespace_id AND current.archetype_id = $1
                 AND current.business_id = old.business_id)
             ORDER BY old.namespace_id, old.business_id
             LIMIT $2
             ON CONFLICT (namespace_id, archetype_id, business_id) DO NOTHING",
        )
        .bind(i64::from(archetype_id))
        .bind(i64::try_from(batch.min(500))?)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(usize::try_from(inserted)?)
    }

    async fn distinct_archetypes(&self) -> Result<Vec<(u32, u64)>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = sqlx::query(
            "SELECT archetype_id, COUNT(*) AS count FROM chasm_current_execution
             GROUP BY archetype_id ORDER BY archetype_id",
        )
        .fetch_all(permit.connection()?)
        .await?;
        rows.iter()
            .map(|row| {
                let archetype: i64 = row.try_get("archetype_id")?;
                let count: i64 = row.try_get("count")?;
                Ok((u32::try_from(archetype)?, u64::try_from(count)?))
            })
            .collect()
    }

    async fn backfill_marker_set(&self, name: &str) -> Result<bool> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        Ok(
            sqlx::query("SELECT 1 FROM chasm_backfill_marker WHERE marker_name = $1")
                .bind(name)
                .fetch_optional(permit.connection()?)
                .await?
                .is_some(),
        )
    }

    async fn set_backfill_marker(&self, name: &str) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        sqlx::query(
            "INSERT INTO chasm_backfill_marker (marker_name) VALUES ($1) ON CONFLICT DO NOTHING",
        )
        .bind(name)
        .execute(permit.connection()?)
        .await?;
        Ok(())
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
        let encoded_prefix = Self::path_key(encoded_prefix)?;
        let encoded_end = Self::path_key(&end)?;
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
        .bind(encoded_end)
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
            "DELETE FROM chasm_current_execution
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
        // determinism). The root is the empty text key.
        let rows = sqlx::query(
            "SELECT namespace_id, business_id, run_id, metadata, data
             FROM chasm_node
             WHERE encoded_path = $1
             ORDER BY namespace_id ASC, business_id ASC, run_id ASC",
        )
        .bind("")
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
    use crate::chasm::pointer_tests;
    use proptest::{prelude::*, test_runner::TestRunner};
    use sqlx::{ConnectOptions, Postgres, QueryBuilder};
    use time::Duration;
    use tokeira_chasm::{
        LifecycleState, NodeMetadata, NodeTree, RetainAllValidator,
        path::{self, PathSegment},
    };

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
        for sql in [
            include_str!("../../migrations/V049__chasm_node.sql"),
            include_str!("../../migrations/V056__chasm_current_run.sql"),
            include_str!("../../migrations/V069__chasm_current_execution.sql"),
            include_str!("../../migrations/V071__chasm_backfill_marker.sql"),
        ] {
            sqlx::query(sql).execute(&pool).await?;
        }
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

    #[test]
    fn dsql_path_key_preserves_canonical_utf8_bytes() {
        let encoded = path::encode(&[
            PathSegment::field("α"),
            PathSegment::collection("escaped$#\\"),
        ])
        .expect("path encodes");
        let key =
            super::DsqlChasmNodeRepository::path_key(&encoded).expect("encoded path is UTF-8");

        assert_eq!(key.as_bytes(), encoded);
    }

    #[test]
    fn dsql_path_key_rejects_non_utf8_bytes() {
        assert!(super::DsqlChasmNodeRepository::path_key(&[0xff]).is_err());
    }

    #[tokio::test]
    async fn dsql_concurrent_starts_fence_pointer_and_roll_back_losing_nodes() -> anyhow::Result<()>
    {
        let Some(store) = dsql_store_from_env().await? else {
            return Ok(());
        };
        pointer_tests::exercise_pointer_fence(
            &store.chasm_node_repository(),
            &uuid::Uuid::new_v4().to_string(),
        )
        .await
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
            repo.persist_new_execution(
                &key,
                7,
                batch,
                pointer_tests::current(key.run_id.clone(), "create", LifecycleState::Running),
                None,
            )
            .await?,
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

        assert_eq!(
            repo.current_run(&key.namespace_id, 7, &key.business_id)
                .await?
                .unwrap()
                .run_id,
            key.run_id
        );
        assert!(matches!(
            repo.persist_new_execution(
                &key,
                7,
                vec![pointer_tests::root(7, LifecycleState::Completed, 1)],
                pointer_tests::current(
                    uuid::Uuid::new_v4().to_string(),
                    "conflict",
                    LifecycleState::Completed
                ),
                repo.current_run(&key.namespace_id, 7, &key.business_id)
                    .await?,
            )
            .await?,
            NodePersistOutcome::Conflict { .. }
        ));
        assert_eq!(
            repo.current_run(&key.namespace_id, 7, &key.business_id)
                .await?
                .unwrap()
                .request_id,
            "create"
        );
        repo.delete_execution(&key).await?;
        assert!(repo.load_execution(&key).await?.is_empty());
        assert!(
            repo.current_run(&key.namespace_id, 7, &key.business_id)
                .await?
                .is_none()
        );

        store.shutdown().await?;
        Ok(())
    }

    #[test]
    fn pointer_migrations_pass_ddl_validator() {
        for (name, sql) in [
            (
                "V069",
                include_str!("../../migrations/V069__chasm_current_execution.sql"),
            ),
            (
                "V070",
                include_str!("../../migrations/V070__idx_chasm_current_execution_status.sql"),
            ),
            (
                "V071",
                include_str!("../../migrations/V071__chasm_backfill_marker.sql"),
            ),
        ] {
            assert!(
                crate::dsql::validation::DdlValidator::validate(sql, name).is_empty(),
                "{name}"
            );
            assert_eq!(sql.matches(';').count(), 1, "one statement per migration");
        }
    }

    // Feature: chasm-extension-archetypes, Property 8: archetype-scoped business ids
    // The same independent-pointer model must hold through real SQL and backfill.
    #[test]
    fn dsql_archetype_scoped_business_ids() -> anyhow::Result<()> {
        let Ok(database_url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // The completion marker is global to a schema. Give this property its own
        // schema so fixture resets never alter another test's or an operator's marker.
        // All dynamic DDL below uses only this UUID-derived identifier and literal
        // table names; neither can contain SQL syntax or user input.
        let schema = format!("chasm_pbt_{}", uuid::Uuid::new_v4().simple());
        let (admin, pool, store) = runtime.block_on(async {
            let admin = sqlx::PgPool::connect(&database_url).await?;
            // SQL safety: schema consists only of a fixed prefix and UUID hex.
            QueryBuilder::<Postgres>::new("CREATE SCHEMA ")
                .push(&schema)
                .build()
                .execute(&admin)
                .await?;
            let options = database_url
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .options([("search_path", schema.as_str())]);
            // SQLx's lossy URL conversion omits startup options. Restore the
            // search path explicitly before the director opens its connections.
            let mut url = options.to_url_lossy();
            url.query_pairs_mut()
                .append_pair("options", &format!("-c search_path={schema}"));
            let pool = sqlx::PgPool::connect_with(options).await?;
            for sql in [
                include_str!("../../migrations/V049__chasm_node.sql"),
                include_str!("../../migrations/V056__chasm_current_run.sql"),
                include_str!("../../migrations/V069__chasm_current_execution.sql"),
                include_str!("../../migrations/V071__chasm_backfill_marker.sql"),
            ] {
                sqlx::query(sql).execute(&pool).await?;
            }
            let store =
                DsqlStore::from_database_url_for_tests(url.to_string(), test_pool_config()).await?;
            Ok::<_, anyhow::Error>((admin, pool, store))
        })?;
        let repo = store.chasm_node_repository();
        runtime.block_on(async {
            let mut permit = repo.director.acquire(crate::DbClass::Read).await?;
            let actual: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(permit.connection()?)
                .await?;
            anyhow::ensure!(actual == schema, "property test schema isolation failed");
            Ok::<_, anyhow::Error>(())
        })?;
        let result = TestRunner::new(ProptestConfig::with_cases(100)).run(
            &pointer_tests::dsql_scenarios(), |scenario| {
                runtime.block_on(async {
                    for sql in ["DELETE FROM chasm_current_execution", "DELETE FROM chasm_current_run",
                        "DELETE FROM chasm_node", "DELETE FROM chasm_backfill_marker"] {
                        sqlx::query(sql).execute(&pool).await?;
                    }
                    let namespace = uuid::Uuid::new_v4().to_string();
                    for (key, pointer) in pointer_tests::legacy_rows(&scenario, &namespace) {
                        repo.persist_dirty(&key, vec![pointer_tests::root(scenario.archetypes[0], pointer.status, 1)]).await?;
                        sqlx::query("INSERT INTO chasm_current_run
                            (namespace_id, business_id, run_id, request_id, status, failover_version, transition_count)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)")
                            .bind(uuid::Uuid::parse_str(&key.namespace_id)?)
                            .bind(&key.business_id)
                            .bind(uuid::Uuid::parse_str(&pointer.run_id)?)
                            .bind(&pointer.request_id)
                            .bind(super::DsqlChasmNodeRepository::encode_status(pointer.status))
                            .bind(pointer.vt_epoch.namespace_failover_version)
                            .bind(pointer.vt_epoch.transition_count)
                            .execute(&pool).await?;
                    }
                    pointer_tests::exercise(&repo, &scenario, &namespace).await
                }).map_err(|error| TestCaseError::fail(error.to_string()))
            },
        );
        drop(repo);
        runtime.block_on(async {
            store.shutdown().await?;
            for table in [
                "chasm_current_execution",
                "chasm_current_run",
                "chasm_node",
                "chasm_backfill_marker",
            ] {
                // SQL safety: UUID-derived schema and allowlisted table literal.
                QueryBuilder::<Postgres>::new("DROP TABLE ")
                    .push(&schema)
                    .push(".")
                    .push(table)
                    .build()
                    .execute(&admin)
                    .await?;
            }
            pool.close().await;
            // SQL safety: this is the same UUID-derived schema created above.
            QueryBuilder::<Postgres>::new("DROP SCHEMA ")
                .push(&schema)
                .build()
                .execute(&admin)
                .await?;
            admin.close().await;
            Ok::<_, anyhow::Error>(())
        })?;
        result.map_err(|error| anyhow::anyhow!("{error}"))
    }
}
