//! Physical placement migration before runtime startup.
//!
//! Every page changes the progress revision in the same transaction as its
//! moves. No helper may commit a move independently of that revision: it is
//! what excludes a late starter's old page after another marks completion.

use std::sync::atomic::Ordering;

use anyhow::{Context, Result, ensure};
use sqlx::{Connection, Postgres, QueryBuilder};
use time::OffsetDateTime;
use tokeira_types::{NamespaceId, RunKey, ShardId, WorkflowId, execution_home_bundle};
use uuid::Uuid;

use super::DsqlRunRepository;
use crate::{
    DbClass, PlacementPage, PlacementPhase, PlacementProgress, TimerPosition,
    dsql::codec,
    placement_upgrade::{
        MOVE_KEYS, PLACEMENT_UPGRADE, SCREEN_KEYS, shard_uuid, validate_identity, validate_source,
    },
    write_budget::{MAX_BYTES_PER_TRANSACTION, MAX_ROWS_PER_TRANSACTION},
};

type HotScreen = (Uuid, Uuid, Uuid, String);
type TimerScreen = (
    Uuid,
    OffsetDateTime,
    Uuid,
    String,
    Option<Uuid>,
    Option<String>,
);

/// This exact SQL is also explained by the live plan contract.
pub(super) fn hot_screen_query(after: Option<RunKey>) -> QueryBuilder<Postgres> {
    let mut q =
        QueryBuilder::new("SELECT run_key, shard_id, namespace_id, workflow_id FROM workflow_hot");
    if let Some(after) = after {
        q.push(" WHERE run_key > ").push_bind(after.0);
    }
    q.push(" ORDER BY run_key LIMIT ")
        .push_bind(SCREEN_KEYS as i64);
    q
}

/// Limit the physical timer keys before the LEFT JOIN: orphans participate in
/// cursor advancement, and hot-row lookup is one bounded database join rather
/// than an application round trip for each timer.
pub(super) fn timer_screen_query(after: Option<&TimerPosition>) -> QueryBuilder<Postgres> {
    let mut q = QueryBuilder::new("WITH timer_page AS MATERIALIZED (");
    // Disjoint scalar ranges make every continuation component an index bound,
    // instead of a tuple filter that rescans the already examined prefix.
    if let Some(a) = after {
        q.push("SELECT * FROM (");
        for branch in 0..4 {
            if branch != 0 {
                q.push(" UNION ALL ");
            }
            q.push("(SELECT shard_id, fire_at, run_key, timer_id FROM timer_bucket WHERE ");
            match branch {
                0 => {
                    q.push("shard_id = ")
                        .push_bind(a.shard)
                        .push(" AND fire_at = ")
                        .push_bind(a.fire_at)
                        .push(" AND run_key = ")
                        .push_bind(a.run_key.0)
                        .push(" AND timer_id > ")
                        .push_bind(a.timer_id.clone());
                }
                1 => {
                    q.push("shard_id = ")
                        .push_bind(a.shard)
                        .push(" AND fire_at = ")
                        .push_bind(a.fire_at)
                        .push(" AND run_key > ")
                        .push_bind(a.run_key.0);
                }
                2 => {
                    q.push("shard_id = ")
                        .push_bind(a.shard)
                        .push(" AND fire_at > ")
                        .push_bind(a.fire_at);
                }
                _ => {
                    q.push("shard_id > ").push_bind(a.shard);
                }
            }
            q.push(" ORDER BY shard_id, fire_at, run_key, timer_id LIMIT ")
                .push_bind(SCREEN_KEYS as i64)
                .push(")");
        }
        q.push(") ranges");
    } else {
        q.push("SELECT shard_id, fire_at, run_key, timer_id FROM timer_bucket");
    }
    q.push(" ORDER BY shard_id, fire_at, run_key, timer_id LIMIT ")
        .push_bind(SCREEN_KEYS as i64)
        // A plain LEFT JOIN can choose a full hot-index hash build for every
        // page. The bounded key array also constrains that side of the join,
        // independently of join strategy, while retaining orphan timer keys.
        .push(") SELECT t.shard_id, t.fire_at, t.run_key, t.timer_id, h.namespace_id, h.workflow_id FROM timer_page t LEFT JOIN (SELECT run_key, namespace_id, workflow_id FROM workflow_hot WHERE run_key = ANY(ARRAY(SELECT run_key FROM timer_page))) h ON h.run_key = t.run_key ORDER BY t.shard_id, t.fire_at, t.run_key, t.timer_id");
    q
}

fn home(namespace: Uuid, workflow: &str, count: u32) -> ShardId {
    execution_home_bundle(namespace.as_bytes(), workflow.as_bytes(), count)
}

/// A long unchanged prefix commits alone. The following page begins at the
/// first move, so a mixed page never processes more than MOVE_KEYS source keys.
fn prefix_len(first_move: Option<usize>, length: usize) -> usize {
    match first_move {
        None => length,
        Some(index) if index >= MOVE_KEYS => index,
        Some(_) => length.min(MOVE_KEYS),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("placement page lost its marker revision")]
struct LostPage;

fn retryable(error: &anyhow::Error) -> bool {
    if matches!(
        error.downcast_ref::<crate::dsql::ReservoirError>(),
        Some(crate::dsql::ReservoirError::Empty)
    ) {
        return true;
    }
    if error.is::<LostPage>() {
        return true;
    }
    error
        .downcast_ref::<sqlx::Error>()
        .is_some_and(|error| match error {
            sqlx::Error::Io(_)
            | sqlx::Error::PoolTimedOut
            | sqlx::Error::WorkerCrashed
            | sqlx::Error::Tls(_) => true,
            sqlx::Error::Database(db) => db
                .code()
                .is_some_and(|code| code == "40001" || code.starts_with("08")),
            _ => false,
        })
}

impl DsqlRunRepository {
    /// Retry lost pages and set readiness only after observing durable completion.
    pub(super) async fn do_prepare_placement_page(&self) -> Result<PlacementPage> {
        match self.placement_page().await {
            Ok(progress) => {
                if progress.complete() {
                    self.placement_ready.store(true, Ordering::Release);
                }
                Ok(PlacementPage::Committed(progress))
            }
            Err(error) if retryable(&error) => Ok(PlacementPage::Retry {
                cause: format!("{error:#}"),
            }),
            Err(error) => Err(error),
        }
    }

    async fn placement_page(&self) -> Result<PlacementProgress> {
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.connection()?.begin().await?;
        let marker = sqlx::query_as::<_, (i64, i64, Vec<u8>)>(
            "SELECT shard_count, revision, progress_data FROM workflow_placement_upgrade WHERE name=$1",
        ).bind(PLACEMENT_UPGRADE).fetch_optional(&mut *tx).await?;
        let (revision, mut progress) = match marker {
            Some((count, revision, bytes)) => {
                ensure!(revision > 0, "invalid placement marker revision {revision}");
                ensure!(
                    count == i64::from(self.shard_count),
                    "placement upgrade routing mismatch: recorded shard_count={count}, configured={}",
                    self.shard_count
                );
                let progress: PlacementProgress = postcard::from_bytes(&bytes)
                    .context("invalid execution-home placement progress")?;
                if progress.complete() {
                    tx.commit().await?;
                    return Ok(progress);
                }
                (revision, progress)
            }
            None => (0, PlacementProgress::default()),
        };
        // This detects existing live owners, including legacy UUID encodings.
        // It cannot fence an older binary acquiring after this snapshot.
        let live: Option<Uuid> = sqlx::query_scalar(
            "SELECT shard_id FROM shard_lease WHERE owner IS NOT NULL AND lease_expiry > now() LIMIT 1",
        ).fetch_optional(&mut *tx).await?;
        ensure!(
            live.is_none(),
            "placement upgrade cannot advance while a live shard lease exists: stored_shard={live:?}; stop every older node and allow its lease to expire"
        );
        if revision == 0 {
            sqlx::query("INSERT INTO workflow_placement_upgrade (name, shard_count, revision, progress_data) VALUES ($1,$2,1,$3)")
                .bind(PLACEMENT_UPGRADE).bind(i64::from(self.shard_count))
                .bind(postcard::to_allocvec(&progress)?).execute(&mut *tx).await
                .map_err(|error| match &error {
                    sqlx::Error::Database(db) if db.code().is_some_and(|code| code == "23505") => anyhow::Error::new(LostPage),
                    _ => error.into(),
                })?;
            tx.commit().await?;
            return Ok(progress);
        }

        // Reserve more than the bounded marker's actual encoded size, including
        // its cursor. Data costs count writes/deletes independently.
        let mut bytes_written = 8_192usize;
        let mut rows_written = 1usize;
        match progress.phase.clone() {
            PlacementPhase::Hot(after) => {
                let rows: Vec<HotScreen> = hot_screen_query(after)
                    .build_query_as()
                    .fetch_all(&mut *tx)
                    .await?;
                let first_move = rows.iter().position(|(_, stored, ns, wf)| {
                    *stored != shard_uuid(home(*ns, wf, self.shard_count))
                });
                let limit = prefix_len(first_move, rows.len());
                for (key, stored, namespace, workflow) in rows.iter().take(limit) {
                    let key = RunKey(*key);
                    let target = home(*namespace, workflow, self.shard_count);
                    if *stored != shard_uuid(target) {
                        validate_source(key, *stored, target, self.shard_count)?;
                        let bytes: Vec<u8> = sqlx::query_scalar(
                            "SELECT state_data FROM workflow_hot WHERE run_key=$1",
                        )
                        .bind(key.0)
                        .fetch_one(&mut *tx)
                        .await?;
                        let state = codec::decode_workflow_state(key, &bytes).with_context(||
                            format!("placement upgrade run={key:?} stored_shard={stored} computed_home={target:?}: cannot decode relocation identity"))?;
                        validate_identity(
                            key,
                            *stored,
                            target,
                            NamespaceId(*namespace),
                            &WorkflowId(workflow.clone()),
                            &state,
                        )?;
                        let cost = bytes.len() + workflow.len() + 128;
                        if bytes_written + cost > MAX_BYTES_PER_TRANSACTION
                            || rows_written + 1 > MAX_ROWS_PER_TRANSACTION
                        {
                            break;
                        }
                        sqlx::query("UPDATE workflow_hot SET shard_id=$1 WHERE run_key=$2")
                            .bind(shard_uuid(target))
                            .bind(key.0)
                            .execute(&mut *tx)
                            .await?;
                        bytes_written += cost;
                        rows_written += 1;
                        progress.hot_moved += 1;
                    }
                    progress.hot_examined += 1;
                    progress.phase = PlacementPhase::Hot(Some(key));
                }
                if rows.is_empty() {
                    progress.phase = PlacementPhase::Timers(None);
                }
            }
            PlacementPhase::Timers(after) => {
                let rows: Vec<TimerScreen> = timer_screen_query(after.as_ref())
                    .build_query_as()
                    .fetch_all(&mut *tx)
                    .await?;
                bytes_written = rows
                    .iter()
                    .map(|(_, _, _, id, _, _)| id.len() + 512)
                    .max()
                    .unwrap_or(0)
                    .max(bytes_written);
                let first_move = rows
                    .iter()
                    .position(|(stored, _, _, _, ns, wf)| match (ns, wf) {
                        (Some(ns), Some(wf)) => {
                            *stored != shard_uuid(home(*ns, wf, self.shard_count))
                        }
                        _ => false,
                    });
                let limit = prefix_len(first_move, rows.len());
                // Validate once per changed run within this transaction. This
                // does not add state reads for the overwhelmingly unchanged walk.
                let mut validated = std::collections::HashSet::new();
                for (stored, fire_at, key, timer_id, namespace, workflow) in rows.iter().take(limit)
                {
                    let key = RunKey(*key);
                    if let (Some(namespace), Some(workflow)) = (namespace, workflow) {
                        let target = home(*namespace, workflow, self.shard_count);
                        if *stored != shard_uuid(target) {
                            validate_source(key, *stored, target, self.shard_count)?;
                            if validated.insert(key) {
                                let bytes: Vec<u8> = sqlx::query_scalar(
                                    "SELECT state_data FROM workflow_hot WHERE run_key=$1",
                                )
                                .bind(key.0)
                                .fetch_one(&mut *tx)
                                .await?;
                                let state = codec::decode_workflow_state(key, &bytes).with_context(||
                                    format!("placement upgrade run={key:?} stored_shard={stored} computed_home={target:?}: cannot decode timer relocation identity"))?;
                                validate_identity(
                                    key,
                                    *stored,
                                    target,
                                    NamespaceId(*namespace),
                                    &WorkflowId(workflow.clone()),
                                    &state,
                                )?;
                            }
                            let (payload, created_at): (Vec<u8>, OffsetDateTime) = sqlx::query_as(
                                "SELECT timer_data, created_at FROM timer_bucket WHERE shard_id=$1 AND fire_at=$2 AND run_key=$3 AND timer_id=$4",
                            ).bind(stored).bind(fire_at).bind(key.0).bind(timer_id).fetch_one(&mut *tx).await?;
                            let cost = payload.len() + timer_id.len() + 128;
                            if bytes_written + cost > MAX_BYTES_PER_TRANSACTION
                                || rows_written + 2 > MAX_ROWS_PER_TRANSACTION
                            {
                                break;
                            }
                            let existing: Option<Vec<u8>> = sqlx::query_scalar(
                                "SELECT timer_data FROM timer_bucket WHERE shard_id=$1 AND fire_at=$2 AND run_key=$3 AND timer_id=$4",
                            ).bind(shard_uuid(target)).bind(fire_at).bind(key.0).bind(timer_id).fetch_optional(&mut *tx).await?;
                            if let Some(existing) = existing {
                                ensure!(
                                    existing == payload,
                                    "placement upgrade run={key:?} stored_shard={stored} computed_home={target:?} timer={timer_id}: conflicting destination timer payload"
                                );
                            } else {
                                sqlx::query("INSERT INTO timer_bucket (shard_id,fire_at,run_key,timer_id,timer_data,created_at) VALUES ($1,$2,$3,$4,$5,$6)")
                                    .bind(shard_uuid(target)).bind(fire_at).bind(key.0).bind(timer_id)
                                    .bind(&payload).bind(created_at).execute(&mut *tx).await?;
                            }
                            sqlx::query("DELETE FROM timer_bucket WHERE shard_id=$1 AND fire_at=$2 AND run_key=$3 AND timer_id=$4")
                                .bind(stored).bind(fire_at).bind(key.0).bind(timer_id).execute(&mut *tx).await?;
                            bytes_written += cost;
                            rows_written += 2;
                            progress.timers_moved += 1;
                        }
                    }
                    progress.timers_examined += 1;
                    progress.phase = PlacementPhase::Timers(Some(TimerPosition {
                        shard: *stored,
                        fire_at: *fire_at,
                        run_key: key,
                        timer_id: timer_id.clone(),
                    }));
                }
                if rows.is_empty() {
                    progress.phase = PlacementPhase::Complete;
                }
            }
            PlacementPhase::Complete => unreachable!("complete marker returns before scanning"),
        }
        let next_revision = revision
            .checked_add(1)
            .context("placement revision exhausted")?;
        let changed = sqlx::query("UPDATE workflow_placement_upgrade SET revision=$1, progress_data=$2 WHERE name=$3 AND revision=$4")
            .bind(next_revision).bind(postcard::to_allocvec(&progress)?)
            .bind(PLACEMENT_UPGRADE).bind(revision).execute(&mut *tx).await?;
        if changed.rows_affected() != 1 {
            return Err(LostPage.into());
        }
        tx.commit().await?;
        Ok(progress)
    }
}
