//! Canonical activity-row maintenance inside the authoritative run transaction.
//!
//! Desired-set deltas replace effect-driven row creation. Every existing page is
//! read under the caller's run serialization; no chunk of an ordinary transition
//! commits independently. Readers use bounded, covered key seeks in one snapshot.

use std::{collections::BTreeMap, num::NonZeroU32};

use sqlx::{Row, postgres::PgRow};

use super::*;
use crate::{
    ACTIVITY_DISPATCH_PAGE_LIMIT, ActivityDiscoveryRange, ActivityDispatchCandidate,
    ActivityDispatchPage, ActivityDispatchPosition, ActivityDispatchRow, ActivityHomePage,
    ActivityHomePosition, ActivityIncarnation, activity_dispatch_key, derive_activity_dispatch,
};

pub(super) const COLUMNS: &str = "key, run_key, activity_id, shard_id, queue_namespace,
    queue_name, task_kind, deployment, build_id, schedule_event_id, attempt,
    dispatch_revision, stamp, priority_data, dispatch_at, queue_key";

pub(super) fn decode(row: &PgRow) -> Result<ActivityDispatchRow> {
    let result = ActivityDispatchRow {
        key: row.try_get("key")?,
        incarnation: ActivityIncarnation {
            run_key: RunKey(row.try_get("run_key")?),
            activity_id: row.try_get("activity_id")?,
            schedule_event_id: row.try_get("schedule_event_id")?,
            attempt: u32::try_from(row.try_get::<i32, _>("attempt")?)?,
            stamp: u64::try_from(row.try_get::<i64, _>("stamp")?)?,
        },
        execution_home: DsqlRunRepository::shard_id_from_uuid(row.try_get("shard_id")?)?,
        queue: QueueKey {
            namespace_id: NamespaceId(row.try_get("queue_namespace")?),
            task_queue: TaskQueueName(row.try_get("queue_name")?),
            task_kind: TaskKind::try_from(row.try_get::<i16, _>("task_kind")?)?,
            deployment: row
                .try_get::<Option<String>, _>("deployment")?
                .map(DeploymentId),
            build_id: row.try_get::<Option<String>, _>("build_id")?.map(BuildId),
        },
        queue_key: row
            .try_get::<Option<String>, _>("queue_key")?
            .context("legacy activity row requires fenced acquisition repair")?,
        priority: row
            .try_get::<Option<Vec<u8>>, _>("priority_data")?
            .as_deref()
            .map(codec::decode_priority)
            .transpose()?,
        dispatch_revision: row.try_get("dispatch_revision")?,
        dispatch_at: row.try_get("dispatch_at")?,
    };
    result.validate()?;
    Ok(result)
}

pub(super) async fn upsert(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &ActivityDispatchRow,
) -> Result<()> {
    row.validate()?;
    // A hash collision must fail the whole transition. The conflict predicate
    // checks raw slot identity even when the colliding row belongs to another
    // run and was therefore absent from the run-local desired-set read.
    let result = sqlx::query(
        "INSERT INTO activity_dispatch
        (key, run_key, activity_id, shard_id, queue_namespace, queue_name, task_kind,
         deployment, build_id, schedule_event_id, attempt, dispatch_revision, stamp,
         input_data, priority_data, dispatch_at, created_at, queue_key)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,now(),$17)
        ON CONFLICT (key) DO UPDATE SET
         shard_id=EXCLUDED.shard_id, queue_namespace=EXCLUDED.queue_namespace,
         queue_name=EXCLUDED.queue_name, task_kind=EXCLUDED.task_kind,
         deployment=EXCLUDED.deployment, build_id=EXCLUDED.build_id,
         schedule_event_id=EXCLUDED.schedule_event_id, attempt=EXCLUDED.attempt,
         dispatch_revision=EXCLUDED.dispatch_revision, stamp=EXCLUDED.stamp,
         input_data=EXCLUDED.input_data, priority_data=EXCLUDED.priority_data,
         dispatch_at=EXCLUDED.dispatch_at, queue_key=EXCLUDED.queue_key
        WHERE activity_dispatch.run_key=EXCLUDED.run_key
          AND activity_dispatch.activity_id=EXCLUDED.activity_id",
    )
    .bind(row.key)
    .bind(row.incarnation.run_key.0)
    .bind(&row.incarnation.activity_id)
    .bind(DsqlRunRepository::shard_id_to_uuid(row.execution_home))
    .bind(row.queue.namespace_id.0)
    .bind(&row.queue.task_queue.0)
    .bind(TaskKind::Activity.to_db_smallint())
    .bind(row.queue.deployment.as_ref().map(|v| v.0.as_str()))
    .bind(row.queue.build_id.as_ref().map(|v| v.0.as_str()))
    .bind(row.incarnation.schedule_event_id)
    .bind(i32::try_from(row.incarnation.attempt)?)
    .bind(row.dispatch_revision)
    .bind(i64::try_from(row.incarnation.stamp)?)
    .bind(codec::empty_input_data()?)
    .bind(
        row.priority
            .as_ref()
            .map(codec::encode_priority)
            .transpose()?,
    )
    .bind(row.dispatch_at)
    .bind(&row.queue_key)
    .execute(&mut **tx)
    .await?;
    anyhow::ensure!(
        result.rows_affected() == 1,
        "activity dispatch slot hash collision"
    );
    Ok(())
}

pub(super) async fn existing_page(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: RunKey,
    after: Option<Uuid>,
) -> Result<Vec<PgRow>> {
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
    query
        .push(COLUMNS)
        .push(" FROM activity_dispatch WHERE run_key=")
        .push_bind(run.0);
    if let Some(after) = after {
        query.push(" AND key>").push_bind(after);
    }
    query
        .push(" ORDER BY key LIMIT ")
        .push_bind(i64::from(ACTIVITY_DISPATCH_PAGE_LIMIT));
    Ok(query.build().fetch_all(&mut **tx).await?)
}

pub(super) async fn maintain(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &WorkflowState,
    home: ShardId,
) -> Result<()> {
    let mut wanted: BTreeMap<_, _> = derive_activity_dispatch(state, home)?;
    let mut after = None;
    loop {
        let existing = existing_page(tx, state.run_key, after).await?;
        let exhausted = existing.len() < ACTIVITY_DISPATCH_PAGE_LIMIT as usize;
        for physical in existing {
            let key: Uuid = physical.try_get("key")?;
            let activity_id: String = physical.try_get("activity_id")?;
            anyhow::ensure!(
                key == activity_dispatch_key(state.run_key, &activity_id),
                "activity dispatch slot disagrees with raw identity"
            );
            if let Some(desired) = wanted.remove(&key) {
                if decode(&physical).ok().as_ref() != Some(&desired) {
                    upsert(tx, &desired).await?;
                }
            } else {
                sqlx::query(
                    "DELETE FROM activity_dispatch WHERE key=$1 AND run_key=$2 AND activity_id=$3",
                )
                .bind(key)
                .bind(state.run_key.0)
                .bind(activity_id)
                .execute(&mut **tx)
                .await?;
            }
            after = Some(key);
        }
        if exhausted {
            break;
        }
    }
    for row in wanted.into_values() {
        upsert(tx, &row).await?;
    }
    Ok(())
}

pub(super) fn due_query(
    range: &ActivityDiscoveryRange,
    cutoff: OffsetDateTime,
    after: Option<ActivityDispatchPosition>,
    limit: NonZeroU32,
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let limit = i64::from(limit.get().min(ACTIVITY_DISPATCH_PAGE_LIMIT));
    let projection = COLUMNS
        .split(',')
        .map(|column| format!("d.{}", column.trim()))
        .collect::<Vec<_>>()
        .join(",");
    let mut query = sqlx::QueryBuilder::new(format!("SELECT {projection} FROM ("));
    if let Some(after) = after {
        // Each disjoint branch is an index-covered seek; bounding both avoids
        // scanning an old timestamp tie as a residual primary-key filter.
        query.push("SELECT key, dispatch_at FROM ((");
        due_prefix(&mut query, range);
        query
            .push(" AND dispatch_at=")
            .push_bind(after.dispatch_at)
            .push(" AND key>")
            .push_bind(after.key)
            // A redundant range on dispatch_at makes DSQL leave key> as a
            // residual storage filter, rescanning the tie. Compare constants
            // for cutoff eligibility so the equality prefix reaches key>.
            .push(" AND ")
            .push_bind(after.dispatch_at)
            .push("<=")
            .push_bind(cutoff)
            .push(" ORDER BY dispatch_at,key LIMIT ")
            .push_bind(limit)
            .push(") UNION ALL (");
        due_prefix(&mut query, range);
        query
            .push(" AND dispatch_at>")
            .push_bind(after.dispatch_at)
            .push(" AND dispatch_at<=")
            .push_bind(cutoff)
            .push(" ORDER BY dispatch_at,key LIMIT ")
            .push_bind(limit)
            .push(")) positions ORDER BY dispatch_at,key LIMIT ")
            .push_bind(limit);
    } else {
        due_prefix(&mut query, range);
        query
            .push(" AND dispatch_at<=")
            .push_bind(cutoff)
            .push(" ORDER BY dispatch_at,key LIMIT ")
            .push_bind(limit);
    }
    query.push(") p JOIN activity_dispatch d ON d.key=p.key ORDER BY p.dispatch_at,p.key");
    query
}

fn due_prefix(query: &mut sqlx::QueryBuilder<sqlx::Postgres>, range: &ActivityDiscoveryRange) {
    query
        .push("SELECT key,dispatch_at FROM activity_dispatch WHERE queue_namespace=")
        .push_bind(range.namespace_id.0)
        .push(" AND queue_key=")
        .push_bind(range.lookup_key());
}

pub(super) fn home_query(
    home: ShardId,
    after: Option<ActivityHomePosition>,
    limit: NonZeroU32,
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let limit = i64::from(limit.get().min(ACTIVITY_DISPATCH_PAGE_LIMIT));
    let mut query = sqlx::QueryBuilder::new("");
    if let Some(after) = after {
        // DSQL leaves a tuple inequality as a residual Filter, even for a
        // covering index. Disjoint scalar seeks bound the work before the
        // merge, including thousands of slots tied on one run key.
        query.push("SELECT run_key,key FROM ((");
        home_prefix(&mut query, home);
        query
            .push(" AND run_key=")
            .push_bind(after.run_key.0)
            .push(" AND key>")
            .push_bind(after.key)
            .push(" ORDER BY run_key,key LIMIT ")
            .push_bind(limit)
            .push(") UNION ALL (");
        home_prefix(&mut query, home);
        query
            .push(" AND run_key>")
            .push_bind(after.run_key.0)
            .push(" ORDER BY run_key,key LIMIT ")
            .push_bind(limit)
            .push(")) positions ORDER BY run_key,key LIMIT ")
            .push_bind(limit);
    } else {
        home_prefix(&mut query, home);
        query.push(" ORDER BY run_key,key LIMIT ").push_bind(limit);
    }
    query
}

fn home_prefix(query: &mut sqlx::QueryBuilder<sqlx::Postgres>, home: ShardId) {
    query
        .push("SELECT run_key,key FROM activity_dispatch WHERE shard_id=")
        .push_bind(DsqlRunRepository::shard_id_to_uuid(home));
}

impl DsqlRunRepository {
    pub(super) async fn do_list_activity_dispatch_page(
        &self,
        range: &ActivityDiscoveryRange,
        cutoff: OffsetDateTime,
        after: Option<ActivityDispatchPosition>,
        limit: NonZeroU32,
    ) -> Result<ActivityDispatchPage> {
        let mut query = due_query(range, cutoff, after, limit);
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = query.build().fetch_all(permit.connection()?).await?;
        metrics::record_dsql_rows_read("list_activity_dispatch_page", rows.len());
        let exhausted = rows.len() < limit.get().min(ACTIVITY_DISPATCH_PAGE_LIMIT) as usize;
        let mut candidates = Vec::with_capacity(rows.len());
        for physical in rows {
            // Without a valid key there is no safe continuation. Payload errors
            // instead remain visible to the caller alongside a valid position.
            let position = ActivityDispatchPosition {
                dispatch_at: physical.try_get("dispatch_at")?,
                key: physical.try_get("key")?,
            };
            let row = decode(&physical);
            candidates.push(ActivityDispatchCandidate { position, row });
        }
        Ok(ActivityDispatchPage {
            last_examined: candidates.last().map(|c| c.position),
            candidates,
            exhausted,
        })
    }

    pub(super) async fn do_list_activity_dispatch_home_page(
        &self,
        home: ShardId,
        after: Option<ActivityHomePosition>,
        limit: NonZeroU32,
    ) -> Result<ActivityHomePage> {
        let mut query = home_query(home, after, limit);
        let limit = i64::from(limit.get().min(ACTIVITY_DISPATCH_PAGE_LIMIT));
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let rows = query
            .build_query_as::<(Uuid, Uuid)>()
            .fetch_all(permit.connection()?)
            .await?;
        metrics::record_dsql_rows_read("list_activity_dispatch_home_page", rows.len());
        let candidates: Vec<_> = rows
            .into_iter()
            .map(|(run, key)| ActivityHomePosition {
                run_key: RunKey(run),
                key,
            })
            .collect();
        Ok(ActivityHomePage {
            last_examined: candidates.last().copied(),
            exhausted: candidates.len() < limit as usize,
            candidates,
        })
    }
}
