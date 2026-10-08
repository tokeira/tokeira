//! Transaction-local maintenance and read-only traversal of workflow intent.
//! Every write uses the transaction containing hot state; consumers use short
//! director-permitted reads and retain only a typed keyset position between pages.

use sqlx::{Row, postgres::PgRow};
use tokeira_types::{LogicalTaskSeq, WorkerIdentity};

use super::*;
use crate::workflow_dispatch::{
    WorkflowDiscoveryRange, WorkflowDispatchPage, WorkflowDispatchPosition,
    WorkflowDispatchRouting, WorkflowDispatchRow, WorkflowTaskIncarnation,
    derive_workflow_dispatch,
};

#[cfg(all(test, feature = "dsql-integration"))]
pub(super) const SELECT_ROW: &str = "SELECT run_key, shard_id, queue_namespace, queue_name,
    normal_queue_name, sticky, routing_mode, deployment, build_id, logical_seq,
    scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline
    FROM workflow_dispatch";

pub(super) async fn maintain(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &WorkflowState,
    home: ShardId,
) -> Result<()> {
    let Some(row) = derive_workflow_dispatch(state, home) else {
        sqlx::query("DELETE FROM workflow_dispatch WHERE run_key = $1")
            .bind(state.run_key.0)
            .execute(&mut **tx)
            .await?;
        return Ok(());
    };
    row.validate()?;
    let [queue_key, deployment_key, build_key] = row.lookup_keys();
    let (mode, deployment, build_id) = row.routing.coordinates();
    // The complete replacement depends solely on the committed state. A prior
    // row cannot authorize delivery, and rollback includes this write and hot state.
    sqlx::query("INSERT INTO workflow_dispatch
        (run_key, shard_id, queue_namespace, queue_name, normal_queue_name, sticky,
         routing_mode, deployment, build_id, queue_key, deployment_key, build_key,
         logical_seq, scheduled_at, priority_key, priority_data, sticky_worker, schedule_to_start_deadline)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
        ON CONFLICT (run_key) DO UPDATE SET
         shard_id=EXCLUDED.shard_id, queue_namespace=EXCLUDED.queue_namespace,
         queue_name=EXCLUDED.queue_name, normal_queue_name=EXCLUDED.normal_queue_name,
         sticky=EXCLUDED.sticky, routing_mode=EXCLUDED.routing_mode,
         deployment=EXCLUDED.deployment, build_id=EXCLUDED.build_id,
         queue_key=EXCLUDED.queue_key, deployment_key=EXCLUDED.deployment_key,
         build_key=EXCLUDED.build_key, logical_seq=EXCLUDED.logical_seq,
         scheduled_at=EXCLUDED.scheduled_at, priority_key=EXCLUDED.priority_key,
         priority_data=EXCLUDED.priority_data, sticky_worker=EXCLUDED.sticky_worker,
         schedule_to_start_deadline=EXCLUDED.schedule_to_start_deadline")
        .bind(row.incarnation.run_key.0)
        .bind(DsqlRunRepository::shard_id_to_uuid(home))
        .bind(row.namespace_id.0).bind(&row.queue_name.0).bind(&row.normal_queue_name.0)
        .bind(row.sticky).bind(mode).bind(deployment).bind(build_id)
        .bind(queue_key).bind(deployment_key).bind(build_key)
        .bind(i64::try_from(row.incarnation.logical_seq.0)?)
        .bind(row.scheduled_at).bind(row.priority_key)
        .bind(row.priority.as_ref().map(codec::encode_priority).transpose()?)
        .bind(row.sticky_worker.as_ref().map(|worker| worker.0.as_str()))
        .bind(row.schedule_to_start_deadline).execute(&mut **tx).await?;
    Ok(())
}

pub(super) fn decode(row: PgRow) -> Result<WorkflowDispatchRow> {
    let deployment: Option<String> = row.try_get("deployment")?;
    let build_id: Option<String> = row.try_get("build_id")?;
    let routing = match (row.try_get::<i16, _>("routing_mode")?, deployment, build_id) {
        (0, None, None) => WorkflowDispatchRouting::Live,
        (1, Some(deployment), build_id) => WorkflowDispatchRouting::Exact {
            deployment: DeploymentId(deployment),
            build_id: build_id.map(BuildId),
        },
        _ => bail!("invalid workflow_dispatch routing coordinates"),
    };
    let result = WorkflowDispatchRow {
        incarnation: WorkflowTaskIncarnation {
            run_key: RunKey(row.try_get("run_key")?),
            logical_seq: LogicalTaskSeq(convert::u64_from_i64(
                row.try_get("logical_seq")?,
                "workflow_dispatch.logical_seq",
            )?),
        },
        execution_home: DsqlRunRepository::shard_id_from_uuid(row.try_get("shard_id")?)?,
        namespace_id: NamespaceId(row.try_get("queue_namespace")?),
        queue_name: TaskQueueName(row.try_get("queue_name")?),
        normal_queue_name: TaskQueueName(row.try_get("normal_queue_name")?),
        sticky: row.try_get("sticky")?,
        routing,
        scheduled_at: row.try_get("scheduled_at")?,
        priority_key: row.try_get("priority_key")?,
        priority: row
            .try_get::<Option<Vec<u8>>, _>("priority_data")?
            .as_deref()
            .map(codec::decode_priority)
            .transpose()?,
        sticky_worker: row
            .try_get::<Option<String>, _>("sticky_worker")?
            .map(WorkerIdentity),
        schedule_to_start_deadline: row.try_get("schedule_to_start_deadline")?,
    };
    result.validate()?;
    Ok(result)
}

impl DsqlRunRepository {
    pub(super) async fn do_reconcile_workflow_dispatch_run(
        &self,
        home: ShardId,
        run_key: RunKey,
    ) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.connection()?.begin().await?;
        let hot = sqlx::query_as::<_, (Uuid, Vec<u8>)>(
            "SELECT shard_id, state_data FROM workflow_hot WHERE run_key=$1",
        )
        .bind(run_key.0)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((stored_home, bytes)) = hot {
            anyhow::ensure!(
                Self::shard_id_from_uuid(stored_home)? == home,
                "workflow dispatch repair home mismatch for {run_key:?}"
            );
            let state = codec::decode_workflow_state(run_key, &bytes)?;
            anyhow::ensure!(
                tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    self.shard_count,
                ) == home,
                "workflow dispatch authoritative placement mismatch for {run_key:?}"
            );
            // Only the derived row is written. The caller excludes single-owner
            // writers while acquiring; transaction-local takeover fencing is a
            // separate prerequisite, not implied by this repeatable-read snapshot.
            maintain(&mut tx, &state, home).await?;
        } else {
            let stored_home: Option<Uuid> =
                sqlx::query_scalar("SELECT shard_id FROM workflow_dispatch WHERE run_key=$1")
                    .bind(run_key.0)
                    .fetch_optional(&mut *tx)
                    .await?;
            if let Some(stored_home) = stored_home {
                anyhow::ensure!(
                    Self::shard_id_from_uuid(stored_home)? == home,
                    "orphan dispatch repair home mismatch for {run_key:?}"
                );
            }
            sqlx::query("DELETE FROM workflow_dispatch WHERE run_key=$1")
                .bind(run_key.0)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn do_list_workflow_dispatch_page(
        &self,
        range: &WorkflowDiscoveryRange,
        after: Option<WorkflowDispatchPosition>,
        limit: std::num::NonZeroU32,
    ) -> Result<WorkflowDispatchPage> {
        let mut query = queue_page_query(range, after, limit);
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit.connection()?.begin().await?;
        let candidates = query
            .build()
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(decode)
            .collect::<Result<Vec<_>>>()?;
        tx.commit().await?;
        Ok(WorkflowDispatchPage {
            last_examined: candidates.last().map(WorkflowDispatchRow::position),
            exhausted: candidates.len() < limit.get() as usize,
            candidates,
        })
    }

    pub(super) async fn do_list_workflow_dispatch_for_home(
        &self,
        home: ShardId,
        after: Option<RunKey>,
        limit: std::num::NonZeroU32,
    ) -> Result<Vec<RunKey>> {
        let sql = home_page_sql(after.is_some());
        let mut query = sqlx::query_scalar::<_, Uuid>(sql)
            .bind(Self::shard_id_to_uuid(home))
            .bind(i64::from(limit.get()));
        if let Some(after) = after {
            query = query.bind(after.0);
        }
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit.connection()?.begin().await?;
        let keys = query.fetch_all(&mut *tx).await?;
        tx.commit().await?;
        Ok(keys.into_iter().map(RunKey).collect())
    }
}

// Shared with live EXPLAIN checks so measurement cannot silently drift from the
// actual first-page or continuation SQL executed through the storage director.
pub(super) fn queue_page_query(
    range: &WorkflowDiscoveryRange,
    after: Option<WorkflowDispatchPosition>,
    limit: std::num::NonZeroU32,
) -> sqlx::QueryBuilder<sqlx::Postgres> {
    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "WITH candidates AS MATERIALIZED (SELECT * FROM (",
    );
    // Disjoint lexicographic intervals let each child seek on scalar index
    // bounds. A tuple comparison is otherwise only a residual DSQL filter,
    // rereading every consumed row on each deeper page. Each child is bounded;
    // the outer limit returns the same one-page work unit to discovery. Select
    // only index-covered keys first: fetching payloads in each seek can make the
    // planner prefer a primary-key tail scan that filters the queue afterwards.
    // The materialized page bounds the subsequent payload lookups in this same
    // read snapshot; no additional query or reservation is needed.
    let branches = if after.is_some() { 3 } else { 1 };
    for branch in 0..branches {
        if branch != 0 {
            query.push(" UNION ALL ");
        }
        query.push("(SELECT run_key, priority_key, scheduled_at FROM workflow_dispatch");
        let [queue_key, deployment_key, build_key] = range.lookup_keys();
        let (mode, _, _) = range.routing.coordinates();
        query
            .push(" WHERE queue_namespace=")
            .push_bind(range.namespace_id.0)
            .push(" AND queue_key=")
            .push_bind(queue_key)
            .push(" AND routing_mode=")
            .push_bind(mode)
            .push(" AND deployment_key=")
            .push_bind(deployment_key)
            .push(" AND build_key=")
            .push_bind(build_key)
            .push(" AND sticky=false");
        if let Some(position) = after {
            query
                .push(" AND priority_key")
                .push(if branch == 2 { ">" } else { "=" })
                .push_bind(position.priority_key);
            if branch < 2 {
                query
                    .push(" AND scheduled_at")
                    .push(if branch == 1 { ">" } else { "=" })
                    .push_bind(position.scheduled_at);
            }
            if branch == 0 {
                query.push(" AND run_key>").push_bind(position.run_key.0);
            }
        }
        query
            .push(" ORDER BY priority_key, scheduled_at, run_key LIMIT ")
            .push_bind(i64::from(limit.get()))
            .push(")");
    }
    query
        .push(") AS candidates ORDER BY priority_key, scheduled_at, run_key LIMIT ")
        .push_bind(i64::from(limit.get()))
        .push(") SELECT intent.* FROM candidates JOIN workflow_dispatch AS intent USING (run_key) ORDER BY candidates.priority_key, candidates.scheduled_at, candidates.run_key");
    query
}

pub(super) fn home_page_sql(continued: bool) -> &'static str {
    if continued {
        "SELECT run_key FROM workflow_dispatch WHERE shard_id=$1 AND run_key > $3 ORDER BY run_key LIMIT $2"
    } else {
        "SELECT run_key FROM workflow_dispatch WHERE shard_id=$1 ORDER BY run_key LIMIT $2"
    }
}
