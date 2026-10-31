//! Transaction-local outbound maintenance, finite discovery and exact retirement.
//!
//! All mutable source reads share the transaction writing the effect. Index
//! repair changes operation rows only; it cannot resurrect retired close work.

use std::collections::BTreeMap;

use sqlx::{Row, postgres::PgRow};
use tokeira_kernel::{OperationIncarnation, OperationKey, OperationKind};

use super::*;
use crate::{
    DeliveryGuard, IntentDescriptor, IntentKey, IntentKind, IntentPayload, IntentRetirement,
    IntentRow, OperationRow, OutboundPage, RecoveryError, ScanBound, dsql::value_parts,
};

const OPERATION_SELECT: &str = "SELECT source_run_key, kind, initiating_event_id, source_home,
    requested_event_id, attempt, due_at, target_namespace, target_workflow_id FROM run_operation";
const INTENT_SELECT: &str = "SELECT source_run_key, intent_id, source_home, close_seq, kind,
    created_at FROM run_intent";

pub(super) fn operation_kind(kind: i16) -> Result<OperationKind> {
    Ok(match kind {
        1 => OperationKind::ChildStart,
        2 => OperationKind::ChildRelease,
        3 => OperationKind::ExternalSignal,
        4 => OperationKind::ExternalCancel,
        5 => OperationKind::NexusStart,
        6 => OperationKind::NexusCancel,
        _ => bail!("unknown outbound operation kind {kind}"),
    })
}

pub(super) fn decode_operation(row: PgRow) -> Result<OperationRow> {
    let namespace: Option<Uuid> = row.try_get("target_namespace")?;
    let workflow: Option<String> = row.try_get("target_workflow_id")?;
    let target = match (namespace, workflow) {
        (None, None) => None,
        (Some(ns), Some(wf)) => Some((NamespaceId(ns), WorkflowId(wf))),
        _ => bail!("partial outbound target identity"),
    };
    Ok(OperationRow {
        incarnation: OperationIncarnation {
            key: OperationKey {
                source_run: RunKey(row.try_get("source_run_key")?),
                kind: operation_kind(row.try_get("kind")?)?,
                initiating_event: row.try_get("initiating_event_id")?,
            },
            requested_event: row.try_get("requested_event_id")?,
            attempt: u32::try_from(row.try_get::<i64, _>("attempt")?)?,
        },
        source_home: DsqlRunRepository::shard_id_from_uuid(row.try_get("source_home")?)?,
        due_at: row.try_get("due_at")?,
        target,
    })
}

fn decode_descriptor(row: &PgRow) -> Result<IntentDescriptor> {
    let storage_id: String = row.try_get("intent_id")?;
    let kind = match row.try_get::<i16, _>("kind")? {
        1 => IntentKind::ParentClose,
        2 => IntentKind::ChildResult,
        3 => IntentKind::Successor,
        kind => bail!("unknown outbound intent kind {kind}"),
    };
    let ordinal = storage_id
        .rsplit('-')
        .next()
        .context("intent ordinal missing")?;
    let key = IntentKey {
        source_run: RunKey(row.try_get("source_run_key")?),
        close_seq: TransitionSeq(convert::u64_from_i64(
            row.try_get("close_seq")?,
            "run_intent.close_seq",
        )?),
        kind,
        ordinal: u32::from_str_radix(ordinal, 16)?,
    };
    anyhow::ensure!(
        key.storage_id() == storage_id,
        "outbound intent key/columns disagree"
    );
    Ok(IntentDescriptor {
        key,
        source_home: DsqlRunRepository::shard_id_from_uuid(row.try_get("source_home")?)?,
        created_at: row.try_get("created_at")?,
    })
}

pub(super) async fn read_source(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    run: RunKey,
) -> Result<Option<WorkflowState>> {
    // Source transition_seq/state_data are non-key columns. FOR UPDATE makes
    // their observed version part of DSQL's commit validation; a plain snapshot
    // read could coexist with parent close and admit a stale destination effect.
    let row = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>(
        "SELECT state_data, state_parts FROM workflow_hot WHERE run_key=$1 FOR UPDATE",
    )
    .bind(run.0)
    .fetch_optional(&mut **tx)
    .await?;
    match row {
        Some((first, parts)) => {
            let bytes = run_value_parts::join_state(tx, run, first, parts).await?;
            Ok(Some(codec::decode_workflow_state(run, &bytes)?))
        }
        None => Ok(None),
    }
}

pub(super) async fn validate_guard(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    guard: DeliveryGuard,
    source_fence: &WriteFence,
    shard_count: u32,
    target: &WorkflowState,
) -> Result<()> {
    // Read both leases in the effect transaction: the source may lose ownership
    // after hydration while the destination remains owned by this node.
    let source_run = match guard {
        DeliveryGuard::Operation { incarnation, .. }
        | DeliveryGuard::ChildStart { incarnation, .. } => incarnation.key.source_run,
        DeliveryGuard::Intent(key) | DeliveryGuard::CurrentIntent { key, .. } => key.source_run,
    };
    if let Some(lost) = super::DsqlRunRepository::check_fence(tx, source_fence, source_run).await? {
        return Err(lost.into());
    }
    if let DeliveryGuard::ChildStart {
        expected_current, ..
    }
    | DeliveryGuard::CurrentIntent {
        expected_current, ..
    } = guard
    {
        let key = super::DsqlRunRepository::current_execution_key(
            target.namespace_id,
            &target.workflow_id,
        );
        let current: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT run_key FROM current_execution WHERE key=$1 FOR UPDATE")
                .bind(key)
                .fetch_optional(&mut **tx)
                .await?;
        if current.map(RunKey) != expected_current {
            return Err(RecoveryError::Obsolete.into());
        }
    }
    match guard {
        DeliveryGuard::Operation {
            incarnation,
            source_seq,
        }
        | DeliveryGuard::ChildStart {
            incarnation,
            source_seq,
            ..
        } => {
            let source = read_source(tx, incarnation.key.source_run)
                .await?
                .ok_or(RecoveryError::Obsolete)?;
            let home = tokeira_types::execution_home_bundle(
                source.namespace_id.0.as_bytes(),
                source.workflow_id.0.as_bytes(),
                shard_count,
            );
            source_fence.ensure_home(home)?;
            if source.transition_seq != source_seq
                || !crate::derive_operations(&source, home)?
                    .iter()
                    .any(|row| row.incarnation == incarnation)
            {
                return Err(RecoveryError::Obsolete.into());
            }
        }
        DeliveryGuard::Intent(key) | DeliveryGuard::CurrentIntent { key, .. } => {
            let home: Option<uuid::Uuid> = sqlx::query_scalar(
                "SELECT source_home FROM run_intent WHERE source_run_key=$1 AND intent_id=$2 FOR KEY SHARE",
            )
            .bind(key.source_run.0)
            .bind(key.storage_id())
            .fetch_optional(&mut **tx)
            .await?;
            source_fence.ensure_home(super::DsqlRunRepository::shard_id_from_uuid(
                home.ok_or(RecoveryError::Obsolete)?,
            )?)?;
        }
    }
    Ok(())
}

pub(super) async fn maintain_parent_reference(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &WorkflowState,
    keep: bool,
) -> Result<()> {
    let parent = state
        .parent_workflow_id
        .as_ref()
        .filter(|_| keep && state.parent_run_key.is_some())
        .map(|workflow| {
            (
                state.parent_namespace_id.unwrap_or(state.namespace_id).0,
                workflow.0.clone(),
            )
        });
    let prior_parent = sqlx::query_as::<_, (Uuid, String)>("SELECT parent_namespace_id,parent_workflow_id FROM outbound_parent_reference WHERE child_run_key=$1")
        .bind(state.run_key.0).fetch_optional(&mut **tx).await?;
    if prior_parent != parent {
        if let Some((namespace, workflow)) = parent {
            sqlx::query("INSERT INTO outbound_parent_reference (child_run_key,parent_namespace_id,parent_workflow_id) VALUES ($1,$2,$3) ON CONFLICT (child_run_key) DO UPDATE SET parent_namespace_id=EXCLUDED.parent_namespace_id,parent_workflow_id=EXCLUDED.parent_workflow_id")
                .bind(state.run_key.0).bind(namespace).bind(workflow).execute(&mut **tx).await?;
        } else {
            sqlx::query("DELETE FROM outbound_parent_reference WHERE child_run_key=$1")
                .bind(state.run_key.0)
                .execute(&mut **tx)
                .await?;
        }
    }
    Ok(())
}

pub(super) async fn maintain_operations(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &WorkflowState,
    home: ShardId,
) -> Result<()> {
    maintain_parent_reference(tx, state, state.is_open()).await?;
    let rows = sqlx::query("SELECT source_run_key, kind, initiating_event_id, source_home, requested_event_id, attempt, due_at, target_namespace, target_workflow_id FROM run_operation WHERE source_run_key=$1")
        .bind(state.run_key.0).fetch_all(&mut **tx).await?;
    let mut prior: BTreeMap<_, _> = rows
        .into_iter()
        .map(decode_operation)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|row| (row.incarnation.key, row))
        .collect();
    for row in crate::derive_operations(state, home)? {
        let key = row.incarnation.key;
        // Keep unchanged entries untouched: a hot run with many pending external
        // operations must not consume a row mutation per slot on every WFT.
        if prior
            .remove(&key)
            .as_ref()
            .is_some_and(|prior| same_index_entry(prior, &row))
        {
            continue;
        }
        write_operation_row(tx, &row).await?;
    }
    for key in prior.into_keys() {
        sqlx::query("DELETE FROM run_operation WHERE source_run_key=$1 AND kind=$2 AND initiating_event_id=$3")
            .bind(key.source_run.0).bind(key.kind as i16).bind(key.initiating_event).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn write_operation_row(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &OperationRow,
) -> Result<()> {
    let key = row.incarnation.key;
    sqlx::query("INSERT INTO run_operation (source_run_key,kind,initiating_event_id,source_home,requested_event_id,attempt,due_at,target_namespace,target_workflow_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (source_run_key,kind,initiating_event_id) DO UPDATE SET
            source_home=EXCLUDED.source_home,requested_event_id=EXCLUDED.requested_event_id,attempt=EXCLUDED.attempt,due_at=EXCLUDED.due_at,
            target_namespace=EXCLUDED.target_namespace,target_workflow_id=EXCLUDED.target_workflow_id")
            .bind(key.source_run.0).bind(key.kind as i16).bind(key.initiating_event)
            .bind(DsqlRunRepository::shard_id_to_uuid(row.source_home)).bind(row.incarnation.requested_event).bind(i64::from(row.incarnation.attempt))
            .bind(row.due_at).bind(row.target.as_ref().map(|(ns, _)| ns.0)).bind(row.target.as_ref().map(|(_, wf)| wf.0.as_str()))
            .execute(&mut **tx).await?;
    Ok(())
}

pub(super) fn same_index_entry(left: &OperationRow, right: &OperationRow) -> bool {
    // PostgreSQL encodes timestamps as whole microseconds since 2000. State
    // retains nanoseconds; direct equality would rewrite every unchanged due
    // entry on each WFT and could exhaust DSQL's row budget. Delivery still
    // evaluates the full-precision deadline in authoritative run state.
    let micros = |at: OffsetDateTime| (at.unix_timestamp_nanos() - 946_684_800_000_000_000) / 1_000;
    left.incarnation == right.incarnation
        && left.source_home == right.source_home
        && left.target == right.target
        && micros(left.due_at) == micros(right.due_at)
}

pub(super) async fn insert_intent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &IntentRow,
) -> Result<()> {
    let bytes = postcard::to_allocvec(&(1_u32, &row.payload))?;
    let parts = value_parts::split(&bytes);
    // No upsert: a conflicting immutable key means the caller failed its source
    // sequence check. Replacing its payload would hide a correctness violation.
    sqlx::query("INSERT INTO run_intent (source_run_key,intent_id,source_home,close_seq,kind,created_at,payload,payload_parts,target_namespace,target_workflow_id)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)")
        .bind(row.key.source_run.0).bind(row.key.storage_id()).bind(DsqlRunRepository::shard_id_to_uuid(row.source_home))
        .bind(convert::i64_from_u64(row.key.close_seq.0, "intent close sequence")?).bind(row.key.kind as i16)
        .bind(row.created_at).bind(parts.first).bind(parts.parts())
        .bind(row.target.as_ref().map(|(ns,_)| ns.0)).bind(row.target.as_ref().map(|(_,wf)| wf.0.as_str()))
        .execute(&mut **tx).await?;
    for (part_no, data) in parts.numbered() {
        sqlx::query("INSERT INTO run_intent_part (source_run_key,intent_id,part_no,data) VALUES ($1,$2,$3,$4)")
            .bind(row.key.source_run.0).bind(row.key.storage_id()).bind(part_no).bind(data).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn maintain(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    transition: &Transition,
    home: ShardId,
) -> Result<()> {
    let state = &transition.next_state;
    if !state.is_open() && !transition.history_events.is_empty() {
        let prior = read_source(tx, state.run_key).await?;
        for row in crate::derive_close_intents(prior.as_ref(), transition, home)? {
            insert_intent(tx, &row).await?;
        }
    }
    maintain_operations(tx, state, home).await
}

impl DsqlRunRepository {
    pub(super) async fn do_read_operation_page(
        &self,
        home: ShardId,
        kind: OperationKind,
        bound: ScanBound<OperationKey>,
        after: Option<OperationKey>,
        limit: usize,
    ) -> Result<OutboundPage<OperationKey, OperationRow>> {
        crate::outbound::validate_page_limit(limit)?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit
            .connection()?
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let through = match bound {
            ScanBound::Through(key) => Some(key),
            ScanBound::Capture => sqlx::query_as::<_, (Uuid, i64)>("SELECT source_run_key,initiating_event_id FROM run_operation WHERE source_home=$1 AND kind=$2 AND EXISTS(SELECT 1 FROM workflow_hot WHERE run_key=run_operation.source_run_key) ORDER BY source_run_key DESC,initiating_event_id DESC LIMIT 1")
                .bind(Self::shard_id_to_uuid(home)).bind(kind as i16).fetch_optional(&mut *tx).await?
                .map(|(source, initiating_event)| OperationKey { source_run: RunKey(source), kind, initiating_event }),
        };
        let Some(end) = through else {
            tx.commit().await?;
            return Ok(OutboundPage {
                through,
                rows: Vec::new(),
            });
        };
        let mut query = sqlx::QueryBuilder::new(OPERATION_SELECT);
        query
            .push(" WHERE source_home=")
            .push_bind(Self::shard_id_to_uuid(home))
            .push(" AND kind=")
            .push_bind(kind as i16)
            .push(" AND EXISTS(SELECT 1 FROM workflow_hot WHERE run_key=run_operation.source_run_key)");
        if let Some(after) = after {
            query
                .push(" AND (source_run_key,initiating_event_id)>(")
                .push_bind(after.source_run.0)
                .push(",")
                .push_bind(after.initiating_event)
                .push(")");
        }
        query
            .push(" AND (source_run_key,initiating_event_id)<=(")
            .push_bind(end.source_run.0)
            .push(",")
            .push_bind(end.initiating_event)
            .push(") ORDER BY source_run_key,initiating_event_id LIMIT ")
            .push_bind(i64::try_from(limit)?);
        let rows = query
            .build()
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(decode_operation)
            .collect::<Result<_>>()?;
        tx.commit().await?;
        Ok(OutboundPage { through, rows })
    }

    pub(super) async fn do_read_intent_page(
        &self,
        home: ShardId,
        bound: ScanBound<IntentKey>,
        after: Option<IntentKey>,
        limit: usize,
    ) -> Result<OutboundPage<IntentKey, IntentDescriptor>> {
        crate::outbound::validate_page_limit(limit)?;
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit
            .connection()?
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let through = match bound {
            ScanBound::Through(key) => Some(key),
            ScanBound::Capture => sqlx::query("SELECT source_run_key,intent_id,source_home,close_seq,kind,created_at FROM run_intent WHERE source_home=$1 ORDER BY source_run_key DESC,intent_id DESC LIMIT 1")
                .bind(Self::shard_id_to_uuid(home)).fetch_optional(&mut *tx).await?.as_ref().map(decode_descriptor).transpose()?.map(|row| row.key),
        };
        let Some(end) = through else {
            tx.commit().await?;
            return Ok(OutboundPage {
                through,
                rows: Vec::new(),
            });
        };
        let mut query = sqlx::QueryBuilder::new(INTENT_SELECT);
        query
            .push(" WHERE source_home=")
            .push_bind(Self::shard_id_to_uuid(home));
        if let Some(after) = after {
            query
                .push(" AND (source_run_key,intent_id)>(")
                .push_bind(after.source_run.0)
                .push(",")
                .push_bind(after.storage_id())
                .push(")");
        }
        query
            .push(" AND (source_run_key,intent_id)<=(")
            .push_bind(end.source_run.0)
            .push(",")
            .push_bind(end.storage_id())
            .push(") ORDER BY source_run_key,intent_id LIMIT ")
            .push_bind(i64::try_from(limit)?);
        let rows = query
            .build()
            .fetch_all(&mut *tx)
            .await?
            .iter()
            .map(decode_descriptor)
            .collect::<Result<_>>()?;
        tx.commit().await?;
        Ok(OutboundPage { through, rows })
    }

    pub(super) async fn do_load_intent(&self, key: IntentKey) -> Result<Option<IntentRow>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit
            .connection()?
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let row = load_intent(&mut tx, key).await?;
        tx.commit().await?;
        Ok(row)
    }

    pub(super) async fn do_reconcile_outbound_run(
        &self,
        home: ShardId,
        run: RunKey,
        fence: WriteFence,
    ) -> Result<()> {
        fence.ensure_home(home)?;
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.connection()?.begin().await?;
        if let Some(lost) = Self::check_fence(&mut tx, &fence, run).await? {
            return Err(lost.into());
        }
        if let Some(state) = read_source(&mut tx, run).await? {
            anyhow::ensure!(
                tokeira_types::execution_home_bundle(
                    state.namespace_id.0.as_bytes(),
                    state.workflow_id.0.as_bytes(),
                    self.shard_count
                ) == home,
                "outbound repair home mismatch"
            );
            maintain_operations(&mut tx, &state, home).await?;
        } else {
            sqlx::query("DELETE FROM run_operation WHERE source_run_key=$1 AND source_home=$2")
                .bind(run.0)
                .bind(Self::shard_id_to_uuid(home))
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn do_retire_intent(
        &self,
        proof: IntentRetirement,
        fence: WriteFence,
    ) -> Result<bool> {
        if proof.key.kind == IntentKind::Successor {
            return Err(RecoveryError::InvalidPreparation(
                "successor intent retires only with its publication",
            )
            .into());
        }
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.connection()?.begin().await?;
        if let Some(lost) = Self::check_fence(&mut tx, &fence, proof.key.source_run).await? {
            return Err(lost.into());
        }
        let home: Option<Uuid> = sqlx::query_scalar(
            "SELECT source_home FROM run_intent WHERE source_run_key=$1 AND intent_id=$2",
        )
        .bind(proof.key.source_run.0)
        .bind(proof.key.storage_id())
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(home) = home {
            fence.ensure_home(Self::shard_id_from_uuid(home)?)?;
        }
        // Source sequence is intentionally NOT changed. Intent retirement is an
        // exact fenced maintenance write; later run repair cannot recreate it.
        sqlx::query("DELETE FROM run_intent_part WHERE source_run_key=$1 AND intent_id=$2")
            .bind(proof.key.source_run.0)
            .bind(proof.key.storage_id())
            .execute(&mut *tx)
            .await?;
        let deleted =
            sqlx::query("DELETE FROM run_intent WHERE source_run_key=$1 AND intent_id=$2")
                .bind(proof.key.source_run.0)
                .bind(proof.key.storage_id())
                .execute(&mut *tx)
                .await?
                .rows_affected()
                != 0;
        tx.commit().await?;
        Ok(deleted)
    }
}

pub(super) async fn load_intent(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: IntentKey,
) -> Result<Option<IntentRow>> {
    let row = sqlx::query("SELECT source_run_key,intent_id,source_home,close_seq,kind,created_at,payload,payload_parts,target_namespace,target_workflow_id FROM run_intent WHERE source_run_key=$1 AND intent_id=$2")
            .bind(key.source_run.0).bind(key.storage_id()).fetch_optional(&mut **tx).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let descriptor = decode_descriptor(&row)?;
    let count: i32 = row.try_get("payload_parts")?;
    anyhow::ensure!(count >= 1, "invalid intent part count");
    let parts = sqlx::query_as::<_, (i32, Vec<u8>)>("SELECT part_no,data FROM run_intent_part WHERE source_run_key=$1 AND intent_id=$2 ORDER BY part_no")
            .bind(key.source_run.0).bind(key.storage_id()).fetch_all(&mut **tx).await?;
    let bytes = value_parts::join(
        || format!("run_intent {key:?}"),
        row.try_get("payload")?,
        (count > 1).then_some(count),
        parts,
    )?;
    let ((version, payload), trailing) = postcard::take_from_bytes::<(u32, IntentPayload)>(&bytes)?;
    anyhow::ensure!(
        version == 1 && trailing.is_empty(),
        "unsupported or malformed outbound intent payload"
    );
    let expected_kind = match &payload {
        IntentPayload::ParentClose { .. } => IntentKind::ParentClose,
        IntentPayload::ChildResult { .. } => IntentKind::ChildResult,
        IntentPayload::Successor { .. } => IntentKind::Successor,
    };
    anyhow::ensure!(
        key == descriptor.key && key.kind == expected_kind,
        "outbound intent payload/key mismatch"
    );
    Ok(Some(IntentRow {
        key,
        source_home: descriptor.source_home,
        created_at: descriptor.created_at,
        target: match (
            row.try_get::<Option<Uuid>, _>("target_namespace")?,
            row.try_get::<Option<String>, _>("target_workflow_id")?,
        ) {
            (Some(ns), Some(wf)) => Some((NamespaceId(ns), WorkflowId(wf))),
            (None, None) => None,
            _ => bail!("partial intent target identity"),
        },
        payload,
    }))
}
