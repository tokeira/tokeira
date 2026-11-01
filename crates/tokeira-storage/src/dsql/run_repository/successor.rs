//! Exceptional successor reservation and settlement in the pointer transaction.
//! An absent-row read alone is not a lock: every claimant also writes the current
//! pointer, so a start racing the close conflicts with that close under DSQL OCC.

use super::*;
use crate::{
    IntentKey, IntentKind, RecoveryError, RunPreparation,
    outbound::successor::{self, SuccessorReservation},
};

async fn reservation(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    key: Uuid,
) -> Result<Option<SuccessorReservation>> {
    let row = sqlx::query_as::<_, (Uuid, i64, Uuid, String)>(
        "SELECT predecessor_run_key,predecessor_close_seq,successor_run_key,intent_id FROM successor_reservation WHERE key=$1 FOR UPDATE",
    ).bind(key).fetch_optional(&mut **tx).await?;
    row.map(|(source, seq, target, id)| {
        let intent = IntentKey {
            source_run: RunKey(source),
            close_seq: TransitionSeq(convert::u64_from_i64(seq, "predecessor close sequence")?),
            kind: IntentKind::Successor,
            ordinal: 0,
        };
        anyhow::ensure!(
            intent.storage_id() == id,
            "invalid successor reservation identity"
        );
        Ok(SuccessorReservation {
            intent,
            successor: RunKey(target),
        })
    })
    .transpose()
}

pub(super) async fn guard_pointer(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    state: &WorkflowState,
) -> Result<()> {
    let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
    if let Some(row) = reservation(tx, key).await? {
        return Err(RecoveryError::SuccessorReserved {
            successor: row.successor,
        }
        .into());
    }
    Ok(())
}

impl DsqlRunRepository {
    pub(super) async fn do_commit_successor_fallback(
        &self,
        run: RunKey,
        closing: Transition,
        start: Box<tokeira_kernel::StartRequest>,
        fence: WriteFence,
    ) -> Result<CommitResult> {
        let mut state = closing.next_state.clone();
        let home = tokeira_types::execution_home_bundle(
            state.namespace_id.0.as_bytes(),
            state.workflow_id.0.as_bytes(),
            self.shard_count,
        );
        fence.ensure_home(home)?;
        let (reserved, intent) = successor::freeze(run, &closing, start, home)?;
        let key = Self::current_execution_key(state.namespace_id, &state.workflow_id);
        let result: Result<CommitResult> = async {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.begin().await?;
            if let Some(lost) = Self::check_fence(&mut tx, &fence, run).await? {
                return Err(lost.into());
            }
            if let Some(existing) = reservation(&mut tx, key).await? {
                if existing == reserved && super::outbound::load_intent(&mut tx, intent.key).await?
                    .is_some_and(|row| row.payload == intent.payload && row.source_home == intent.source_home) {
                    tx.commit().await?;
                    return Ok(CommitResult::Duplicate);
                }
                return Err(RecoveryError::SuccessorReserved { successor: existing.successor }.into());
            }
            let current: Option<Uuid> = sqlx::query_scalar("SELECT run_key FROM current_execution WHERE key=$1 FOR UPDATE")
                .bind(key).fetch_optional(&mut *tx).await?;
            let prior = sqlx::query_as::<_, (i64, Option<i64>, Option<i32>)>("SELECT transition_seq,history_size_bytes,state_parts FROM workflow_hot WHERE run_key=$1 FOR UPDATE")
                .bind(run.0).fetch_optional(&mut *tx).await?;
            let Some((seq, size, parts)) = prior else {
                return Ok(CommitResult::Conflict { reason: "successor predecessor is absent".into() });
            };
            if current != Some(run.0) || convert::u64_from_i64(seq, "transition_seq")? != closing.expected_seq.0 {
                return Ok(CommitResult::Conflict { reason: "successor predecessor or current pointer changed".into() });
            }
            super::commit::write_transition(&mut tx, run, home, self.projection_partition_count, &closing, &mut state, size.unwrap_or(0), parts).await?;
            super::outbound::insert_intent(&mut tx, &intent).await?;
            super::outbound::maintain_parent_reference(&mut tx, &state, true).await?;
            sqlx::query("INSERT INTO successor_reservation (key,namespace_id,workflow_id,source_home,predecessor_run_key,predecessor_close_seq,successor_run_key,intent_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
                .bind(key).bind(state.namespace_id.0).bind(&state.workflow_id.0).bind(Self::shard_id_to_uuid(home))
                .bind(run.0).bind(convert::i64_from_u64(intent.key.close_seq.0, "close sequence")?).bind(reserved.successor.0).bind(intent.key.storage_id())
                .execute(&mut *tx).await?;
            // The smaller transaction may also exceed the database budget. In
            // that case its error rolls back the close, recipe and reservation.
            tx.commit().await?;
            Ok(CommitResult::Applied { new_state: state })
        }.await;
        match result {
            Err(error)
                if error.chain().any(|cause| {
                    cause
                        .downcast_ref::<sqlx::Error>()
                        .is_some_and(Self::is_serialization_failure)
                }) =>
            {
                Ok(CommitResult::Conflict {
                    reason: "DSQL successor fallback serialization conflict".into(),
                })
            }
            result => result,
        }
    }
}

/// Called only inside the final publication transaction, before its pointer
/// write. Removing both rows here grants no other writer a visible gap: commit
/// publishes the new current run together with their retirement.
pub(super) async fn settle_publication(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    publication: &RunPreparation,
) -> Result<()> {
    let state = &publication.new_run.next_state;
    let Some(intent_key) = publication.successor_intent else {
        return guard_pointer(tx, state).await;
    };
    let key = DsqlRunRepository::current_execution_key(state.namespace_id, &state.workflow_id);
    let reservation = reservation(tx, key)
        .await?
        .ok_or(RecoveryError::InvalidPreparation(
            "successor reservation is absent",
        ))?;
    let intent = super::outbound::load_intent(tx, intent_key).await?.ok_or(
        RecoveryError::InvalidPreparation("successor intent is absent"),
    )?;
    successor::validate_publication(&reservation, &intent, intent_key, state)?;
    // The successor's parent reference is installed by the same publication
    // transaction. Its predecessor no longer needs the temporary gap pin.
    sqlx::query("DELETE FROM outbound_parent_reference WHERE child_run_key=$1")
        .bind(intent_key.source_run.0)
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM run_intent_part WHERE source_run_key=$1 AND intent_id=$2")
        .bind(intent_key.source_run.0)
        .bind(intent_key.storage_id())
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM run_intent WHERE source_run_key=$1 AND intent_id=$2")
        .bind(intent_key.source_run.0)
        .bind(intent_key.storage_id())
        .execute(&mut **tx)
        .await?;
    sqlx::query("DELETE FROM successor_reservation WHERE key=$1")
        .bind(key)
        .execute(&mut **tx)
        .await?;
    Ok(())
}
