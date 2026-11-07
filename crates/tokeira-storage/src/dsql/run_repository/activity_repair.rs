//! Small fenced repair transactions serialize against the run's authoritative row.
//! Continuations carry an observed sequence, never decoded state from another
//! transaction. Stale pages restart from wanted slots before removing old ones.

use std::collections::BTreeMap;

use sqlx::{Row, postgres::PgRow};

use super::*;
use crate::{
    ActivityRepairCursor, ActivityRepairPage, ActivityRepairPhase,
    activity_dispatch::{activity_repair_continuation, activity_repair_fits},
};

/// Compare the observed incarnation as well as its raw identity/home. DSQL OCC
/// also validates the read snapshot at commit, protecting replacements that only
/// change routing metadata while retaining these incarnation coordinates.
async fn delete_observed(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    row: &PgRow,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM activity_dispatch WHERE key=$1 AND run_key=$2 AND activity_id=$3
        AND shard_id=$4 AND schedule_event_id=$5 AND attempt=$6 AND stamp=$7",
    )
    .bind(row.try_get::<Uuid, _>("key")?)
    .bind(row.try_get::<Uuid, _>("run_key")?)
    .bind(row.try_get::<String, _>("activity_id")?)
    .bind(row.try_get::<Uuid, _>("shard_id")?)
    .bind(row.try_get::<i64, _>("schedule_event_id")?)
    .bind(row.try_get::<i32, _>("attempt")?)
    .bind(row.try_get::<i64, _>("stamp")?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

impl DsqlRunRepository {
    pub(super) async fn do_validate_activity_start_observation(
        &self,
        observed: &WorkflowState,
        activity_id: &str,
        home: ShardId,
        fence: &WriteFence,
    ) -> Result<bool> {
        fence.ensure_home(home)?;
        crate::read_probe::logical(observed.run_key);
        let mut permit = self.director.acquire(DbClass::Read).await?;
        crate::read_probe::sql(observed.run_key, "sql/begin-activity-observation");
        let mut tx = permit.begin().await?;
        if matches!(fence, WriteFence::Lease(_)) {
            crate::read_probe::sql(observed.run_key, "sql/activity-observation-fence");
        }
        if let Some(lost) = Self::check_fence(&mut tx, fence, observed.run_key).await? {
            tx.rollback().await?;
            return Err(lost.into());
        }
        // FOR UPDATE validates this observation against resolution/control
        // writers at commit; the fence key-share similarly orders a takeover.
        // The transaction changes neither the row nor its transition sequence.
        crate::read_probe::sql(observed.run_key, "sql/activity-observation-state");
        let hot = sqlx::query_as::<_, (Uuid, Vec<u8>, Option<i32>)>(
            "SELECT shard_id, state_data, state_parts FROM workflow_hot WHERE run_key=$1 FOR UPDATE",
        )
        .bind(observed.run_key.0)
        .fetch_optional(&mut *tx)
        .await?;
        let matches = if let Some((stored_home, first, parts)) = hot {
            anyhow::ensure!(
                Self::shard_id_from_uuid(stored_home)? == home,
                "activity start observation home mismatch"
            );
            let bytes =
                super::run_value_parts::join_state(&mut tx, observed.run_key, first, parts).await?;
            let current = codec::decode_workflow_state(observed.run_key, &bytes)?;
            crate::activity_dispatch::activity_start_observation_matches(
                observed,
                &current,
                activity_id,
            )
        } else {
            false
        };
        #[cfg(all(test, feature = "dsql-integration"))]
        pause_point::reached(pause_point::PauseKey::ActivityObservation(observed.run_key)).await;
        crate::read_probe::sql(observed.run_key, "sql/commit-activity-observation");
        tx.commit().await?;
        Ok(matches)
    }

    pub(super) async fn do_reconcile_activity_dispatch_page(
        &self,
        home: ShardId,
        run: RunKey,
        after: Option<ActivityRepairCursor>,
        fence: &WriteFence,
    ) -> Result<ActivityRepairPage> {
        fence.ensure_home(home)?;
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.begin().await?;
        if let Some(lost) = Self::check_fence(&mut tx, fence, run).await? {
            tx.rollback().await?;
            return Err(lost.into());
        }
        // Use the same locking key as every ordinary transition. Parts must be
        // hydrated inside this snapshot, after the hot row has been serialized.
        let hot = sqlx::query_as::<_, (Uuid, i64, Vec<u8>, Option<i32>, Option<i16>, Option<i64>)>(
            "SELECT shard_id, transition_seq, state_data, state_parts, derived_kinds, \
             derived_kinds_seq FROM workflow_hot WHERE run_key=$1 FOR UPDATE",
        )
        .bind(run.0)
        .fetch_optional(&mut *tx)
        .await?;
        let mut record = None;
        let state = if let Some((stored_home, seq, first, parts, kinds, kinds_seq)) = hot {
            record = super::derived_record::read_record(seq, kinds, kinds_seq);
            anyhow::ensure!(
                Self::shard_id_from_uuid(stored_home)? == home,
                "activity repair stored home mismatch for {run:?}"
            );
            let bytes = super::run_value_parts::join_state(&mut tx, run, first, parts).await?;
            let state = codec::decode_workflow_state(run, &bytes)?;
            let actual = tokeira_types::execution_home_bundle(
                state.namespace_id.0.as_bytes(),
                state.workflow_id.0.as_bytes(),
                self.shard_count,
            );
            anyhow::ensure!(
                state.run_key == run && actual == home,
                "activity repair authoritative home mismatch for {run:?}"
            );
            anyhow::ensure!(
                state.transition_seq.0 == u64::try_from(seq)?,
                "activity repair state sequence mismatch for {run:?}"
            );
            Some(state)
        } else {
            None
        };
        let mut cursor =
            ActivityRepairCursor::resume(after, state.as_ref().map(|state| state.transition_seq));
        let wanted = state
            .as_ref()
            .map(|state| crate::derive_activity_dispatch(state, home))
            .transpose()?
            .unwrap_or_default();
        let limit = crate::ACTIVITY_DISPATCH_PAGE_LIMIT as usize;
        let (keys, observed) = match cursor.phase {
            ActivityRepairPhase::Wanted => {
                let keys = wanted
                    .keys()
                    .copied()
                    .filter(|key| cursor.after.is_none_or(|after| *key > after))
                    .take(limit)
                    .collect::<Vec<_>>();
                let observed = if keys.is_empty() {
                    Vec::new()
                } else {
                    let mut query = sqlx::QueryBuilder::<sqlx::Postgres>::new("SELECT ");
                    query
                        .push(super::activity_dispatch::COLUMNS)
                        .push(" FROM activity_dispatch WHERE key = ANY(")
                        .push_bind(&keys)
                        .push(")");
                    query.build().fetch_all(&mut *tx).await?
                };
                (keys, observed)
            }
            ActivityRepairPhase::Existing => {
                let observed =
                    super::activity_dispatch::existing_page(&mut tx, run, cursor.after).await?;
                let keys = observed
                    .iter()
                    .map(|row| row.try_get::<Uuid, _>("key"))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                (keys, observed)
            }
        };
        let observed = observed
            .into_iter()
            .map(|row| Ok((row.try_get::<Uuid, _>("key")?, row)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut exhausted = keys.len() < limit;
        let mut bytes = 0;
        let mut examined = 0;
        let mut changed = 0;
        let mut upserted = false;
        for key in keys {
            let physical = observed.get(&key);
            if let Some(row) = physical {
                let raw_run: Uuid = row.try_get("run_key")?;
                let raw_id: String = row.try_get("activity_id")?;
                anyhow::ensure!(
                    raw_run == run.0 && key == crate::activity_dispatch_key(run, &raw_id),
                    "activity repair slot disagrees with raw identity for {run:?}"
                );
                if let Some(desired) = wanted.get(&key) {
                    anyhow::ensure!(
                        desired.incarnation.activity_id == raw_id,
                        "activity repair slot hash collision for {run:?}"
                    );
                }
                if state.is_none() {
                    anyhow::ensure!(
                        Self::shard_id_from_uuid(row.try_get("shard_id")?)? == home,
                        "orphan activity repair home mismatch for {run:?}"
                    );
                }
            }
            let desired = wanted.get(&key);
            let unchanged = match (physical, desired) {
                (Some(physical), Some(desired)) => {
                    super::activity_dispatch::decode(physical).ok().as_ref() == Some(desired)
                }
                (None, None) => true,
                _ => false,
            };
            if !unchanged {
                let cost = desired
                    .map(crate::ActivityDispatchRow::write_cost)
                    .transpose()?
                    .unwrap_or(crate::write_budget::WriteCost::DELETE);
                if !activity_repair_fits(bytes, cost)? {
                    exhausted = false;
                    break;
                }
                bytes += cost.bytes;
                if let Some(row) = desired {
                    super::activity_dispatch::upsert(&mut tx, row).await?;
                    upserted = true;
                } else if let Some(row) = physical {
                    delete_observed(&mut tx, row).await?;
                }
                changed += 1;
            }
            cursor.after = Some(key);
            examined += 1;
        }
        // The record was read in the hot-row lock with the state, so a valid
        // one that rules these rows out came from the commit that wrote this
        // state and derived none from it. Inserting one anyway (a release whose
        // derivation differs) must not leave the record claiming the run holds
        // none (`dsql-derived-row-record` Requirement 3). Deletes leave it as is.
        if upserted
            && super::derived_record::must_forget(
                record,
                super::derived_record::DerivedKinds::ACTIVITY_DISPATCH,
            )
        {
            super::derived_record::forget(&mut tx, run).await?;
        }
        #[cfg(all(test, feature = "dsql-integration"))]
        pause_point::reached(pause_point::PauseKey::ActivityRepair(run)).await;
        tx.commit().await?;
        Ok(ActivityRepairPage {
            continuation: activity_repair_continuation(cursor, exhausted),
            examined,
            changed,
        })
    }
}
