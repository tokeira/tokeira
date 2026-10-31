//! Stopped-cluster outbound preparation in short, revision-fenced transactions.
//!
//! A metadata page writes no public history and creates no historical close
//! intents. Source and marker reads participate in OCC; a lost reply resumes the
//! committed cursor. Old unfenced binaries still require operator shutdown.

use super::*;
use crate::{
    dsql::value_parts,
    outbound_upgrade::{
        OUTBOUND_UPGRADE, OutboundUpgradeProgress, OutboundUpgradeStatus, OutboundUpgradeWrite,
        validate_page, validate_write,
    },
};
use anyhow::ensure;
use std::sync::atomic::Ordering;

async fn marker(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    lock: bool,
) -> Result<Option<OutboundUpgradeProgress>> {
    let sql = if lock {
        "SELECT algorithm_version,revision,phase,progress_data FROM outbound_recovery_upgrade WHERE name=$1 FOR UPDATE"
    } else {
        "SELECT algorithm_version,revision,phase,progress_data FROM outbound_recovery_upgrade WHERE name=$1"
    };
    let row = sqlx::query_as::<_, (i32, i64, i16, Vec<u8>)>(sql)
        .bind(OUTBOUND_UPGRADE)
        .fetch_optional(&mut **tx)
        .await?;
    row.map(|(algorithm, revision, phase, bytes)| {
        let (progress, trailing) = postcard::take_from_bytes::<OutboundUpgradeProgress>(&bytes)?;
        ensure!(
            algorithm == 1
                && trailing.is_empty()
                && progress.revision == u64::try_from(revision)?
                && phase == i16::from(progress.complete),
            "unsupported or corrupt outbound preparation marker"
        );
        Ok(progress)
    })
    .transpose()
}

async fn leases(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<u64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM shard_lease WHERE owner IS NOT NULL AND lease_expiry > now()",
    )
    .fetch_one(&mut **tx)
    .await?;
    Ok(u64::try_from(count)?)
}

async fn writer_claims(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> Result<u64> {
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tokeira_control_lease WHERE claim_name IN ('tokeirad-owner','embedded-owner') AND owner_id IS NOT NULL AND expires_at > now()")
        .fetch_one(&mut **tx).await?;
    Ok(u64::try_from(count)?)
}

async fn next_key(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    after: Option<RunKey>,
) -> Result<Option<RunKey>> {
    let key: Option<Uuid> = if let Some(after) = after {
        sqlx::query_scalar(
            "SELECT run_key FROM workflow_hot WHERE run_key > $1 ORDER BY run_key LIMIT 1",
        )
        .bind(after.0)
        .fetch_optional(&mut **tx)
        .await?
    } else {
        sqlx::query_scalar("SELECT run_key FROM workflow_hot ORDER BY run_key LIMIT 1")
            .fetch_optional(&mut **tx)
            .await?
    };
    Ok(key.map(RunKey))
}

async fn write_marker(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    progress: &OutboundUpgradeProgress,
    exists: bool,
) -> Result<()> {
    let bytes = postcard::to_allocvec(progress)?;
    let sql = if exists {
        "UPDATE outbound_recovery_upgrade SET algorithm_version=1,revision=$2,phase=$3,progress_data=$4 WHERE name=$1"
    } else {
        "INSERT INTO outbound_recovery_upgrade(name,algorithm_version,revision,phase,progress_data) VALUES ($1,1,$2,$3,$4)"
    };
    sqlx::query(sql)
        .bind(OUTBOUND_UPGRADE)
        .bind(i64::try_from(progress.revision)?)
        .bind(i16::from(progress.complete))
        .bind(bytes)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn validate_stage(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    expected: &Option<OutboundUpgradeProgress>,
    write: &OutboundUpgradeWrite,
) -> Result<()> {
    ensure!(
        marker(tx, true).await? == *expected,
        "maintenance revision changed during staging"
    );
    ensure!(
        expected.as_ref().is_some_and(|progress| !progress.complete),
        "staging requires an incomplete preparation"
    );
    ensure!(
        leases(tx).await? + writer_claims(tx).await? == 0,
        "live writers prevent preparation staging"
    );
    ensure!(
        next_key(tx, expected.as_ref().and_then(|progress| progress.after)).await?
            == Some(write.state.run_key),
        "staging source is not the next reviewed key"
    );
    let source = super::outbound::read_source(tx, write.state.run_key)
        .await?
        .context("staged source disappeared")?;
    validate_write(&source, write)
}

impl DsqlRunRepository {
    /// Source data and operation fanout are staged under the same maintenance
    /// revision. Only final publication advances the cursor. A cancelled page
    /// may leave immutable parts or provisional indexes, which a resume verifies
    /// or replaces before the startup gate can open.
    async fn stage_outbound_upgrade(
        &self,
        expected: &Option<OutboundUpgradeProgress>,
        write: &OutboundUpgradeWrite,
    ) -> Result<Option<Vec<u8>>> {
        let key = write.state.run_key;
        let changed =
            crate::outbound_upgrade::outbound_source_digest(&write.state)? != write.expected_digest;
        let pointer = if changed {
            let bytes = codec::encode_workflow_state(&write.state)?;
            let pointer = super::outbound_upgrade_parts::StatePointer::new(&bytes)?;
            loop {
                let mut permit = self.director.acquire(DbClass::Maintenance).await?;
                let mut tx = permit.connection()?.begin().await?;
                validate_stage(&mut tx, expected, write).await?;
                let obsolete: Vec<(Uuid, i32)> = sqlx::query_as("SELECT generation,part_no FROM outbound_upgrade_state_part WHERE run_key=$1 AND generation <> $2 ORDER BY generation,part_no LIMIT 900")
                    .bind(key.0).bind(pointer.generation).fetch_all(&mut *tx).await?;
                let Some((generation, part)) = obsolete.last() else {
                    tx.rollback().await?;
                    break;
                };
                sqlx::query("DELETE FROM outbound_upgrade_state_part WHERE run_key=$1 AND generation <> $2 AND (generation,part_no) <= ($3,$4)")
                    .bind(key.0).bind(pointer.generation).bind(generation).bind(part).execute(&mut *tx).await?;
                tx.commit().await?;
            }
            for (page, chunk) in bytes.chunks(4 * value_parts::PART_BYTES).enumerate() {
                let mut permit = self.director.acquire(DbClass::Maintenance).await?;
                let mut tx = permit.connection()?.begin().await?;
                validate_stage(&mut tx, expected, write).await?;
                for (index, part) in chunk.chunks(value_parts::PART_BYTES).enumerate() {
                    let number = i32::try_from(page * 4 + index)?;
                    sqlx::query("INSERT INTO outbound_upgrade_state_part(run_key,generation,part_no,data) VALUES ($1,$2,$3,$4) ON CONFLICT (run_key,generation,part_no) DO NOTHING")
                        .bind(key.0).bind(pointer.generation).bind(number).bind(part).execute(&mut *tx).await?;
                    let stored: Vec<u8> = sqlx::query_scalar("SELECT data FROM outbound_upgrade_state_part WHERE run_key=$1 AND generation=$2 AND part_no=$3")
                        .bind(key.0).bind(pointer.generation).bind(number).fetch_one(&mut *tx).await?;
                    ensure!(
                        stored == part,
                        "immutable preparation part differs from reviewed state"
                    );
                }
                tx.commit().await?;
            }
            Some(pointer.encode()?)
        } else {
            None
        };
        // Provisional indexes have no authority while the startup gate is
        // closed. Deleting/rebuilding in pages also bounds a legacy run with
        // thousands of pending operations; no page guesses a close obligation.
        loop {
            let mut permit = self.director.acquire(DbClass::Maintenance).await?;
            let mut tx = permit.connection()?.begin().await?;
            validate_stage(&mut tx, expected, write).await?;
            let keys: Vec<(i16, i64)> = sqlx::query_as("SELECT kind,initiating_event_id FROM run_operation WHERE source_run_key=$1 ORDER BY kind,initiating_event_id LIMIT 900")
                .bind(key.0).fetch_all(&mut *tx).await?;
            let Some((kind, initiation)) = keys.last() else {
                tx.rollback().await?;
                break;
            };
            sqlx::query("DELETE FROM run_operation WHERE source_run_key=$1 AND (kind,initiating_event_id) <= ($2,$3)")
                .bind(key.0).bind(kind).bind(initiation).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        let home = tokeira_types::execution_home_bundle(
            write.state.namespace_id.0.as_bytes(),
            write.state.workflow_id.0.as_bytes(),
            self.shard_count,
        );
        let operations = crate::derive_operations(&write.state, home)?;
        for page in operations.chunks(100) {
            let mut permit = self.director.acquire(DbClass::Maintenance).await?;
            let mut tx = permit.connection()?.begin().await?;
            validate_stage(&mut tx, expected, write).await?;
            ensure!(
                postcard::to_allocvec(page)?.len() + 8192
                    <= crate::write_budget::MAX_BYTES_PER_TRANSACTION,
                "operation metadata exceeds page budget"
            );
            for row in page {
                super::outbound::write_operation_row(&mut tx, row).await?;
            }
            tx.commit().await?;
        }
        Ok(pointer)
    }

    pub(super) async fn do_outbound_upgrade_status(&self) -> Result<OutboundUpgradeStatus> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit
            .connection()?
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await?;
        let (pending_intents, oldest_intent_at): (i64, Option<OffsetDateTime>) =
            sqlx::query_as("SELECT count(*),min(created_at) FROM run_intent")
                .fetch_one(&mut *tx)
                .await?;
        let status = OutboundUpgradeStatus {
            progress: marker(&mut tx, false).await?,
            pending_intents: u64::try_from(pending_intents)?,
            oldest_intent_at,
            live_leases: leases(&mut tx).await? + writer_claims(&mut tx).await?,
        };
        tx.commit().await?;
        Ok(status)
    }

    pub(super) async fn do_read_outbound_upgrade_run(
        &self,
        after: Option<RunKey>,
    ) -> Result<Option<WorkflowState>> {
        let mut permit = self.director.acquire(DbClass::Read).await?;
        let mut tx = permit.connection()?.begin().await?;
        let state = match next_key(&mut tx, after).await? {
            Some(key) => super::outbound::read_source(&mut tx, key).await?,
            None => None,
        };
        tx.rollback().await?;
        Ok(state)
    }

    pub(super) async fn do_ensure_outbound_prepared(&self) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.connection()?.begin().await?;
        let existing = marker(&mut tx, true).await?;
        if let Some(progress) = existing {
            ensure!(
                progress.complete,
                "outbound recovery preparation is incomplete; resume tokeirad storage outbound-recovery apply before startup"
            );
        } else {
            ensure!(
                next_key(&mut tx, None).await?.is_none(),
                "legacy database requires tokeirad storage outbound-recovery plan/apply with every writer stopped"
            );
            ensure!(
                leases(&mut tx).await? == 0,
                "empty database still has live leases; stop writers before preparation"
            );
            let progress = OutboundUpgradeProgress {
                revision: 1,
                complete: true,
                plan_digest: crate::schema_contract::sha256_hex(b"empty-outbound-recovery-v1"),
                repairs_digest: crate::schema_contract::sha256_hex(b"no-repairs"),
                ..Default::default()
            };
            write_marker(&mut tx, &progress, false).await?;
        }
        tx.commit().await?;
        self.outbound_ready.store(true, Ordering::Release);
        Ok(())
    }

    pub(super) async fn do_commit_outbound_upgrade_page(
        &self,
        expected: Option<OutboundUpgradeProgress>,
        next: OutboundUpgradeProgress,
        write: Option<OutboundUpgradeWrite>,
    ) -> Result<()> {
        let pointer = if let Some(write) = &write {
            self.stage_outbound_upgrade(&expected, write).await?
        } else {
            None
        };
        let mut permit = self.director.acquire(DbClass::Maintenance).await?;
        let mut tx = permit.connection()?.begin().await?;
        ensure!(
            marker(&mut tx, true).await? == expected,
            "maintenance revision changed; review or resume the owning plan"
        );
        ensure!(
            leases(&mut tx).await? + writer_claims(&mut tx).await? == 0,
            "live leases prevent stopped-cluster preparation"
        );
        let first = next_key(
            &mut tx,
            expected.as_ref().and_then(|progress| progress.after),
        )
        .await?;
        validate_page(expected.as_ref(), &next, first, write.as_ref())?;
        if let Some(write) = &write {
            let key = write.state.run_key;
            let prior = super::outbound::read_source(&mut tx, key)
                .await?
                .context("reviewed source disappeared")?;
            validate_write(&prior, write)?;
            let home = tokeira_types::execution_home_bundle(
                write.state.namespace_id.0.as_bytes(),
                write.state.workflow_id.0.as_bytes(),
                self.shard_count,
            );
            // Another applier may have been staging the same revision. Verify
            // every index row in this transaction: FOR UPDATE makes a concurrent
            // clear/rebuild conflict with final publication, including no-op rows.
            let rows = sqlx::query("SELECT source_run_key,kind,initiating_event_id,source_home,requested_event_id,attempt,due_at,target_namespace,target_workflow_id FROM run_operation WHERE source_run_key=$1 ORDER BY kind,initiating_event_id FOR UPDATE")
                .bind(key.0).fetch_all(&mut *tx).await?;
            let observed = rows
                .into_iter()
                .map(super::outbound::decode_operation)
                .collect::<Result<Vec<_>>>()?;
            let mut wanted = crate::derive_operations(&write.state, home)?;
            wanted.sort_by_key(|row| row.incarnation.key);
            ensure!(
                observed.len() == wanted.len()
                    && observed
                        .iter()
                        .zip(&wanted)
                        .all(|(left, right)| super::outbound::same_index_entry(left, right)),
                "operation staging changed before final publication; resume the reviewed plan"
            );
            if prior.outbound.is_none() {
                let pointer = pointer.as_ref().context("legacy page lacks staged state")?;
                ensure!(
                    super::outbound_upgrade_parts::read(&mut tx, key, pointer)
                        .await?
                        .as_ref()
                        == Some(&codec::encode_workflow_state(&write.state)?),
                    "prepared state changed before publication"
                );
                let tails: i64 = sqlx::query_scalar("SELECT count(*) FROM run_value_part WHERE run_key=$1 AND value_kind=1 AND value_key=0")
                    .bind(key.0).fetch_one(&mut *tx).await?;
                ensure!(
                    tails <= 900,
                    "legacy canonical state exceeds final part-retirement budget"
                );
                sqlx::query("UPDATE workflow_hot SET transition_seq=$2,state_data=$3,state_parts=NULL,recovery_needed=$4,updated_at=now() WHERE run_key=$1")
                    .bind(key.0).bind(i64::try_from(write.state.transition_seq.0)?).bind(pointer)
                    .bind(recovery_needed(&write.state)).execute(&mut *tx).await?;
                run_value_parts::delete_parts_from(
                    &mut tx,
                    key,
                    run_value_parts::RunValueKind::State,
                    run_value_parts::STATE_VALUE_KEY,
                    0,
                )
                .await?;
            }
            super::outbound::maintain_parent_reference(
                &mut tx,
                &write.state,
                write.state.is_open(),
            )
            .await?;
        }
        write_marker(&mut tx, &next, expected.is_some()).await?;
        tx.commit().await?;
        self.outbound_ready.store(next.complete, Ordering::Release);
        Ok(())
    }
}
