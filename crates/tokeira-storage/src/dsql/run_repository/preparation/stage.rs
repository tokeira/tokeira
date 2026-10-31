//! Bounded immutable staging pages. Even a single oversized event is split into
//! separately budgeted rows before its header is written.

use super::{
    super::run_value_parts::{self, OnConflict, RunValueKind, STATE_VALUE_KEY},
    *,
};

enum StageRow<'a> {
    Part {
        kind: RunValueKind,
        key: i64,
        number: i32,
        bytes: &'a [u8],
    },
    History {
        first: i64,
        last: i64,
        events: value_parts::Split<'a>,
        principals: Option<value_parts::Split<'a>>,
    },
    Timer {
        timer: &'a tokeira_kernel::TimerState,
        bytes: Vec<u8>,
    },
}

impl StageRow<'_> {
    fn cost(&self) -> WriteCost {
        WriteCost::write(
            128 + match self {
                Self::Part { bytes, .. } => bytes.len(),
                Self::History {
                    events, principals, ..
                } => events.first.len() + principals.as_ref().map_or(0, |v| v.first.len()),
                Self::Timer { bytes, timer } => bytes.len() + timer.timer_id.len(),
            },
        )
    }

    async fn write(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        token: &PreparationToken,
        seq: TransitionSeq,
    ) -> Result<()> {
        match self {
            Self::Part {
                kind,
                key,
                number,
                bytes,
            } => {
                run_value_parts::write_part(
                    tx,
                    token.target_run,
                    *kind,
                    *key,
                    *number,
                    bytes,
                    OnConflict::Keep,
                )
                .await?;
                let (stored,) = sqlx::query_as::<_, (Vec<u8>,)>("SELECT data FROM run_value_part WHERE run_key=$1 AND value_kind=$2 AND value_key=$3 AND part_no=$4")
                    .bind(token.target_run.0).bind(kind.to_db()).bind(key).bind(number).fetch_one(&mut **tx).await?;
                ensure!(stored == *bytes, "immutable prepared value part changed");
            }
            Self::History {
                first,
                last,
                events,
                principals,
            } => {
                sqlx::query("INSERT INTO history_batch(run_key,first_event_id,last_event_id,transition_seq,events_data,events_parts,principals_data,principals_parts,created_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,now()) ON CONFLICT(run_key,first_event_id) DO NOTHING")
                    .bind(token.target_run.0).bind(first).bind(last).bind(convert::i64_from_u64(seq.0, "transition_seq")?)
                    .bind(events.first).bind(events.count).bind(principals.as_ref().map(|v|v.first)).bind(principals.as_ref().and_then(|v|v.count)).execute(&mut **tx).await?;
                let row = sqlx::query_as::<_, (i64,i64,Vec<u8>,Option<i32>,Option<Vec<u8>>,Option<i32>)>("SELECT last_event_id,transition_seq,events_data,events_parts,principals_data,principals_parts FROM history_batch WHERE run_key=$1 AND first_event_id=$2")
                    .bind(token.target_run.0).bind(first).fetch_one(&mut **tx).await?;
                ensure!(
                    row.0 == *last
                        && row.1 == convert::i64_from_u64(seq.0, "transition_seq")?
                        && row.2 == events.first
                        && row.3 == events.count
                        && row.4.as_deref() == principals.as_ref().map(|v| v.first)
                        && row.5 == principals.as_ref().and_then(|v| v.count),
                    "immutable prepared history header changed"
                );
            }
            Self::Timer { timer, bytes } => {
                sqlx::query("INSERT INTO timer_bucket(shard_id,fire_at,run_key,timer_id,timer_data,created_at) VALUES($1,$2,$3,$4,$5,now()) ON CONFLICT(shard_id,fire_at,run_key,timer_id) DO NOTHING")
                    .bind(DsqlRunRepository::shard_id_to_uuid(token.home)).bind(timer.fire_at).bind(token.target_run.0).bind(&timer.timer_id).bind(bytes).execute(&mut **tx).await?;
                let (stored,) = sqlx::query_as::<_, (Vec<u8>,)>("SELECT timer_data FROM timer_bucket WHERE shard_id=$1 AND fire_at=$2 AND run_key=$3 AND timer_id=$4")
                    .bind(DsqlRunRepository::shard_id_to_uuid(token.home)).bind(timer.fire_at).bind(token.target_run.0).bind(&timer.timer_id).fetch_one(&mut **tx).await?;
                ensure!(stored == *bytes, "immutable prepared timer changed");
            }
        }
        Ok(())
    }
}

impl DsqlRunRepository {
    pub(super) async fn stage_publication(
        &self,
        token: &PreparationToken,
        publication: &RunPreparation,
        fence: &WriteFence,
    ) -> Result<()> {
        let transition = &publication.new_run;
        let state = codec::encode_workflow_state(&transition.next_state)?;
        let events = (!transition.history_events.is_empty())
            .then(|| codec::encode_history_events(&transition.history_events))
            .transpose()?;
        let principals = transition
            .event_principals
            .iter()
            .any(Option::is_some)
            .then(|| codec::encode_history_principals(&transition.event_principals))
            .transpose()?;
        let state_parts = value_parts::split(&state);
        let mut rows = Vec::new();
        rows.extend(
            state_parts
                .numbered()
                .map(|(number, bytes)| StageRow::Part {
                    kind: RunValueKind::State,
                    key: STATE_VALUE_KEY,
                    number,
                    bytes,
                }),
        );
        if let Some(events) = &events {
            let first = transition
                .history_events
                .first()
                .expect("encoded nonempty history")
                .event_id;
            let last = transition
                .history_events
                .last()
                .expect("encoded nonempty history")
                .event_id;
            let events = value_parts::split(events);
            let principals = principals.as_deref().map(value_parts::split);
            rows.extend(events.numbered().map(|(number, bytes)| StageRow::Part {
                kind: RunValueKind::Events,
                key: first,
                number,
                bytes,
            }));
            if let Some(principals) = &principals {
                rows.extend(principals.numbered().map(|(number, bytes)| StageRow::Part {
                    kind: RunValueKind::Principals,
                    key: first,
                    number,
                    bytes,
                }));
            }
            rows.push(StageRow::History {
                first,
                last,
                events,
                principals,
            });
        }
        for timer in transition.next_state.timers.values() {
            rows.push(StageRow::Timer {
                timer,
                bytes: codec::encode_timer_state(timer)?,
            });
        }
        let costs: Vec<_> = rows.iter().map(StageRow::cost).collect();
        // Each part is independently budgeted. Never use pages' oversized-single-
        // item exception to smuggle a many-part event into one transaction.
        ensure!(
            costs
                .iter()
                .all(|c| c.bytes <= WriteBudget::TRANSACTION.bytes
                    && c.rows <= WriteBudget::TRANSACTION.rows),
            "prepared row exceeds staging budget"
        );
        for page in pages(&costs, WriteBudget::TRANSACTION) {
            let mut permit = self.director.acquire(DbClass::Commit).await?;
            let mut tx = permit.connection()?.begin().await?;
            if let Some(lost) = Self::check_fence(&mut tx, fence, token.target_run).await? {
                return Err(lost.into());
            }
            let record = by_token(&mut tx, token).await?;
            if record.phase >= PUBLISHED || record.phase == READY {
                return Ok(());
            }
            ensure!(
                record.phase == STAGING,
                "preparation is not accepting data pages"
            );
            for row in &rows[page] {
                row.write(&mut tx, token, transition.next_state.transition_seq)
                    .await?;
            }
            tx.commit().await?;
        }
        let mut permit = self.director.acquire(DbClass::Commit).await?;
        let mut tx = permit.connection()?.begin().await?;
        if let Some(lost) = Self::check_fence(&mut tx, fence, token.target_run).await? {
            return Err(lost.into());
        }
        let record = by_token(&mut tx, token).await?;
        if record.phase == STAGING {
            sqlx::query("UPDATE run_preparation SET phase=$2 WHERE preparation_id=$1")
                .bind(token.id)
                .bind(READY)
                .execute(&mut *tx)
                .await?;
        } else {
            ensure!(
                record.phase == READY || record.phase >= PUBLISHED,
                "preparation is not ready for publication"
            );
        }
        tx.commit().await?;
        Ok(())
    }
}

pub(super) async fn verify(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    token: &PreparationToken,
    transition: &Transition,
) -> Result<()> {
    let state = codec::encode_workflow_state(&transition.next_state)?;
    let split = value_parts::split(&state);
    ensure!(
        run_value_parts::join_state(tx, token.target_run, split.first.to_vec(), split.count)
            .await?
            == state,
        "prepared state parts differ from manifest"
    );
    let batches = sqlx::query_as::<_, super::super::run_value_parts::StoredBatchRow>(
        "SELECT first_event_id,last_event_id,transition_seq,events_data,events_parts,principals_data,principals_parts FROM history_batch WHERE run_key=$1 ORDER BY first_event_id")
        .bind(token.target_run.0).fetch_all(&mut **tx).await?;
    if let Some(batch) = batches.first() {
        ensure!(
            Some(batch.0)
                == transition
                    .history_events
                    .first()
                    .map(|event| event.event_id)
                && Some(batch.1) == transition.history_events.last().map(|event| event.event_id)
                && batch.2
                    == convert::i64_from_u64(
                        transition.next_state.transition_seq.0,
                        "transition_seq"
                    )?,
            "prepared history boundaries differ from manifest"
        );
    }
    let batches = run_value_parts::join_batches(tx, token.target_run, batches).await?;
    let expected = (!transition.history_events.is_empty())
        .then(|| codec::encode_history_events(&transition.history_events))
        .transpose()?;
    ensure!(
        batches.len() == usize::from(expected.is_some()),
        "prepared history batch count differs from manifest"
    );
    if let Some(expected) = expected {
        let batch = &batches[0];
        ensure!(
            batch.events_data == expected,
            "prepared history differs from manifest"
        );
        let principals = transition
            .event_principals
            .iter()
            .any(Option::is_some)
            .then(|| codec::encode_history_principals(&transition.event_principals))
            .transpose()?;
        ensure!(
            batch.principals_data == principals,
            "prepared principals differ from manifest"
        );
    }
    let timers = sqlx::query_as::<_, (String, Vec<u8>, Uuid, OffsetDateTime)>(
        "SELECT timer_id,timer_data,shard_id,fire_at FROM timer_bucket WHERE run_key=$1 ORDER BY timer_id",
    )
    .bind(token.target_run.0)
    .fetch_all(&mut **tx)
    .await?;
    ensure!(
        timers.len() == transition.next_state.timers.len(),
        "prepared timer count differs from manifest"
    );
    for (id, data, home, fire_at) in timers {
        let timer =
            transition
                .next_state
                .timers
                .get(&id)
                .ok_or(RecoveryError::InvalidPreparation(
                    "unexpected prepared timer",
                ))?;
        ensure!(
            data == codec::encode_timer_state(timer)?
                && home == DsqlRunRepository::shard_id_to_uuid(token.home)
                && fire_at.unix_timestamp_nanos() / 1_000
                    == timer.fire_at.unix_timestamp_nanos() / 1_000,
            "prepared timer differs from manifest"
        );
    }
    Ok(())
}
