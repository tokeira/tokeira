//! The parts of a run's values over the part size (`large-value-parts`).
//!
//! `run_value_part` holds the parts after the first of a run's state, and of each
//! history batch's events and principals. The first part stays in the value's own
//! column, beside a count column (`workflow_hot.state_parts`,
//! `history_batch.events_parts`, `history_batch.principals_parts`) that is NULL for
//! a whole value; [`value_parts`] cuts and joins them.
//!
//! Each writer writes a value's row and all its parts in one transaction. A reader
//! joins a split value's parts in the transaction that reads its row: a run's state
//! changes with every commit, so a state read in two transactions could join the
//! first part of one version to the parts of another. A history batch never changes,
//! but a purge removes it: the purge removes a run's parts only after every one of
//! its batch rows, so a batch read in one snapshot has its parts in that snapshot.

use std::collections::BTreeMap;

use sqlx::PgConnection;

use super::*;
use crate::dsql::value_parts::{self, Split};

/// Which of a run's values a part belongs to, as `run_value_part.value_kind`
/// stores it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RunValueKind {
    /// The run's state, whose value key is [`STATE_VALUE_KEY`].
    State,
    /// A history batch's encoded events, whose value key is the batch's first
    /// event id.
    Events,
    /// A history batch's encoded principals, whose value key is the batch's first
    /// event id.
    Principals,
}

impl RunValueKind {
    pub(super) const fn to_db(self) -> i16 {
        match self {
            Self::State => 1,
            Self::Events => 2,
            Self::Principals => 3,
        }
    }

    const fn column(self) -> &'static str {
        match self {
            Self::State => "workflow_hot.state_data",
            Self::Events => "history_batch.events_data",
            Self::Principals => "history_batch.principals_data",
        }
    }
}

/// The value key of a run's state: a run has one.
pub(super) const STATE_VALUE_KEY: i64 = 0;

/// A value's parts after the first, each with its part number, in order.
pub(super) type NumberedParts = Vec<(i32, Vec<u8>)>;

/// What a part's write does to a part already stored at its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OnConflict {
    /// Replace it: a rewritten state's parts replace the previous state's.
    Replace,
    /// Keep it: a repeated write of a value that never changes, such as a reset's
    /// copied batch, writes the same bytes.
    Keep,
}

const UPSERT_PART_SQL: &str =
    "INSERT INTO run_value_part (run_key, value_kind, value_key, part_no, data)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (run_key, value_kind, value_key, part_no) DO UPDATE SET data = EXCLUDED.data";

const INSERT_PART_SQL: &str =
    "INSERT INTO run_value_part (run_key, value_kind, value_key, part_no, data)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (run_key, value_kind, value_key, part_no) DO NOTHING";

/// Write the parts after the first of a split value, one statement each, so that
/// no statement carries more than one part. A whole value writes nothing.
pub(super) async fn write_parts(
    connection: &mut PgConnection,
    run_key: RunKey,
    kind: RunValueKind,
    value_key: i64,
    split: &Split<'_>,
    on_conflict: OnConflict,
) -> Result<()> {
    for (part_no, data) in split.numbered() {
        write_part(
            connection,
            run_key,
            kind,
            value_key,
            part_no,
            data,
            on_conflict,
        )
        .await?;
    }
    Ok(())
}

/// Write one part after the first of a split value.
pub(super) async fn write_part(
    connection: &mut PgConnection,
    run_key: RunKey,
    kind: RunValueKind,
    value_key: i64,
    part_no: i32,
    data: &[u8],
    on_conflict: OnConflict,
) -> Result<()> {
    let sql = match on_conflict {
        OnConflict::Replace => UPSERT_PART_SQL,
        OnConflict::Keep => INSERT_PART_SQL,
    };
    sqlx::query(sql)
        .bind(run_key.0)
        .bind(kind.to_db())
        .bind(value_key)
        .bind(part_no)
        .bind(data)
        .execute(connection)
        .await?;
    Ok(())
}

/// Delete a value's parts numbered `from` and above: those a shorter version of
/// the value no longer uses.
pub(super) async fn delete_parts_from(
    connection: &mut PgConnection,
    run_key: RunKey,
    kind: RunValueKind,
    value_key: i64,
    from: i32,
) -> Result<()> {
    sqlx::query(
        "DELETE FROM run_value_part
         WHERE run_key = $1 AND value_kind = $2 AND value_key = $3 AND part_no >= $4",
    )
    .bind(run_key.0)
    .bind(kind.to_db())
    .bind(value_key)
    .bind(from)
    .execute(connection)
    .await?;
    Ok(())
}

/// The parts after the first of one of the run's values, in part-number order,
/// read on `connection`.
pub(super) async fn read_parts(
    connection: &mut PgConnection,
    run_key: RunKey,
    kind: RunValueKind,
    value_key: i64,
) -> Result<NumberedParts> {
    crate::read_probe::sql(run_key, "sql/value-parts");
    Ok(sqlx::query_as(
        "SELECT part_no, data FROM run_value_part
         WHERE run_key = $1 AND value_kind = $2 AND value_key = $3
         ORDER BY part_no",
    )
    .bind(run_key.0)
    .bind(kind.to_db())
    .bind(value_key)
    .fetch_all(connection)
    .await?)
}

/// Join one of the run's values from what its own column holds and its count,
/// reading its other parts on `connection`, which must be the transaction that
/// read the row whenever a later write can replace the value.
pub(super) async fn join_value(
    connection: &mut PgConnection,
    run_key: RunKey,
    kind: RunValueKind,
    value_key: i64,
    first: Vec<u8>,
    count: Option<i32>,
) -> Result<Vec<u8>> {
    let rest = if count.is_some() {
        read_parts(connection, run_key, kind, value_key).await?
    } else {
        Vec::new()
    };
    join(run_key, kind, value_key, first, count, rest)
}

/// Join a run's state, read on `connection` with its count, in the transaction
/// that read it.
pub(super) async fn join_state(
    connection: &mut PgConnection,
    run_key: RunKey,
    first: Vec<u8>,
    count: Option<i32>,
) -> Result<Vec<u8>> {
    join_value(
        connection,
        run_key,
        RunValueKind::State,
        STATE_VALUE_KEY,
        first,
        count,
    )
    .await
}

/// The parts after the first of a run's state, for a reader that joins them with
/// [`join_state_parts`] apart from reading them.
pub(super) async fn read_state_parts(
    connection: &mut PgConnection,
    run_key: RunKey,
) -> Result<NumberedParts> {
    read_parts(connection, run_key, RunValueKind::State, STATE_VALUE_KEY).await
}

/// Join a run's state from parts already read. Fails only when the parts don't
/// fit together.
pub(super) fn join_state_parts(
    run_key: RunKey,
    first: Vec<u8>,
    count: Option<i32>,
    rest: NumberedParts,
) -> Result<Vec<u8>> {
    join(
        run_key,
        RunValueKind::State,
        STATE_VALUE_KEY,
        first,
        count,
        rest,
    )
}

fn join(
    run_key: RunKey,
    kind: RunValueKind,
    value_key: i64,
    first: Vec<u8>,
    count: Option<i32>,
    rest: NumberedParts,
) -> Result<Vec<u8>> {
    Ok(value_parts::join(
        || match kind {
            RunValueKind::State => format!("{} of run {}", kind.column(), run_key.0),
            RunValueKind::Events | RunValueKind::Principals => format!(
                "{} of run {} at event {value_key}",
                kind.column(),
                run_key.0
            ),
        },
        first,
        count,
        rest,
    )?)
}

/// Read a run's state whole in a read-only snapshot of its own, or `None` when the
/// run has no state. For a reader that met the state split outside a transaction.
pub(super) async fn read_state_in_snapshot(
    connection: &mut PgConnection,
    run_key: RunKey,
) -> Result<Option<Vec<u8>>> {
    let mut tx = connection
        .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    let row = sqlx::query_as::<_, (Vec<u8>, Option<i32>)>(
        "SELECT state_data, state_parts FROM workflow_hot WHERE run_key = $1",
    )
    .bind(run_key.0)
    .fetch_optional(&mut *tx)
    .await?;
    let state = match row {
        Some((first, count)) => Some(join_state(&mut tx, run_key, first, count).await?),
        None => None,
    };
    tx.commit().await?;
    Ok(state)
}

/// States read outside a transaction, as `(run_key, state_data, state_parts)`,
/// with each split state read again whole, in a snapshot of its own. A run whose
/// state is gone by then reads as `None`, as if the read had come a moment later;
/// the row stays in the result, so a caller that pages by rows read keeps its
/// count.
pub(super) async fn whole_states(
    connection: &mut PgConnection,
    rows: Vec<(Uuid, Vec<u8>, Option<i32>)>,
) -> Result<Vec<(Uuid, Option<Vec<u8>>)>> {
    let mut states = Vec::with_capacity(rows.len());
    for (run_key, first, count) in rows {
        let state = if count.is_none() {
            Some(first)
        } else {
            read_state_in_snapshot(connection, RunKey(run_key)).await?
        };
        states.push((run_key, state));
    }
    Ok(states)
}

/// A history batch row as a read selects it: `first_event_id, last_event_id,
/// transition_seq, events_data, events_parts, principals_data, principals_parts`,
/// each of its values whole, or its first part with its count.
pub(super) type StoredBatchRow = (
    i64,
    i64,
    i64,
    Vec<u8>,
    Option<i32>,
    Option<Vec<u8>>,
    Option<i32>,
);

/// Whether any of the batches is split, so that its parts must be read in the
/// transaction that reads it.
pub(super) fn any_split(rows: &[StoredBatchRow]) -> bool {
    rows.iter()
        .any(|(_, _, _, _, events_parts, _, principals_parts)| {
            events_parts.is_some() || principals_parts.is_some()
        })
}

/// A history batch with its values whole.
#[derive(Debug)]
pub(super) struct WholeBatch {
    pub(super) last_event_id: i64,
    pub(super) transition_seq: i64,
    pub(super) events_data: Vec<u8>,
    pub(super) principals_data: Option<Vec<u8>>,
}

/// Join stored batches, reading the parts of the split ones on `connection` in one
/// statement. `connection` must be the transaction that read the rows when any is
/// split.
pub(super) async fn join_batches(
    connection: &mut PgConnection,
    run_key: RunKey,
    rows: Vec<StoredBatchRow>,
) -> Result<Vec<WholeBatch>> {
    let split = rows
        .iter()
        .filter(|(_, _, _, _, events_parts, _, principals_parts)| {
            events_parts.is_some() || principals_parts.is_some()
        })
        .map(|(first_event_id, ..)| *first_event_id)
        .collect::<Vec<_>>();
    let mut parts: BTreeMap<(i16, i64), NumberedParts> = BTreeMap::new();
    if !split.is_empty() {
        let fetched = sqlx::query_as::<_, (i16, i64, i32, Vec<u8>)>(
            "SELECT value_kind, value_key, part_no, data FROM run_value_part
             WHERE run_key = $1 AND value_kind IN ($2, $3) AND value_key = ANY($4)
             ORDER BY value_kind, value_key, part_no",
        )
        .bind(run_key.0)
        .bind(RunValueKind::Events.to_db())
        .bind(RunValueKind::Principals.to_db())
        .bind(&split)
        .fetch_all(connection)
        .await?;
        for (kind, value_key, part_no, data) in fetched {
            parts
                .entry((kind, value_key))
                .or_default()
                .push((part_no, data));
        }
    }
    let mut take = |kind: RunValueKind, value_key: i64| {
        parts.remove(&(kind.to_db(), value_key)).unwrap_or_default()
    };
    rows.into_iter()
        .map(
            |(
                first_event_id,
                last_event_id,
                transition_seq,
                events_data,
                events_parts,
                principals_data,
                principals_parts,
            )| {
                let events_data = join(
                    run_key,
                    RunValueKind::Events,
                    first_event_id,
                    events_data,
                    events_parts,
                    take(RunValueKind::Events, first_event_id),
                )?;
                let principals_rest = take(RunValueKind::Principals, first_event_id);
                let principals_data = match principals_data {
                    Some(first) => Some(join(
                        run_key,
                        RunValueKind::Principals,
                        first_event_id,
                        first,
                        principals_parts,
                        principals_rest,
                    )?),
                    None if principals_parts.is_none() && principals_rest.is_empty() => None,
                    None => bail!(
                        "{} of run {} at event {first_event_id} has parts but no first part",
                        RunValueKind::Principals.column(),
                        run_key.0
                    ),
                };
                Ok(WholeBatch {
                    last_event_id,
                    transition_seq,
                    events_data,
                    principals_data,
                })
            },
        )
        .collect()
}
