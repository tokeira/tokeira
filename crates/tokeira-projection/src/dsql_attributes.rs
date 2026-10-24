//! The DSQL layout of the visibility attribute index (`bounded-visibility-apply`).
//!
//! An image's index rows (cells) and encoded values are keyed by the image's version,
//! `(authority_epoch, source_transition_seq)`. A run's rows at its
//! `execution_visibility_current` version are its attributes, and every query matches
//! only those, so advancing the row under the version guard is what makes an image's
//! rows current. Rows below the run's version are deleted afterwards. No transaction
//! here writes more than [`TRANSACTION_ROW_BUDGET`] rows.
//!
//! The copy moves the earlier `execution_visibility_attr_index` into this layout once,
//! before anything can apply an image. Its position and completion live in
//! `execution_visibility_attr_copy`, read `FOR UPDATE` by each of its transactions.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    ops::Range,
    time::Duration,
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder};
use time::OffsetDateTime;
use tokeira_storage::{ConnectionDirector, DbClass, dsql::codec};
use tokeira_types::{NamespaceId, RunKey, SearchAttrValue};
use tracing::{info, warn};
use uuid::Uuid;

use super::{AttrCell, DsqlVisibilityStore, i64_from_u64, stored_attr_cells};
use crate::types::{AttrDescriptor, ExecutionRow};

/// Rows one transaction of the apply, its cleanup or the copy may write: under DSQL's
/// 3,000, with room for the run's row and its rollup deltas.
pub(super) const TRANSACTION_ROW_BUDGET: usize = 2_500;

/// Rows one statement writing cells, values or copies carries.
pub(super) const ROWS_PER_STATEMENT: usize = 128;

/// The versioned index rows: one per value, element or token.
pub(super) const ATTR_CELL_TABLE: &str = "execution_visibility_attr_cell";

/// The encoded values, one per attribute per image.
pub(super) const ATTR_VALUE_TABLE: &str = "execution_visibility_attr_value";

/// The copy's progress row in `execution_visibility_attr_copy`.
pub(super) const ATTR_COPY_NAME: &str = "visibility_attr_cell_copy";

/// Runs one copy transaction reads from `execution_visibility_current`.
const COPY_RUN_PAGE: usize = 256;

/// Copy transactions between progress logs.
const COPY_LOG_EVERY: usize = 100;

/// Conflicts a cleanup transaction retries before the cleanup gives up; what it leaves
/// matches nothing and goes at the run's next cleanup.
const CLEANUP_CONFLICT_ATTEMPTS: u32 = 20;

/// An image's version, `(authority_epoch, source_transition_seq)`, ordered as the
/// version guard orders it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct Version {
    pub(super) epoch: i64,
    pub(super) seq: i64,
}

impl Version {
    /// The version of the image `row` was built from.
    pub(super) fn of_row(row: &ExecutionRow) -> Self {
        Self {
            epoch: row.authority_epoch,
            seq: row.source_transition_seq.0 as i64,
        }
    }
}

/// One cell of an image.
pub(super) struct ImageCell {
    pub(super) attr_id: i64,
    pub(super) attr_type: i16,
    pub(super) cell: AttrCell,
}

/// One encoded value of an image.
pub(super) struct ImageValue {
    pub(super) attr_id: i64,
    pub(super) attr_type: i16,
    pub(super) value_data: Vec<u8>,
}

/// The cells and values an image writes at its version.
#[derive(Default)]
pub(super) struct ImageAttributes {
    pub(super) cells: Vec<ImageCell>,
    pub(super) values: Vec<ImageValue>,
}

impl ImageAttributes {
    /// The cells `stored_attr_cells` gives each attribute, each once, and the value of
    /// every attribute that has a cell. The earlier index kept a value only on an
    /// attribute's cells, so an attribute without cells, such as a workflow's empty
    /// list, keeps no value here either.
    pub(super) fn build(
        attributes: &[(AttrDescriptor, SearchAttrValue)],
        preserve_empty: bool,
    ) -> Result<Self> {
        let mut image = Self::default();
        let mut keys = BTreeSet::new();
        for (attr, value) in attributes {
            let attr_id = i64_from_u64(attr.attr_id.0, "search attribute id")?;
            let attr_type = attr.attr_type.to_db_smallint();
            let cells = stored_attr_cells(attr.attr_type, value, preserve_empty)?;
            if cells.is_empty() {
                continue;
            }
            for cell in cells {
                if keys.insert((attr_id, attr_type, cell.discriminator.clone())) {
                    image.cells.push(ImageCell {
                        attr_id,
                        attr_type,
                        cell,
                    });
                }
            }
            image.values.push(ImageValue {
                attr_id,
                attr_type,
                value_data: codec::encode(value)?,
            });
        }
        Ok(image)
    }
}

/// The rows an apply would write in one transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ApplyRows {
    pub(super) new_cells: usize,
    pub(super) new_values: usize,
    pub(super) older_cells: usize,
    pub(super) older_values: usize,
    pub(super) deltas: usize,
}

/// How an apply commits an image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ApplyPlan {
    /// One transaction: the cells, the guarded row, the values, the deletes below the
    /// version and the rollup deltas.
    Single,
    /// The cells in batches, then the flip, then cleanup.
    Staged,
}

impl ApplyRows {
    /// The row, the cells and values at the version, the deletes below it and the
    /// rollup deltas.
    pub(super) fn single_transaction(self) -> usize {
        1 + self.new_cells + self.new_values + self.older_cells + self.older_values + self.deltas
    }

    pub(super) fn plan(self) -> ApplyPlan {
        if self.single_transaction() <= TRANSACTION_ROW_BUDGET {
            ApplyPlan::Single
        } else {
            ApplyPlan::Staged
        }
    }

    /// The flip writes the row, the values and the deltas. It also deletes the values
    /// below the version when they fit; otherwise cleanup does.
    pub(super) fn flip_deletes_older_values(self) -> bool {
        1 + self.new_values + self.older_values + self.deltas <= TRANSACTION_ROW_BUDGET
    }

    /// Whether the flip alone fits, which the search-attribute limits always allow.
    pub(super) fn flip_fits(self) -> bool {
        1 + self.new_values + self.deltas <= TRANSACTION_ROW_BUDGET
    }
}

/// The cell batches of a staged apply, each one transaction of at most
/// [`TRANSACTION_ROW_BUDGET`] cells.
pub(super) fn cell_batches(cells: usize) -> Vec<Range<usize>> {
    (0..cells)
        .step_by(TRANSACTION_ROW_BUDGET)
        .map(|start| start..(start + TRANSACTION_ROW_BUDGET).min(cells))
        .collect()
}

const CELL_COLUMNS: &str = "namespace_id, run_key, authority_epoch, source_transition_seq, \
    attr_id, attr_type, value_discriminator, keyword_value, int_value, bool_value, \
    double_value, datetime_value, text_token";

/// Write `cells` at `version`, at most [`ROWS_PER_STATEMENT`] rows a statement. A
/// cell already there, from a retried or concurrent apply of the same image, is left.
pub(super) async fn insert_cells(
    connection: &mut PgConnection,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
    cells: &[ImageCell],
) -> Result<()> {
    for chunk in cells.chunks(ROWS_PER_STATEMENT) {
        cell_insert(namespace_id, run_key, version, chunk)
            .build()
            .execute(&mut *connection)
            .await?;
    }
    Ok(())
}

/// The statement that writes `chunk` at `version`.
fn cell_insert(
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
    chunk: &[ImageCell],
) -> QueryBuilder<Postgres> {
    let mut insert = QueryBuilder::<Postgres>::new("INSERT INTO ");
    insert
        .push(ATTR_CELL_TABLE)
        .push(" (")
        .push(CELL_COLUMNS)
        .push(") ");
    insert.push_values(chunk, |mut row, cell| {
        row.push_bind(namespace_id.0)
            .push_bind(run_key.0)
            .push_bind(version.epoch)
            .push_bind(version.seq)
            .push_bind(cell.attr_id)
            .push_bind(cell.attr_type)
            .push_bind(cell.cell.discriminator.as_str())
            .push_bind(cell.cell.keyword_value.as_deref())
            .push_bind(cell.cell.int_value)
            .push_bind(cell.cell.bool_value)
            .push_bind(cell.cell.double_value)
            .push_bind(cell.cell.datetime_value)
            .push_bind(cell.cell.text_token.as_deref());
    });
    insert.push(" ON CONFLICT DO NOTHING");
    insert
}

/// Write `values` at `version`, at most [`ROWS_PER_STATEMENT`] rows a statement.
pub(super) async fn insert_values(
    connection: &mut PgConnection,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
    values: &[ImageValue],
) -> Result<()> {
    for chunk in values.chunks(ROWS_PER_STATEMENT) {
        let mut insert = QueryBuilder::<Postgres>::new("INSERT INTO ");
        insert.push(ATTR_VALUE_TABLE).push(
            " (namespace_id, run_key, authority_epoch, source_transition_seq, attr_id, attr_type, \
             value_data) ",
        );
        insert.push_values(chunk, |mut row, value| {
            row.push_bind(namespace_id.0)
                .push_bind(run_key.0)
                .push_bind(version.epoch)
                .push_bind(version.seq)
                .push_bind(value.attr_id)
                .push_bind(value.attr_type)
                .push_bind(value.value_data.as_slice());
        });
        insert.push(" ON CONFLICT DO NOTHING");
        insert.build().execute(&mut *connection).await?;
    }
    Ok(())
}

/// A run's cells and values below a version.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct BelowCounts {
    pub(super) cells: usize,
    pub(super) values: usize,
}

/// Count the run's cells and values below `version`, over the run's key prefix.
pub(super) async fn count_below(
    connection: &mut PgConnection,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
) -> Result<BelowCounts> {
    let (cells, values): (i64, i64) = sqlx::query_as(
        "SELECT \
           (SELECT COUNT(*) FROM execution_visibility_attr_cell \
            WHERE namespace_id = $1 AND run_key = $2 \
              AND (authority_epoch, source_transition_seq) < ($3, $4)), \
           (SELECT COUNT(*) FROM execution_visibility_attr_value \
            WHERE namespace_id = $1 AND run_key = $2 \
              AND (authority_epoch, source_transition_seq) < ($3, $4))",
    )
    .bind(namespace_id.0)
    .bind(run_key.0)
    .bind(version.epoch)
    .bind(version.seq)
    .fetch_one(connection)
    .await?;
    Ok(BelowCounts {
        cells: usize::try_from(cells)?,
        values: usize::try_from(values)?,
    })
}

/// Push the cleanup's predicate: the run's rows strictly below `version`. Rows at or
/// above it belong to the current image or to one not yet flipped.
pub(super) fn push_below_version(
    query: &mut QueryBuilder<Postgres>,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
) {
    query
        .push(" WHERE namespace_id = ")
        .push_bind(namespace_id.0)
        .push(" AND run_key = ")
        .push_bind(run_key.0)
        .push(" AND (authority_epoch, source_transition_seq) < (")
        .push_bind(version.epoch)
        .push(", ")
        .push_bind(version.seq)
        .push(")");
}

/// Delete every one of the run's cells below `version`, in the caller's transaction,
/// which has counted them.
pub(super) async fn delete_cells_below(
    connection: &mut PgConnection,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
) -> Result<u64> {
    delete_below(connection, ATTR_CELL_TABLE, namespace_id, run_key, version).await
}

/// Delete every one of the run's values below `version`, in the caller's transaction.
pub(super) async fn delete_values_below(
    connection: &mut PgConnection,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
) -> Result<u64> {
    delete_below(connection, ATTR_VALUE_TABLE, namespace_id, run_key, version).await
}

async fn delete_below(
    connection: &mut PgConnection,
    table: &'static str,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
) -> Result<u64> {
    let mut delete = QueryBuilder::<Postgres>::new("DELETE FROM ");
    delete.push(table);
    push_below_version(&mut delete, namespace_id, run_key, version);
    Ok(delete.build().execute(connection).await?.rows_affected())
}

/// A cleanup chunk's key columns after the version, in each table's key order.
fn cleanup_key_columns(table: &'static str) -> &'static str {
    if table == ATTR_CELL_TABLE {
        "authority_epoch, source_transition_seq, attr_id, attr_type, value_discriminator"
    } else {
        "authority_epoch, source_transition_seq, attr_id, attr_type"
    }
}

/// The last key a cleanup chunk deletes: the `limit`-th key below the version, or
/// none when fewer remain.
#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
pub(super) struct ChunkBoundary {
    pub(super) authority_epoch: i64,
    pub(super) source_transition_seq: i64,
    pub(super) attr_id: i64,
    pub(super) attr_type: i16,
    #[sqlx(default)]
    pub(super) value_discriminator: Option<String>,
}

/// Delete up to `limit` of the run's rows below `version` from `table`, in key order,
/// in the caller's transaction. Answers the rows deleted and whether any are left.
async fn delete_below_chunk(
    connection: &mut PgConnection,
    table: &'static str,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
    limit: usize,
) -> Result<(u64, bool)> {
    let columns = cleanup_key_columns(table);
    let mut select = QueryBuilder::<Postgres>::new("SELECT ");
    select.push(columns).push(" FROM ").push(table);
    push_below_version(&mut select, namespace_id, run_key, version);
    select
        .push(" ORDER BY ")
        .push(columns)
        .push(" OFFSET ")
        .push_bind(i64::try_from(limit.saturating_sub(1))?)
        .push(" LIMIT 1");
    let boundary: Option<ChunkBoundary> = select
        .build_query_as()
        .fetch_optional(&mut *connection)
        .await?;
    let deleted = cleanup_delete("", table, namespace_id, run_key, version, boundary.as_ref())
        .build()
        .execute(&mut *connection)
        .await?
        .rows_affected();
    Ok((deleted, boundary.is_some()))
}

/// The cleanup's delete of a run's rows below `version`, up to `boundary` in key
/// order. `prefix` is empty, or `EXPLAIN ` for the live plan check.
pub(super) fn cleanup_delete(
    prefix: &str,
    table: &'static str,
    namespace_id: NamespaceId,
    run_key: RunKey,
    version: Version,
    boundary: Option<&ChunkBoundary>,
) -> QueryBuilder<Postgres> {
    let columns = cleanup_key_columns(table);
    let mut delete = QueryBuilder::<Postgres>::new(prefix);
    delete.push("DELETE FROM ").push(table);
    push_below_version(&mut delete, namespace_id, run_key, version);
    if let Some(boundary) = boundary {
        delete
            .push(" AND (")
            .push(columns)
            .push(") <= (")
            .push_bind(boundary.authority_epoch)
            .push(", ")
            .push_bind(boundary.source_transition_seq)
            .push(", ")
            .push_bind(boundary.attr_id)
            .push(", ")
            .push_bind(boundary.attr_type);
        if let Some(discriminator) = &boundary.value_discriminator {
            delete.push(", ").push_bind(discriminator.clone());
        }
        delete.push(")");
    }
    delete
}

impl DsqlVisibilityStore {
    /// Delete the run's cells and values below `version`, in transactions of at most
    /// [`TRANSACTION_ROW_BUDGET`] rows, after the run's row has advanced to it. Rows at
    /// or above the version are never touched. Answers the rows deleted.
    pub(super) async fn cleanup_below(
        &self,
        namespace_id: NamespaceId,
        run_key: RunKey,
        version: Version,
    ) -> Result<u64> {
        let mut deleted = 0;
        for table in [ATTR_CELL_TABLE, ATTR_VALUE_TABLE] {
            loop {
                let (chunk, more) = retry_conflicts(Some(CLEANUP_CONFLICT_ATTEMPTS), || async {
                    let mut permit = self.director.acquire(DbClass::Projection).await?;
                    let mut tx = permit.connection()?.begin().await?;
                    let result = delete_below_chunk(
                        &mut tx,
                        table,
                        namespace_id,
                        run_key,
                        version,
                        TRANSACTION_ROW_BUDGET,
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(result)
                })
                .await?;
                deleted += chunk;
                if !more {
                    break;
                }
            }
        }
        Ok(deleted)
    }
}

/// Run `transaction` again whenever DSQL refuses it with a write conflict, backing off
/// up to a second, `attempts` times in all or until it succeeds. Every transaction
/// passed here is safe to repeat.
pub(super) async fn retry_conflicts<T, F, Fut>(
    attempts: Option<u32>,
    mut transaction: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut attempt = 1u32;
    loop {
        match transaction().await {
            Err(error)
                if DsqlVisibilityStore::is_occ_conflict(&error)
                    && attempts.is_none_or(|limit| attempt < limit) =>
            {
                let backoff = 10u64.saturating_mul(1 << attempt.min(7)).min(1_000);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                attempt += 1;
            }
            result => return result,
        }
    }
}

/// Where the copy has reached: the last `execution_visibility_current` key it
/// finished, and the old table's last copied key within that run while the run is
/// split across transactions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CopyPosition {
    pub namespace_id: Uuid,
    pub archetype_id: i64,
    pub run_key: Uuid,
    /// The last old-table key copied within `run_key`, while the run is unfinished.
    pub within: Option<LegacyCellKey>,
}

/// An `execution_visibility_attr_index` row's key within its run.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, sqlx::FromRow)]
pub struct LegacyCellKey {
    pub attr_id: i64,
    pub attr_type: i16,
    pub value_discriminator: String,
}

/// What one call of the copy did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CopyReport {
    /// Copy transactions this call committed, the final one included.
    pub transactions: usize,
    /// Runs whose old rows this call copied, in full or in part.
    pub runs: usize,
    /// Cells and values this call copied.
    pub cells: u64,
    pub values: u64,
    /// The position this call's first transaction started from: none at the head.
    pub started_from: Option<CopyPosition>,
    /// Whether the copy is complete.
    pub completed: bool,
}

/// Which copy runs and over what: production copies every namespace under
/// [`ATTR_COPY_NAME`]; a live test copies its own namespace under its own name.
#[derive(Clone, Debug)]
pub(crate) struct CopyScope {
    pub(crate) name: String,
    pub(crate) namespace: Option<NamespaceId>,
}

impl CopyScope {
    pub(crate) fn cluster() -> Self {
        Self {
            name: ATTR_COPY_NAME.to_owned(),
            namespace: None,
        }
    }
}

/// The copy's sizes. Tests shrink them to split runs and stop part-way.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CopyLimits {
    pub(crate) budget: usize,
    pub(crate) run_page: usize,
    /// Stop after this many committed transactions, as an interrupted copy would.
    pub(crate) max_transactions: Option<usize>,
}

impl Default for CopyLimits {
    fn default() -> Self {
        Self {
            budget: TRANSACTION_ROW_BUDGET,
            run_page: COPY_RUN_PAGE,
            max_transactions: None,
        }
    }
}

/// A run of `execution_visibility_current` with its version.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CurrentRun {
    namespace_id: Uuid,
    archetype_id: i64,
    run_key: Uuid,
    version: Version,
}

impl CurrentRun {
    fn after(&self) -> CopyPosition {
        CopyPosition {
            namespace_id: self.namespace_id,
            archetype_id: self.archetype_id,
            run_key: self.run_key,
            within: None,
        }
    }

    fn within(&self, key: LegacyCellKey) -> CopyPosition {
        CopyPosition {
            within: Some(key),
            ..self.after()
        }
    }
}

/// What one copy transaction did.
#[derive(Default)]
struct CopyStep {
    started_from: Option<CopyPosition>,
    runs: usize,
    cells: u64,
    values: u64,
    completed: bool,
}

/// The copy's pages of `execution_visibility_current`. Each fixes a key prefix and takes
/// a range on the next column, which the primary key serves as a seek; a row comparison
/// over the whole key would be filtered, not sought.
pub(super) const RUNS_FROM_HEAD: &str = "SELECT namespace_id, archetype_id, run_key, authority_epoch, source_transition_seq \
     FROM execution_visibility_current ORDER BY namespace_id, archetype_id, run_key LIMIT $1";
pub(super) const RUNS_IN_NAMESPACE: &str = "SELECT namespace_id, archetype_id, run_key, authority_epoch, source_transition_seq \
     FROM execution_visibility_current WHERE namespace_id = $1 \
     ORDER BY archetype_id, run_key LIMIT $2";
pub(super) const RUNS_AFTER_IN_ARCHETYPE: &str = "SELECT namespace_id, archetype_id, run_key, authority_epoch, source_transition_seq \
     FROM execution_visibility_current WHERE namespace_id = $1 AND archetype_id = $2 \
     AND run_key > $3 ORDER BY run_key LIMIT $4";
pub(super) const RUNS_AFTER_ARCHETYPE: &str = "SELECT namespace_id, archetype_id, run_key, authority_epoch, source_transition_seq \
     FROM execution_visibility_current WHERE namespace_id = $1 AND archetype_id > $2 \
     ORDER BY archetype_id, run_key LIMIT $3";
pub(super) const RUNS_AFTER_NAMESPACE: &str = "SELECT namespace_id, archetype_id, run_key, authority_epoch, source_transition_seq \
     FROM execution_visibility_current WHERE namespace_id > $1 \
     ORDER BY namespace_id, archetype_id, run_key LIMIT $2";

/// The old rows of a page of runs in one namespace.
pub(super) const LEGACY_CELL_COUNTS: &str = "SELECT run_key, COUNT(*) FROM execution_visibility_attr_index \
     WHERE namespace_id = $1 AND run_key = ANY($2) GROUP BY run_key";

type CurrentRunRow = (Uuid, i64, Uuid, i64, i64);

fn current_runs(rows: Vec<CurrentRunRow>) -> Vec<CurrentRun> {
    rows.into_iter()
        .map(
            |(namespace_id, archetype_id, run_key, epoch, seq)| CurrentRun {
                namespace_id,
                archetype_id,
                run_key,
                version: Version { epoch, seq },
            },
        )
        .collect()
}

/// The next runs after `after` in `execution_visibility_current`'s key order, up to
/// `limit`.
async fn next_runs(
    connection: &mut PgConnection,
    scope: &CopyScope,
    after: Option<&CopyPosition>,
    limit: usize,
) -> Result<Vec<CurrentRun>> {
    let mut runs = Vec::new();
    let take = |runs: &Vec<CurrentRun>| i64::try_from(limit - runs.len());
    match (scope.namespace, after) {
        (Some(namespace_id), None) => {
            runs.extend(current_runs(
                sqlx::query_as(RUNS_IN_NAMESPACE)
                    .bind(namespace_id.0)
                    .bind(take(&runs)?)
                    .fetch_all(&mut *connection)
                    .await?,
            ));
        }
        (None, None) => {
            runs.extend(current_runs(
                sqlx::query_as(RUNS_FROM_HEAD)
                    .bind(take(&runs)?)
                    .fetch_all(&mut *connection)
                    .await?,
            ));
        }
        (namespace, Some(position)) => {
            runs.extend(current_runs(
                sqlx::query_as(RUNS_AFTER_IN_ARCHETYPE)
                    .bind(position.namespace_id)
                    .bind(position.archetype_id)
                    .bind(position.run_key)
                    .bind(take(&runs)?)
                    .fetch_all(&mut *connection)
                    .await?,
            ));
            if runs.len() < limit {
                runs.extend(current_runs(
                    sqlx::query_as(RUNS_AFTER_ARCHETYPE)
                        .bind(position.namespace_id)
                        .bind(position.archetype_id)
                        .bind(take(&runs)?)
                        .fetch_all(&mut *connection)
                        .await?,
                ));
            }
            if runs.len() < limit && namespace.is_none() {
                runs.extend(current_runs(
                    sqlx::query_as(RUNS_AFTER_NAMESPACE)
                        .bind(position.namespace_id)
                        .bind(take(&runs)?)
                        .fetch_all(&mut *connection)
                        .await?,
                ));
            }
        }
    }
    Ok(runs)
}

/// The old rows and distinct attributes of each of `runs` that has any, by
/// `(namespace_id, run_key)`.
async fn legacy_counts(
    connection: &mut PgConnection,
    runs: &[CurrentRun],
) -> Result<BTreeMap<(Uuid, Uuid), (usize, usize)>> {
    let mut by_namespace: BTreeMap<Uuid, Vec<Uuid>> = BTreeMap::new();
    for run in runs {
        by_namespace
            .entry(run.namespace_id)
            .or_default()
            .push(run.run_key);
    }
    let mut counts = BTreeMap::new();
    for (namespace_id, run_keys) in by_namespace {
        let cells: Vec<(Uuid, i64)> = sqlx::query_as(LEGACY_CELL_COUNTS)
            .bind(namespace_id)
            .bind(&run_keys)
            .fetch_all(&mut *connection)
            .await?;
        let attrs: Vec<(Uuid, i64)> = sqlx::query_as(
            "SELECT run_key, COUNT(*) FROM (SELECT DISTINCT run_key, attr_id, attr_type \
             FROM execution_visibility_attr_index WHERE namespace_id = $1 AND run_key = ANY($2)) d \
             GROUP BY run_key",
        )
        .bind(namespace_id)
        .bind(&run_keys)
        .fetch_all(&mut *connection)
        .await?;
        let attrs: BTreeMap<Uuid, i64> = attrs.into_iter().collect();
        for (run_key, count) in cells {
            let distinct = attrs.get(&run_key).copied().unwrap_or(0);
            counts.insert(
                (namespace_id, run_key),
                (usize::try_from(count)?, usize::try_from(distinct)?),
            );
        }
    }
    Ok(counts)
}

/// Push `AND (attr_id, attr_type, value_discriminator) <op> (key)` onto a statement
/// over the old table, unqualified or aliased `a`.
fn push_legacy_bound(query: &mut QueryBuilder<Postgres>, op: &str, key: &LegacyCellKey) {
    query
        .push(" AND (attr_id, attr_type, value_discriminator) ")
        .push(op)
        .push(" (")
        .push_bind(key.attr_id)
        .push(", ")
        .push_bind(key.attr_type)
        .push(", ")
        .push_bind(key.value_discriminator.clone())
        .push(")");
}

/// The `limit`-th old-table key of `run` after `after`, or none when fewer remain.
async fn legacy_boundary(
    connection: &mut PgConnection,
    run: &CurrentRun,
    after: Option<&LegacyCellKey>,
    limit: usize,
) -> Result<Option<LegacyCellKey>> {
    let mut select = QueryBuilder::<Postgres>::new(
        "SELECT attr_id, attr_type, value_discriminator FROM execution_visibility_attr_index \
         WHERE namespace_id = ",
    );
    select
        .push_bind(run.namespace_id)
        .push(" AND run_key = ")
        .push_bind(run.run_key);
    if let Some(after) = after {
        push_legacy_bound(&mut select, ">", after);
    }
    select
        .push(" ORDER BY attr_id, attr_type, value_discriminator OFFSET ")
        .push_bind(i64::try_from(limit.saturating_sub(1))?)
        .push(" LIMIT 1");
    Ok(select
        .build_query_as()
        .fetch_optional(&mut *connection)
        .await?)
}

/// The old rows of `run` after `after`.
async fn legacy_count_after(
    connection: &mut PgConnection,
    run: &CurrentRun,
    after: &LegacyCellKey,
) -> Result<usize> {
    let mut select = QueryBuilder::<Postgres>::new(
        "SELECT COUNT(*) FROM execution_visibility_attr_index WHERE namespace_id = ",
    );
    select
        .push_bind(run.namespace_id)
        .push(" AND run_key = ")
        .push_bind(run.run_key);
    push_legacy_bound(&mut select, ">", after);
    let (count,): (i64,) = select.build_query_as().fetch_one(&mut *connection).await?;
    Ok(usize::try_from(count)?)
}

/// Copy `run`'s old rows in `(after, upto]` into cells at the run's version, on the
/// server.
async fn copy_run_cells(
    connection: &mut PgConnection,
    run: &CurrentRun,
    after: Option<&LegacyCellKey>,
    upto: Option<&LegacyCellKey>,
) -> Result<u64> {
    let mut insert = QueryBuilder::<Postgres>::new("INSERT INTO ");
    insert
        .push(ATTR_CELL_TABLE)
        .push(" (")
        .push(CELL_COLUMNS)
        .push(") SELECT namespace_id, run_key, ")
        .push_bind(run.version.epoch)
        .push(", ")
        .push_bind(run.version.seq)
        .push(
            ", attr_id, attr_type, value_discriminator, keyword_value, int_value, bool_value, \
             double_value, datetime_value, text_token FROM execution_visibility_attr_index \
             WHERE namespace_id = ",
        )
        .push_bind(run.namespace_id)
        .push(" AND run_key = ")
        .push_bind(run.run_key);
    if let Some(after) = after {
        push_legacy_bound(&mut insert, ">", after);
    }
    if let Some(upto) = upto {
        push_legacy_bound(&mut insert, "<=", upto);
    }
    insert.push(" ON CONFLICT DO NOTHING");
    Ok(insert
        .build()
        .execute(&mut *connection)
        .await?
        .rows_affected())
}

/// Copy the value of each attribute of `run` whose first old row is in `(after, upto]`,
/// at the run's version. The old index put the whole value on each of an attribute's
/// rows, so its first row in key order gives it, and a run split across transactions
/// copies each value once, with the chunk that holds the attribute's first row.
async fn copy_run_values(
    connection: &mut PgConnection,
    run: &CurrentRun,
    after: Option<&LegacyCellKey>,
    upto: Option<&LegacyCellKey>,
) -> Result<u64> {
    let mut insert = QueryBuilder::<Postgres>::new("INSERT INTO ");
    insert
        .push(ATTR_VALUE_TABLE)
        .push(
            " (namespace_id, run_key, authority_epoch, source_transition_seq, attr_id, attr_type, \
             value_data) SELECT a.namespace_id, a.run_key, ",
        )
        .push_bind(run.version.epoch)
        .push(", ")
        .push_bind(run.version.seq)
        .push(
            ", a.attr_id, a.attr_type, a.value_data FROM execution_visibility_attr_index a \
             WHERE a.namespace_id = ",
        )
        .push_bind(run.namespace_id)
        .push(" AND a.run_key = ")
        .push_bind(run.run_key)
        .push(
            " AND a.value_discriminator = (SELECT MIN(b.value_discriminator) \
             FROM execution_visibility_attr_index b WHERE b.namespace_id = a.namespace_id \
             AND b.run_key = a.run_key AND b.attr_id = a.attr_id AND b.attr_type = a.attr_type)",
        );
    if let Some(after) = after {
        push_legacy_bound(&mut insert, ">", after);
    }
    if let Some(upto) = upto {
        push_legacy_bound(&mut insert, "<=", upto);
    }
    insert.push(" ON CONFLICT DO NOTHING");
    Ok(insert
        .build()
        .execute(&mut *connection)
        .await?
        .rows_affected())
}

/// `run`'s version, read by its whole key, when its row exists.
async fn current_run(
    connection: &mut PgConnection,
    position: &CopyPosition,
) -> Result<Option<CurrentRun>> {
    let row: Option<(i64, i64)> = sqlx::query_as(
        "SELECT authority_epoch, source_transition_seq FROM execution_visibility_current \
         WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3",
    )
    .bind(position.namespace_id)
    .bind(position.archetype_id)
    .bind(position.run_key)
    .fetch_optional(connection)
    .await?;
    Ok(row.map(|(epoch, seq)| CurrentRun {
        namespace_id: position.namespace_id,
        archetype_id: position.archetype_id,
        run_key: position.run_key,
        version: Version { epoch, seq },
    }))
}

impl DsqlVisibilityStore {
    /// Copy `execution_visibility_attr_index` into the versioned cells and values,
    /// unless the copy has finished. Call it before anything can apply an image: a
    /// copy transaction that reads the completion unset then reads every run's row as
    /// the earlier release left it, which is the version its old rows belong to.
    ///
    /// Each transaction reads the progress row `FOR UPDATE`, copies at most 2,500 rows
    /// from the stored position, and writes the new position, so a restarted copy
    /// resumes where it stopped and two copiers take turns. A transaction refused with
    /// a write conflict runs again. One whose snapshot predates completion on another
    /// node can still write rows at a run's old version: at most one straggling
    /// transaction per node, matching nothing, gone at that run's next cleanup or
    /// deletion.
    pub async fn copy_legacy_attributes(&self) -> Result<CopyReport> {
        self.copy_legacy_attributes_with(&CopyScope::cluster(), CopyLimits::default())
            .await
    }

    pub(crate) async fn copy_legacy_attributes_with(
        &self,
        scope: &CopyScope,
        limits: CopyLimits,
    ) -> Result<CopyReport> {
        anyhow::ensure!(
            limits.budget > 2 && limits.run_page > 0,
            "the copy needs room for its progress row and at least one run"
        );
        retry_conflicts(None, || async {
            let mut permit = self.director.acquire(DbClass::Projection).await?;
            sqlx::query(
                "INSERT INTO execution_visibility_attr_copy (name) VALUES ($1) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(&scope.name)
            .execute(permit.connection()?)
            .await?;
            Ok(())
        })
        .await
        .context("create the visibility attribute copy's progress row")?;
        let mut report = CopyReport::default();
        loop {
            if limits
                .max_transactions
                .is_some_and(|max| report.transactions >= max)
            {
                return Ok(report);
            }
            let step = retry_conflicts(None, || self.copy_step(scope, limits))
                .await
                .context("copy the visibility attribute index")?;
            if report.transactions == 0 {
                report.started_from = step.started_from.clone();
            }
            report.transactions += 1;
            report.runs += step.runs;
            report.cells += step.cells;
            report.values += step.values;
            if step.completed {
                report.completed = true;
                if report.cells + report.values > 0 {
                    info!(
                        runs = report.runs,
                        cells = report.cells,
                        values = report.values,
                        transactions = report.transactions,
                        "copied the visibility attribute index into its versioned layout"
                    );
                }
                return Ok(report);
            }
            if report.transactions.is_multiple_of(COPY_LOG_EVERY) {
                info!(
                    runs = report.runs,
                    cells = report.cells,
                    transactions = report.transactions,
                    "copying the visibility attribute index"
                );
            }
        }
    }

    /// One copy transaction.
    async fn copy_step(&self, scope: &CopyScope, limits: CopyLimits) -> Result<CopyStep> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let mut tx = permit.connection()?.begin().await?;
        let (position_data, completed_at): (Option<Vec<u8>>, Option<OffsetDateTime>) =
            sqlx::query_as(
                "SELECT position_data, completed_at FROM execution_visibility_attr_copy \
                 WHERE name = $1 FOR UPDATE",
            )
            .bind(&scope.name)
            .fetch_one(&mut *tx)
            .await?;
        let position: Option<CopyPosition> = position_data
            .as_deref()
            .map(codec::decode)
            .transpose()
            .context("decode the visibility attribute copy's position")?;
        let mut step = CopyStep {
            started_from: position.clone(),
            ..CopyStep::default()
        };
        if completed_at.is_some() {
            tx.commit().await?;
            step.completed = true;
            return Ok(step);
        }
        // The progress row's update takes one row of the budget.
        let mut room = limits.budget - 1;
        let mut next = position.clone();
        // A run split across transactions finishes first.
        if let Some(
            position @ CopyPosition {
                within: Some(after),
                ..
            },
        ) = &position
        {
            match current_run(&mut tx, position).await? {
                Some(run) => {
                    // Each remaining row writes its cell and, for an attribute's first
                    // row, the attribute's value: at most two rows.
                    let remaining = legacy_count_after(&mut tx, &run, after).await?;
                    if 2 * remaining <= room {
                        step.cells += copy_run_cells(&mut tx, &run, Some(after), None).await?;
                        step.values += copy_run_values(&mut tx, &run, Some(after), None).await?;
                        room -= 2 * remaining;
                        next = Some(run.after());
                    } else {
                        let upto = legacy_boundary(&mut tx, &run, Some(after), room / 2)
                            .await?
                            .context("a split run's boundary exists while rows remain")?;
                        step.cells +=
                            copy_run_cells(&mut tx, &run, Some(after), Some(&upto)).await?;
                        step.values +=
                            copy_run_values(&mut tx, &run, Some(after), Some(&upto)).await?;
                        room = 0;
                        next = Some(run.within(upto));
                    }
                    step.runs += 1;
                }
                None => {
                    next = Some(CopyPosition {
                        within: None,
                        ..position.clone()
                    });
                }
            }
        }
        if room > 0 {
            let runs = next_runs(&mut tx, scope, next.as_ref(), limits.run_page).await?;
            if runs.is_empty() {
                sqlx::query(
                    "UPDATE execution_visibility_attr_copy \
                     SET position_data = $2, completed_at = COALESCE(completed_at, now()) \
                     WHERE name = $1",
                )
                .bind(&scope.name)
                .bind(next.as_ref().map(codec::encode).transpose()?)
                .execute(&mut *tx)
                .await?;
                tx.commit().await?;
                step.completed = true;
                return Ok(step);
            }
            let counts = legacy_counts(&mut tx, &runs).await?;
            for run in runs {
                let Some(&(cells, attrs)) = counts.get(&(run.namespace_id, run.run_key)) else {
                    next = Some(run.after());
                    continue;
                };
                if cells + attrs <= room {
                    step.cells += copy_run_cells(&mut tx, &run, None, None).await?;
                    step.values += copy_run_values(&mut tx, &run, None, None).await?;
                    step.runs += 1;
                    room -= cells + attrs;
                    next = Some(run.after());
                } else if room >= 2 {
                    // Copy the run's first rows, each with its attribute's value when
                    // it is the attribute's first row; the rest go in later
                    // transactions, from the position's key within the run. Every
                    // attribute has a row, so the run has more than `room / 2` rows.
                    let upto = legacy_boundary(&mut tx, &run, None, room / 2)
                        .await?
                        .context("a split run's boundary exists while rows remain")?;
                    step.cells += copy_run_cells(&mut tx, &run, None, Some(&upto)).await?;
                    step.values += copy_run_values(&mut tx, &run, None, Some(&upto)).await?;
                    step.runs += 1;
                    next = Some(run.within(upto));
                    break;
                } else {
                    break;
                }
            }
        }
        sqlx::query("UPDATE execution_visibility_attr_copy SET position_data = $2 WHERE name = $1")
            .bind(&scope.name)
            .bind(next.as_ref().map(codec::encode).transpose()?)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(step)
    }
}

/// Note a cleanup that gave up; what it left matches nothing.
pub(super) fn warn_cleanup_failed(run_key: RunKey, error: &anyhow::Error) {
    warn!(
        run_key = %run_key.0,
        error = %format!("{error:#}"),
        "visibility attribute cleanup stopped; the rows left below the run's version match \
         nothing and go at its next cleanup"
    );
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use proptest::prelude::*;
    use time::OffsetDateTime;

    use super::*;
    use crate::types::{AttrId, SearchAttrType};

    /// The search-attribute limits Tokeira enforces, as v1.31.0 does.
    const MAX_KEYS: usize = 100;
    const MAX_VALUE_BYTES: usize = 2 * 1024;
    const MAX_TOTAL_BYTES: usize = 40 * 1024;

    /// The bytes an attribute's value takes in its JSON payload.
    fn payload_bytes(value: &SearchAttrValue) -> usize {
        match value {
            SearchAttrValue::Keyword(text) | SearchAttrValue::Text(text) => text.len() + 2,
            SearchAttrValue::KeywordList(items) => {
                2 + items.iter().map(|item| item.len() + 3).sum::<usize>()
            }
            _ => 24,
        }
    }

    /// A `Text` value of short, mostly distinct tokens, or a list of short elements,
    /// within one value's limit: the shapes that need the most cells.
    fn arb_attribute() -> impl Strategy<Value = (SearchAttrType, SearchAttrValue)> {
        let token = "[a-z0-9]{1,3}";
        prop_oneof![
            4 => prop::collection::vec(token, 0..700).prop_map(|tokens| {
                let mut text = String::new();
                for token in tokens {
                    if text.len() + token.len() + 3 > MAX_VALUE_BYTES {
                        break;
                    }
                    if !text.is_empty() {
                        text.push(' ');
                    }
                    text.push_str(&token);
                }
                (SearchAttrType::Text, SearchAttrValue::Text(text))
            }),
            3 => prop::collection::vec("[a-z]{1,4}", 0..500).prop_map(|items| {
                let mut kept = Vec::new();
                let mut bytes = 2;
                for item in items {
                    if bytes + item.len() + 3 > MAX_VALUE_BYTES {
                        break;
                    }
                    bytes += item.len() + 3;
                    kept.push(item);
                }
                (SearchAttrType::KeywordList, SearchAttrValue::KeywordList(kept))
            }),
            1 => "[a-z]{0,40}".prop_map(|text| (SearchAttrType::Keyword, SearchAttrValue::Keyword(text))),
            1 => any::<i64>().prop_map(|value| (SearchAttrType::Int, SearchAttrValue::Int(value))),
            1 => any::<bool>().prop_map(|value| (SearchAttrType::Bool, SearchAttrValue::Bool(value))),
            1 => (-1_000_000i64..1_000_000).prop_map(|seconds| (
                SearchAttrType::Datetime,
                SearchAttrValue::Datetime(OffsetDateTime::from_unix_timestamp(seconds).unwrap()),
            )),
        ]
    }

    /// An image's attributes within the limits: at most 100 keys and 40 KiB in all.
    fn arb_image() -> impl Strategy<Value = Vec<(AttrDescriptor, SearchAttrValue)>> {
        prop::collection::vec(arb_attribute(), 0..30).prop_map(|attributes| {
            let mut kept = Vec::new();
            let mut total = 0;
            for (index, (attr_type, value)) in attributes.into_iter().enumerate() {
                let bytes = payload_bytes(&value) + 32;
                if kept.len() == MAX_KEYS || total + bytes > MAX_TOTAL_BYTES {
                    break;
                }
                total += bytes;
                kept.push((
                    AttrDescriptor {
                        attr_id: AttrId(index as u64 + 1),
                        attr_type,
                    },
                    value,
                ));
            }
            kept
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        // Feature: bounded-visibility-apply, Property 1: Any image within the limits applies
        #[test]
        fn any_image_within_the_limits_applies_within_the_budget(
            attributes in arb_image(),
            preserve_empty in any::<bool>(),
            older_cells in 0usize..15_000,
            older_values in 0usize..300,
            deltas in 0usize..12,
        ) {
            let image = ImageAttributes::build(&attributes, preserve_empty).unwrap();
            let keys: BTreeSet<_> = image
                .cells
                .iter()
                .map(|cell| (cell.attr_id, cell.attr_type, cell.cell.discriminator.clone()))
                .collect();
            prop_assert_eq!(keys.len(), image.cells.len(), "each cell once");
            prop_assert!(image.cells.len() <= 14_000, "{} cells", image.cells.len());
            let rows = ApplyRows {
                new_cells: image.cells.len(),
                new_values: image.values.len(),
                older_cells,
                older_values,
                deltas,
            };
            match rows.plan() {
                ApplyPlan::Single => prop_assert!(rows.single_transaction() <= TRANSACTION_ROW_BUDGET),
                ApplyPlan::Staged => {
                    prop_assert!(rows.flip_fits());
                    let batches = cell_batches(image.cells.len());
                    let mut next = 0;
                    for batch in &batches {
                        prop_assert_eq!(batch.start, next);
                        prop_assert!(!batch.is_empty() && batch.len() <= TRANSACTION_ROW_BUDGET);
                        next = batch.end;
                    }
                    prop_assert_eq!(next, image.cells.len(), "the batches write every cell");
                }
            }
        }

        // Feature: bounded-visibility-apply, Property 4: Values read as before
        #[test]
        fn values_read_as_before(attributes in arb_image(), preserve_empty in any::<bool>()) {
            let image = ImageAttributes::build(&attributes, preserve_empty).unwrap();
            let values: BTreeMap<i64, &ImageValue> =
                image.values.iter().map(|value| (value.attr_id, value)).collect();
            prop_assert_eq!(values.len(), image.values.len(), "one value per attribute");
            for (attr, value) in &attributes {
                let attr_id = i64::try_from(attr.attr_id.0).unwrap();
                let has_cells =
                    !stored_attr_cells(attr.attr_type, value, preserve_empty).unwrap().is_empty();
                match values.get(&attr_id) {
                    // The earlier index put `codec::encode(value)` on every cell of the
                    // attribute; the value table holds the same bytes once.
                    Some(stored) => {
                        prop_assert!(has_cells);
                        prop_assert_eq!(&stored.value_data, &codec::encode(value).unwrap());
                        prop_assert_eq!(
                            &codec::decode::<SearchAttrValue>(&stored.value_data).unwrap(),
                            value
                        );
                    }
                    None => prop_assert!(!has_cells, "an attribute with cells keeps its value"),
                }
            }
        }
    }

    /// The protocol over sets of cells and values keyed by version, with the run's
    /// current version: the model the apply's statements implement.
    #[derive(Default)]
    struct Store {
        current: Option<u8>,
        cells: BTreeSet<(u8, u16)>,
        values: BTreeSet<(u8, u16)>,
    }

    #[derive(Clone, Debug)]
    struct Image {
        version: u8,
        cells: BTreeSet<u16>,
        values: BTreeSet<u16>,
    }

    /// One projector applying one image, a step at a time.
    #[derive(Debug)]
    enum Apply {
        Start,
        Batches(usize),
        Flip,
        Cleanup,
        Done,
    }

    const MODEL_BUDGET: usize = 6;

    impl Store {
        fn newer(&self, version: u8) -> bool {
            self.current.is_none_or(|current| version > current)
        }

        fn below(set: &BTreeSet<(u8, u16)>, version: u8) -> usize {
            set.iter().filter(|(at, _)| *at < version).count()
        }

        /// One step of `apply` for `image`, as `apply_row` takes it.
        fn step(&mut self, apply: &mut Apply, image: &Image) {
            *apply = match std::mem::replace(apply, Apply::Done) {
                Apply::Start => {
                    if !self.newer(image.version) {
                        Apply::Done
                    } else {
                        let rows = 1
                            + image.cells.len()
                            + image.values.len()
                            + Self::below(&self.cells, image.version)
                            + Self::below(&self.values, image.version);
                        if rows <= MODEL_BUDGET {
                            self.cells
                                .extend(image.cells.iter().map(|&cell| (image.version, cell)));
                            self.values
                                .extend(image.values.iter().map(|&value| (image.version, value)));
                            self.cells.retain(|(at, _)| *at >= image.version);
                            self.values.retain(|(at, _)| *at >= image.version);
                            self.current = Some(image.version);
                            Apply::Done
                        } else {
                            Apply::Batches(0)
                        }
                    }
                }
                Apply::Batches(written) => {
                    let batch: Vec<u16> = image
                        .cells
                        .iter()
                        .skip(written)
                        .take(MODEL_BUDGET)
                        .copied()
                        .collect();
                    self.cells
                        .extend(batch.iter().map(|&cell| (image.version, cell)));
                    if written + batch.len() >= image.cells.len() {
                        Apply::Flip
                    } else {
                        Apply::Batches(written + batch.len())
                    }
                }
                Apply::Flip => {
                    if self.newer(image.version) {
                        self.current = Some(image.version);
                        self.values
                            .extend(image.values.iter().map(|&value| (image.version, value)));
                        self.values.retain(|(at, _)| *at >= image.version);
                        Apply::Cleanup
                    } else {
                        Apply::Done
                    }
                }
                Apply::Cleanup => {
                    // Property 3: cleanup deletes below the version it flipped, never
                    // at or above the run's current version.
                    let current = self.current.expect("a flipped run has a version");
                    let doomed: Vec<(u8, u16)> = self
                        .cells
                        .iter()
                        .filter(|(at, _)| *at < image.version)
                        .take(MODEL_BUDGET)
                        .copied()
                        .collect();
                    assert!(doomed.iter().all(|(at, _)| *at < current));
                    for cell in &doomed {
                        self.cells.remove(cell);
                    }
                    if doomed.len() < MODEL_BUDGET {
                        Apply::Done
                    } else {
                        Apply::Cleanup
                    }
                }
                Apply::Done => Apply::Done,
            };
        }

        /// The cells a query matches: those at the run's current version.
        fn matched(&self) -> BTreeSet<u16> {
            self.cells
                .iter()
                .filter(|(at, _)| Some(*at) == self.current)
                .map(|&(_, cell)| cell)
                .collect()
        }
    }

    fn arb_images() -> impl Strategy<Value = Vec<Image>> {
        prop::collection::vec(
            (
                prop::collection::btree_set(0u16..40, 0..16),
                prop::collection::btree_set(0u16..6, 0..3),
            ),
            2..4,
        )
        .prop_flat_map(|images| {
            let count = images.len();
            (
                Just(images),
                Just((1..=count as u8).collect::<Vec<_>>()).prop_shuffle(),
            )
        })
        .prop_map(|(images, versions)| {
            images
                .into_iter()
                .zip(versions)
                .map(|((cells, values), version)| Image {
                    version,
                    cells,
                    values,
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        // Feature: bounded-visibility-apply, Property 2: A query matches one image
        // Feature: bounded-visibility-apply, Property 3: Cleanup never touches the current image
        #[test]
        fn a_query_matches_exactly_one_image_at_every_step(
            images in arb_images(),
            duplicates in prop::collection::vec(any::<prop::sample::Index>(), 0..2),
            schedule in prop::collection::vec(any::<prop::sample::Index>(), 400),
        ) {
            // One projector per image, and a second projector for some images, as
            // during a partition move.
            let mut projectors: Vec<(usize, Apply)> =
                (0..images.len()).map(|image| (image, Apply::Start)).collect();
            for duplicate in duplicates {
                projectors.push((duplicate.index(images.len()), Apply::Start));
            }
            let mut store = Store::default();
            for pick in schedule {
                let live: Vec<usize> = projectors
                    .iter()
                    .enumerate()
                    .filter(|(_, (_, apply))| !matches!(apply, Apply::Done))
                    .map(|(index, _)| index)
                    .collect();
                if live.is_empty() {
                    break;
                }
                let index = live[pick.index(live.len())];
                let (image, apply) = &mut projectors[index];
                store.step(apply, &images[*image]);
                let expected = store
                    .current
                    .and_then(|current| images.iter().find(|image| image.version == current))
                    .map(|image| image.cells.clone())
                    .unwrap_or_default();
                prop_assert_eq!(store.matched(), expected, "at version {:?}", store.current);
            }
        }
    }

    /// The copy over a model of the earlier index: the decisions `copy_step` takes,
    /// on runs in key order, each with its old cells in key order and its attributes.
    #[derive(Debug)]
    struct LegacyRun {
        version: u8,
        cells: Vec<(u8, u16)>,
    }

    #[derive(Default)]
    struct Copied {
        cells: BTreeSet<(usize, u8, u8, u16)>,
        values: BTreeSet<(usize, u8, u8)>,
        position: Option<(usize, Option<(u8, u16)>)>,
        completed: bool,
    }

    impl Copied {
        fn copy_cells(&mut self, runs: &[LegacyRun], run: usize, cells: &[(u8, u16)]) -> usize {
            for &(attr, cell) in cells {
                self.cells.insert((run, runs[run].version, attr, cell));
            }
            cells.len()
        }

        /// The values of the attributes whose first cell is among `cells`.
        fn copy_values(&mut self, runs: &[LegacyRun], run: usize, cells: &[(u8, u16)]) -> usize {
            let firsts: BTreeSet<u8> = cells
                .iter()
                .filter(|&&(attr, cell)| {
                    runs[run].cells.iter().find(|&&(first, _)| first == attr) == Some(&(attr, cell))
                })
                .map(|&(attr, _)| attr)
                .collect();
            for &attr in &firsts {
                self.values.insert((run, runs[run].version, attr));
            }
            firsts.len()
        }

        /// One copy transaction; answers the rows it wrote, the progress row included.
        fn step(&mut self, runs: &[LegacyRun], budget: usize, run_page: usize) -> usize {
            if self.completed {
                return 0;
            }
            let mut room = budget - 1;
            let mut written = 1;
            let mut next = self.position;
            if let Some((run, Some(after))) = self.position {
                let remaining: Vec<(u8, u16)> = runs[run]
                    .cells
                    .iter()
                    .copied()
                    .filter(|cell| *cell > after)
                    .collect();
                if 2 * remaining.len() <= room {
                    written += self.copy_cells(runs, run, &remaining);
                    written += self.copy_values(runs, run, &remaining);
                    room -= 2 * remaining.len();
                    next = Some((run, None));
                } else {
                    let chunk = &remaining[..room / 2];
                    written += self.copy_cells(runs, run, chunk);
                    written += self.copy_values(runs, run, chunk);
                    next = Some((run, Some(chunk[chunk.len() - 1])));
                    room = 0;
                }
            }
            if room > 0 {
                let start = next.map_or(0, |(run, _)| run + 1);
                let page: Vec<usize> = (start..runs.len()).take(run_page).collect();
                if page.is_empty() {
                    self.position = next;
                    self.completed = true;
                    return written;
                }
                for run in page {
                    let cells = runs[run].cells.len();
                    if cells == 0 {
                        next = Some((run, None));
                        continue;
                    }
                    let attrs: BTreeSet<u8> =
                        runs[run].cells.iter().map(|&(attr, _)| attr).collect();
                    if cells + attrs.len() <= room {
                        written += self.copy_cells(runs, run, &runs[run].cells);
                        written += self.copy_values(runs, run, &runs[run].cells);
                        room -= cells + attrs.len();
                        next = Some((run, None));
                    } else if room >= 2 {
                        let chunk = &runs[run].cells[..room / 2];
                        written += self.copy_cells(runs, run, chunk);
                        written += self.copy_values(runs, run, chunk);
                        next = Some((run, Some(chunk[chunk.len() - 1])));
                        break;
                    } else {
                        break;
                    }
                }
            }
            self.position = next;
            written
        }
    }

    fn arb_legacy() -> impl Strategy<Value = Vec<LegacyRun>> {
        prop::collection::vec(
            (
                any::<u8>(),
                prop::collection::btree_set((0u8..4, 0u16..30), 0..40),
            ),
            0..20,
        )
        .prop_map(|runs| {
            runs.into_iter()
                .map(|(version, cells)| LegacyRun {
                    version,
                    cells: cells.into_iter().collect(),
                })
                .collect()
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(200))]

        // Feature: bounded-visibility-apply, Property 5: The copy keeps every answer
        #[test]
        fn the_copy_keeps_every_answer(
            runs in arb_legacy(),
            budget in 3usize..20,
            run_page in 1usize..6,
        ) {
            // Every step reads and writes the one progress row, so a call that stops
            // part-way, a later call resuming from the stored position and two copiers
            // taking turns all run this same sequence of steps.
            let mut copied = Copied::default();
            let mut steps = 0;
            while !copied.completed {
                let written = copied.step(&runs, budget, run_page);
                prop_assert!(written <= budget, "a step wrote {written} rows");
                steps += 1;
                prop_assert!(steps < 10_000, "the copy makes progress");
            }
            let expected_cells: BTreeSet<_> = runs
                .iter()
                .enumerate()
                .flat_map(|(run, legacy)| legacy.cells.iter().map(move |&(attr, cell)| (run, legacy.version, attr, cell)))
                .collect();
            let expected_values: BTreeSet<_> = runs
                .iter()
                .enumerate()
                .flat_map(|(run, legacy)| {
                    legacy.cells.iter().map(|&(attr, _)| attr).collect::<BTreeSet<_>>()
                        .into_iter()
                        .map(move |attr| (run, legacy.version, attr))
                })
                .collect();
            prop_assert_eq!(copied.cells, expected_cells, "every old row at its run's version");
            prop_assert_eq!(copied.values, expected_values, "one value per attribute");
        }
    }

    #[test]
    fn cleanup_deletes_strictly_below_the_version() {
        let mut delete = QueryBuilder::<Postgres>::new("DELETE FROM ");
        delete.push(ATTR_CELL_TABLE);
        push_below_version(
            &mut delete,
            NamespaceId(Uuid::nil()),
            RunKey(Uuid::nil()),
            Version { epoch: 1, seq: 2 },
        );
        let sql = delete.sql();
        let sql = sql.as_str();
        assert!(
            sql.contains("(authority_epoch, source_transition_seq) < ("),
            "{sql}"
        );
        assert!(!sql.contains("<="), "{sql}");
    }

    #[test]
    fn cells_are_written_by_statements_of_at_most_128_rows() {
        let cells: Vec<ImageCell> = (0..300)
            .map(|index| ImageCell {
                attr_id: 1,
                attr_type: SearchAttrType::KeywordList.to_db_smallint(),
                cell: AttrCell {
                    discriminator: index.to_string(),
                    keyword_value: Some(index.to_string()),
                    ..AttrCell::default()
                },
            })
            .collect();
        let mut rows = 0;
        for chunk in cells.chunks(ROWS_PER_STATEMENT) {
            let insert = cell_insert(
                NamespaceId(Uuid::nil()),
                RunKey(Uuid::nil()),
                Version { epoch: 1, seq: 1 },
                chunk,
            );
            let sql = insert.sql();
            let statement_rows = sql.as_str().matches("), (").count() + 1;
            assert!(
                statement_rows <= 128,
                "{statement_rows} rows in one statement"
            );
            assert!(sql.as_str().ends_with(" ON CONFLICT DO NOTHING"));
            rows += statement_rows;
        }
        assert_eq!(rows, 300);
    }
}
