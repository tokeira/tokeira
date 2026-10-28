//! Visibility memo parts and their row costs (`large-value-parts`).
//!
//! The guarded row upsert is the authority for replacement: only its winning
//! transaction may replace parts. Reads join in the snapshot that read the row.
//! The format itself belongs to storage and is shared with every DSQL value.

use anyhow::{Result, ensure};
use sqlx::{PgConnection, Row, postgres::PgRow};
use tokeira_storage::dsql::{
    codec,
    value_parts::{self, Split},
};
use tokeira_types::Memo;

use crate::ExecutionRow;

/// Parts written and deleted by replacing a memo, excluding the owning row.
pub(super) fn replacement_rows(new_parts: usize, previous: Option<i32>) -> Result<usize> {
    let previous = previous.unwrap_or(1);
    ensure!(
        previous >= 1,
        "invalid visibility memo part count {previous}"
    );
    // Rewritten parts count once; any old tail is deleted. A shrink below the
    // threshold therefore costs every old overflow row, even though it writes none.
    Ok(new_parts.max(usize::try_from(previous)?).saturating_sub(1))
}

/// Replace the parts only after the owning row's guard admitted this image.
pub(super) async fn replace(
    connection: &mut PgConnection,
    row: &ExecutionRow,
    value: &Split<'_>,
    previous: Option<i32>,
) -> Result<()> {
    for (part_no, data) in value.numbered() {
        sqlx::query(
            "INSERT INTO visibility_memo_part (namespace_id, archetype_id, run_key, part_no, data)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (namespace_id, archetype_id, run_key, part_no)
             DO UPDATE SET data = EXCLUDED.data",
        )
        .bind(row.namespace_id.0)
        .bind(i64::from(row.archetype_id.0))
        .bind(row.run_key.0)
        .bind(part_no)
        .bind(data)
        .execute(&mut *connection)
        .await?;
    }
    if previous.unwrap_or(1) > value.parts() {
        sqlx::query(
            "DELETE FROM visibility_memo_part
             WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3 AND part_no >= $4",
        )
        .bind(row.namespace_id.0)
        .bind(i64::from(row.archetype_id.0))
        .bind(row.run_key.0)
        .bind(value.parts())
        .execute(connection)
        .await?;
    }
    Ok(())
}

/// Decode a memo using the transaction that supplied its row. A missing count
/// retains the legacy whole-value path, including a NULL memo.
pub(super) async fn read(connection: &mut PgConnection, row: &PgRow) -> Result<Memo> {
    let first: Option<Vec<u8>> = row.try_get("memo_blob")?;
    let count: Option<i32> = row.try_get("memo_parts")?;
    let Some(count) = count else {
        return Ok(super::decode_optional_memo(first)?.unwrap_or_default());
    };
    let namespace: uuid::Uuid = row.try_get("namespace_id")?;
    let archetype: i64 = row.try_get("archetype_id")?;
    let run: uuid::Uuid = row.try_get("run_key")?;
    let rest = sqlx::query_as::<_, (i32, Vec<u8>)>(
        "SELECT part_no, data FROM visibility_memo_part
         WHERE namespace_id = $1 AND archetype_id = $2 AND run_key = $3 ORDER BY part_no",
    )
    .bind(namespace)
    .bind(archetype)
    .bind(run)
    .fetch_all(connection)
    .await?;
    let data = value_parts::join(
        || format!("execution_visibility_current.memo_blob of {namespace}/{archetype}/{run}"),
        first.unwrap_or_default(),
        Some(count),
        rest,
    )?;
    codec::decode(&data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_budget_counts_writes_and_deleted_tail() {
        for (new, previous, expected) in [
            (1, None, 0),
            (1, Some(3), 2),
            (3, None, 2),
            (3, Some(3), 2),
            (4, Some(2), 3),
            (2, Some(4), 3),
        ] {
            assert_eq!(replacement_rows(new, previous).unwrap(), expected);
        }
        assert!(replacement_rows(1, Some(-1)).is_err());
    }
}
