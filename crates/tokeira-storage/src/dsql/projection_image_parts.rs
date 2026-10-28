//! Overflow parts of immutable projection images (`large-value-parts`).
//!
//! The full image key includes its partition and fanout. Writers and deleters use
//! the transaction owning the image; readers must use the snapshot that read its
//! first part and count, so concurrent deletion cannot detach the rest of a value.

use anyhow::Result;
use sqlx::PgConnection;

use super::value_parts::{self, Split};
use crate::ProjectionImageKey;

/// Write the parts after the first in the image's inserting transaction.
pub(crate) async fn insert(
    connection: &mut PgConnection,
    partition: i32,
    fanout: i16,
    key: ProjectionImageKey,
    value: &Split<'_>,
) -> Result<()> {
    for (part_no, data) in value.numbered() {
        sqlx::query(
            "INSERT INTO projection_image_part
             (partition_id, fanout, run_key, transition_seq, part_no, data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(partition)
        .bind(fanout)
        .bind(key.run_key.0)
        .bind(i64::try_from(key.transition_seq.0)?)
        .bind(part_no)
        .bind(data)
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

/// Join an image in the snapshot that read its first part and count. Whole images
/// need no part-table read, including rows written before parts existed.
pub(crate) async fn join(
    connection: &mut PgConnection,
    partition: i32,
    fanout: i16,
    key: ProjectionImageKey,
    first: Vec<u8>,
    count: Option<i32>,
) -> Result<Vec<u8>> {
    let rest = if count.is_some() {
        sqlx::query_as::<_, (i32, Vec<u8>)>(
            "SELECT part_no, data FROM projection_image_part
             WHERE partition_id = $1 AND fanout = $2 AND run_key = $3
                 AND transition_seq = $4 ORDER BY part_no",
        )
        .bind(partition)
        .bind(fanout)
        .bind(key.run_key.0)
        .bind(i64::try_from(key.transition_seq.0)?)
        .fetch_all(connection)
        .await?
    } else {
        Vec::new()
    };
    Ok(value_parts::join(
        || format!("projection_log.context_data of image {partition}/{fanout}/{key:?}"),
        first,
        count,
        rest,
    )?)
}
