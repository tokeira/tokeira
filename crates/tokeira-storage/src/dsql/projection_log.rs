//! DSQL-backed projection log reader.
//!
//! Projection records are written by transition commits and read by projection
//! workers in a stable `(partition_id, fanout, run_key, transition_seq)` order.
//! This module deliberately owns only the read side; checkpointing belongs to the
//! projection sink because that is the surface the live worker already calls.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokeira_types::{ProjectionCursor, RunKey, TransitionSeq};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    DbClass, ProjectionBatch, ProjectionContext, ProjectionLog, ProjectionRecord,
    dsql::{DsqlConnectionAcquirer, DsqlConnectionDirector, codec, convert},
};

#[derive(Debug)]
pub struct DsqlProjectionLog {
    director: Arc<dyn DsqlConnectionAcquirer>,
}

impl DsqlProjectionLog {
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self {
            director: director as Arc<dyn DsqlConnectionAcquirer>,
        }
    }

    #[cfg(test)]
    fn new_with_acquirer(director: Arc<dyn DsqlConnectionAcquirer>) -> Self {
        Self { director }
    }
}

#[async_trait]
impl ProjectionLog for DsqlProjectionLog {
    #[instrument(skip_all, fields(partition_id = cursor.partition_id, fanout = cursor.fanout, limit))]
    async fn read_from(&self, cursor: &ProjectionCursor, limit: usize) -> Result<ProjectionBatch> {
        validate_cursor_position(cursor)?;
        if limit == 0 {
            return Ok(ProjectionBatch {
                records: Vec::new(),
                next_cursor: cursor.clone(),
            });
        }

        let partition_id =
            convert::i32_from_u32(cursor.partition_id, "projection cursor partition_id")?;
        let fanout = convert::i16_from_u16(cursor.fanout, "projection cursor fanout")?;
        let limit = convert::i64_from_usize(limit, "projection read limit")?;
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let rows = match (cursor.last_run_key, cursor.last_transition_seq) {
            (None, None) => {
                sqlx::query_as::<_, (Uuid, i64, Vec<u8>, Vec<u8>)>(
                    r#"
                    SELECT run_key, transition_seq, context_data, ops_data
                    FROM projection_log
                    WHERE partition_id = $1 AND fanout = $2
                    ORDER BY run_key ASC, transition_seq ASC
                    LIMIT $3
                    "#,
                )
                .bind(partition_id)
                .bind(fanout)
                .bind(limit)
                .fetch_all(permit.connection()?)
                .await?
            }
            (Some(run_key), Some(transition_seq)) => {
                let transition_seq = convert::i64_from_u64(
                    transition_seq.0,
                    "projection cursor last_transition_seq",
                )?;
                sqlx::query_as::<_, (Uuid, i64, Vec<u8>, Vec<u8>)>(
                    r#"
                    SELECT run_key, transition_seq, context_data, ops_data
                    FROM projection_log
                    WHERE partition_id = $1
                      AND fanout = $2
                      AND (run_key, transition_seq) > ($3, $4)
                    ORDER BY run_key ASC, transition_seq ASC
                    LIMIT $5
                    "#,
                )
                .bind(partition_id)
                .bind(fanout)
                .bind(run_key.0)
                .bind(transition_seq)
                .bind(limit)
                .fetch_all(permit.connection()?)
                .await?
            }
            _ => unreachable!("cursor position is validated before query construction"),
        };

        let records = decode_projection_rows(cursor.partition_id, cursor.fanout, rows)?;
        let next_cursor = records
            .last()
            .map(|record| ProjectionCursor {
                partition_id: cursor.partition_id,
                fanout: cursor.fanout,
                last_run_key: Some(record.run_key),
                last_transition_seq: Some(record.transition_seq),
            })
            .unwrap_or_else(|| cursor.clone());

        Ok(ProjectionBatch {
            records,
            next_cursor,
        })
    }
}

fn validate_cursor_position(cursor: &ProjectionCursor) -> Result<()> {
    match (cursor.last_run_key, cursor.last_transition_seq) {
        (None, None) | (Some(_), Some(_)) => Ok(()),
        _ => bail!(
            "projection cursor position must contain both last_run_key and last_transition_seq or neither"
        ),
    }
}

fn decode_projection_rows(
    partition_id: u32,
    fanout: u16,
    rows: Vec<(Uuid, i64, Vec<u8>, Vec<u8>)>,
) -> Result<Vec<ProjectionRecord>> {
    rows.into_iter()
        .map(|(run_key, transition_seq, context_data, ops_data)| {
            let context: ProjectionContext = codec::decode_projection_context(&context_data)?;
            let ops = codec::decode_projection_ops(&ops_data)?;
            Ok(ProjectionRecord {
                partition_id,
                fanout,
                run_key: RunKey(run_key),
                transition_seq: TransitionSeq(convert::u64_from_i64(
                    transition_seq,
                    "projection_log.transition_seq",
                )?),
                context,
                ops,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::dsql::DsqlPermit;

    use super::*;

    #[test]
    fn cursor_position_must_be_pairwise_optional() {
        let run_key = RunKey(Uuid::nil());

        assert!(validate_cursor_position(&ProjectionCursor::beginning(0, 1)).is_ok());
        assert!(
            validate_cursor_position(&ProjectionCursor {
                partition_id: 0,
                fanout: 1,
                last_run_key: Some(run_key),
                last_transition_seq: Some(TransitionSeq(1)),
            })
            .is_ok()
        );
        assert!(
            validate_cursor_position(&ProjectionCursor {
                partition_id: 0,
                fanout: 1,
                last_run_key: Some(run_key),
                last_transition_seq: None,
            })
            .is_err()
        );
        assert!(
            validate_cursor_position(&ProjectionCursor {
                partition_id: 0,
                fanout: 1,
                last_run_key: None,
                last_transition_seq: Some(TransitionSeq(1)),
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_negative_transition_sequence_from_database() {
        assert!(convert::u64_from_i64(-1, "transition_seq").is_err());
    }

    #[tokio::test]
    async fn zero_limit_does_not_acquire_connection() {
        let log = DsqlProjectionLog::new_with_acquirer(Arc::new(PanicAcquirer));
        let cursor = ProjectionCursor::beginning(0, 1);

        let batch = log.read_from(&cursor, 0).await.unwrap();

        assert!(batch.records.is_empty());
        assert_eq!(batch.next_cursor, cursor);
    }

    #[derive(Debug)]
    struct PanicAcquirer;

    #[async_trait]
    impl DsqlConnectionAcquirer for PanicAcquirer {
        async fn acquire(&self, _class: DbClass) -> Result<DsqlPermit> {
            panic!("zero-limit projection reads must not acquire a DSQL connection")
        }
    }
}
