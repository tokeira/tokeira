//! DSQL-backed projection log reader.
//!
//! Projection records are written by transition commits and read by projection
//! workers in a stable `(partition_id, fanout, run_key, transition_seq)` order.
//! This module deliberately owns only the read side; checkpointing belongs to the
//! projection sink because that is the surface the live worker already calls.

use std::{sync::Arc, time::Instant};

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokeira_types::{ProjectionCursor, RunKey, TransitionSeq};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    DbClass, ProjectionBatch, ProjectionContext, ProjectionLog, ProjectionRecord,
    dsql::{DsqlConnectionAcquirer, DsqlConnectionDirector, codec, convert},
    metrics,
};

#[derive(Clone, Debug)]
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
            // A zero-limit poll is a bookkeeping operation. Avoiding a
            // connection checkout keeps idle projection loops from consuming
            // class budget.
            metrics::record_dsql_projection_batch_size(cursor.partition_id, 0);
            return Ok(ProjectionBatch {
                records: Vec::new(),
                next_cursor: cursor.clone(),
            });
        }

        let started = Instant::now();
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
                // Tuple comparison gives a stable strict-after cursor over the
                // same ordering used by the SELECT. This avoids duplicate
                // delivery without requiring wall-clock timestamps.
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
        metrics::record_dsql_projection_read_duration(cursor.partition_id, started.elapsed());
        metrics::record_dsql_projection_batch_size(cursor.partition_id, rows.len());

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
    // The SQL cursor is a composite `(run_key, transition_seq)`. Accepting only
    // both-or-neither prevents ambiguous "start after run but no sequence"
    // semantics.
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
        .map(|(run_key, transition_seq, context_data, _ops_data)| {
            let context: ProjectionContext = codec::decode_projection_context(&context_data)?;
            // `ops_data` remains in the DSQL projection log for build-phase
            // compatibility with existing rows, but the CHASM visibility
            // contract is snapshot-only. Decoding it here would re-expose the
            // retired delta surface to projection consumers.
            Ok(ProjectionRecord {
                partition_id,
                fanout,
                run_key: RunKey(run_key),
                transition_seq: TransitionSeq(convert::u64_from_i64(
                    transition_seq,
                    "projection_log.transition_seq",
                )?),
                context,
            })
        })
        .collect()
}

#[cfg(test)]
fn interpret_read_from(
    entries: &[(RunKey, TransitionSeq)],
    cursor: &ProjectionCursor,
    limit: usize,
) -> Result<(Vec<(RunKey, TransitionSeq)>, ProjectionCursor)> {
    validate_cursor_position(cursor)?;
    if limit == 0 {
        return Ok((Vec::new(), cursor.clone()));
    }

    let selected = entries
        .iter()
        .copied()
        .filter(|(run_key, transition_seq)| {
            match (cursor.last_run_key, cursor.last_transition_seq) {
                (None, None) => true,
                (Some(last_run_key), Some(last_transition_seq)) => {
                    (run_key.0, transition_seq.0) > (last_run_key.0, last_transition_seq.0)
                }
                _ => false,
            }
        })
        .take(limit)
        .collect::<Vec<_>>();
    let next_cursor = selected
        .last()
        .map(|(run_key, transition_seq)| ProjectionCursor {
            partition_id: cursor.partition_id,
            fanout: cursor.fanout,
            last_run_key: Some(*run_key),
            last_transition_seq: Some(*transition_seq),
        })
        .unwrap_or_else(|| cursor.clone());
    Ok((selected, next_cursor))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use proptest::prelude::*;
    use uuid::Uuid;

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

    #[tokio::test]
    async fn read_from_acquires_projection_class() {
        let acquirer = Arc::new(RecordingAcquirer::default());
        let log = DsqlProjectionLog::new_with_acquirer(acquirer.clone());
        let cursor = ProjectionCursor::beginning(0, 1);

        assert!(log.read_from(&cursor, 1).await.is_err());
        assert_eq!(
            acquirer.classes.lock().unwrap().as_slice(),
            &[DbClass::Projection]
        );
    }

    #[test]
    fn beginning_cursor_reads_from_first_entry() {
        let entries = vec![
            (RunKey(Uuid::from_u128(1)), TransitionSeq(1)),
            (RunKey(Uuid::from_u128(2)), TransitionSeq(1)),
        ];
        let cursor = ProjectionCursor::beginning(3, 4);

        let (selected, next_cursor) = interpret_read_from(&entries, &cursor, 1).unwrap();

        assert_eq!(selected, vec![entries[0]]);
        assert_eq!(next_cursor.last_run_key, Some(entries[0].0));
        assert_eq!(next_cursor.last_transition_seq, Some(entries[0].1));
    }

    #[test]
    fn empty_partition_returns_original_cursor() {
        let cursor = ProjectionCursor {
            partition_id: 3,
            fanout: 4,
            last_run_key: Some(RunKey(Uuid::from_u128(1))),
            last_transition_seq: Some(TransitionSeq(7)),
        };

        let (selected, next_cursor) = interpret_read_from(&[], &cursor, 10).unwrap();

        assert!(selected.is_empty());
        assert_eq!(next_cursor, cursor);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn cursor_based_pagination_is_strictly_after_cursor(
            mut entries in proptest::collection::vec((any::<u128>(), 0u64..10_000), 0..100),
            cursor_index in proptest::option::of(0usize..100),
            limit in 1usize..=50,
        ) {
            entries.sort_unstable();
            entries.dedup();
            let entries = entries
                .into_iter()
                .map(|(run_key, transition_seq)| {
                    (RunKey(Uuid::from_u128(run_key)), TransitionSeq(transition_seq))
                })
                .collect::<Vec<_>>();
            let cursor = cursor_index
                .and_then(|index| entries.get(index).copied())
                .map(|(run_key, transition_seq)| ProjectionCursor {
                    partition_id: 0,
                    fanout: 1,
                    last_run_key: Some(run_key),
                    last_transition_seq: Some(transition_seq),
                })
                .unwrap_or_else(|| ProjectionCursor::beginning(0, 1));

            let (selected, next_cursor) = interpret_read_from(&entries, &cursor, limit).unwrap();

            prop_assert!(selected.len() <= limit);
            for window in selected.windows(2) {
                prop_assert!((window[0].0.0, window[0].1.0) < (window[1].0.0, window[1].1.0));
            }
            if let (Some(last_run_key), Some(last_transition_seq)) =
                (cursor.last_run_key, cursor.last_transition_seq)
            {
                for (run_key, transition_seq) in &selected {
                    prop_assert!((run_key.0, transition_seq.0) > (last_run_key.0, last_transition_seq.0));
                }
            }
            if let Some((run_key, transition_seq)) = selected.last() {
                prop_assert_eq!(next_cursor.last_run_key, Some(*run_key));
                prop_assert_eq!(next_cursor.last_transition_seq, Some(*transition_seq));
            } else {
                prop_assert_eq!(next_cursor, cursor);
            }
        }
    }

    #[derive(Debug)]
    struct PanicAcquirer;

    #[async_trait]
    impl DsqlConnectionAcquirer for PanicAcquirer {
        async fn acquire(&self, _class: DbClass) -> Result<DsqlPermit> {
            panic!("zero-limit projection reads must not acquire a DSQL connection")
        }
    }

    #[derive(Debug, Default)]
    struct RecordingAcquirer {
        classes: Mutex<Vec<DbClass>>,
    }

    #[async_trait]
    impl DsqlConnectionAcquirer for RecordingAcquirer {
        async fn acquire(&self, class: DbClass) -> Result<DsqlPermit> {
            self.classes.lock().unwrap().push(class);
            bail!("recording acquirer intentionally has no SQL connection")
        }
    }
}
