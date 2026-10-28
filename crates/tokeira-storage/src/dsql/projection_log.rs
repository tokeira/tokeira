//! DSQL-backed projection log.
//!
//! Transition commits write images; projectors read each partition a page at a time
//! in `(partition_id, fanout, run_key, transition_seq)` order, and delete exactly the
//! images they read once their runs' visibility rows are at least as new
//! (`projection-passes`). Nothing here keeps a position between calls.

use std::{sync::Arc, time::Instant};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use sqlx::{Connection, PgConnection, Postgres, QueryBuilder};
use time::OffsetDateTime;
use tokeira_types::{RunKey, TransitionSeq};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    DbClass, ProjectionContext, ProjectionImageKey, ProjectionLog, ProjectionPage,
    ProjectionRecord,
    dsql::{
        DsqlConnectionAcquirer, DsqlConnectionDirector, codec, convert, projection_image_parts,
    },
    metrics,
};

/// The most images one delete statement names. The transaction also deletes their
/// parts, reducing this batch when their combined row count exceeds 3,000.
pub(crate) const DELETE_STATEMENT_KEYS: usize = 100;

/// The page read from a partition's head.
pub(crate) const READ_HEAD_SQL: &str = r#"
    SELECT run_key, transition_seq, context_data, created_at, context_parts
    FROM projection_log
    WHERE partition_id = $1 AND fanout = $2
    ORDER BY run_key ASC, transition_seq ASC
    LIMIT $3
"#;

/// The page read after a position. The tuple comparison gives a strict-after read in
/// the same order as the `SELECT`. DSQL evaluates it as a filter over the partition's
/// primary-key range, so each read passes over the images still behind the position;
/// a pass deletes what it finishes with, which leaves few.
pub(crate) const READ_AFTER_SQL: &str = r#"
    SELECT run_key, transition_seq, context_data, created_at, context_parts
    FROM projection_log
    WHERE partition_id = $1
      AND fanout = $2
      AND (run_key, transition_seq) > ($3, $4)
    ORDER BY run_key ASC, transition_seq ASC
    LIMIT $5
"#;

type ImageRow = (Uuid, i64, Vec<u8>, OffsetDateTime, Option<i32>);

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
    #[instrument(skip_all, fields(partition_id, fanout, limit))]
    async fn read_page(
        &self,
        partition_id: u32,
        fanout: u16,
        after: Option<ProjectionImageKey>,
        limit: usize,
    ) -> Result<ProjectionPage> {
        if limit == 0 {
            // A zero-limit read is a bookkeeping operation. Avoiding a connection
            // checkout keeps it from consuming class budget.
            metrics::record_dsql_projection_batch_size(partition_id, 0);
            return Ok(ProjectionPage {
                images: Vec::new(),
                oldest_written_at: None,
            });
        }

        let started = Instant::now();
        let db_partition_id = convert::i32_from_u32(partition_id, "projection partition_id")?;
        let db_fanout = convert::i16_from_u16(fanout, "projection fanout")?;
        let db_limit = convert::i64_from_usize(limit, "projection read limit")?;
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let read = ImageRead {
            partition: db_partition_id,
            fanout: db_fanout,
            after,
            limit: db_limit,
        };
        let connection = permit.connection()?;
        let mut rows = read.rows(connection).await?;
        let (images, oldest_written_at) = if image_page_needs_snapshot(&rows) {
            // Throw away the first read. A deletion commits the image and its parts
            // together, so only a snapshot that rereads both can return it whole.
            let mut tx = connection
                .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                .await?;
            rows = read.rows(&mut tx).await?;
            let oldest = rows.iter().map(|row| row.3).min();
            let images = decode_projection_rows(&mut tx, partition_id, fanout, rows).await?;
            tx.commit().await?;
            (images, oldest)
        } else {
            let oldest = rows.iter().map(|row| row.3).min();
            (
                decode_projection_rows(connection, partition_id, fanout, rows).await?,
                oldest,
            )
        };
        metrics::record_dsql_projection_read_duration(partition_id, started.elapsed());
        metrics::record_dsql_projection_batch_size(partition_id, images.len());

        Ok(ProjectionPage {
            images,
            oldest_written_at,
        })
    }

    #[instrument(skip_all, fields(partition_id, fanout, keys = keys.len()))]
    async fn delete_images(
        &self,
        partition_id: u32,
        fanout: u16,
        keys: &[ProjectionImageKey],
    ) -> Result<u64> {
        if keys.is_empty() {
            return Ok(0);
        }
        let db_partition_id = convert::i32_from_u32(partition_id, "projection partition_id")?;
        let db_fanout = convert::i16_from_u16(fanout, "projection fanout")?;
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let mut deleted = 0;
        let mut offset = 0;
        let mut chunk_len = DELETE_STATEMENT_KEYS.min(keys.len());
        while offset < keys.len() {
            let chunk = &keys[offset..offset + chunk_len];
            let mut tx = permit.connection()?.begin().await?;
            let mut query = QueryBuilder::new("");
            push_delete_images(&mut query, db_partition_id, db_fanout, chunk)?;
            query.push(" RETURNING run_key, transition_seq, context_parts");
            let removed = query
                .build_query_as::<(Uuid, i64, Option<i32>)>()
                .fetch_all(&mut *tx)
                .await?;
            let cost = image_delete_rows(removed.iter().map(|row| row.2))?;
            if cost > 3_000 {
                // Counts are immutable with their images. Retry a smaller chunk
                // after rollback; no transaction can delete more than the budget.
                tx.rollback().await?;
                ensure!(
                    chunk_len > 1,
                    "one projection image exceeds the 3,000-row deletion budget"
                );
                chunk_len = chunk_len.div_ceil(2);
                continue;
            }
            let split_keys = removed
                .iter()
                .filter(|row| row.2.is_some())
                .map(|row| {
                    Ok(ProjectionImageKey {
                        run_key: RunKey(row.0),
                        transition_seq: TransitionSeq(convert::u64_from_i64(
                            row.1,
                            "projection transition_seq",
                        )?),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            if !split_keys.is_empty() {
                let mut parts = QueryBuilder::new("");
                push_delete_rows(
                    &mut parts,
                    "projection_image_part",
                    db_partition_id,
                    db_fanout,
                    &split_keys,
                )?;
                parts.build().execute(&mut *tx).await?;
            }
            tx.commit().await?;
            deleted += u64::try_from(removed.len())?;
            offset += chunk_len;
            chunk_len = DELETE_STATEMENT_KEYS.min(keys.len() - offset);
        }
        Ok(deleted)
    }
}

/// Push the statement that deletes `keys` from one partition, naming each image by
/// its whole primary key. Shared with the live `EXPLAIN` check.
pub(crate) fn push_delete_images(
    query: &mut QueryBuilder<Postgres>,
    partition_id: i32,
    fanout: i16,
    keys: &[ProjectionImageKey],
) -> Result<()> {
    push_delete_rows(query, "projection_log", partition_id, fanout, keys)
}

fn push_delete_rows(
    query: &mut QueryBuilder<Postgres>,
    table: &'static str,
    partition_id: i32,
    fanout: i16,
    keys: &[ProjectionImageKey],
) -> Result<()> {
    query
        .push("DELETE FROM ")
        .push(table)
        .push(" WHERE partition_id = ")
        .push_bind(partition_id)
        .push(" AND fanout = ")
        .push_bind(fanout)
        .push(" AND (run_key, transition_seq) IN (VALUES ");
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            query.push(", ");
        }
        query
            .push("(")
            .push_bind(key.run_key.0)
            .push(", ")
            .push_bind(convert::i64_from_u64(
                key.transition_seq.0,
                "projection image transition_seq",
            )?)
            .push(")");
    }
    query.push(")");
    Ok(())
}

/// Presence of any count, even an invalid one, requires a snapshot before joining.
fn image_page_needs_snapshot(rows: &[ImageRow]) -> bool {
    rows.iter().any(|row| row.4.is_some())
}

/// Count the owning rows and all their parts, including rows deleted from either table.
fn image_delete_rows(mut counts: impl Iterator<Item = Option<i32>>) -> Result<usize> {
    counts.try_fold(0usize, |total, count| {
        let count = count.unwrap_or(1);
        ensure!(count >= 1, "invalid projection image part count {count}");
        total
            .checked_add(usize::try_from(count)?)
            .ok_or_else(|| anyhow::anyhow!("projection deletion row count overflow"))
    })
}

struct ImageRead {
    partition: i32,
    fanout: i16,
    after: Option<ProjectionImageKey>,
    limit: i64,
}

impl ImageRead {
    async fn rows(&self, connection: &mut PgConnection) -> Result<Vec<ImageRow>> {
        Ok(match self.after {
            None => {
                sqlx::query_as::<_, ImageRow>(READ_HEAD_SQL)
                    .bind(self.partition)
                    .bind(self.fanout)
                    .bind(self.limit)
                    .fetch_all(connection)
                    .await?
            }
            Some(after) => {
                let transition_seq = convert::i64_from_u64(
                    after.transition_seq.0,
                    "projection page position transition_seq",
                )?;
                sqlx::query_as::<_, ImageRow>(READ_AFTER_SQL)
                    .bind(self.partition)
                    .bind(self.fanout)
                    .bind(after.run_key.0)
                    .bind(transition_seq)
                    .bind(self.limit)
                    .fetch_all(connection)
                    .await?
            }
        })
    }
}

async fn decode_projection_rows(
    connection: &mut PgConnection,
    partition_id: u32,
    fanout: u16,
    rows: Vec<ImageRow>,
) -> Result<Vec<ProjectionRecord>> {
    let mut images = Vec::with_capacity(rows.len());
    for (run_key, transition_seq, context_data, _, count) in rows {
        let key = ProjectionImageKey {
            run_key: RunKey(run_key),
            transition_seq: TransitionSeq(convert::u64_from_i64(
                transition_seq,
                "projection_log.transition_seq",
            )?),
        };
        let data = projection_image_parts::join(
            connection,
            i32::try_from(partition_id)?,
            i16::try_from(fanout)?,
            key,
            context_data,
            count,
        )
        .await?;
        let context: ProjectionContext = codec::decode_projection_context(&data)?;
        images.push(ProjectionRecord {
            partition_id,
            fanout,
            run_key: key.run_key,
            transition_seq: key.transition_seq,
            context,
        });
    }
    Ok(images)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anyhow::bail;
    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::dsql::DsqlPermit;

    use super::*;

    #[test]
    fn only_pages_with_counts_need_a_snapshot() {
        assert!(!image_page_needs_snapshot(&[]));
        for count in [None, Some(0), Some(2), Some(3)] {
            let rows = vec![(
                Uuid::nil(),
                1,
                Vec::new(),
                OffsetDateTime::UNIX_EPOCH,
                count,
            )];
            assert_eq!(image_page_needs_snapshot(&rows), count.is_some());
        }
    }

    #[test]
    fn deletion_budget_includes_every_parts_row() {
        assert_eq!(
            image_delete_rows([None, Some(3), Some(2)].into_iter()).unwrap(),
            6
        );
        assert_eq!(image_delete_rows([Some(3); 100].into_iter()).unwrap(), 300);
        assert!(image_delete_rows([Some(-1)].into_iter()).is_err());
        assert!(image_delete_rows([Some(31); 100].into_iter()).unwrap() > 3_000);
    }

    #[test]
    fn rejects_negative_transition_sequence_from_database() {
        assert!(convert::u64_from_i64(-1, "transition_seq").is_err());
    }

    #[tokio::test]
    async fn zero_limit_does_not_acquire_connection() {
        let log = DsqlProjectionLog::new_with_acquirer(Arc::new(PanicAcquirer));

        let page = log.read_page(0, 1, None, 0).await.unwrap();

        assert!(page.images.is_empty());
        assert_eq!(page.oldest_written_at, None);
    }

    #[tokio::test]
    async fn deleting_no_keys_does_not_acquire_connection() {
        let log = DsqlProjectionLog::new_with_acquirer(Arc::new(PanicAcquirer));

        assert_eq!(log.delete_images(0, 1, &[]).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn reads_and_deletes_acquire_the_projection_class() {
        let acquirer = Arc::new(RecordingAcquirer::default());
        let log = DsqlProjectionLog::new_with_acquirer(acquirer.clone());
        let key = ProjectionImageKey {
            run_key: RunKey(Uuid::nil()),
            transition_seq: TransitionSeq(1),
        };

        assert!(log.read_page(0, 1, None, 1).await.is_err());
        assert!(log.read_page(0, 1, Some(key), 1).await.is_err());
        assert!(log.delete_images(0, 1, &[key]).await.is_err());
        assert_eq!(
            acquirer.classes.lock().unwrap().as_slice(),
            &[DbClass::Projection; 3]
        );
    }

    #[test]
    fn a_delete_names_each_image_by_its_whole_key() {
        let keys = (1..=3)
            .map(|index| ProjectionImageKey {
                run_key: RunKey(Uuid::from_u128(index)),
                transition_seq: TransitionSeq(index as u64),
            })
            .collect::<Vec<_>>();

        let mut query = QueryBuilder::new("");
        push_delete_images(&mut query, 7, 1, &keys).unwrap();

        assert_eq!(
            query.sql().as_str(),
            "DELETE FROM projection_log WHERE partition_id = $1 AND fanout = $2 \
             AND (run_key, transition_seq) IN (VALUES ($3, $4), ($5, $6), ($7, $8))"
        );
    }

    #[test]
    fn a_delete_refuses_a_transition_past_the_column() {
        let key = ProjectionImageKey {
            run_key: RunKey(Uuid::nil()),
            transition_seq: TransitionSeq(u64::MAX),
        };

        assert!(push_delete_images(&mut QueryBuilder::new(""), 0, 1, &[key]).is_err());
    }

    /// The log against an operator-owned ephemeral cluster. No connection occurs
    /// unless the dedicated test URL is provided.
    #[cfg(feature = "dsql-integration")]
    mod live {
        use std::path::Path;

        use sqlx::{PgPool, postgres::PgPoolOptions};
        use tokeira_types::{
            ArchetypeId, ExecutionStatus, Memo, NamespaceId, RunId, SearchAttributes,
            TaskQueueName, VisibilityLifecycleState, WorkflowId, WorkflowType,
        };

        use super::*;
        use crate::dsql::{DsqlPoolConfig, DsqlStore, MigrationConfig};

        /// Runs, and images of each, in the partition whose plans are read.
        const PLAN_RUNS: u128 = 500;
        const PLAN_TRANSITIONS: i64 = 4;

        fn context(run_key: RunKey) -> ProjectionContext {
            let workflow_id = WorkflowId(format!("workflow-{}", run_key.0));
            ProjectionContext {
                archetype_id: ArchetypeId::WORKFLOW,
                namespace_id: NamespaceId(Uuid::from_u128(1)),
                business_id: workflow_id.0.clone(),
                authority_epoch: 0,
                status_keyword: "Running".to_owned(),
                lifecycle_state: VisibilityLifecycleState::Open,
                workflow_id,
                run_id: RunId(Uuid::new_v4()),
                workflow_type: WorkflowType("workflow-type".to_owned()),
                task_queue: TaskQueueName("queue".to_owned()),
                execution_status: ExecutionStatus::Running,
                start_time: OffsetDateTime::UNIX_EPOCH,
                update_time: OffsetDateTime::UNIX_EPOCH,
                execution_time: None,
                close_time: None,
                history_length: 1,
                execution_duration: None,
                state_transition_count: 1,
                transition_count: 1,
                history_size_bytes: 0,
                parent_workflow_id: None,
                parent_run_id: None,
                root_workflow_id: None,
                root_run_id: None,
                search_attr_generation: 0,
                memo: Memo::default(),
                search_attributes: SearchAttributes::default(),
            }
        }

        async fn plan<'q>(
            pool: &PgPool,
            explain: sqlx::query::QueryAs<'q, Postgres, (String,), sqlx::postgres::PgArguments>,
        ) -> Result<Vec<String>> {
            Ok(explain
                .fetch_all(pool)
                .await?
                .into_iter()
                .map(|(line,)| line)
                .collect())
        }

        fn assert_reads_the_partition_by_key(lines: &[String]) {
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains("using projection_log_pkey")),
                "a lookup through the primary key: {lines:#?}"
            );
            assert!(
                lines.iter().any(|line| line.contains("Index Cond:")
                    && line.contains("partition_id =")
                    && line.contains("fanout =")),
                "the partition in the index condition: {lines:#?}"
            );
            assert!(
                !lines.iter().any(|line| line.contains("Sort")),
                "the key order without a sort: {lines:#?}"
            );
        }

        // `projection-passes` criteria 2.1 and 2.3: a partition's pages follow its key
        // order and read it through the primary key, and a delete removes exactly the
        // images it names, through the primary key, each at most once.
        #[tokio::test]
        async fn dsql_projection_log_pages_and_deletes_through_the_primary_key() -> Result<()> {
            let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
                return Ok(());
            };
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await?;
            let store = DsqlStore::from_database_url_for_tests(
                url,
                DsqlPoolConfig {
                    migration: MigrationConfig {
                        migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations"),
                    },
                    ..DsqlPoolConfig::default()
                },
            )
            .await?;
            store.migration_runner().apply(&pool).await?;
            let log = store.projection_log().clone();
            // A partition no other suite writes, so the pages hold only these images.
            let partition_id = ((Uuid::new_v4().as_u128() as u32) & 0x3fff_ffff) | 0x4000_0000;
            let db_partition_id = i32::try_from(partition_id)?;
            let mut keys = Vec::new();
            for run in 0..PLAN_RUNS {
                let run_key = RunKey(Uuid::from_u128((run << 64) | u128::from(partition_id)));
                let mut insert = QueryBuilder::<Postgres>::new(
                    "INSERT INTO projection_log \
                     (partition_id, fanout, run_key, transition_seq, context_data, ops_data) ",
                );
                insert.push_values(1..=PLAN_TRANSITIONS, |mut row, transition_seq| {
                    row.push_bind(db_partition_id)
                        .push_bind(1i16)
                        .push_bind(run_key.0)
                        .push_bind(transition_seq)
                        .push_bind(codec::encode_projection_context(&context(run_key)).unwrap())
                        .push_bind(codec::LEGACY_EMPTY_PROJECTION_OPS_DATA);
                });
                insert.build().execute(&pool).await?;
                keys.extend(
                    (1..=PLAN_TRANSITIONS).map(|transition_seq| ProjectionImageKey {
                        run_key,
                        transition_seq: TransitionSeq(transition_seq as u64),
                    }),
                );
            }
            keys.sort_unstable();
            sqlx::query("ANALYZE projection_log").execute(&pool).await?;

            let mut read = Vec::new();
            let mut after = None;
            loop {
                let page = log.read_page(partition_id, 1, after, 256).await?;
                assert!(page.oldest_written_at.is_some() || page.images.is_empty());
                let Some(last) = page.images.last() else {
                    break;
                };
                after = Some(last.key());
                read.extend(page.images.iter().map(ProjectionRecord::key));
            }
            assert_eq!(read, keys, "every image once, in key order");

            let first = &keys[..DELETE_STATEMENT_KEYS + 50];
            assert_eq!(
                log.delete_images(partition_id, 1, first).await?,
                first.len() as u64
            );
            assert_eq!(log.delete_images(partition_id, 1, first).await?, 0);
            let head = log.read_page(partition_id, 1, None, 1).await?;
            assert_eq!(head.images[0].key(), keys[first.len()]);

            let position = keys[keys.len() / 2];
            let mut explain = QueryBuilder::<Postgres>::new("EXPLAIN ANALYZE ");
            explain.push(READ_HEAD_SQL);
            let head_lines = plan(
                &pool,
                explain
                    .build_query_as::<(String,)>()
                    .bind(db_partition_id)
                    .bind(1i16)
                    .bind(256i64),
            )
            .await?;
            assert_reads_the_partition_by_key(&head_lines);
            let mut explain = QueryBuilder::<Postgres>::new("EXPLAIN ANALYZE ");
            explain.push(READ_AFTER_SQL);
            let after_lines = plan(
                &pool,
                explain
                    .build_query_as::<(String,)>()
                    .bind(db_partition_id)
                    .bind(1i16)
                    .bind(position.run_key.0)
                    .bind(i64::try_from(position.transition_seq.0)?)
                    .bind(256i64),
            )
            .await?;
            assert_reads_the_partition_by_key(&after_lines);
            // A plain `EXPLAIN`: analysing the delete would run it.
            let mut explain = QueryBuilder::<Postgres>::new("EXPLAIN ");
            push_delete_images(
                &mut explain,
                db_partition_id,
                1,
                &keys[keys.len() - DELETE_STATEMENT_KEYS..],
            )?;
            let delete_lines = plan(&pool, explain.build_query_as::<(String,)>()).await?;
            assert!(
                delete_lines
                    .iter()
                    .any(|line| line.contains("projection_log_pkey")),
                "the delete reaches its rows through the primary key: {delete_lines:#?}"
            );
            assert!(
                delete_lines.iter().any(|line| line.contains("Index Cond:")
                    && line.contains("run_key")
                    && line.contains("transition_seq")),
                "each named key in the index condition: {delete_lines:#?}"
            );
            let mut report = String::new();
            for (name, lines) in [
                ("page from the head", &head_lines),
                ("page after a position", &after_lines),
                ("delete of 100 named images", &delete_lines),
            ] {
                report.push_str(&format!("The {name}:\n"));
                for line in lines {
                    report.push_str(line);
                    report.push('\n');
                }
            }
            if let Some(path) = std::env::var_os("TOKEIRA_PROJECTION_LOG_PLAN_OUTPUT") {
                std::fs::write(path, report)?;
            }

            sqlx::query("DELETE FROM projection_log WHERE partition_id = $1 AND fanout = 1")
                .bind(db_partition_id)
                .execute(&pool)
                .await?;
            pool.close().await;
            store.shutdown().await
        }
    }

    #[derive(Debug)]
    struct PanicAcquirer;

    #[async_trait]
    impl DsqlConnectionAcquirer for PanicAcquirer {
        async fn acquire(&self, _class: DbClass) -> Result<DsqlPermit> {
            panic!("an empty projection read or delete must not acquire a DSQL connection")
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
