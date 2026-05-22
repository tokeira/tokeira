//! DSQL-backed visibility store and projection sink.
//!
//! This implementation lives in the projection crate because the live worker
//! owns the `ProjectionSink` and `VisibilityStore` traits. Storage provides the
//! DSQL connection foundation and codecs; projection owns how semantic projection
//! ops become visibility rows.

use std::{collections::BTreeSet, sync::Arc, time::Instant};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use sqlx::{
    Connection, PgConnection, Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::Query,
};
use tokeira_kernel::ProjectionOp;
use tokeira_storage::{
    ConnectionDirector, DbClass, ProjectionRecord,
    dsql::{DsqlConnectionDirector, codec},
    metrics as storage_metrics,
};
use tokeira_types::{
    ExecutionStatus, Memo, NamespaceId, ProjectionCursor, RunKey, SearchAttrValue,
    SearchAttributes, dsql_spread_uuid,
};
use tracing::{instrument, warn};
use uuid::Uuid;

use crate::{
    ProjectionSink, VisibilityStore, metrics as projection_metrics,
    rollup::compute_rollup_deltas,
    types::{
        AttrDescriptor, AttrId, CompareOp, CompiledFilter, CountResult, ExecutionRow, FieldRef,
        FilterExpr, FilterValue, GroupByField, ListResult, PageBounds, PageToken, RollupCounter,
        RollupDelta, RollupDimension, SearchAttrType, SortOrder, SystemField, search_attr_type_of,
        text_search_tokens,
    },
};

#[derive(Clone, Debug)]
pub struct DsqlVisibilityStore {
    director: Arc<DsqlConnectionDirector>,
}

impl DsqlVisibilityStore {
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self { director }
    }

    fn is_occ_conflict(error: &anyhow::Error) -> bool {
        error
            .chain()
            .find_map(|cause| cause.downcast_ref::<sqlx::Error>())
            .is_some_and(|error| {
                matches!(
                    error,
                    sqlx::Error::Database(database_error)
                        if matches!(database_error.code().as_deref(), Some("OC000" | "40001"))
                )
            })
    }

    fn retry_delay(attempt: u32) -> tokio::time::Duration {
        let jitter = u64::from(attempt.wrapping_mul(17) % 50);
        tokio::time::Duration::from_millis(10 * u64::from(attempt) + jitter)
    }

    async fn accumulate_rollup_partitioned(
        &self,
        partition_id: u32,
        entries: &[RollupDelta],
    ) -> Result<()> {
        for entry in entries {
            let mut attempts = 0u32;
            loop {
                let mut permit = self.director.acquire(DbClass::Projection).await?;
                let started = Instant::now();
                let result = sqlx::query(
                    r#"
                    INSERT INTO vis_rollup (namespace_id, dimension, value, partition_id, counter)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (namespace_id, dimension, value, partition_id) DO UPDATE
                    SET counter = vis_rollup.counter + EXCLUDED.counter
                    "#,
                )
                .bind(entry.namespace_id.0)
                .bind(entry.dimension.to_db_smallint())
                .bind(&entry.value)
                .bind(i32_from_u32(partition_id, "vis_rollup.partition_id")?)
                .bind(entry.delta)
                .execute(permit.connection()?)
                .await
                .map(|_| ())
                .map_err(anyhow::Error::from);
                storage_metrics::record_dsql_statement_duration(
                    "projection_apply",
                    "upsert_rollup",
                    started.elapsed(),
                );
                drop(permit);

                match result {
                    Ok(()) => {
                        if attempts > 0 {
                            storage_metrics::record_dsql_retry(
                                tokeira_observability::StorageOperationLabel::AccumulateRollup,
                                tokeira_observability::RetryOutcomeLabel::Success,
                            );
                        }
                        break;
                    }
                    Err(error) if Self::is_occ_conflict(&error) && attempts < 5 => {
                        attempts += 1;
                        storage_metrics::record_dsql_occ_conflict(
                            tokeira_observability::StorageOperationLabel::AccumulateRollup,
                        );
                        tokio::time::sleep(Self::retry_delay(attempts)).await;
                    }
                    Err(error) if Self::is_occ_conflict(&error) => {
                        storage_metrics::record_dsql_retry(
                            tokeira_observability::StorageOperationLabel::AccumulateRollup,
                            tokeira_observability::RetryOutcomeLabel::Exhausted,
                        );
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl VisibilityStore for DsqlVisibilityStore {
    #[instrument(skip_all, fields(run_key = %row.run_key.0))]
    async fn upsert_execution(&self, row: &ExecutionRow) -> Result<()> {
        let memo = codec::encode(&row.memo)?;
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        upsert_execution_row(permit.connection()?, row, Some(memo)).await
    }

    #[instrument(skip_all, fields(run_key = %run_key.0))]
    async fn delete_execution(&self, run_key: RunKey) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        sqlx::query("DELETE FROM vis_execution WHERE run_key = $1")
            .bind(run_key.0)
            .execute(permit.connection()?)
            .await?;
        Ok(())
    }

    async fn upsert_search_attr_index(
        &self,
        run_key: RunKey,
        namespace_id: NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
        value: &SearchAttrValue,
    ) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let connection = permit.connection()?;
        upsert_search_attr_index_row(connection, run_key, namespace_id, attr_id, attr_type, value)
            .await
    }

    async fn remove_search_attr_index(
        &self,
        run_key: RunKey,
        namespace_id: NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
    ) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let connection = permit.connection()?;
        remove_search_attr_index_row(connection, run_key, namespace_id, attr_id, attr_type).await
    }

    async fn accumulate_rollup(&self, entries: &[RollupDelta]) -> Result<()> {
        self.accumulate_rollup_partitioned(0, entries).await
    }

    async fn list_executions(
        &self,
        namespace_id: NamespaceId,
        filter: &CompiledFilter,
        sort: SortOrder,
        page: &PageBounds,
    ) -> Result<ListResult> {
        let (filter_sql, mut values, next_param) = compile_filter(filter, 2)?;
        let (cursor_sql, cursor_values, next_param) =
            cursor_predicate(sort, page.after.as_ref(), next_param)?;
        values.extend(cursor_values);
        let limit = page.limit.min(crate::types::MAX_PAGE_SIZE);
        let sql = format!(
            r#"
            SELECT
                run_key,
                namespace_id,
                workflow_id,
                run_id,
                workflow_type,
                task_queue,
                execution_status,
                start_time,
                execution_time,
                close_time,
                history_length,
                state_transition_count,
                memo
            FROM vis_execution
            WHERE namespace_id = $1
              {filter_sql}
              {cursor_sql}
            ORDER BY {}
            LIMIT ${}
            "#,
            sort_clause(sort),
            next_param
        );
        let mut query = sqlx::query(&sql).bind(namespace_id.0);
        query = bind_sql_values(query, &values);
        query = query.bind(i64::try_from(limit + 1)?);
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let referenced_tables = referenced_search_attr_index_tables(filter, None)?;
        let started = Instant::now();
        let rows = query.fetch_all(permit.connection()?).await;
        let duration = started.elapsed();
        projection_metrics::record_visibility_query_duration("list", duration);
        for table in referenced_tables {
            projection_metrics::record_sa_index_scan_duration(table, duration);
        }
        let rows = rows?;
        let mut executions = rows
            .into_iter()
            .map(row_to_execution)
            .collect::<Result<Vec<_>>>()?;
        let next_page_token = if executions.len() > limit {
            let last = executions[limit - 1].clone();
            executions.truncate(limit);
            Some(PageToken {
                close_time: last.close_time,
                start_time: last.start_time,
                run_key: last.run_key,
                sort_order: sort,
            })
        } else {
            None
        };
        Ok(ListResult {
            rows: executions,
            next_page_token,
        })
    }

    async fn count_executions(
        &self,
        namespace_id: NamespaceId,
        filter: &CompiledFilter,
        group_by: Option<GroupByField>,
    ) -> Result<CountResult> {
        let group_attr_type = match group_by.as_ref() {
            Some(GroupByField::Custom { attr_type, .. }) => Some(*attr_type),
            _ => None,
        };
        let referenced_tables = referenced_search_attr_index_tables(filter, group_attr_type)?;
        let started = Instant::now();
        let result = match group_by {
            Some(GroupByField::Custom {
                attr_id, attr_type, ..
            }) => {
                count_custom_group(
                    self.director.as_ref(),
                    namespace_id,
                    filter,
                    attr_id,
                    attr_type,
                )
                .await
            }
            Some(GroupByField::System(field)) => {
                count_system_group(self.director.as_ref(), namespace_id, filter, field).await
            }
            None => count_without_group(self.director.as_ref(), namespace_id, filter).await,
        };
        let duration = started.elapsed();
        projection_metrics::record_visibility_query_duration("count", duration);
        for table in referenced_tables {
            projection_metrics::record_sa_index_scan_duration(table, duration);
        }
        result
    }

    async fn count_from_rollup(
        &self,
        namespace_id: NamespaceId,
        dimension: RollupDimension,
    ) -> Result<CountResult> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let rows = sqlx::query(
            r#"
            SELECT value, SUM(counter) AS counter
            FROM vis_rollup
            WHERE namespace_id = $1 AND dimension = $2
            GROUP BY value
            "#,
        )
        .bind(namespace_id.0)
        .bind(dimension.to_db_smallint())
        .fetch_all(permit.connection()?)
        .await?;
        rows_to_count_result(rows)
    }

    // The trait takes only sink_id. The runtime must ensure sink_id is unique
    // per (partition_id, fanout) substream. When multi-partition-per-sink is
    // added, the trait signature will need to accept partition/fanout.
    #[instrument(skip_all, fields(sink_id))]
    async fn load_checkpoint(&self, sink_id: &str) -> Result<Option<ProjectionCursor>> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let row = sqlx::query(
            r#"
            SELECT last_applied_cursor
            FROM projector_checkpoint
            WHERE sink_id = $1
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(sink_id)
        .fetch_optional(permit.connection()?)
        .await?;

        row.map(|row| {
            let data: Vec<u8> = row.try_get("last_applied_cursor")?;
            codec::decode_projection_cursor(&data)
        })
        .transpose()
    }

    #[instrument(skip_all, fields(sink_id, partition_id = cursor.partition_id, fanout = cursor.fanout))]
    async fn save_checkpoint(&self, sink_id: &str, cursor: &ProjectionCursor) -> Result<()> {
        let partition_id = i32_from_u32(cursor.partition_id, "projection cursor partition_id")?;
        let fanout = i16_from_u16(cursor.fanout, "projection cursor fanout")?;
        let data = codec::encode_projection_cursor(cursor)?;
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let started = Instant::now();
        let result = sqlx::query(
            r#"
            INSERT INTO projector_checkpoint (
                sink_id,
                partition_id,
                fanout,
                last_applied_cursor,
                updated_at
            )
            VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (sink_id, partition_id, fanout) DO UPDATE
            SET last_applied_cursor = EXCLUDED.last_applied_cursor,
                updated_at = now()
            "#,
        )
        .bind(sink_id)
        .bind(partition_id)
        .bind(fanout)
        .bind(data)
        .execute(permit.connection()?)
        .await;
        projection_metrics::record_checkpoint_write_duration(
            cursor.partition_id,
            started.elapsed(),
        );
        result?;
        Ok(())
    }

    async fn resolve_attr(
        &self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<AttrDescriptor>> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let row = sqlx::query(
            r#"
            SELECT attr_id, attr_type
            FROM sa_registry
            WHERE namespace_id = $1 AND attr_name = $2
            "#,
        )
        .bind(namespace_id.0)
        .bind(name)
        .fetch_optional(permit.connection()?)
        .await?;
        row.map(row_to_attr_descriptor).transpose()
    }

    async fn register_attr(
        &self,
        namespace_id: NamespaceId,
        name: String,
        attr_type: SearchAttrType,
    ) -> Result<AttrId> {
        let attr_id = deterministic_attr_id(namespace_id, &name);
        let attr_id_i64 = i64_from_u64(attr_id.0, "search attribute id")?;
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let insert = sqlx::query(
            r#"
            INSERT INTO sa_registry (attr_id, namespace_id, attr_name, attr_type)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (namespace_id, attr_name) DO NOTHING
            RETURNING attr_id
            "#,
        )
        .bind(attr_id_i64)
        .bind(namespace_id.0)
        .bind(&name)
        .bind(attr_type.to_db_smallint())
        .fetch_optional(permit.connection()?)
        .await;

        match insert {
            Ok(Some(row)) => return attr_id_from_i64(row.try_get("attr_id")?),
            Ok(None) => {}
            Err(sqlx::Error::Database(error)) if error.constraint() == Some("sa_registry_pkey") => {
                bail!(
                    "search attribute id hash collision for namespace {} attribute {name}",
                    namespace_id.0
                );
            }
            Err(error) => return Err(error.into()),
        }

        let row = sqlx::query(
            r#"
            SELECT attr_id, attr_type
            FROM sa_registry
            WHERE namespace_id = $1 AND attr_name = $2
            "#,
        )
        .bind(namespace_id.0)
        .bind(&name)
        .fetch_one(permit.connection()?)
        .await?;
        let existing_type: i16 = row.try_get("attr_type")?;
        let existing_type = SearchAttrType::try_from(existing_type)?;
        if existing_type != attr_type {
            bail!(
                "search attribute {name} already registered as {:?}, not {:?}",
                existing_type,
                attr_type
            );
        }
        attr_id_from_i64(row.try_get("attr_id")?)
    }

    // The trait returns Option, not Result, so transient DSQL errors
    // (connection timeout, OCC) are indistinguishable from "row not found."
    // This is a trait-level limitation — the warn! log is the best we can do.
    async fn get_row(&self, run_key: RunKey) -> Option<ExecutionRow> {
        match self.director.acquire(DbClass::Projection).await {
            Ok(mut permit) => match get_execution_row(permit.connection().ok()?, run_key).await {
                Ok(row) => row,
                Err(error) => {
                    warn!(%error, run_key = %run_key.0, "failed to read DSQL visibility row");
                    None
                }
            },
            Err(error) => {
                warn!(%error, run_key = %run_key.0, "failed to acquire DSQL projection permit");
                None
            }
        }
    }
}

#[async_trait]
impl ProjectionSink for DsqlVisibilityStore {
    #[instrument(skip_all, fields(run_key = %record.run_key.0, transition_seq = record.transition_seq.0))]
    async fn apply(&self, record: &ProjectionRecord, partition_id: u32) -> Result<()> {
        let sink_started = Instant::now();
        let previous = self.get_row(record.run_key).await;
        let mut row = previous.clone().unwrap_or_else(|| ExecutionRow {
            run_key: record.run_key,
            namespace_id: record.context.namespace_id,
            workflow_id: record.context.workflow_id.clone(),
            run_id: record.context.run_id,
            workflow_type: record.context.workflow_type.clone(),
            task_queue: record.context.task_queue.clone(),
            status: record.context.execution_status,
            start_time: record.context.start_time,
            execution_time: record.context.execution_time,
            close_time: record.context.close_time,
            history_length: record.context.history_length,
            state_transition_count: record.context.state_transition_count,
            memo: Memo::default(),
            search_attr_version: 0,
        });
        let mut search_patch = SearchAttributes::default();

        row.namespace_id = record.context.namespace_id;
        row.workflow_id = record.context.workflow_id.clone();
        row.run_id = record.context.run_id;
        row.workflow_type = record.context.workflow_type.clone();
        row.task_queue = record.context.task_queue.clone();
        row.start_time = record.context.start_time;
        row.execution_time = record.context.execution_time;
        row.history_length = record.context.history_length;
        row.state_transition_count = record.context.state_transition_count;

        for op in &record.ops {
            match op {
                ProjectionOp::UpsertExecution {
                    status,
                    memo_patch,
                    search_attr_patch,
                } => {
                    row.status = *status;
                    row.memo.0.extend(memo_patch.0.clone());
                    search_patch.0.extend(search_attr_patch.0.clone());
                }
                ProjectionOp::CloseExecution { status, closed_at } => {
                    row.status = *status;
                    row.close_time = Some(*closed_at);
                }
            }
        }

        let mut resolved_search_attrs = Vec::new();
        for (name, value) in &search_patch.0 {
            let Some(attr) = self.resolve_attr(record.context.namespace_id, name).await? else {
                projection_metrics::record_sink_error_with_kind(
                    partition_id,
                    tokeira_observability::ProjectionErrorKindLabel::Sink,
                );
                bail!("unknown search attribute: {name}");
            };
            let actual = search_attr_type_of(value);
            if attr.attr_type != actual {
                projection_metrics::record_sink_error_with_kind(
                    partition_id,
                    tokeira_observability::ProjectionErrorKindLabel::Serialization,
                );
                bail!(
                    "search attribute type mismatch for {name}: expected {:?}, got {:?}",
                    attr.attr_type,
                    actual
                );
            }
            resolved_search_attrs.push((attr, value));
            row.search_attr_version += 1;
        }

        let memo = codec::encode(&row.memo)?;
        let mut attempts = 0u32;
        loop {
            let mut permit = self.director.acquire(DbClass::Projection).await?;
            let result = async {
                let mut tx = permit.connection()?.begin().await?;

                let started = Instant::now();
                upsert_execution_row(&mut *tx, &row, Some(memo.clone())).await?;
                storage_metrics::record_dsql_statement_duration(
                    "projection_apply",
                    "upsert_execution",
                    started.elapsed(),
                );

                for (attr, value) in &resolved_search_attrs {
                    remove_search_attr_index_row(
                        &mut *tx,
                        record.run_key,
                        record.context.namespace_id,
                        attr.attr_id,
                        attr.attr_type,
                    )
                    .await?;
                    let started = Instant::now();
                    upsert_search_attr_index_row(
                        &mut *tx,
                        record.run_key,
                        record.context.namespace_id,
                        attr.attr_id,
                        attr.attr_type,
                        value,
                    )
                    .await?;
                    storage_metrics::record_dsql_statement_duration(
                        "projection_apply",
                        "upsert_search_attr",
                        started.elapsed(),
                    );
                }

                tx.commit().await?;
                Ok::<(), anyhow::Error>(())
            }
            .await;
            drop(permit);

            match result {
                Ok(()) => break,
                Err(error) if Self::is_occ_conflict(&error) && attempts < 5 => {
                    attempts += 1;
                    storage_metrics::record_dsql_occ_conflict(
                        tokeira_observability::StorageOperationLabel::ProjectionApplyTx,
                    );
                    tokio::time::sleep(Self::retry_delay(attempts)).await;
                }
                Err(error) if Self::is_occ_conflict(&error) => {
                    storage_metrics::record_dsql_retry(
                        tokeira_observability::StorageOperationLabel::ProjectionApplyTx,
                        tokeira_observability::RetryOutcomeLabel::Exhausted,
                    );
                    projection_metrics::record_sink_error_with_kind(
                        partition_id,
                        tokeira_observability::ProjectionErrorKindLabel::Storage,
                    );
                    return Err(error);
                }
                Err(error) => {
                    projection_metrics::record_sink_error_with_kind(
                        partition_id,
                        tokeira_observability::ProjectionErrorKindLabel::Storage,
                    );
                    return Err(error);
                }
            }
        }
        if attempts > 0 {
            storage_metrics::record_dsql_retry(
                tokeira_observability::StorageOperationLabel::ProjectionApplyTx,
                tokeira_observability::RetryOutcomeLabel::Success,
            );
        }

        let deltas = compute_rollup_deltas(previous.as_ref(), &row);
        if let Err(error) = self
            .accumulate_rollup_partitioned(partition_id, &deltas)
            .await
        {
            projection_metrics::record_sink_error_with_kind(
                partition_id,
                tokeira_observability::ProjectionErrorKindLabel::Storage,
            );
            return Err(error);
        }
        projection_metrics::record_sink_write_duration(partition_id, sink_started.elapsed());
        Ok(())
    }
}

async fn upsert_execution_row(
    connection: &mut PgConnection,
    row: &ExecutionRow,
    memo: Option<Vec<u8>>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO vis_execution (
            run_key,
            namespace_id,
            workflow_id,
            run_id,
            workflow_type,
            task_queue,
            execution_status,
            start_time,
            execution_time,
            close_time,
            history_length,
            state_transition_count,
            memo
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (run_key) DO UPDATE
        SET execution_status = EXCLUDED.execution_status,
            execution_time = EXCLUDED.execution_time,
            close_time = EXCLUDED.close_time,
            history_length = EXCLUDED.history_length,
            state_transition_count = EXCLUDED.state_transition_count,
            memo = COALESCE(EXCLUDED.memo, vis_execution.memo)
        "#,
    )
    .bind(row.run_key.0)
    .bind(row.namespace_id.0)
    .bind(&row.workflow_id.0)
    .bind(row.run_id.0)
    .bind(&row.workflow_type.0)
    .bind(&row.task_queue.0)
    .bind(row.status.to_db_smallint())
    .bind(row.start_time)
    .bind(row.execution_time)
    .bind(row.close_time)
    .bind(row.history_length)
    .bind(row.state_transition_count)
    .bind(memo)
    .execute(connection)
    .await?;
    Ok(())
}

async fn upsert_search_attr_index_row(
    connection: &mut PgConnection,
    run_key: RunKey,
    namespace_id: NamespaceId,
    attr_id: AttrId,
    attr_type: SearchAttrType,
    value: &SearchAttrValue,
) -> Result<()> {
    let attr_id = i64_from_u64(attr_id.0, "search attribute id")?;
    let value_data = codec::encode(value)?;
    sqlx::query(
        r#"
        INSERT INTO sa_current (run_key, attr_id, value_data)
        VALUES ($1, $2, $3)
        ON CONFLICT (run_key, attr_id) DO UPDATE
        SET value_data = EXCLUDED.value_data
        "#,
    )
    .bind(run_key.0)
    .bind(attr_id)
    .bind(value_data)
    .execute(&mut *connection)
    .await?;

    match (attr_type, value) {
        (SearchAttrType::Keyword, SearchAttrValue::Keyword(value)) => {
            insert_text_index(
                connection,
                "sa_keyword_idx",
                namespace_id,
                attr_id,
                value,
                run_key,
            )
            .await?;
        }
        (SearchAttrType::KeywordList, SearchAttrValue::KeywordList(values)) => {
            for value in values {
                insert_text_index(
                    connection,
                    "sa_keyword_list_idx",
                    namespace_id,
                    attr_id,
                    value,
                    run_key,
                )
                .await?;
            }
        }
        (SearchAttrType::Int, SearchAttrValue::Int(value)) => {
            sqlx::query(
                r#"
                INSERT INTO sa_int_idx (namespace_id, attr_id, value, run_key)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(*value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Bool, SearchAttrValue::Bool(value)) => {
            sqlx::query(
                r#"
                INSERT INTO sa_bool_idx (namespace_id, attr_id, value, run_key)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(*value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Double, SearchAttrValue::Double(value)) => {
            sqlx::query(
                r#"
                INSERT INTO sa_double_idx (namespace_id, attr_id, value, run_key)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(*value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Datetime, SearchAttrValue::Datetime(value)) => {
            sqlx::query(
                r#"
                INSERT INTO sa_datetime_idx (namespace_id, attr_id, value, run_key)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(*value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Text, SearchAttrValue::Text(value)) => {
            for token in text_search_tokens(value) {
                insert_text_index(
                    connection,
                    "sa_text_token_idx",
                    namespace_id,
                    attr_id,
                    &token,
                    run_key,
                )
                .await?;
            }
        }
        (expected, actual) => {
            bail!(
                "search attribute type mismatch: expected {:?}, got {:?}",
                expected,
                search_attr_type_of(actual)
            );
        }
    }
    Ok(())
}

async fn remove_search_attr_index_row(
    connection: &mut PgConnection,
    run_key: RunKey,
    namespace_id: NamespaceId,
    attr_id: AttrId,
    attr_type: SearchAttrType,
) -> Result<()> {
    let attr_id = i64_from_u64(attr_id.0, "search attribute id")?;
    let row = sqlx::query("SELECT value_data FROM sa_current WHERE run_key = $1 AND attr_id = $2")
        .bind(run_key.0)
        .bind(attr_id)
        .fetch_optional(&mut *connection)
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let data: Vec<u8> = row.try_get("value_data")?;
    let value = codec::decode::<SearchAttrValue>(&data)?;

    match (attr_type, value) {
        (SearchAttrType::Keyword, SearchAttrValue::Keyword(value)) => {
            delete_text_index(
                connection,
                "sa_keyword_idx",
                namespace_id,
                attr_id,
                &value,
                run_key,
            )
            .await?;
        }
        (SearchAttrType::KeywordList, SearchAttrValue::KeywordList(values)) => {
            for value in values {
                delete_text_index(
                    connection,
                    "sa_keyword_list_idx",
                    namespace_id,
                    attr_id,
                    &value,
                    run_key,
                )
                .await?;
            }
        }
        (SearchAttrType::Int, SearchAttrValue::Int(value)) => {
            sqlx::query(
                r#"
                DELETE FROM sa_int_idx
                WHERE namespace_id = $1 AND attr_id = $2 AND value = $3 AND run_key = $4
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Bool, SearchAttrValue::Bool(value)) => {
            sqlx::query(
                r#"
                DELETE FROM sa_bool_idx
                WHERE namespace_id = $1 AND attr_id = $2 AND value = $3 AND run_key = $4
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Double, SearchAttrValue::Double(value)) => {
            sqlx::query(
                r#"
                DELETE FROM sa_double_idx
                WHERE namespace_id = $1 AND attr_id = $2 AND value = $3 AND run_key = $4
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Datetime, SearchAttrValue::Datetime(value)) => {
            sqlx::query(
                r#"
                DELETE FROM sa_datetime_idx
                WHERE namespace_id = $1 AND attr_id = $2 AND value = $3 AND run_key = $4
                "#,
            )
            .bind(namespace_id.0)
            .bind(attr_id)
            .bind(value)
            .bind(run_key.0)
            .execute(&mut *connection)
            .await?;
        }
        (SearchAttrType::Text, SearchAttrValue::Text(value)) => {
            for token in text_search_tokens(&value) {
                delete_text_index(
                    connection,
                    "sa_text_token_idx",
                    namespace_id,
                    attr_id,
                    &token,
                    run_key,
                )
                .await?;
            }
        }
        (expected, actual) => {
            bail!(
                "stored search attribute type mismatch: expected {:?}, got {:?}",
                expected,
                search_attr_type_of(&actual)
            );
        }
    }

    sqlx::query("DELETE FROM sa_current WHERE run_key = $1 AND attr_id = $2")
        .bind(run_key.0)
        .bind(attr_id)
        .execute(connection)
        .await?;
    Ok(())
}

async fn insert_text_index(
    connection: &mut PgConnection,
    table: &str,
    namespace_id: NamespaceId,
    attr_id: i64,
    value: &str,
    run_key: RunKey,
) -> Result<()> {
    let sql = format!(
        r#"
        INSERT INTO {table} (namespace_id, attr_id, value, run_key)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        "#
    );
    sqlx::query(&sql)
        .bind(namespace_id.0)
        .bind(attr_id)
        .bind(value)
        .bind(run_key.0)
        .execute(connection)
        .await?;
    Ok(())
}

async fn delete_text_index(
    connection: &mut PgConnection,
    table: &str,
    namespace_id: NamespaceId,
    attr_id: i64,
    value: &str,
    run_key: RunKey,
) -> Result<()> {
    let sql = format!(
        r#"
        DELETE FROM {table}
        WHERE namespace_id = $1 AND attr_id = $2 AND value = $3 AND run_key = $4
        "#
    );
    sqlx::query(&sql)
        .bind(namespace_id.0)
        .bind(attr_id)
        .bind(value)
        .bind(run_key.0)
        .execute(connection)
        .await?;
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
enum SqlValue {
    Bool(bool),
    Float(f64),
    Int(i64),
    OptionalTimestamp(Option<time::OffsetDateTime>),
    Smallint(i16),
    Text(String),
    Timestamp(time::OffsetDateTime),
    Uuid(Uuid),
}

struct SqlCompiler {
    next_param: usize,
    values: Vec<SqlValue>,
}

impl SqlCompiler {
    fn new(param_offset: usize) -> Self {
        Self {
            next_param: param_offset,
            values: Vec::new(),
        }
    }

    fn push(&mut self, value: SqlValue) -> String {
        let placeholder = format!("${}", self.next_param);
        self.next_param += 1;
        self.values.push(value);
        placeholder
    }
}

fn compile_filter(
    filter: &CompiledFilter,
    param_offset: usize,
) -> Result<(String, Vec<SqlValue>, usize)> {
    let Some(expr) = &filter.expr else {
        return Ok((String::new(), Vec::new(), param_offset));
    };
    let mut compiler = SqlCompiler::new(param_offset);
    let sql = compile_expr(expr, &mut compiler)?;
    Ok((format!("AND {sql}"), compiler.values, compiler.next_param))
}

fn referenced_search_attr_index_tables(
    filter: &CompiledFilter,
    group_attr_type: Option<SearchAttrType>,
) -> Result<Vec<&'static str>> {
    let mut tables = BTreeSet::new();
    if let Some(expr) = &filter.expr {
        collect_referenced_tables(expr, &mut tables)?;
    }
    if let Some(attr_type) = group_attr_type {
        tables.insert(index_table(attr_type)?);
    }
    Ok(tables.into_iter().collect())
}

fn collect_referenced_tables(expr: &FilterExpr, tables: &mut BTreeSet<&'static str>) -> Result<()> {
    match expr {
        FilterExpr::And(lhs, rhs) | FilterExpr::Or(lhs, rhs) => {
            collect_referenced_tables(lhs, tables)?;
            collect_referenced_tables(rhs, tables)?;
        }
        FilterExpr::Compare { field, .. }
        | FilterExpr::In { field, .. }
        | FilterExpr::Between { field, .. }
        | FilterExpr::StartsWith { field, .. } => {
            if let FieldRef::Custom { attr_type, .. } = field {
                tables.insert(index_table(*attr_type)?);
            }
        }
    }
    Ok(())
}

fn compile_expr(expr: &FilterExpr, compiler: &mut SqlCompiler) -> Result<String> {
    match expr {
        FilterExpr::And(lhs, rhs) => Ok(format!(
            "({} AND {})",
            compile_expr(lhs, compiler)?,
            compile_expr(rhs, compiler)?
        )),
        FilterExpr::Or(lhs, rhs) => Ok(format!(
            "({} OR {})",
            compile_expr(lhs, compiler)?,
            compile_expr(rhs, compiler)?
        )),
        FilterExpr::Compare { field, op, value } => compile_compare(field, *op, value, compiler),
        FilterExpr::In { field, values } => compile_in(field, values, compiler),
        FilterExpr::Between { field, low, high } => compile_between(field, low, high, compiler),
        FilterExpr::StartsWith { field, prefix } => compile_starts_with(field, prefix, compiler),
    }
}

fn compile_compare(
    field: &FieldRef,
    op: CompareOp,
    value: &FilterValue,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    match field {
        FieldRef::System(field) => {
            let placeholder = compiler.push(sql_value_from_filter(value)?);
            Ok(format!(
                "{} {} {placeholder}",
                system_column(*field),
                compare_operator(op)
            ))
        }
        FieldRef::Custom {
            attr_id, attr_type, ..
        } => compile_custom_compare(*attr_id, *attr_type, op, value, compiler),
    }
}

fn compile_custom_compare(
    attr_id: AttrId,
    attr_type: SearchAttrType,
    op: CompareOp,
    value: &FilterValue,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    if matches!(
        attr_type,
        SearchAttrType::KeywordList | SearchAttrType::Text
    ) && matches!(
        op,
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
    ) {
        bail!("{attr_type:?} does not support ordered comparison operators");
    }
    if attr_type == SearchAttrType::Text {
        let FilterValue::String(value) = value else {
            return Ok(match op {
                CompareOp::Ne => "TRUE".to_owned(),
                _ => "FALSE".to_owned(),
            });
        };
        let Some(value) = normalize_text_literal(value) else {
            return Ok(match op {
                CompareOp::Ne => "TRUE".to_owned(),
                _ => "FALSE".to_owned(),
            });
        };
        return compile_text_token_compare(attr_id, op, value, compiler);
    }
    if attr_type == SearchAttrType::KeywordList {
        let value = expect_string_filter(value, attr_type)?;
        return compile_multi_value_compare(
            "sa_keyword_list_idx",
            attr_id,
            op,
            SqlValue::Text(value),
            compiler,
        );
    }
    compile_scalar_custom_compare(attr_id, attr_type, op, value, compiler)
}

fn compile_text_token_compare(
    attr_id: AttrId,
    op: CompareOp,
    value: String,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    compile_multi_value_compare(
        "sa_text_token_idx",
        attr_id,
        op,
        SqlValue::Text(value),
        compiler,
    )
}

fn compile_multi_value_compare(
    table: &str,
    attr_id: AttrId,
    op: CompareOp,
    value: SqlValue,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    match op {
        CompareOp::Eq => {
            let attr = compiler.push(SqlValue::Int(i64_from_u64(
                attr_id.0,
                "search attribute id",
            )?));
            let value = compiler.push(value);
            Ok(format!(
                "run_key IN (SELECT run_key FROM {table} WHERE namespace_id = $1 AND attr_id = {attr} AND value = {value})"
            ))
        }
        CompareOp::Ne => {
            let attr = compiler.push(SqlValue::Int(i64_from_u64(
                attr_id.0,
                "search attribute id",
            )?));
            let value = compiler.push(value);
            Ok(format!(
                "NOT EXISTS (SELECT 1 FROM {table} idx WHERE idx.namespace_id = vis_execution.namespace_id AND idx.run_key = vis_execution.run_key AND idx.attr_id = {attr} AND idx.value = {value})"
            ))
        }
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
            bail!("multi-value search attributes do not support ordered comparison operators")
        }
    }
}

fn compile_scalar_custom_compare(
    attr_id: AttrId,
    attr_type: SearchAttrType,
    op: CompareOp,
    value: &FilterValue,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    let table = index_table(attr_type)?;
    let attr = compiler.push(SqlValue::Int(i64_from_u64(
        attr_id.0,
        "search attribute id",
    )?));
    let value = compiler.push(sql_value_for_attr(attr_type, value)?);
    Ok(format!(
        "run_key IN (SELECT run_key FROM {table} WHERE namespace_id = $1 AND attr_id = {attr} AND value {} {value})",
        compare_operator(op)
    ))
}

fn compile_in(
    field: &FieldRef,
    values: &[FilterValue],
    compiler: &mut SqlCompiler,
) -> Result<String> {
    if values.is_empty() {
        return Ok("FALSE".to_owned());
    }
    match field {
        FieldRef::System(field) => {
            let placeholders = values
                .iter()
                .map(|value| Ok(compiler.push(sql_value_from_filter(value)?)))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!(
                "{} IN ({})",
                system_column(*field),
                placeholders.join(", ")
            ))
        }
        FieldRef::Custom {
            attr_id, attr_type, ..
        } => compile_custom_in(*attr_id, *attr_type, values, compiler),
    }
}

fn compile_custom_in(
    attr_id: AttrId,
    attr_type: SearchAttrType,
    values: &[FilterValue],
    compiler: &mut SqlCompiler,
) -> Result<String> {
    let table = index_table(attr_type)?;
    let sql_values = if attr_type == SearchAttrType::Text {
        values
            .iter()
            .filter_map(|value| {
                let FilterValue::String(value) = value else {
                    return None;
                };
                normalize_text_literal(value).map(SqlValue::Text)
            })
            .collect::<Vec<_>>()
    } else {
        values
            .iter()
            .map(|value| sql_value_for_attr(attr_type, value))
            .collect::<Result<Vec<_>>>()?
    };
    if sql_values.is_empty() {
        return Ok("FALSE".to_owned());
    }
    let attr = compiler.push(SqlValue::Int(i64_from_u64(
        attr_id.0,
        "search attribute id",
    )?));
    let placeholders = sql_values
        .into_iter()
        .map(|value| compiler.push(value))
        .collect::<Vec<_>>();
    Ok(format!(
        "run_key IN (SELECT run_key FROM {table} WHERE namespace_id = $1 AND attr_id = {attr} AND value IN ({}))",
        placeholders.join(", ")
    ))
}

fn compile_between(
    field: &FieldRef,
    low: &FilterValue,
    high: &FilterValue,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    match field {
        FieldRef::System(field) => {
            let low = compiler.push(sql_value_from_filter(low)?);
            let high = compiler.push(sql_value_from_filter(high)?);
            Ok(format!(
                "{} BETWEEN {low} AND {high}",
                system_column(*field)
            ))
        }
        FieldRef::Custom {
            attr_id, attr_type, ..
        } if matches!(
            attr_type,
            SearchAttrType::KeywordList | SearchAttrType::Text
        ) =>
        {
            bail!("{attr_type:?} does not support BETWEEN")
        }
        FieldRef::Custom {
            attr_id, attr_type, ..
        } => {
            let table = index_table(*attr_type)?;
            let attr = compiler.push(SqlValue::Int(i64_from_u64(
                attr_id.0,
                "search attribute id",
            )?));
            let low = compiler.push(sql_value_for_attr(*attr_type, low)?);
            let high = compiler.push(sql_value_for_attr(*attr_type, high)?);
            Ok(format!(
                "run_key IN (SELECT run_key FROM {table} WHERE namespace_id = $1 AND attr_id = {attr} AND value BETWEEN {low} AND {high})"
            ))
        }
    }
}

fn compile_starts_with(
    field: &FieldRef,
    prefix: &str,
    compiler: &mut SqlCompiler,
) -> Result<String> {
    match field {
        FieldRef::System(field) => {
            let pattern = compiler.push(SqlValue::Text(like_prefix(prefix)));
            Ok(format!(
                "{} LIKE {pattern} ESCAPE '\\'",
                system_column(*field)
            ))
        }
        FieldRef::Custom {
            attr_id, attr_type, ..
        } => {
            if !matches!(
                attr_type,
                SearchAttrType::Keyword | SearchAttrType::KeywordList | SearchAttrType::Text
            ) {
                bail!("{attr_type:?} does not support STARTS_WITH");
            }
            let table = index_table(*attr_type)?;
            let attr = compiler.push(SqlValue::Int(i64_from_u64(
                attr_id.0,
                "search attribute id",
            )?));
            let prefix = if *attr_type == SearchAttrType::Text {
                prefix.to_ascii_lowercase()
            } else {
                prefix.to_owned()
            };
            let pattern = compiler.push(SqlValue::Text(like_prefix(&prefix)));
            Ok(format!(
                "run_key IN (SELECT run_key FROM {table} WHERE namespace_id = $1 AND attr_id = {attr} AND value LIKE {pattern} ESCAPE '\\')"
            ))
        }
    }
}

fn sql_value_from_filter(value: &FilterValue) -> Result<SqlValue> {
    Ok(match value {
        FilterValue::String(value) => SqlValue::Text(value.clone()),
        FilterValue::Int(value) => SqlValue::Int(*value),
        FilterValue::Float(value) => SqlValue::Float(*value),
        FilterValue::Bool(value) => SqlValue::Bool(*value),
        FilterValue::Datetime(value) => SqlValue::Timestamp(*value),
        FilterValue::Status(value) => SqlValue::Smallint(value.to_db_smallint()),
    })
}

fn sql_value_for_attr(attr_type: SearchAttrType, value: &FilterValue) -> Result<SqlValue> {
    match (attr_type, value) {
        (SearchAttrType::Keyword | SearchAttrType::KeywordList, FilterValue::String(value)) => {
            Ok(SqlValue::Text(value.clone()))
        }
        (SearchAttrType::Int, FilterValue::Int(value)) => Ok(SqlValue::Int(*value)),
        (SearchAttrType::Bool, FilterValue::Bool(value)) => Ok(SqlValue::Bool(*value)),
        (SearchAttrType::Double, FilterValue::Float(value)) => Ok(SqlValue::Float(*value)),
        (SearchAttrType::Datetime, FilterValue::Datetime(value)) => Ok(SqlValue::Timestamp(*value)),
        (SearchAttrType::Text, FilterValue::String(value)) => normalize_text_literal(value)
            .map(SqlValue::Text)
            .ok_or_else(|| {
                anyhow!("Text equality predicates require a literal that normalizes to one token")
            }),
        _ => bail!("filter value type does not match {attr_type:?} search attribute"),
    }
}

fn expect_string_filter(value: &FilterValue, attr_type: SearchAttrType) -> Result<String> {
    let FilterValue::String(value) = value else {
        bail!("filter value type does not match {attr_type:?} search attribute");
    };
    Ok(value.clone())
}

fn normalize_text_literal(value: &str) -> Option<String> {
    let mut tokens = text_search_tokens(value);
    if tokens.len() == 1 {
        tokens.pop()
    } else {
        None
    }
}

fn like_prefix(prefix: &str) -> String {
    let mut escaped = String::with_capacity(prefix.len() + 1);
    for ch in prefix.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '%' => escaped.push_str("\\%"),
            '_' => escaped.push_str("\\_"),
            ch => escaped.push(ch),
        }
    }
    escaped.push('%');
    escaped
}

fn compare_operator(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "<>",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
    }
}

fn system_column(field: SystemField) -> &'static str {
    match field {
        SystemField::WorkflowId => "workflow_id",
        SystemField::RunId => "run_id::TEXT",
        SystemField::WorkflowType => "workflow_type",
        SystemField::TaskQueue => "task_queue",
        SystemField::ExecutionStatus => "execution_status",
        SystemField::StartTime => "start_time",
        SystemField::CloseTime => "close_time",
        SystemField::HistoryLength => "history_length",
        SystemField::StateTransitionCount => "state_transition_count",
    }
}

fn index_table(attr_type: SearchAttrType) -> Result<&'static str> {
    match attr_type {
        SearchAttrType::Keyword => Ok("sa_keyword_idx"),
        SearchAttrType::KeywordList => Ok("sa_keyword_list_idx"),
        SearchAttrType::Int => Ok("sa_int_idx"),
        SearchAttrType::Bool => Ok("sa_bool_idx"),
        SearchAttrType::Double => Ok("sa_double_idx"),
        SearchAttrType::Datetime => Ok("sa_datetime_idx"),
        SearchAttrType::Text => Ok("sa_text_token_idx"),
    }
}

fn sort_clause(sort: SortOrder) -> &'static str {
    match sort {
        SortOrder::Default => "close_time DESC NULLS LAST, start_time DESC, run_key DESC",
        SortOrder::StartTimeAsc => "start_time ASC, run_key ASC",
        SortOrder::StartTimeDesc => "start_time DESC, run_key DESC",
        SortOrder::CloseTimeAsc => "close_time ASC NULLS FIRST, run_key ASC",
        SortOrder::CloseTimeDesc => "close_time DESC NULLS LAST, run_key DESC",
    }
}

fn cursor_predicate(
    sort: SortOrder,
    token: Option<&PageToken>,
    param_offset: usize,
) -> Result<(String, Vec<SqlValue>, usize)> {
    let Some(token) = token else {
        return Ok((String::new(), Vec::new(), param_offset));
    };
    if token.sort_order != sort {
        bail!("page token sort order does not match requested sort order");
    }
    let mut compiler = SqlCompiler::new(param_offset);
    let sql = match sort {
        SortOrder::Default => {
            let close = compiler.push(SqlValue::OptionalTimestamp(token.close_time));
            let start = compiler.push(SqlValue::Timestamp(token.start_time));
            let run_key = compiler.push(SqlValue::Uuid(token.run_key.0));
            format!(
                "AND (COALESCE(close_time, '-infinity'::timestamptz), start_time, run_key) < (COALESCE({close}, '-infinity'::timestamptz), {start}, {run_key})"
            )
        }
        SortOrder::StartTimeAsc => {
            let start = compiler.push(SqlValue::Timestamp(token.start_time));
            let run_key = compiler.push(SqlValue::Uuid(token.run_key.0));
            format!("AND (start_time, run_key) > ({start}, {run_key})")
        }
        SortOrder::StartTimeDesc => {
            let start = compiler.push(SqlValue::Timestamp(token.start_time));
            let run_key = compiler.push(SqlValue::Uuid(token.run_key.0));
            format!("AND (start_time, run_key) < ({start}, {run_key})")
        }
        SortOrder::CloseTimeAsc => {
            let close = compiler.push(SqlValue::OptionalTimestamp(token.close_time));
            let run_key = compiler.push(SqlValue::Uuid(token.run_key.0));
            format!(
                "AND (COALESCE(close_time, '-infinity'::timestamptz), run_key) > (COALESCE({close}, '-infinity'::timestamptz), {run_key})"
            )
        }
        SortOrder::CloseTimeDesc => {
            let close = compiler.push(SqlValue::OptionalTimestamp(token.close_time));
            let run_key = compiler.push(SqlValue::Uuid(token.run_key.0));
            format!(
                "AND (COALESCE(close_time, '-infinity'::timestamptz), run_key) < (COALESCE({close}, '-infinity'::timestamptz), {run_key})"
            )
        }
    };
    Ok((sql, compiler.values, compiler.next_param))
}

fn bind_sql_values<'q>(
    mut query: Query<'q, Postgres, PgArguments>,
    values: &[SqlValue],
) -> Query<'q, Postgres, PgArguments> {
    for value in values {
        query = match value {
            SqlValue::Bool(value) => query.bind(*value),
            SqlValue::Float(value) => query.bind(*value),
            SqlValue::Int(value) => query.bind(*value),
            SqlValue::OptionalTimestamp(value) => query.bind(*value),
            SqlValue::Smallint(value) => query.bind(*value),
            SqlValue::Text(value) => query.bind(value.clone()),
            SqlValue::Timestamp(value) => query.bind(*value),
            SqlValue::Uuid(value) => query.bind(*value),
        };
    }
    query
}

async fn count_without_group(
    director: &DsqlConnectionDirector,
    namespace_id: NamespaceId,
    filter: &CompiledFilter,
) -> Result<CountResult> {
    let (filter_sql, values, _next_param) = compile_filter(filter, 2)?;
    let sql = format!(
        r#"
        SELECT COUNT(*) AS total_count
        FROM vis_execution
        WHERE namespace_id = $1
          {filter_sql}
        "#
    );
    let mut query = sqlx::query(&sql).bind(namespace_id.0);
    query = bind_sql_values(query, &values);
    let mut permit = director.acquire(DbClass::Projection).await?;
    let row = query.fetch_one(permit.connection()?).await?;
    Ok(CountResult {
        total_count: row.try_get("total_count")?,
        groups: Vec::new(),
    })
}

async fn count_system_group(
    director: &DsqlConnectionDirector,
    namespace_id: NamespaceId,
    filter: &CompiledFilter,
    field: SystemField,
) -> Result<CountResult> {
    let (filter_sql, values, _next_param) = compile_filter(filter, 2)?;
    let group_column = system_column(field);
    let sql = format!(
        r#"
        SELECT {group_column} AS group_value, COUNT(*) AS group_count
        FROM vis_execution
        WHERE namespace_id = $1
          {filter_sql}
        GROUP BY {group_column}
        "#
    );
    let mut query = sqlx::query(&sql).bind(namespace_id.0);
    query = bind_sql_values(query, &values);
    let mut permit = director.acquire(DbClass::Projection).await?;
    let rows = query.fetch_all(permit.connection()?).await?;
    let mut total_count = 0;
    let mut groups = Vec::new();
    for row in rows {
        let count = row.try_get("group_count")?;
        total_count += count;
        let value = system_group_value(field, &row)?;
        groups.push(RollupCounter { value, count });
    }
    Ok(CountResult {
        total_count,
        groups,
    })
}

async fn count_custom_group(
    director: &DsqlConnectionDirector,
    namespace_id: NamespaceId,
    filter: &CompiledFilter,
    attr_id: AttrId,
    attr_type: SearchAttrType,
) -> Result<CountResult> {
    if matches!(
        attr_type,
        SearchAttrType::KeywordList | SearchAttrType::Text
    ) {
        bail!("group-by is not supported for KeywordList or Text search attributes");
    }
    let table = index_table(attr_type)?;
    let (filter_sql, mut values, next_param) = compile_filter(filter, 2)?;
    let attr_placeholder = format!("${next_param}");
    values.push(SqlValue::Int(i64_from_u64(
        attr_id.0,
        "search attribute id",
    )?));
    let sql = format!(
        r#"
        SELECT idx.value AS group_value, COUNT(*) AS group_count
        FROM (
            SELECT *
            FROM vis_execution
            WHERE namespace_id = $1
              {filter_sql}
        ) ve
        LEFT JOIN {table} idx ON idx.run_key = ve.run_key
            AND idx.namespace_id = ve.namespace_id
            AND idx.attr_id = {attr_placeholder}
        GROUP BY idx.value
        "#
    );
    let mut query = sqlx::query(&sql).bind(namespace_id.0);
    query = bind_sql_values(query, &values);
    let mut permit = director.acquire(DbClass::Projection).await?;
    let rows = query.fetch_all(permit.connection()?).await?;
    let mut total_count = 0;
    let mut groups = Vec::new();
    for row in rows {
        let count = row.try_get("group_count")?;
        total_count += count;
        groups.push(RollupCounter {
            value: custom_group_value(attr_type, &row)?,
            count,
        });
    }
    Ok(CountResult {
        total_count,
        groups,
    })
}

fn system_group_value(field: SystemField, row: &PgRow) -> Result<String> {
    Ok(match field {
        SystemField::ExecutionStatus => {
            let value: i16 = row.try_get("group_value")?;
            format!("{:?}", ExecutionStatus::try_from(value)?)
        }
        SystemField::StartTime | SystemField::CloseTime => row
            .try_get::<Option<time::OffsetDateTime>, _>("group_value")?
            .map(|value| value.to_string())
            .unwrap_or_default(),
        SystemField::HistoryLength | SystemField::StateTransitionCount => row
            .try_get::<Option<i64>, _>("group_value")?
            .map(|value| value.to_string())
            .unwrap_or_default(),
        _ => row
            .try_get::<Option<String>, _>("group_value")?
            .unwrap_or_default(),
    })
}

fn custom_group_value(attr_type: SearchAttrType, row: &PgRow) -> Result<String> {
    Ok(match attr_type {
        SearchAttrType::Keyword => row
            .try_get::<Option<String>, _>("group_value")?
            .unwrap_or_default(),
        SearchAttrType::Int => row
            .try_get::<Option<i64>, _>("group_value")?
            .map(|value| value.to_string())
            .unwrap_or_default(),
        SearchAttrType::Bool => row
            .try_get::<Option<bool>, _>("group_value")?
            .map(|value| value.to_string())
            .unwrap_or_default(),
        SearchAttrType::Double => row
            .try_get::<Option<f64>, _>("group_value")?
            .map(|value| value.to_string())
            .unwrap_or_default(),
        SearchAttrType::Datetime => row
            .try_get::<Option<time::OffsetDateTime>, _>("group_value")?
            .map(|value| value.to_string())
            .unwrap_or_default(),
        SearchAttrType::KeywordList | SearchAttrType::Text => {
            bail!("group-by is not supported for KeywordList or Text search attributes")
        }
    })
}

fn rows_to_count_result(rows: Vec<PgRow>) -> Result<CountResult> {
    let mut total_count = 0;
    let mut groups = Vec::new();
    for row in rows {
        let count = row.try_get("counter")?;
        total_count += count;
        groups.push(RollupCounter {
            value: row.try_get("value")?,
            count,
        });
    }
    Ok(CountResult {
        total_count,
        groups,
    })
}

async fn get_execution_row(
    connection: &mut PgConnection,
    run_key: RunKey,
) -> Result<Option<ExecutionRow>> {
    let row = sqlx::query(
        r#"
        SELECT
            run_key,
            namespace_id,
            workflow_id,
            run_id,
            workflow_type,
            task_queue,
            execution_status,
            start_time,
            execution_time,
            close_time,
            history_length,
            state_transition_count,
            memo
        FROM vis_execution
        WHERE run_key = $1
        "#,
    )
    .bind(run_key.0)
    .fetch_optional(connection)
    .await?;

    row.map(row_to_execution).transpose()
}

fn row_to_execution(row: sqlx::postgres::PgRow) -> Result<ExecutionRow> {
    let memo = decode_optional_memo(row.try_get("memo")?)?.unwrap_or_default();
    let status: i16 = row.try_get("execution_status")?;
    Ok(ExecutionRow {
        run_key: RunKey(row.try_get::<Uuid, _>("run_key")?),
        namespace_id: NamespaceId(row.try_get::<Uuid, _>("namespace_id")?),
        workflow_id: tokeira_types::WorkflowId(row.try_get("workflow_id")?),
        run_id: tokeira_types::RunId(row.try_get::<Uuid, _>("run_id")?),
        workflow_type: tokeira_types::WorkflowType(row.try_get("workflow_type")?),
        task_queue: tokeira_types::TaskQueueName(row.try_get("task_queue")?),
        status: ExecutionStatus::try_from(status)?,
        start_time: row.try_get("start_time")?,
        execution_time: row.try_get("execution_time")?,
        close_time: row.try_get("close_time")?,
        history_length: row.try_get("history_length")?,
        state_transition_count: row.try_get("state_transition_count")?,
        memo,
        search_attr_version: 0,
    })
}

fn row_to_attr_descriptor(row: PgRow) -> Result<AttrDescriptor> {
    let attr_id: i64 = row.try_get("attr_id")?;
    let attr_type: i16 = row.try_get("attr_type")?;
    Ok(AttrDescriptor {
        attr_id: attr_id_from_i64(attr_id)?,
        attr_type: SearchAttrType::try_from(attr_type)?,
    })
}

fn deterministic_attr_id(namespace_id: NamespaceId, name: &str) -> AttrId {
    let uuid = dsql_spread_uuid(&[b"search-attr", namespace_id.0.as_bytes(), name.as_bytes()]);
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&uuid.as_bytes()[8..]);
    let value = u64::from_be_bytes(bytes) & i64::MAX as u64;
    AttrId(value.max(1))
}

fn attr_id_from_i64(value: i64) -> Result<AttrId> {
    if value < 0 {
        bail!("search attribute id {value} is negative");
    }
    Ok(AttrId(value as u64))
}

fn decode_optional_memo(data: Option<Vec<u8>>) -> Result<Option<Memo>> {
    data.map(|bytes| codec::decode::<Memo>(&bytes)).transpose()
}

#[cfg(test)]
fn merge_memo(existing: Option<Memo>, patch: &Memo) -> Memo {
    let mut memo = existing.unwrap_or_default();
    memo.0.extend(patch.0.clone());
    memo
}

#[cfg(test)]
fn resolve_final_vis_state(
    context: &tokeira_storage::ProjectionContext,
    run_key: RunKey,
    ops: &[ProjectionOp],
) -> ExecutionRow {
    let mut row = ExecutionRow {
        run_key,
        namespace_id: context.namespace_id,
        workflow_id: context.workflow_id.clone(),
        run_id: context.run_id,
        workflow_type: context.workflow_type.clone(),
        task_queue: context.task_queue.clone(),
        status: context.execution_status,
        start_time: context.start_time,
        execution_time: context.execution_time,
        close_time: context.close_time,
        history_length: context.history_length,
        state_transition_count: context.state_transition_count,
        memo: Memo::default(),
        search_attr_version: 0,
    };
    for op in ops {
        match op {
            ProjectionOp::UpsertExecution {
                status, memo_patch, ..
            } => {
                row.status = *status;
                row.memo.0.extend(memo_patch.0.clone());
            }
            ProjectionOp::CloseExecution { status, closed_at } => {
                row.status = *status;
                row.close_time = Some(*closed_at);
            }
        }
    }
    row
}

fn i16_from_u16(value: u16, field: &str) -> Result<i16> {
    i16::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i16 range"))
}

fn i32_from_u32(value: u32, field: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i32 range"))
}

fn i64_from_u64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{field} {value} exceeds i64 range"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_storage::ProjectionContext;
    use tokeira_types::{Payload, RunId, SearchAttrValue, TaskQueueName, WorkflowId, WorkflowType};

    use super::*;

    #[test]
    fn memo_patch_extends_existing_memo() {
        let mut existing_entries = BTreeMap::new();
        existing_entries.insert("one".to_owned(), Payload::new(b"1"));
        let mut patch_entries = BTreeMap::new();
        patch_entries.insert("two".to_owned(), Payload::new(b"2"));

        let merged = merge_memo(Some(Memo(existing_entries)), &Memo(patch_entries));

        assert_eq!(merged.0.len(), 2);
        assert!(merged.0.contains_key("one"));
        assert!(merged.0.contains_key("two"));
    }

    #[test]
    fn memo_patch_overwrites_existing_key() {
        let mut existing_entries = BTreeMap::new();
        existing_entries.insert("key".to_owned(), Payload::new(b"old"));
        let mut patch_entries = BTreeMap::new();
        patch_entries.insert("key".to_owned(), Payload::new(b"new"));

        let merged = merge_memo(Some(Memo(existing_entries)), &Memo(patch_entries));

        assert_eq!(merged.0["key"].data, b"new");
    }

    #[test]
    fn empty_memo_patch_preserves_existing_memo() {
        let mut existing_entries = BTreeMap::new();
        existing_entries.insert("key".to_owned(), Payload::new(b"value"));
        let existing = Memo(existing_entries.clone());

        let merged = merge_memo(Some(existing), &Memo::default());

        assert_eq!(merged, Memo(existing_entries));
    }

    #[test]
    fn projection_apply_decision_logic_preserves_operation_order() {
        let context = test_projection_context(ExecutionStatus::Running);
        let run_key = RunKey(uuid_from_u128(10));
        let closed_at = OffsetDateTime::from_unix_timestamp(200).unwrap();
        let ops = vec![
            ProjectionOp::UpsertExecution {
                status: ExecutionStatus::Paused,
                memo_patch: Memo::default(),
                search_attr_patch: SearchAttributes::default(),
            },
            ProjectionOp::CloseExecution {
                status: ExecutionStatus::Completed,
                closed_at,
            },
        ];

        let row = resolve_final_vis_state(&context, run_key, &ops);

        assert_eq!(row.status, ExecutionStatus::Completed);
        assert_eq!(row.close_time, Some(closed_at));
        assert_eq!(
            row.status.to_db_smallint(),
            ExecutionStatus::Completed.to_db_smallint()
        );
    }

    #[test]
    fn close_execution_without_prior_upsert_produces_complete_row() {
        let context = test_projection_context(ExecutionStatus::Running);
        let run_key = RunKey(uuid_from_u128(11));
        let closed_at = OffsetDateTime::from_unix_timestamp(300).unwrap();
        let ops = [ProjectionOp::CloseExecution {
            status: ExecutionStatus::Failed,
            closed_at,
        }];

        let row = resolve_final_vis_state(&context, run_key, &ops);

        assert_eq!(row.run_key, run_key);
        assert_eq!(row.namespace_id, context.namespace_id);
        assert_eq!(row.workflow_id, context.workflow_id);
        assert_eq!(row.run_id, context.run_id);
        assert_eq!(row.workflow_type, context.workflow_type);
        assert_eq!(row.task_queue, context.task_queue);
        assert_eq!(row.status, ExecutionStatus::Failed);
        assert_eq!(row.close_time, Some(closed_at));
    }

    #[test]
    fn keyword_list_ne_uses_anti_join() {
        let attr_id = AttrId(7);
        let filter = CompiledFilter {
            expr: Some(FilterExpr::Compare {
                field: FieldRef::Custom {
                    name: "tags".to_owned(),
                    attr_id,
                    attr_type: SearchAttrType::KeywordList,
                },
                op: CompareOp::Ne,
                value: FilterValue::String("a".to_owned()),
            }),
        };

        let (sql, values, _) = compile_filter(&filter, 2).unwrap();

        assert!(sql.contains("NOT EXISTS"));
        assert!(sql.contains("sa_keyword_list_idx"));
        assert!(!sql.contains("value <>"));
        assert_eq!(
            values,
            vec![SqlValue::Int(7), SqlValue::Text("a".to_owned())]
        );
    }

    #[test]
    fn text_eq_normalizes_single_token_and_rejects_multi_token() {
        let field = FieldRef::Custom {
            name: "text".to_owned(),
            attr_id: AttrId(9),
            attr_type: SearchAttrType::Text,
        };
        let single = CompiledFilter {
            expr: Some(FilterExpr::Compare {
                field: field.clone(),
                op: CompareOp::Eq,
                value: FilterValue::String("Hello".to_owned()),
            }),
        };
        let multi = CompiledFilter {
            expr: Some(FilterExpr::Compare {
                field,
                op: CompareOp::Eq,
                value: FilterValue::String("hello world".to_owned()),
            }),
        };

        let (sql, values, _) = compile_filter(&single, 2).unwrap();
        assert!(sql.contains("sa_text_token_idx"));
        assert_eq!(
            values,
            vec![SqlValue::Int(9), SqlValue::Text("hello".to_owned())]
        );
        let (sql, values, _) = compile_filter(&multi, 2).unwrap();
        assert_eq!(sql, "AND FALSE");
        assert!(values.is_empty());
    }

    #[test]
    fn text_in_ignores_invalid_candidates() {
        let filter = CompiledFilter {
            expr: Some(FilterExpr::In {
                field: FieldRef::Custom {
                    name: "text".to_owned(),
                    attr_id: AttrId(9),
                    attr_type: SearchAttrType::Text,
                },
                values: vec![
                    FilterValue::String("Hello".to_owned()),
                    FilterValue::String("two words".to_owned()),
                ],
            }),
        };

        let (sql, values, _) = compile_filter(&filter, 2).unwrap();

        assert!(sql.contains("sa_text_token_idx"));
        assert_eq!(
            values,
            vec![SqlValue::Int(9), SqlValue::Text("hello".to_owned())]
        );
    }

    #[test]
    fn like_prefix_escapes_semantic_like_characters() {
        assert_eq!(like_prefix(""), "%");
        assert_eq!(like_prefix("abc"), "abc%");
        assert_eq!(like_prefix(r"%_\"), r"\%\_\\%");
        assert_eq!(like_prefix(r"a\b"), r"a\\b%");
    }

    prop_compose! {
        fn arb_search_attr_value()(
            variant in 0u8..7,
            text in "[a-zA-Z0-9 _%-]{0,24}",
            int_value in any::<i64>(),
            bool_value in any::<bool>(),
            double_value in any::<f64>(),
            timestamp in -1_000_000i64..1_000_000,
            list in proptest::collection::vec("[a-z]{0,8}", 0..6),
        ) -> SearchAttrValue {
            match variant {
                0 => SearchAttrValue::Keyword(text),
                1 => SearchAttrValue::KeywordList(list),
                2 => SearchAttrValue::Int(int_value),
                3 => SearchAttrValue::Bool(bool_value),
                4 => SearchAttrValue::Double(double_value),
                5 => SearchAttrValue::Datetime(
                    OffsetDateTime::from_unix_timestamp(timestamp).unwrap(),
                ),
                _ => SearchAttrValue::Text(text),
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn search_attr_value_codec_round_trips(value in arb_search_attr_value()) {
            let encoded = codec::encode(&value).unwrap();
            let decoded = codec::decode::<SearchAttrValue>(&encoded).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn like_prefix_escape_property(prefix in "[%_\\\\a-zA-Z0-9]{0,32}") {
            let escaped = like_prefix(&prefix);
            prop_assert!(escaped.ends_with('%'));
            prop_assert_eq!(escaped.matches('%').count(), prefix.matches('%').count() + 1);
            prop_assert_eq!(escaped.matches('_').count(), prefix.matches('_').count());
            let expected_backslashes = prefix.matches('\\').count() * 2
                + prefix.matches('%').count()
                + prefix.matches('_').count();
            prop_assert_eq!(escaped.matches('\\').count(), expected_backslashes);
        }

        #[test]
        fn visibility_operation_ordering_matches_last_semantic_op(
            ops in arb_projection_ops(),
            run_key in any::<u128>(),
        ) {
            let context = test_projection_context(ExecutionStatus::Running);
            let row = resolve_final_vis_state(&context, RunKey(uuid_from_u128(run_key)), &ops);
            let last = ops.last().unwrap();

            match last {
                ProjectionOp::UpsertExecution { status, .. } => {
                    prop_assert_eq!(row.status, *status);
                    prop_assert_eq!(row.close_time, None);
                }
                ProjectionOp::CloseExecution { status, closed_at } => {
                    prop_assert_eq!(row.status, *status);
                    prop_assert_eq!(row.close_time, Some(*closed_at));
                }
            }
        }
    }

    fn uuid_from_u128(value: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(value)
    }

    fn test_projection_context(status: ExecutionStatus) -> ProjectionContext {
        ProjectionContext {
            namespace_id: NamespaceId(uuid_from_u128(1)),
            workflow_id: WorkflowId("workflow".to_owned()),
            run_id: RunId(uuid_from_u128(2)),
            workflow_type: WorkflowType("workflow_type".to_owned()),
            task_queue: TaskQueueName("queue".to_owned()),
            execution_status: status,
            start_time: OffsetDateTime::from_unix_timestamp(100).unwrap(),
            execution_time: None,
            close_time: None,
            history_length: 1,
            state_transition_count: 1,
        }
    }

    fn arb_execution_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Running),
            Just(ExecutionStatus::Paused),
            Just(ExecutionStatus::Completed),
            Just(ExecutionStatus::Failed),
            Just(ExecutionStatus::Cancelled),
            Just(ExecutionStatus::Terminated),
            Just(ExecutionStatus::ContinuedAsNew),
            Just(ExecutionStatus::TimedOut),
        ]
    }

    prop_compose! {
        fn arb_closed_at()(seconds in 1_000i64..1_000_000) -> OffsetDateTime {
            OffsetDateTime::from_unix_timestamp(seconds).unwrap()
        }
    }

    prop_compose! {
        fn arb_projection_ops()(
            upsert_statuses in proptest::collection::vec(arb_execution_status(), 1..4),
            terminal in proptest::option::of((arb_execution_status(), arb_closed_at())),
        ) -> Vec<ProjectionOp> {
            let mut ops = upsert_statuses
                .into_iter()
                .map(|status| ProjectionOp::UpsertExecution {
                    status,
                    memo_patch: Memo::default(),
                    search_attr_patch: SearchAttributes::default(),
                })
                .collect::<Vec<_>>();
            if let Some((status, closed_at)) = terminal {
                ops.push(ProjectionOp::CloseExecution { status, closed_at });
            }
            ops
        }
    }
}
