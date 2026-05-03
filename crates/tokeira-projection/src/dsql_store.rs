//! DSQL-backed visibility store and projection sink.
//!
//! This implementation lives in the projection crate because the live worker
//! owns the `ProjectionSink` and `VisibilityStore` traits. Storage provides the
//! DSQL connection foundation and codecs; projection owns how semantic projection
//! ops become visibility rows.

use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use sqlx::{PgConnection, Row};
use tokeira_kernel::ProjectionOp;
use tokeira_storage::{
    ConnectionDirector, DbClass, ProjectionRecord,
    dsql::{DsqlConnectionDirector, codec},
};
use tokeira_types::{
    ExecutionStatus, Memo, NamespaceId, ProjectionCursor, RunKey, SearchAttrValue,
};
use tracing::{instrument, warn};
use uuid::Uuid;

use crate::{
    ProjectionSink, VisibilityStore,
    types::{
        AttrDescriptor, AttrId, CompiledFilter, CountResult, ExecutionRow, GroupByField,
        ListResult, PageBounds, RollupDelta, RollupDimension, SearchAttrType, SortOrder,
    },
};

#[derive(Debug)]
pub struct DsqlVisibilityStore {
    director: Arc<DsqlConnectionDirector>,
}

impl DsqlVisibilityStore {
    pub fn new(director: Arc<DsqlConnectionDirector>) -> Self {
        Self { director }
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
        _run_key: RunKey,
        _namespace_id: NamespaceId,
        _attr_id: AttrId,
        _attr_type: SearchAttrType,
        _value: &SearchAttrValue,
    ) -> Result<()> {
        unsupported("search-attribute index writes")
    }

    async fn remove_search_attr_index(
        &self,
        _run_key: RunKey,
        _namespace_id: NamespaceId,
        _attr_id: AttrId,
        _attr_type: SearchAttrType,
    ) -> Result<()> {
        unsupported("search-attribute index deletes")
    }

    async fn accumulate_rollup(&self, _entries: &[RollupDelta]) -> Result<()> {
        unsupported("visibility rollups")
    }

    async fn list_executions(
        &self,
        _namespace_id: NamespaceId,
        _filter: &CompiledFilter,
        _sort: SortOrder,
        _page: &PageBounds,
    ) -> Result<ListResult> {
        unsupported("visibility list queries")
    }

    async fn count_executions(
        &self,
        _namespace_id: NamespaceId,
        _filter: &CompiledFilter,
        _group_by: Option<GroupByField>,
    ) -> Result<CountResult> {
        unsupported("visibility count queries")
    }

    async fn count_from_rollup(
        &self,
        _namespace_id: NamespaceId,
        _dimension: RollupDimension,
    ) -> Result<CountResult> {
        unsupported("visibility rollup count queries")
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
        sqlx::query(
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
        .await?;
        Ok(())
    }

    async fn resolve_attr(
        &self,
        _namespace_id: NamespaceId,
        _name: &str,
    ) -> Result<Option<AttrDescriptor>> {
        unsupported("search-attribute descriptor lookup")
    }

    async fn register_attr(
        &self,
        _namespace_id: NamespaceId,
        _name: String,
        _attr_type: SearchAttrType,
    ) -> Result<AttrId> {
        unsupported("search-attribute registration")
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
    async fn apply(&self, record: &ProjectionRecord) -> Result<()> {
        let mut permit = self.director.acquire(DbClass::Projection).await?;
        let connection = permit.connection()?;
        for op in &record.ops {
            match op {
                ProjectionOp::UpsertExecution {
                    status,
                    memo_patch,
                    // TODO(projection-visibility): apply search_attr_patch when
                    // search-attribute tables exist in the DSQL schema.
                    search_attr_patch: _,
                } => {
                    upsert_projection_record(connection, record, *status, memo_patch).await?;
                }
                ProjectionOp::CloseExecution { status, closed_at } => {
                    close_projection_record(connection, record, *status, *closed_at).await?;
                }
            }
        }
        Ok(())
    }
}

async fn upsert_projection_record(
    connection: &mut PgConnection,
    record: &ProjectionRecord,
    status: ExecutionStatus,
    memo_patch: &Memo,
) -> Result<()> {
    let memo = if memo_patch.0.is_empty() {
        None
    } else {
        let existing = get_memo(connection, record.run_key).await?;
        Some(codec::encode(&merge_memo(existing, memo_patch))?)
    };
    let row = ExecutionRow {
        run_key: record.run_key,
        namespace_id: record.context.namespace_id,
        workflow_id: record.context.workflow_id.clone(),
        run_id: record.context.run_id,
        workflow_type: record.context.workflow_type.clone(),
        task_queue: record.context.task_queue.clone(),
        status,
        start_time: record.context.start_time,
        execution_time: record.context.execution_time,
        close_time: record.context.close_time,
        history_length: record.context.history_length,
        state_transition_count: record.context.state_transition_count,
        memo: Memo::default(),
        search_attr_version: 0,
    };
    upsert_execution_row(connection, &row, memo).await
}

async fn close_projection_record(
    connection: &mut PgConnection,
    record: &ProjectionRecord,
    status: ExecutionStatus,
    closed_at: time::OffsetDateTime,
) -> Result<()> {
    let result = sqlx::query(
        r#"
        UPDATE vis_execution
        SET execution_status = $2,
            close_time = $3,
            history_length = $4,
            state_transition_count = $5
        WHERE run_key = $1
        "#,
    )
    .bind(record.run_key.0)
    .bind(status.to_db_smallint())
    .bind(closed_at)
    .bind(record.context.history_length)
    .bind(record.context.state_transition_count)
    .execute(&mut *connection)
    .await?;

    if result.rows_affected() == 0 {
        let row = ExecutionRow {
            run_key: record.run_key,
            namespace_id: record.context.namespace_id,
            workflow_id: record.context.workflow_id.clone(),
            run_id: record.context.run_id,
            workflow_type: record.context.workflow_type.clone(),
            task_queue: record.context.task_queue.clone(),
            status,
            start_time: record.context.start_time,
            execution_time: record.context.execution_time,
            close_time: Some(closed_at),
            history_length: record.context.history_length,
            state_transition_count: record.context.state_transition_count,
            memo: Memo::default(),
            search_attr_version: 0,
        };
        upsert_execution_row(connection, &row, None).await?;
    }
    Ok(())
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

async fn get_memo(connection: &mut PgConnection, run_key: RunKey) -> Result<Option<Memo>> {
    let row = sqlx::query("SELECT memo FROM vis_execution WHERE run_key = $1")
        .bind(run_key.0)
        .fetch_optional(connection)
        .await?;

    row.map(|row| {
        let data: Option<Vec<u8>> = row.try_get("memo")?;
        decode_optional_memo(data)
    })
    .transpose()
    .map(|memo| memo.flatten())
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

fn decode_optional_memo(data: Option<Vec<u8>>) -> Result<Option<Memo>> {
    data.map(|bytes| codec::decode::<Memo>(&bytes)).transpose()
}

fn merge_memo(existing: Option<Memo>, patch: &Memo) -> Memo {
    let mut memo = existing.unwrap_or_default();
    memo.0.extend(patch.0.clone());
    memo
}

fn unsupported<T>(feature: &str) -> Result<T> {
    bail!("{feature} are not implemented by the DSQL visibility MVP")
}

fn i16_from_u16(value: u16, field: &str) -> Result<i16> {
    i16::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i16 range"))
}

fn i32_from_u32(value: u32, field: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i32 range"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokeira_types::Payload;

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
}
