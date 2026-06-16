//! In-memory visibility store for development and tests.
//!
//! Maintains a multi-index structure over execution rows and search attributes:
//! per-type inverted indexes (`sa_keyword_idx`, `sa_int_idx`, etc.) enable
//! efficient filter evaluation, while `rollups` tracks pre-aggregated counters
//! by status, workflow type, and task queue for fast count queries.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Result, bail};
use async_trait::async_trait;
use tokeira_types::{ArchetypeId, NamespaceId, ProjectionCursor, RunKey, SearchAttrValue};
use tokio::sync::Mutex;

use crate::{
    store::VisibilityStore,
    types::{
        AttrDescriptor, AttrId, CompareOp, CompiledFilter, CountResult, ExecutionRow, FieldRef,
        FilterExpr, FilterValue, GroupByField, ListResult, PageBounds, PageToken, RollupCounter,
        RollupDelta, RollupDimension, SearchAttrType, SortOrder, SystemField, VisibilitySnapshot,
        text_search_tokens,
    },
};

#[derive(Default, Clone)]
pub struct InMemoryVisibilityStore {
    inner: Arc<Mutex<VisibilityState>>,
}

#[derive(Default)]
struct VisibilityState {
    rows: HashMap<RunKey, ExecutionRow>,
    snapshots: HashMap<(NamespaceId, ArchetypeId, RunKey), VisibilitySnapshot>,
    sa_current: HashMap<(RunKey, AttrId), SearchAttrValue>,
    sa_keyword_idx: HashMap<(NamespaceId, AttrId, String), HashSet<RunKey>>,
    sa_keyword_list_idx: HashMap<(NamespaceId, AttrId, String), HashSet<RunKey>>,
    sa_int_idx: HashMap<(NamespaceId, AttrId, i64), HashSet<RunKey>>,
    sa_bool_idx: HashMap<(NamespaceId, AttrId, bool), HashSet<RunKey>>,
    sa_double_idx: HashMap<(NamespaceId, AttrId, String), HashSet<RunKey>>,
    sa_datetime_idx: HashMap<(NamespaceId, AttrId, i128), HashSet<RunKey>>,
    sa_text_idx: HashMap<(NamespaceId, AttrId, String), HashSet<RunKey>>,
    rollups: HashMap<(NamespaceId, ArchetypeId, RollupDimension, String), i64>,
    registry: HashMap<(NamespaceId, String), AttrDescriptor>,
    checkpoints: HashMap<u32, ProjectionCursor>,
    next_attr_id: u64,
}

impl InMemoryVisibilityStore {
    pub(crate) fn index_text(text: &str) -> Vec<String> {
        text_search_tokens(text)
    }
}

#[async_trait]
impl VisibilityStore for InMemoryVisibilityStore {
    async fn upsert_snapshot(&self, snapshot: &VisibilitySnapshot) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.snapshots.insert(
            (
                snapshot.namespace_id,
                snapshot.archetype_id,
                snapshot.run_key,
            ),
            snapshot.clone(),
        );
        if let Some(row) = ExecutionRow::from_workflow_snapshot(snapshot) {
            inner.rows.insert(row.run_key, row);
        }
        Ok(())
    }

    async fn upsert_execution(&self, row: &ExecutionRow) -> Result<()> {
        let mut inner = self.inner.lock().await;
        inner.rows.insert(row.run_key, row.clone());
        let snapshot = row.to_snapshot();
        inner.snapshots.insert(
            (
                snapshot.namespace_id,
                snapshot.archetype_id,
                snapshot.run_key,
            ),
            snapshot,
        );
        Ok(())
    }

    async fn delete_execution(&self, run_key: RunKey) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let Some(row) = inner.rows.remove(&run_key) else {
            return Ok(());
        };

        let attr_ids: Vec<_> = inner
            .sa_current
            .keys()
            .filter_map(|(candidate_run_key, attr_id)| {
                if *candidate_run_key == run_key {
                    Some(*attr_id)
                } else {
                    None
                }
            })
            .collect();

        for attr_id in attr_ids {
            let Some(previous) = inner.sa_current.remove(&(run_key, attr_id)) else {
                continue;
            };
            match previous {
                SearchAttrValue::Keyword(v) => {
                    if let Some(set) = inner
                        .sa_keyword_idx
                        .get_mut(&(row.namespace_id, attr_id, v))
                    {
                        set.remove(&run_key);
                    }
                }
                SearchAttrValue::KeywordList(values) => {
                    for v in values {
                        if let Some(set) =
                            inner
                                .sa_keyword_list_idx
                                .get_mut(&(row.namespace_id, attr_id, v))
                        {
                            set.remove(&run_key);
                        }
                    }
                }
                SearchAttrValue::Int(v) => {
                    if let Some(set) = inner.sa_int_idx.get_mut(&(row.namespace_id, attr_id, v)) {
                        set.remove(&run_key);
                    }
                }
                SearchAttrValue::Bool(v) => {
                    if let Some(set) = inner.sa_bool_idx.get_mut(&(row.namespace_id, attr_id, v)) {
                        set.remove(&run_key);
                    }
                }
                SearchAttrValue::Double(v) => {
                    if let Some(set) = inner.sa_double_idx.get_mut(&(
                        row.namespace_id,
                        attr_id,
                        format!("{v:.17}"),
                    )) {
                        set.remove(&run_key);
                    }
                }
                SearchAttrValue::Datetime(v) => {
                    if let Some(set) = inner.sa_datetime_idx.get_mut(&(
                        row.namespace_id,
                        attr_id,
                        v.unix_timestamp_nanos(),
                    )) {
                        set.remove(&run_key);
                    }
                }
                SearchAttrValue::Text(v) => {
                    for token in Self::index_text(&v) {
                        if let Some(set) =
                            inner
                                .sa_text_idx
                                .get_mut(&(row.namespace_id, attr_id, token))
                        {
                            set.remove(&run_key);
                        }
                    }
                }
            }
        }

        for (dimension, value) in [
            (RollupDimension::ExecutionStatus, row.status_keyword.clone()),
            (RollupDimension::WorkflowType, row.workflow_type.0),
            (RollupDimension::TaskQueue, row.task_queue.0),
        ] {
            let key = (row.namespace_id, row.archetype_id, dimension, value);
            if let Some(count) = inner.rollups.get_mut(&key) {
                *count -= 1;
                if *count <= 0 {
                    inner.rollups.remove(&key);
                }
            }
        }

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
        let mut inner = self.inner.lock().await;
        inner.sa_current.insert((run_key, attr_id), value.clone());
        match (attr_type, value) {
            (SearchAttrType::Keyword, SearchAttrValue::Keyword(v)) => {
                inner
                    .sa_keyword_idx
                    .entry((namespace_id, attr_id, v.clone()))
                    .or_default()
                    .insert(run_key);
            }
            (SearchAttrType::KeywordList, SearchAttrValue::KeywordList(values)) => {
                for v in values {
                    inner
                        .sa_keyword_list_idx
                        .entry((namespace_id, attr_id, v.clone()))
                        .or_default()
                        .insert(run_key);
                }
            }
            (SearchAttrType::Int, SearchAttrValue::Int(v)) => {
                inner
                    .sa_int_idx
                    .entry((namespace_id, attr_id, *v))
                    .or_default()
                    .insert(run_key);
            }
            (SearchAttrType::Bool, SearchAttrValue::Bool(v)) => {
                inner
                    .sa_bool_idx
                    .entry((namespace_id, attr_id, *v))
                    .or_default()
                    .insert(run_key);
            }
            (SearchAttrType::Double, SearchAttrValue::Double(v)) => {
                inner
                    .sa_double_idx
                    .entry((namespace_id, attr_id, format!("{v:.17}")))
                    .or_default()
                    .insert(run_key);
            }
            (SearchAttrType::Datetime, SearchAttrValue::Datetime(v)) => {
                inner
                    .sa_datetime_idx
                    .entry((namespace_id, attr_id, v.unix_timestamp_nanos()))
                    .or_default()
                    .insert(run_key);
            }
            (SearchAttrType::Text, SearchAttrValue::Text(v)) => {
                for token in Self::index_text(v) {
                    inner
                        .sa_text_idx
                        .entry((namespace_id, attr_id, token))
                        .or_default()
                        .insert(run_key);
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn remove_search_attr_index(
        &self,
        run_key: RunKey,
        namespace_id: NamespaceId,
        attr_id: AttrId,
        attr_type: SearchAttrType,
    ) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let Some(previous) = inner.sa_current.remove(&(run_key, attr_id)) else {
            return Ok(());
        };
        match (attr_type, previous) {
            (SearchAttrType::Keyword, SearchAttrValue::Keyword(v)) => {
                if let Some(set) = inner
                    .sa_keyword_idx
                    .get_mut(&(namespace_id, attr_id, v.clone()))
                {
                    set.remove(&run_key);
                }
            }
            (SearchAttrType::KeywordList, SearchAttrValue::KeywordList(values)) => {
                for v in values {
                    if let Some(set) =
                        inner
                            .sa_keyword_list_idx
                            .get_mut(&(namespace_id, attr_id, v.clone()))
                    {
                        set.remove(&run_key);
                    }
                }
            }
            (SearchAttrType::Int, SearchAttrValue::Int(v)) => {
                if let Some(set) = inner.sa_int_idx.get_mut(&(namespace_id, attr_id, v)) {
                    set.remove(&run_key);
                }
            }
            (SearchAttrType::Bool, SearchAttrValue::Bool(v)) => {
                if let Some(set) = inner.sa_bool_idx.get_mut(&(namespace_id, attr_id, v)) {
                    set.remove(&run_key);
                }
            }
            (SearchAttrType::Double, SearchAttrValue::Double(v)) => {
                if let Some(set) =
                    inner
                        .sa_double_idx
                        .get_mut(&(namespace_id, attr_id, format!("{v:.17}")))
                {
                    set.remove(&run_key);
                }
            }
            (SearchAttrType::Datetime, SearchAttrValue::Datetime(v)) => {
                if let Some(set) = inner.sa_datetime_idx.get_mut(&(
                    namespace_id,
                    attr_id,
                    v.unix_timestamp_nanos(),
                )) {
                    set.remove(&run_key);
                }
            }
            (SearchAttrType::Text, SearchAttrValue::Text(v)) => {
                for token in Self::index_text(&v) {
                    if let Some(set) = inner.sa_text_idx.get_mut(&(namespace_id, attr_id, token)) {
                        set.remove(&run_key);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn accumulate_rollup(&self, entries: &[RollupDelta]) -> Result<()> {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            // The in-memory store aggregates per `(namespace, archetype, dimension,
            // value)` and ignores `stripe` — striping is a DSQL hot-row concern.
            *inner
                .rollups
                .entry((
                    entry.namespace_id,
                    entry.archetype_id,
                    entry.dimension,
                    entry.value.clone(),
                ))
                .or_insert(0) += entry.delta;
        }
        Ok(())
    }

    async fn list_executions(
        &self,
        namespace_id: NamespaceId,
        filter: &CompiledFilter,
        sort: SortOrder,
        page: &PageBounds,
    ) -> Result<ListResult> {
        let inner = self.inner.lock().await;
        let mut rows = Vec::new();
        for row in inner.rows.values() {
            if row.namespace_id == namespace_id && matches_filter(&inner, row, filter)? {
                rows.push(row.clone());
            }
        }
        sort_rows(&mut rows, sort);
        if let Some(after) = &page.after {
            rows.retain(|row| row_after(row, after));
        }
        let limit = page.limit.min(crate::types::MAX_PAGE_SIZE);
        let next_page_token = if rows.len() > limit {
            let last = rows[limit - 1].clone();
            Some(PageToken {
                close_time: last.close_time,
                start_time: last.start_time,
                run_key: last.run_key,
                sort_order: sort,
            })
        } else {
            None
        };
        rows.truncate(limit);
        Ok(ListResult {
            rows,
            next_page_token,
        })
    }

    async fn count_executions(
        &self,
        namespace_id: NamespaceId,
        filter: &CompiledFilter,
        group_by: Option<GroupByField>,
    ) -> Result<CountResult> {
        let inner = self.inner.lock().await;
        let mut rows = Vec::new();
        for row in inner.rows.values() {
            if row.namespace_id == namespace_id && matches_filter(&inner, row, filter)? {
                rows.push(row);
            }
        }
        let total_count = rows.len() as i64;
        let mut groups = Vec::new();
        if let Some(group_by) = group_by {
            let mut counts: BTreeMap<String, i64> = BTreeMap::new();
            for row in rows {
                let value = group_value(&inner, row, &group_by).unwrap_or_default();
                *counts.entry(value).or_insert(0) += 1;
            }
            groups = counts
                .into_iter()
                .map(|(value, count)| RollupCounter { value, count })
                .collect();
        }
        Ok(CountResult {
            total_count,
            groups,
        })
    }

    async fn count_from_rollup(
        &self,
        namespace_id: NamespaceId,
        archetype_id: ArchetypeId,
        dimension: RollupDimension,
    ) -> Result<CountResult> {
        let inner = self.inner.lock().await;
        let mut groups = Vec::new();
        let mut total_count = 0;
        for ((ns, arch, dim, value), count) in &inner.rollups {
            if *ns == namespace_id && *arch == archetype_id && *dim == dimension {
                total_count += *count;
                groups.push(RollupCounter {
                    value: value.clone(),
                    count: *count,
                });
            }
        }
        Ok(CountResult {
            total_count,
            groups,
        })
    }

    async fn load_checkpoint(&self, partition_id: u32) -> Result<Option<ProjectionCursor>> {
        Ok(self.inner.lock().await.checkpoints.get(&partition_id).cloned())
    }

    async fn save_checkpoint(&self, cursor: &ProjectionCursor) -> Result<()> {
        self.inner
            .lock()
            .await
            .checkpoints
            .insert(cursor.partition_id, cursor.clone());
        Ok(())
    }

    async fn resolve_attr(
        &self,
        namespace_id: NamespaceId,
        name: &str,
    ) -> Result<Option<AttrDescriptor>> {
        Ok(self
            .inner
            .lock()
            .await
            .registry
            .get(&(namespace_id, name.to_string()))
            .cloned())
    }

    async fn register_attr(
        &self,
        namespace_id: NamespaceId,
        name: String,
        attr_type: SearchAttrType,
    ) -> Result<AttrId> {
        let mut inner = self.inner.lock().await;
        if let Some(existing) = inner.registry.get(&(namespace_id, name.clone())) {
            return Ok(existing.attr_id);
        }
        inner.next_attr_id += 1;
        let attr_id = AttrId(inner.next_attr_id);
        inner
            .registry
            .insert((namespace_id, name), AttrDescriptor { attr_id, attr_type });
        Ok(attr_id)
    }

    async fn get_row(&self, run_key: RunKey) -> Option<ExecutionRow> {
        self.inner.lock().await.rows.get(&run_key).cloned()
    }
}

fn sort_rows(rows: &mut [ExecutionRow], sort: SortOrder) {
    rows.sort_by(|a, b| match sort {
        SortOrder::Default => b
            .close_time
            .cmp(&a.close_time)
            .then_with(|| b.start_time.cmp(&a.start_time))
            .then_with(|| b.run_key.0.cmp(&a.run_key.0)),
        SortOrder::StartTimeAsc => a
            .start_time
            .cmp(&b.start_time)
            .then_with(|| a.run_key.0.cmp(&b.run_key.0)),
        SortOrder::StartTimeDesc => b
            .start_time
            .cmp(&a.start_time)
            .then_with(|| b.run_key.0.cmp(&a.run_key.0)),
        SortOrder::CloseTimeAsc => a
            .close_time
            .cmp(&b.close_time)
            .then_with(|| a.run_key.0.cmp(&b.run_key.0)),
        SortOrder::CloseTimeDesc => b
            .close_time
            .cmp(&a.close_time)
            .then_with(|| b.run_key.0.cmp(&a.run_key.0)),
    });
}

fn row_after(row: &ExecutionRow, token: &PageToken) -> bool {
    (row.close_time, row.start_time, row.run_key.0)
        < (token.close_time, token.start_time, token.run_key.0)
}

fn matches_filter(
    inner: &VisibilityState,
    row: &ExecutionRow,
    filter: &CompiledFilter,
) -> Result<bool> {
    // Forced archetype scope (Requirement 13.1): a row outside the query's
    // archetype is never visible, regardless of the user expression.
    if let Some(archetype) = filter.archetype {
        if row.archetype_id != archetype {
            return Ok(false);
        }
    }
    match &filter.expr {
        Some(expr) => eval_expr(inner, row, expr),
        None => Ok(true),
    }
}

fn eval_expr(inner: &VisibilityState, row: &ExecutionRow, expr: &FilterExpr) -> Result<bool> {
    match expr {
        FilterExpr::And(lhs, rhs) => Ok(eval_expr(inner, row, lhs)? && eval_expr(inner, row, rhs)?),
        FilterExpr::Or(lhs, rhs) => Ok(eval_expr(inner, row, lhs)? || eval_expr(inner, row, rhs)?),
        FilterExpr::Compare { field, op, value } => eval_compare(inner, row, field, *op, value),
        FilterExpr::In { field, values } => eval_in(inner, row, field, values),
        FilterExpr::Between { field, low, high } => {
            reject_multi_value_range(field, "BETWEEN")?;
            Ok(compare(field_value(inner, row, field), CompareOp::Ge, low)
                && compare(field_value(inner, row, field), CompareOp::Le, high))
        }
        FilterExpr::StartsWith { field, prefix } => eval_starts_with(inner, row, field, prefix),
    }
}

fn eval_compare(
    inner: &VisibilityState,
    row: &ExecutionRow,
    field: &FieldRef,
    op: CompareOp,
    value: &FilterValue,
) -> Result<bool> {
    if matches!(
        op,
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
    ) {
        reject_multi_value_range(field, "range comparison")?;
    }
    match (field, custom_value(inner, row, field)) {
        (_, Some(SearchAttrValue::KeywordList(values))) => {
            eval_keyword_list_compare(&values, op, value)
        }
        (_, Some(SearchAttrValue::Text(text))) => eval_text_compare(&text, op, value),
        (
            FieldRef::Custom {
                attr_type: SearchAttrType::KeywordList | SearchAttrType::Text,
                ..
            },
            None,
        ) => Ok(matches!(op, CompareOp::Ne)),
        _ => Ok(compare(field_value(inner, row, field), op, value)),
    }
}

fn eval_in(
    inner: &VisibilityState,
    row: &ExecutionRow,
    field: &FieldRef,
    values: &[FilterValue],
) -> Result<bool> {
    match custom_value(inner, row, field) {
        Some(SearchAttrValue::KeywordList(elements)) => Ok(values.iter().any(|value| {
            let FilterValue::String(candidate) = value else {
                return false;
            };
            elements.iter().any(|element| element == candidate)
        })),
        Some(SearchAttrValue::Text(text)) => {
            let candidates = values
                .iter()
                .filter_map(|value| {
                    let FilterValue::String(candidate) = value else {
                        return None;
                    };
                    normalize_text_literal(candidate)
                })
                .collect::<HashSet<_>>();
            if candidates.is_empty() {
                return Ok(false);
            }
            Ok(InMemoryVisibilityStore::index_text(&text)
                .iter()
                .any(|token| candidates.contains(token)))
        }
        _ => Ok(values
            .iter()
            .any(|v| compare(field_value(inner, row, field), CompareOp::Eq, v))),
    }
}

fn eval_starts_with(
    inner: &VisibilityState,
    row: &ExecutionRow,
    field: &FieldRef,
    prefix: &str,
) -> Result<bool> {
    match custom_value(inner, row, field) {
        Some(SearchAttrValue::KeywordList(values)) => {
            Ok(values.iter().any(|value| value.starts_with(prefix)))
        }
        Some(SearchAttrValue::Text(text)) => {
            let prefix = prefix.to_ascii_lowercase();
            Ok(InMemoryVisibilityStore::index_text(&text)
                .iter()
                .any(|token| token.starts_with(&prefix)))
        }
        _ => Ok(matches!(
            field_value(inner, row, field),
            Some(FilterValue::String(v)) if v.starts_with(prefix)
        )),
    }
}

fn eval_keyword_list_compare(
    values: &[String],
    op: CompareOp,
    value: &FilterValue,
) -> Result<bool> {
    let FilterValue::String(candidate) = value else {
        return Ok(false);
    };
    match op {
        CompareOp::Eq => Ok(values.iter().any(|value| value == candidate)),
        CompareOp::Ne => Ok(values.iter().all(|value| value != candidate)),
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
            bail!("KeywordList does not support ordered comparison operators")
        }
    }
}

fn eval_text_compare(text: &str, op: CompareOp, value: &FilterValue) -> Result<bool> {
    let FilterValue::String(candidate) = value else {
        return Ok(false);
    };
    let Some(candidate) = normalize_text_literal(candidate) else {
        return Ok(matches!(op, CompareOp::Ne));
    };
    let contains = InMemoryVisibilityStore::index_text(text)
        .iter()
        .any(|token| token == &candidate);
    match op {
        CompareOp::Eq => Ok(contains),
        CompareOp::Ne => Ok(!contains),
        CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
            bail!("Text does not support ordered comparison operators")
        }
    }
}

fn normalize_text_literal(value: &str) -> Option<String> {
    let mut tokens = text_search_tokens(value);
    if tokens.len() == 1 {
        tokens.pop()
    } else {
        None
    }
}

fn custom_value(
    inner: &VisibilityState,
    row: &ExecutionRow,
    field: &FieldRef,
) -> Option<SearchAttrValue> {
    let FieldRef::Custom { attr_id, .. } = field else {
        return None;
    };
    inner.sa_current.get(&(row.run_key, *attr_id)).cloned()
}

fn reject_multi_value_range(field: &FieldRef, op: &str) -> Result<()> {
    if matches!(
        field,
        FieldRef::Custom {
            attr_type: SearchAttrType::KeywordList | SearchAttrType::Text,
            ..
        }
    ) {
        bail!("{op} is not supported for KeywordList or Text search attributes");
    }
    Ok(())
}

fn field_value(
    inner: &VisibilityState,
    row: &ExecutionRow,
    field: &FieldRef,
) -> Option<FilterValue> {
    match field {
        FieldRef::System(SystemField::Archetype) => {
            Some(FilterValue::Int(i64::from(row.archetype_id.0)))
        }
        FieldRef::System(SystemField::WorkflowId) => {
            Some(FilterValue::String(row.workflow_id.0.clone()))
        }
        FieldRef::System(SystemField::RunId) => Some(FilterValue::String(row.run_id.0.to_string())),
        FieldRef::System(SystemField::WorkflowType) => {
            Some(FilterValue::String(row.workflow_type.0.clone()))
        }
        FieldRef::System(SystemField::TaskQueue) => {
            Some(FilterValue::String(row.task_queue.0.clone()))
        }
        // Status is filtered as the generic `status_keyword` string, not the
        // workflow `ExecutionStatus` enum (Requirement 10.5).
        FieldRef::System(SystemField::ExecutionStatus) => {
            Some(FilterValue::String(row.status_keyword.clone()))
        }
        FieldRef::System(SystemField::StartTime) => Some(FilterValue::Datetime(row.start_time)),
        FieldRef::System(SystemField::ExecutionTime) => {
            row.execution_time.map(FilterValue::Datetime)
        }
        FieldRef::System(SystemField::CloseTime) => row.close_time.map(FilterValue::Datetime),
        FieldRef::System(SystemField::HistoryLength) => Some(FilterValue::Int(row.history_length)),
        FieldRef::System(SystemField::ExecutionDuration) => {
            row.execution_duration.map(FilterValue::Int)
        }
        FieldRef::System(SystemField::StateTransitionCount) => {
            Some(FilterValue::Int(row.state_transition_count))
        }
        FieldRef::System(SystemField::HistorySizeBytes) => {
            Some(FilterValue::Int(row.history_size_bytes))
        }
        FieldRef::System(SystemField::ParentWorkflowId) => row
            .parent_workflow_id
            .as_ref()
            .map(|value| FilterValue::String(value.0.clone())),
        FieldRef::System(SystemField::ParentRunId) => row
            .parent_run_id
            .map(|value| FilterValue::String(value.0.to_string())),
        FieldRef::System(SystemField::RootWorkflowId) => {
            Some(FilterValue::String(row.root_workflow_id.0.clone()))
        }
        FieldRef::System(SystemField::RootRunId) => {
            Some(FilterValue::String(row.root_run_id.0.to_string()))
        }
        FieldRef::Custom { attr_id, .. } => inner
            .sa_current
            .get(&(row.run_key, *attr_id))
            .map(search_attr_to_filter),
    }
}

fn search_attr_to_filter(value: &SearchAttrValue) -> FilterValue {
    match value {
        SearchAttrValue::Keyword(v) => FilterValue::String(v.clone()),
        SearchAttrValue::KeywordList(v) => FilterValue::String(v.join(",")),
        SearchAttrValue::Int(v) => FilterValue::Int(*v),
        SearchAttrValue::Double(v) => FilterValue::Float(*v),
        SearchAttrValue::Bool(v) => FilterValue::Bool(*v),
        SearchAttrValue::Datetime(v) => FilterValue::Datetime(*v),
        SearchAttrValue::Text(v) => FilterValue::String(v.clone()),
    }
}

fn compare(left: Option<FilterValue>, op: CompareOp, right: &FilterValue) -> bool {
    let Some(left) = left else {
        return false;
    };
    match (left, right) {
        (FilterValue::String(a), FilterValue::String(b)) => cmp_ord(&a, b, op),
        (FilterValue::Int(a), FilterValue::Int(b)) => cmp_ord(&a, b, op),
        (FilterValue::Float(a), FilterValue::Float(b)) => cmp_partial(a, *b, op),
        (FilterValue::Bool(a), FilterValue::Bool(b)) => cmp_ord(&a, b, op),
        (FilterValue::Datetime(a), FilterValue::Datetime(b)) => cmp_ord(&a, b, op),
        _ => false,
    }
}

fn cmp_ord<T: Ord>(a: &T, b: &T, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

fn cmp_partial(a: f64, b: f64, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a < b,
        CompareOp::Le => a <= b,
        CompareOp::Gt => a > b,
        CompareOp::Ge => a >= b,
    }
}

fn group_value(
    inner: &VisibilityState,
    row: &ExecutionRow,
    field: &GroupByField,
) -> Option<String> {
    match field {
        GroupByField::System(SystemField::Archetype) => Some(row.archetype_id.0.to_string()),
        GroupByField::System(SystemField::ExecutionStatus) => Some(row.status_keyword.clone()),
        GroupByField::System(SystemField::WorkflowType) => Some(row.workflow_type.0.clone()),
        GroupByField::System(SystemField::TaskQueue) => Some(row.task_queue.0.clone()),
        GroupByField::System(SystemField::WorkflowId) => Some(row.workflow_id.0.clone()),
        GroupByField::System(SystemField::RunId) => Some(row.run_id.0.to_string()),
        GroupByField::System(SystemField::StartTime) => Some(row.start_time.to_string()),
        GroupByField::System(SystemField::ExecutionTime) => {
            row.execution_time.map(|v| v.to_string())
        }
        GroupByField::System(SystemField::CloseTime) => row.close_time.map(|v| v.to_string()),
        GroupByField::System(SystemField::HistoryLength) => Some(row.history_length.to_string()),
        GroupByField::System(SystemField::ExecutionDuration) => {
            row.execution_duration.map(|v| v.to_string())
        }
        GroupByField::System(SystemField::StateTransitionCount) => {
            Some(row.state_transition_count.to_string())
        }
        GroupByField::System(SystemField::HistorySizeBytes) => {
            Some(row.history_size_bytes.to_string())
        }
        GroupByField::System(SystemField::ParentWorkflowId) => {
            row.parent_workflow_id.as_ref().map(|v| v.0.clone())
        }
        GroupByField::System(SystemField::ParentRunId) => {
            row.parent_run_id.map(|v| v.0.to_string())
        }
        GroupByField::System(SystemField::RootWorkflowId) => Some(row.root_workflow_id.0.clone()),
        GroupByField::System(SystemField::RootRunId) => Some(row.root_run_id.0.to_string()),
        GroupByField::Custom { attr_id, .. } => {
            inner
                .sa_current
                .get(&(row.run_key, *attr_id))
                .map(|v| match v {
                    SearchAttrValue::Keyword(v) => v.clone(),
                    SearchAttrValue::KeywordList(v) => v.join(","),
                    SearchAttrValue::Int(v) => v.to_string(),
                    SearchAttrValue::Double(v) => v.to_string(),
                    SearchAttrValue::Bool(v) => v.to_string(),
                    SearchAttrValue::Datetime(v) => v.to_string(),
                    SearchAttrValue::Text(v) => v.clone(),
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use time::OffsetDateTime;
    use tokeira_types::{
        ArchetypeId, ExecutionStatus, Memo, RunId, RunKey, SearchAttributes, TaskQueueName,
        TransitionSeq, WorkflowId, WorkflowType,
    };
    use uuid::Uuid;

    fn arb_projection_cursor() -> impl Strategy<Value = ProjectionCursor> {
        (
            0u32..100,
            1u16..64,
            proptest::option::of(any::<u128>().prop_map(|v| RunKey(Uuid::from_u128(v)))),
            proptest::option::of(any::<u64>().prop_map(TransitionSeq)),
        )
            .prop_map(|(pid, fanout, rk, ts)| ProjectionCursor {
                partition_id: pid,
                fanout,
                last_run_key: rk,
                last_transition_seq: ts,
            })
    }

    fn arb_status() -> impl Strategy<Value = ExecutionStatus> {
        prop_oneof![
            Just(ExecutionStatus::Running),
            Just(ExecutionStatus::Completed),
            Just(ExecutionStatus::Failed),
            Just(ExecutionStatus::Cancelled),
            Just(ExecutionStatus::Terminated),
            Just(ExecutionStatus::ContinuedAsNew),
            Just(ExecutionStatus::TimedOut),
        ]
    }

    fn arb_execution_row(ns: NamespaceId) -> impl Strategy<Value = ExecutionRow> {
        (
            any::<u128>(),
            "[a-z]{1,8}",
            any::<u128>(),
            "[A-Z][a-z]{1,6}",
            "[a-z]{1,6}",
            arb_status(),
            1i64..1_000_000,
            proptest::option::of(1i64..2_000_000),
            1i64..100,
            1i64..100,
        )
            .prop_map(
                move |(rk, wf_id, run_id, wf_type, tq, status, start, close, hl, stc)| {
                    ExecutionRow {
                        run_key: RunKey(Uuid::from_u128(rk)),
                        namespace_id: ns,
                        archetype_id: ArchetypeId::WORKFLOW,
                        business_id: wf_id.clone(),
                        workflow_id: WorkflowId(wf_id.clone()),
                        run_id: RunId(Uuid::from_u128(run_id)),
                        authority_epoch: 0,
                        source_transition_seq: TransitionSeq(stc as u64),
                        status_keyword: crate::types::workflow_status_keyword(status),
                        lifecycle_state: crate::types::workflow_lifecycle_state(status),
                        workflow_type: WorkflowType(wf_type),
                        task_queue: TaskQueueName(tq),
                        status,
                        start_time: OffsetDateTime::from_unix_timestamp(start).unwrap(),
                        update_time: close
                            .map(|c| OffsetDateTime::from_unix_timestamp(c).unwrap())
                            .unwrap_or_else(|| OffsetDateTime::from_unix_timestamp(start).unwrap()),
                        execution_time: None,
                        close_time: close.map(|c| OffsetDateTime::from_unix_timestamp(c).unwrap()),
                        history_length: hl,
                        execution_duration: close.map(|c| (c - start) * 1_000_000_000),
                        state_transition_count: stc,
                        history_size_bytes: 0,
                        parent_workflow_id: None,
                        parent_run_id: None,
                        root_workflow_id: WorkflowId(wf_id.clone()),
                        root_run_id: RunId(Uuid::from_u128(run_id)),
                        memo: Memo::default(),
                        search_attributes: SearchAttributes::default(),
                        transition_count: stc,
                        search_attr_generation: 0,
                        search_attr_version: 0,
                    }
                },
            )
    }

    fn row(run_key: u128) -> ExecutionRow {
        ExecutionRow {
            run_key: RunKey(Uuid::from_u128(run_key)),
            namespace_id: NamespaceId(Uuid::from_u128(1)),
            archetype_id: ArchetypeId::WORKFLOW,
            business_id: format!("wf-{run_key}"),
            workflow_id: WorkflowId(format!("wf-{run_key}")),
            run_id: RunId(Uuid::from_u128(run_key + 100)),
            authority_epoch: 0,
            source_transition_seq: TransitionSeq(1),
            status_keyword: crate::types::workflow_status_keyword(ExecutionStatus::Running),
            lifecycle_state: crate::types::workflow_lifecycle_state(ExecutionStatus::Running),
            workflow_type: WorkflowType("Example".to_owned()),
            task_queue: tokeira_types::TaskQueueName("main".to_owned()),
            status: ExecutionStatus::Running,
            start_time: OffsetDateTime::from_unix_timestamp(1).unwrap(),
            update_time: OffsetDateTime::from_unix_timestamp(1).unwrap(),
            execution_time: None,
            close_time: None,
            history_length: 1,
            execution_duration: None,
            state_transition_count: 1,
            history_size_bytes: 0,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: WorkflowId(format!("wf-{run_key}")),
            root_run_id: RunId(Uuid::from_u128(run_key + 100)),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            transition_count: 1,
            search_attr_generation: 0,
            search_attr_version: 0,
        }
    }

    fn page() -> PageBounds {
        PageBounds {
            limit: 10,
            after: None,
        }
    }

    #[tokio::test]
    async fn list_and_count_are_archetype_scoped() {
        // The shared index holds multiple archetypes (workflows + activities). A
        // scoped query must never see another archetype's rows (Requirement 13.1).
        let store = InMemoryVisibilityStore::default();
        let ns = NamespaceId(Uuid::from_u128(1));
        let activity = ArchetypeId(7);

        let wf = row(1);
        let mut act = row(2);
        act.archetype_id = activity;
        store.upsert_execution(&wf).await.unwrap();
        store.upsert_execution(&act).await.unwrap();

        let scoped = |arch: Option<ArchetypeId>| CompiledFilter {
            expr: None,
            archetype: arch,
        };

        // Workflow scope sees only the workflow row, never the activity.
        let wf_list = store
            .list_executions(
                ns,
                &scoped(Some(ArchetypeId::WORKFLOW)),
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert_eq!(wf_list.rows.len(), 1);
        assert_eq!(wf_list.rows[0].archetype_id, ArchetypeId::WORKFLOW);

        // Activity scope sees only the activity row.
        let act_list = store
            .list_executions(ns, &scoped(Some(activity)), SortOrder::Default, &page())
            .await
            .unwrap();
        assert_eq!(act_list.rows.len(), 1);
        assert_eq!(act_list.rows[0].archetype_id, activity);

        // Unscoped (`None`, the store-mechanics default) spans both.
        let all = store
            .list_executions(ns, &scoped(None), SortOrder::Default, &page())
            .await
            .unwrap();
        assert_eq!(all.rows.len(), 2);

        // Count is scoped the same way.
        let wf_count = store
            .count_executions(ns, &scoped(Some(ArchetypeId::WORKFLOW)), None)
            .await
            .unwrap();
        assert_eq!(wf_count.total_count, 1);
    }

    #[tokio::test]
    async fn temporal_namespace_division_resolves_to_archetype() {
        use crate::filter::compile_filter;
        let store = InMemoryVisibilityStore::default();
        let ns = NamespaceId(Uuid::from_u128(1));
        let wf = row(1); // archetype WORKFLOW (0)
        let mut act = row(2);
        act.archetype_id = ArchetypeId(7);
        store.upsert_execution(&wf).await.unwrap();
        store.upsert_execution(&act).await.unwrap();

        // TemporalNamespaceDivision and the reserved `archetype` resolve to the
        // archetype_id column instead of erroring as an unknown SA (Req 13.2); the
        // empty/default/workflow division selects the workflow archetype.
        for query in [
            "TemporalNamespaceDivision = 'workflow'",
            "archetype = 'workflow'",
            "TemporalNamespaceDivision = ''",
        ] {
            let filter = compile_filter(Some(query), ns, &store).await.unwrap();
            let result = store
                .list_executions(ns, &filter, SortOrder::Default, &page())
                .await
                .unwrap();
            assert_eq!(result.rows.len(), 1, "{query}");
            assert_eq!(result.rows[0].archetype_id, ArchetypeId::WORKFLOW);
        }

        // A division name with no tokeira archetype maps to a sentinel → matches
        // nothing (a contradicting predicate yields empty, never a scope escape).
        let filter = compile_filter(Some("TemporalNamespaceDivision = 'scheduler'"), ns, &store)
            .await
            .unwrap();
        let result = store
            .list_executions(ns, &filter, SortOrder::Default, &page())
            .await
            .unwrap();
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn keyword_list_filter_uses_element_membership() {
        let store = InMemoryVisibilityStore::default();
        let ns = NamespaceId(Uuid::from_u128(1));
        let row = row(1);
        let attr_id = store
            .register_attr(ns, "tags".to_owned(), SearchAttrType::KeywordList)
            .await
            .unwrap();
        store.upsert_execution(&row).await.unwrap();
        store
            .upsert_search_attr_index(
                row.run_key,
                ns,
                attr_id,
                SearchAttrType::KeywordList,
                &SearchAttrValue::KeywordList(vec!["a".to_owned(), "b".to_owned()]),
            )
            .await
            .unwrap();
        let field = FieldRef::Custom {
            name: "tags".to_owned(),
            attr_id,
            attr_type: SearchAttrType::KeywordList,
        };

        let result = store
            .list_executions(
                ns,
                &CompiledFilter {
                archetype: None,
                    expr: Some(FilterExpr::Compare {
                        field: field.clone(),
                        op: CompareOp::Eq,
                        value: FilterValue::String("a".to_owned()),
                    }),
                },
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        let result = store
            .list_executions(
                ns,
                &CompiledFilter {
                archetype: None,
                    expr: Some(FilterExpr::Compare {
                        field,
                        op: CompareOp::Ne,
                        value: FilterValue::String("a".to_owned()),
                    }),
                },
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn paused_status_filter_matches_and_clears_on_unpause() {
        let store = InMemoryVisibilityStore::default();
        let ns = NamespaceId(Uuid::from_u128(1));

        // Seed a paused execution. Status is queried via `status_keyword`, so it
        // must stay consistent with the typed `status` on a manual mutation.
        let mut paused = row(42);
        paused.status = ExecutionStatus::Paused;
        paused.status_keyword = crate::types::workflow_status_keyword(ExecutionStatus::Paused);
        store.upsert_execution(&paused).await.unwrap();

        let status_filter = |status: ExecutionStatus| CompiledFilter {
                archetype: None,
            expr: Some(FilterExpr::Compare {
                field: FieldRef::System(SystemField::ExecutionStatus),
                op: CompareOp::Eq,
                value: FilterValue::String(crate::types::workflow_status_keyword(status)),
            }),
        };

        // ListWorkflowExecutions for ExecutionStatus = "Paused" returns the run.
        let listed = store
            .list_executions(
                ns,
                &status_filter(ExecutionStatus::Paused),
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert_eq!(listed.rows.len(), 1);
        assert_eq!(listed.rows[0].run_key, paused.run_key);

        // CountWorkflowExecutions for the same predicate counts it.
        let counted = store
            .count_executions(ns, &status_filter(ExecutionStatus::Paused), None)
            .await
            .unwrap();
        assert_eq!(counted.total_count, 1);

        // Unpause projects a Running status update for the same run.
        let mut running = paused.clone();
        running.status = ExecutionStatus::Running;
        running.status_keyword = crate::types::workflow_status_keyword(ExecutionStatus::Running);
        store.upsert_execution(&running).await.unwrap();

        // The run no longer matches "Paused".
        let listed = store
            .list_executions(
                ns,
                &status_filter(ExecutionStatus::Paused),
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert!(listed.rows.is_empty());

        // The run now matches "Running".
        let listed = store
            .list_executions(
                ns,
                &status_filter(ExecutionStatus::Running),
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert_eq!(listed.rows.len(), 1);
        assert_eq!(listed.rows[0].run_key, paused.run_key);
    }

    #[tokio::test]
    async fn text_filter_uses_normalized_token_matching() {
        let store = InMemoryVisibilityStore::default();
        let ns = NamespaceId(Uuid::from_u128(1));
        let row = row(2);
        let attr_id = store
            .register_attr(ns, "text".to_owned(), SearchAttrType::Text)
            .await
            .unwrap();
        store.upsert_execution(&row).await.unwrap();
        store
            .upsert_search_attr_index(
                row.run_key,
                ns,
                attr_id,
                SearchAttrType::Text,
                &SearchAttrValue::Text("Hello World".to_owned()),
            )
            .await
            .unwrap();
        let field = FieldRef::Custom {
            name: "text".to_owned(),
            attr_id,
            attr_type: SearchAttrType::Text,
        };

        let result = store
            .list_executions(
                ns,
                &CompiledFilter {
                archetype: None,
                    expr: Some(FilterExpr::Compare {
                        field: field.clone(),
                        op: CompareOp::Eq,
                        value: FilterValue::String("hello".to_owned()),
                    }),
                },
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);

        let result = store
            .list_executions(
                ns,
                &CompiledFilter {
                archetype: None,
                    expr: Some(FilterExpr::Compare {
                        field: field.clone(),
                        op: CompareOp::Eq,
                        value: FilterValue::String("hello world".to_owned()),
                    }),
                },
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert!(result.rows.is_empty());

        let result = store
            .list_executions(
                ns,
                &CompiledFilter {
                archetype: None,
                    expr: Some(FilterExpr::In {
                        field,
                        values: vec![FilterValue::String("HEL".to_owned())],
                    }),
                },
                SortOrder::Default,
                &page(),
            )
            .await
            .unwrap();
        assert!(result.rows.is_empty());
    }

    // Feature: projection-visibility, Property 15:
    // Checkpoint Round-Trip
    // **Validates: Requirements 9.1, 9.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_checkpoint_round_trip(
            cursor in arb_projection_cursor(),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let store =
                    InMemoryVisibilityStore::default();
                store
                    .save_checkpoint(&cursor)
                    .await
                    .unwrap();
                let loaded = store
                    .load_checkpoint(cursor.partition_id)
                    .await
                    .unwrap()
                    .unwrap();
                prop_assert_eq!(loaded, cursor);
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 3:
    // Search Attribute Indexing
    // **Validates: Requirements 2.1, 2.2, 2.6**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_search_attr_indexing(
            keyword in "[a-z]{1,10}",
            int_val in any::<i64>(),
            bool_val in any::<bool>(),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let rk = RunKey(Uuid::from_u128(42));
                let store =
                    InMemoryVisibilityStore::default();
                let kw_id = store
                    .register_attr(
                        ns,
                        "kw".into(),
                        SearchAttrType::Keyword,
                    )
                    .await
                    .unwrap();
                let int_id = store
                    .register_attr(
                        ns,
                        "num".into(),
                        SearchAttrType::Int,
                    )
                    .await
                    .unwrap();
                let bool_id = store
                    .register_attr(
                        ns,
                        "flag".into(),
                        SearchAttrType::Bool,
                    )
                    .await
                    .unwrap();
                store
                    .upsert_search_attr_index(
                        rk,
                        ns,
                        kw_id,
                        SearchAttrType::Keyword,
                        &SearchAttrValue::Keyword(
                            keyword.clone(),
                        ),
                    )
                    .await
                    .unwrap();
                store
                    .upsert_search_attr_index(
                        rk,
                        ns,
                        int_id,
                        SearchAttrType::Int,
                        &SearchAttrValue::Int(int_val),
                    )
                    .await
                    .unwrap();
                store
                    .upsert_search_attr_index(
                        rk,
                        ns,
                        bool_id,
                        SearchAttrType::Bool,
                        &SearchAttrValue::Bool(bool_val),
                    )
                    .await
                    .unwrap();
                let inner = store.inner.lock().await;
                let kw_set = inner
                    .sa_keyword_idx
                    .get(&(ns, kw_id, keyword));
                prop_assert!(
                    kw_set
                        .map(|s| s.contains(&rk))
                        .unwrap_or(false)
                );
                let int_set = inner
                    .sa_int_idx
                    .get(&(ns, int_id, int_val));
                prop_assert!(
                    int_set
                        .map(|s| s.contains(&rk))
                        .unwrap_or(false)
                );
                let bool_set = inner
                    .sa_bool_idx
                    .get(&(ns, bool_id, bool_val));
                prop_assert!(
                    bool_set
                        .map(|s| s.contains(&rk))
                        .unwrap_or(false)
                );
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 4:
    // Index Update Cleanup
    // **Validates: Requirements 2.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_index_update_cleanup(
            old_val in "[a-z]{1,10}",
            new_val in "[a-z]{1,10}",
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let rk = RunKey(Uuid::from_u128(42));
                let store =
                    InMemoryVisibilityStore::default();
                let attr_id = store
                    .register_attr(
                        ns,
                        "tag".into(),
                        SearchAttrType::Keyword,
                    )
                    .await
                    .unwrap();
                store
                    .upsert_search_attr_index(
                        rk,
                        ns,
                        attr_id,
                        SearchAttrType::Keyword,
                        &SearchAttrValue::Keyword(
                            old_val.clone(),
                        ),
                    )
                    .await
                    .unwrap();
                // Remove old, insert new
                store
                    .remove_search_attr_index(
                        rk,
                        ns,
                        attr_id,
                        SearchAttrType::Keyword,
                    )
                    .await
                    .unwrap();
                store
                    .upsert_search_attr_index(
                        rk,
                        ns,
                        attr_id,
                        SearchAttrType::Keyword,
                        &SearchAttrValue::Keyword(
                            new_val.clone(),
                        ),
                    )
                    .await
                    .unwrap();
                let inner = store.inner.lock().await;
                // Old value should not contain rk
                // (unless old == new)
                if old_val != new_val {
                    let old_set = inner
                        .sa_keyword_idx
                        .get(&(ns, attr_id, old_val));
                    prop_assert!(
                        old_set
                            .map(|s| !s.contains(&rk))
                            .unwrap_or(true)
                    );
                }
                // New value should contain rk
                let new_set = inner
                    .sa_keyword_idx
                    .get(&(ns, attr_id, new_val));
                prop_assert!(
                    new_set
                        .map(|s| s.contains(&rk))
                        .unwrap_or(false)
                );
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 5:
    // Text Tokenization
    // **Validates: Requirements 2.7**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_text_tokenization(
            text in "[a-zA-Z0-9 ]{1,50}",
        ) {
            let tokens =
                InMemoryVisibilityStore::index_text(&text);
            let expected: std::collections::HashSet<String> = {
                let mut out =
                    std::collections::HashSet::new();
                let mut token = String::new();
                for ch in text.chars() {
                    if ch.is_ascii_alphanumeric() {
                        token.push(
                            ch.to_ascii_lowercase(),
                        );
                    } else if !token.is_empty() {
                        out.insert(
                            std::mem::take(&mut token),
                        );
                    }
                }
                if !token.is_empty() {
                    out.insert(token);
                }
                out
            };
            let actual: std::collections::HashSet<String> =
                tokens.into_iter().collect();
            prop_assert_eq!(actual, expected);
        }
    }

    // Feature: projection-visibility, Property 7:
    // Pagination Completeness
    // **Validates: Requirements 4.1, 4.2, 4.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_pagination_completeness(
            rows in proptest::collection::vec(
                arb_execution_row(
                    NamespaceId(Uuid::from_u128(1))
                ),
                1..20,
            ),
            page_size in 1usize..10,
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let store =
                    InMemoryVisibilityStore::default();
                // Deduplicate by run_key
                let mut seen =
                    std::collections::HashSet::new();
                let mut unique_rows = Vec::new();
                for row in &rows {
                    if seen.insert(row.run_key) {
                        store
                            .upsert_execution(row)
                            .await
                            .unwrap();
                        unique_rows.push(row.clone());
                    }
                }
                let filter = CompiledFilter::default();
                // Get all rows in one shot
                let all = store
                    .list_executions(
                        ns,
                        &filter,
                        SortOrder::Default,
                        &PageBounds {
                            limit: 10000,
                            after: None,
                        },
                    )
                    .await
                    .unwrap();
                // Paginate
                let mut collected = Vec::new();
                let mut token: Option<PageToken> = None;
                loop {
                    let result = store
                        .list_executions(
                            ns,
                            &filter,
                            SortOrder::Default,
                            &PageBounds {
                                limit: page_size,
                                after: token,
                            },
                        )
                        .await
                        .unwrap();
                    collected
                        .extend(result.rows.clone());
                    if result.next_page_token.is_none() {
                        break;
                    }
                    token = result.next_page_token;
                }
                let all_keys: Vec<_> = all
                    .rows
                    .iter()
                    .map(|r| r.run_key)
                    .collect();
                let paged_keys: Vec<_> = collected
                    .iter()
                    .map(|r| r.run_key)
                    .collect();
                prop_assert_eq!(paged_keys, all_keys);
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 8:
    // Sort Order Correctness
    // **Validates: Requirements 4.4, 4.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_sort_order_correctness(
            rows in proptest::collection::vec(
                arb_execution_row(
                    NamespaceId(Uuid::from_u128(1))
                ),
                2..15,
            ),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let store =
                    InMemoryVisibilityStore::default();
                let mut seen =
                    std::collections::HashSet::new();
                for row in &rows {
                    if seen.insert(row.run_key) {
                        store
                            .upsert_execution(row)
                            .await
                            .unwrap();
                    }
                }
                let filter = CompiledFilter::default();
                let page = PageBounds {
                    limit: 10000,
                    after: None,
                };
                // Default sort
                let result = store
                    .list_executions(
                        ns,
                        &filter,
                        SortOrder::Default,
                        &page,
                    )
                    .await
                    .unwrap();
                for w in result.rows.windows(2) {
                    let a = &w[0];
                    let b = &w[1];
                    let ord = b
                        .close_time
                        .cmp(&a.close_time)
                        .then(
                            b.start_time
                                .cmp(&a.start_time),
                        )
                        .then(
                            b.run_key.0.cmp(&a.run_key.0),
                        );
                    prop_assert!(
                        !ord.is_gt(),
                        "default sort violated"
                    );
                }
                // StartTimeAsc
                let result = store
                    .list_executions(
                        ns,
                        &filter,
                        SortOrder::StartTimeAsc,
                        &page,
                    )
                    .await
                    .unwrap();
                for w in result.rows.windows(2) {
                    prop_assert!(
                        w[0].start_time
                            <= w[1].start_time,
                        "StartTimeAsc violated"
                    );
                }
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 9:
    // Count-List Consistency
    // **Validates: Requirements 5.1**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_count_list_consistency(
            rows in proptest::collection::vec(
                arb_execution_row(
                    NamespaceId(Uuid::from_u128(1))
                ),
                1..20,
            ),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let store =
                    InMemoryVisibilityStore::default();
                let mut seen =
                    std::collections::HashSet::new();
                for row in &rows {
                    if seen.insert(row.run_key) {
                        store
                            .upsert_execution(row)
                            .await
                            .unwrap();
                    }
                }
                let filter = CompiledFilter::default();
                let list = store
                    .list_executions(
                        ns,
                        &filter,
                        SortOrder::Default,
                        &PageBounds {
                            limit: 10000,
                            after: None,
                        },
                    )
                    .await
                    .unwrap();
                let count = store
                    .count_executions(
                        ns, &filter, None,
                    )
                    .await
                    .unwrap();
                prop_assert_eq!(
                    count.total_count,
                    list.rows.len() as i64
                );
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 10:
    // Group-By Count Correctness
    // **Validates: Requirements 5.2, 5.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_group_by_count_correctness(
            rows in proptest::collection::vec(
                arb_execution_row(
                    NamespaceId(Uuid::from_u128(1))
                ),
                1..20,
            ),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let store =
                    InMemoryVisibilityStore::default();
                let mut seen =
                    std::collections::HashSet::new();
                for row in &rows {
                    if seen.insert(row.run_key) {
                        store
                            .upsert_execution(row)
                            .await
                            .unwrap();
                    }
                }
                let filter = CompiledFilter::default();
                let group_by = Some(
                    GroupByField::System(
                        SystemField::ExecutionStatus,
                    ),
                );
                let result = store
                    .count_executions(
                        ns,
                        &filter,
                        group_by,
                    )
                    .await
                    .unwrap();
                let group_sum: i64 = result
                    .groups
                    .iter()
                    .map(|g| g.count)
                    .sum();
                prop_assert_eq!(
                    group_sum,
                    result.total_count
                );
                Ok(())
            })?;
        }
    }

    // Feature: projection-visibility, Property 13:
    // Rollup-Accelerated Count Consistency
    // **Validates: Requirements 6.4**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_rollup_count_consistency(
            rows in proptest::collection::vec(
                arb_execution_row(
                    NamespaceId(Uuid::from_u128(1))
                ),
                1..15,
            ),
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let ns = NamespaceId(Uuid::from_u128(1));
                let store =
                    InMemoryVisibilityStore::default();
                let mut seen =
                    std::collections::HashSet::new();
                for row in &rows {
                    if seen.insert(row.run_key) {
                        // Upsert row
                        store
                            .upsert_execution(row)
                            .await
                            .unwrap();
                        // Compute and accumulate rollup
                        // deltas for new row
                        let deltas =
                            crate::rollup::compute_rollup_deltas(
                                None, row,
                            );
                        store
                            .accumulate_rollup(&deltas)
                            .await
                            .unwrap();
                    }
                }
                // Compare rollup count vs scan count
                // for ExecutionStatus dimension
                let rollup_result = store
                    .count_from_rollup(
                        ns,
                        ArchetypeId::WORKFLOW,
                        RollupDimension::ExecutionStatus,
                    )
                    .await
                    .unwrap();
                let scan_result = store
                    .count_executions(
                        ns,
                        &CompiledFilter::default(),
                        Some(GroupByField::System(
                            SystemField::ExecutionStatus,
                        )),
                    )
                    .await
                    .unwrap();
                prop_assert_eq!(
                    rollup_result.total_count,
                    scan_result.total_count
                );
                Ok(())
            })?;
        }
    }
}
