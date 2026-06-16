//! Visibility data model for the projection plane.
//!
//! `VisibilitySnapshot` is the archetype-neutral post-transition image the
//! projection sink applies monotonically. `ExecutionRow` remains the workflow
//! compatibility view consumed by existing Temporal list/count translation
//! while workflows migrate onto the snapshot protocol. `FilterExpr` is the
//! compiled representation of a user-supplied visibility query, and
//! `SearchAttrType` defines the typed dimensions that custom search attributes
//! can occupy. Together these types form the contract between the projection
//! sink and the query path.

use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{
    ArchetypeId, ExecutionStatus, Memo, NamespaceId, ProjectionCursor, RunId, RunKey,
    SearchAttrValue, SearchAttributes, TaskQueueName, TransitionSeq, VisibilityLifecycleState,
    WorkflowId, WorkflowType,
};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AttrId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SearchAttrType {
    Keyword,
    KeywordList,
    Int,
    Bool,
    Double,
    Datetime,
    Text,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown search attribute type database value {value}")]
pub struct SearchAttrTypeDecodeError {
    pub value: i16,
}

impl SearchAttrType {
    pub fn to_db_smallint(self) -> i16 {
        match self {
            Self::Keyword => 0,
            Self::KeywordList => 1,
            Self::Int => 2,
            Self::Bool => 3,
            Self::Double => 4,
            Self::Datetime => 5,
            Self::Text => 6,
        }
    }
}

impl TryFrom<i16> for SearchAttrType {
    type Error = SearchAttrTypeDecodeError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Keyword),
            1 => Ok(Self::KeywordList),
            2 => Ok(Self::Int),
            3 => Ok(Self::Bool),
            4 => Ok(Self::Double),
            5 => Ok(Self::Datetime),
            6 => Ok(Self::Text),
            value => Err(SearchAttrTypeDecodeError { value }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttrDescriptor {
    pub attr_id: AttrId,
    pub attr_type: SearchAttrType,
}

/// Complete post-transition visibility image for one execution.
///
/// This is the archetype-neutral contract between correctness producers and
/// the projection plane. It intentionally carries a full row image rather than
/// deltas: the sink can then apply "newest version wins" per
/// `(namespace_id, archetype_id, run_key)` and safely ignore retried or
/// out-of-order projection records.
#[derive(Clone, Debug, PartialEq)]
pub struct VisibilitySnapshot {
    /// Namespace owning the projected execution.
    pub namespace_id: NamespaceId,
    /// Execution archetype; `0` is the legacy workflow engine.
    pub archetype_id: ArchetypeId,
    /// Durable storage key for the execution.
    pub run_key: RunKey,
    /// User/business identifier for the archetype (`workflow_id`, `activity_id`, etc.).
    pub business_id: String,
    /// User-visible run identifier.
    pub run_id: RunId,
    /// Producer authority fence. A future authority migration bumps this value
    /// so older producers cannot regress the same projected row.
    pub authority_epoch: i64,
    /// Source transition sequence that produced this image.
    pub source_transition_seq: TransitionSeq,
    /// Archetype-specific status string used by compatibility queries.
    pub status_keyword: String,
    /// Cross-archetype open/closed/deleted discriminator.
    pub lifecycle_state: VisibilityLifecycleState,
    /// Execution start timestamp.
    pub start_time: OffsetDateTime,
    /// Last update timestamp represented by this snapshot.
    pub update_time: OffsetDateTime,
    /// Close timestamp when the execution is terminal.
    pub close_time: Option<OffsetDateTime>,
    /// Archetype-specific execution type, if one exists.
    pub execution_type: Option<String>,
    /// Task queue associated with the execution, if any.
    pub task_queue: Option<TaskQueueName>,
    /// Generic transition counter exposed through archetype-specific wire fields.
    pub transition_count: i64,
    /// Approximate serialized history/state size in bytes.
    pub history_size_bytes: i64,
    /// Memo values stored with the execution.
    pub memo: Memo,
    /// Complete typed search attributes for the current generation.
    pub search_attributes: SearchAttributes,
    /// Current search-attribute generation pointer.
    pub search_attr_generation: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionRow {
    /// Durable storage key for the projected workflow run.
    pub run_key: RunKey,
    /// Namespace that owns the workflow execution.
    pub namespace_id: NamespaceId,
    /// Archetype discriminator in the shared visibility index.
    ///
    /// Existing workflow rows always carry [`ArchetypeId::WORKFLOW`]. Keeping
    /// the field on the compatibility row prevents workflow visibility from
    /// depending on null-as-workflow conventions while CHASM activity rows are
    /// introduced beside it.
    pub archetype_id: ArchetypeId,
    /// Generic business identifier mirrored from `workflow_id` for workflow rows.
    pub business_id: String,
    /// Workflow identifier exposed through Temporal workflow list APIs.
    pub workflow_id: WorkflowId,
    /// User-visible run identifier.
    pub run_id: RunId,
    /// Projection authority fence for this row.
    ///
    /// The initial workflow producer uses epoch 0. A future workflow-to-CHASM
    /// migration must bump this value so a retired producer cannot overwrite
    /// rows emitted by the new authority.
    pub authority_epoch: i64,
    /// Transition sequence that produced the stored row image.
    pub source_transition_seq: TransitionSeq,
    /// Generic status keyword used by the shared visibility plane.
    pub status_keyword: String,
    /// Cross-archetype open/closed/deleted lifecycle state.
    pub lifecycle_state: VisibilityLifecycleState,
    /// Workflow type used by existing workflow visibility responses.
    pub workflow_type: WorkflowType,
    /// Workflow task queue used by existing workflow visibility filters.
    pub task_queue: TaskQueueName,
    /// Workflow-typed status retained for Temporal workflow API translation.
    pub status: ExecutionStatus,
    /// Workflow start timestamp.
    pub start_time: OffsetDateTime,
    /// Last projected update timestamp for the generic snapshot row.
    pub update_time: OffsetDateTime,
    /// Scheduled execution timestamp when distinct from start time.
    pub execution_time: Option<OffsetDateTime>,
    /// Close timestamp for terminal workflow rows.
    pub close_time: Option<OffsetDateTime>,
    /// Durable history length after the source transition.
    pub history_length: i64,
    /// Closed-run duration in nanoseconds, matching Temporal visibility shape.
    pub execution_duration: Option<i64>,
    /// Number of committed workflow transitions after the source transition.
    pub state_transition_count: i64,
    /// Approximate serialized history size in bytes.
    pub history_size_bytes: i64,
    /// Parent workflow id for child workflow rows.
    pub parent_workflow_id: Option<WorkflowId>,
    /// Parent run id for child workflow rows.
    pub parent_run_id: Option<RunId>,
    /// Canonical root workflow id for describe/list metadata.
    pub root_workflow_id: WorkflowId,
    /// Canonical root run id for describe/list metadata.
    pub root_run_id: RunId,
    /// Memo values attached to the workflow row.
    pub memo: Memo,
    /// Complete search-attribute image for the current generation.
    ///
    /// The old sink applied search attributes as patches. Task 23 moves toward
    /// full snapshots; retaining the image here lets the compatibility row
    /// participate in generation-based application without rereading index rows.
    pub search_attributes: SearchAttributes,
    /// Generic transition counter exposed through archetype-specific fields.
    pub transition_count: i64,
    /// Search-attribute generation pointer for the snapshot protocol.
    pub search_attr_generation: u64,
    /// Legacy search-attribute mutation counter used by existing tests.
    pub search_attr_version: u64,
}

impl ExecutionRow {
    /// Mirror a workflow visibility snapshot into the legacy workflow query row.
    ///
    /// During task 23 the storage contract moves to archetype-neutral
    /// snapshots before the workflow edge is fully generalized. This conversion
    /// is intentionally workflow-only: activity snapshots stay in the generic
    /// snapshot index until task 25 adds their dedicated list/count surface.
    pub fn from_workflow_snapshot(snapshot: &VisibilitySnapshot) -> Option<Self> {
        if snapshot.archetype_id != ArchetypeId::WORKFLOW {
            return None;
        }
        let status = workflow_status_from_keyword(&snapshot.status_keyword)?;
        let workflow_id = WorkflowId(snapshot.business_id.clone());
        let workflow_type = WorkflowType(snapshot.execution_type.clone().unwrap_or_default());
        let task_queue = snapshot
            .task_queue
            .clone()
            .unwrap_or_else(|| TaskQueueName(String::new()));
        Some(Self {
            run_key: snapshot.run_key,
            namespace_id: snapshot.namespace_id,
            archetype_id: snapshot.archetype_id,
            business_id: snapshot.business_id.clone(),
            workflow_id,
            run_id: snapshot.run_id,
            authority_epoch: snapshot.authority_epoch,
            source_transition_seq: snapshot.source_transition_seq,
            status_keyword: snapshot.status_keyword.clone(),
            lifecycle_state: snapshot.lifecycle_state,
            workflow_type,
            task_queue,
            status,
            start_time: snapshot.start_time,
            update_time: snapshot.update_time,
            execution_time: Some(snapshot.start_time),
            close_time: snapshot.close_time,
            history_length: snapshot.transition_count,
            execution_duration: None,
            state_transition_count: snapshot.transition_count,
            history_size_bytes: snapshot.history_size_bytes,
            parent_workflow_id: None,
            parent_run_id: None,
            root_workflow_id: WorkflowId(snapshot.business_id.clone()),
            root_run_id: snapshot.run_id,
            memo: snapshot.memo.clone(),
            search_attributes: snapshot.search_attributes.clone(),
            transition_count: snapshot.transition_count,
            search_attr_generation: snapshot.search_attr_generation,
            search_attr_version: snapshot.search_attr_generation,
        })
    }

    /// Build the generic snapshot for this workflow row.
    ///
    /// The workflow-specific fields remain available for existing Temporal
    /// list/count translation, while the sink can reason over the
    /// archetype-neutral `VisibilitySnapshot` contract introduced for CHASM.
    pub fn to_snapshot(&self) -> VisibilitySnapshot {
        VisibilitySnapshot {
            namespace_id: self.namespace_id,
            archetype_id: self.archetype_id,
            run_key: self.run_key,
            business_id: self.business_id.clone(),
            run_id: self.run_id,
            authority_epoch: self.authority_epoch,
            source_transition_seq: self.source_transition_seq,
            status_keyword: self.status_keyword.clone(),
            lifecycle_state: self.lifecycle_state,
            start_time: self.start_time,
            update_time: self.update_time,
            close_time: self.close_time,
            execution_type: Some(self.workflow_type.0.clone()),
            task_queue: Some(self.task_queue.clone()),
            transition_count: self.transition_count,
            history_size_bytes: self.history_size_bytes,
            memo: self.memo.clone(),
            search_attributes: self.search_attributes.clone(),
            search_attr_generation: self.search_attr_generation,
        }
    }
}

/// Return true when an incoming visibility version can replace `current`.
///
/// Projection workers may retry or observe records out of order. Comparing the
/// authority epoch first and source transition second enforces the design rule
/// that a retired producer cannot regress a row owned by a newer authority, and
/// that duplicate records from the same authority are idempotent no-ops.
pub fn visibility_version_is_newer(
    current: Option<&ExecutionRow>,
    authority_epoch: i64,
    source_transition_seq: TransitionSeq,
) -> bool {
    current.is_none_or(|row| {
        (authority_epoch, source_transition_seq.0)
            > (row.authority_epoch, row.source_transition_seq.0)
    })
}

/// Return the canonical workflow status keyword stored in generic visibility.
pub fn workflow_status_keyword(status: ExecutionStatus) -> String {
    format!("{status:?}")
}

/// Decode a workflow status keyword produced by [`workflow_status_keyword`].
pub fn workflow_status_from_keyword(value: &str) -> Option<ExecutionStatus> {
    match value {
        "Running" => Some(ExecutionStatus::Running),
        "Paused" => Some(ExecutionStatus::Paused),
        "Completed" => Some(ExecutionStatus::Completed),
        "Failed" => Some(ExecutionStatus::Failed),
        "Cancelled" => Some(ExecutionStatus::Cancelled),
        "Terminated" => Some(ExecutionStatus::Terminated),
        "ContinuedAsNew" => Some(ExecutionStatus::ContinuedAsNew),
        "TimedOut" => Some(ExecutionStatus::TimedOut),
        _ => None,
    }
}

/// Map workflow terminal/open status into the archetype-neutral lifecycle column.
pub fn workflow_lifecycle_state(status: ExecutionStatus) -> VisibilityLifecycleState {
    if status.is_open() {
        VisibilityLifecycleState::Open
    } else {
        VisibilityLifecycleState::Closed
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterExpr {
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Compare {
        field: FieldRef,
        op: CompareOp,
        value: FilterValue,
    },
    In {
        field: FieldRef,
        values: Vec<FilterValue>,
    },
    Between {
        field: FieldRef,
        low: FilterValue,
        high: FilterValue,
    },
    StartsWith {
        field: FieldRef,
        prefix: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum FieldRef {
    System(SystemField),
    Custom {
        name: String,
        attr_id: AttrId,
        attr_type: SearchAttrType,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SystemField {
    WorkflowId,
    RunId,
    WorkflowType,
    TaskQueue,
    ExecutionStatus,
    StartTime,
    ExecutionTime,
    CloseTime,
    HistoryLength,
    ExecutionDuration,
    StateTransitionCount,
    HistorySizeBytes,
    ParentWorkflowId,
    ParentRunId,
    RootWorkflowId,
    RootRunId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FilterValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Datetime(OffsetDateTime),
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CompiledFilter {
    pub expr: Option<FilterExpr>,
    /// Forced archetype scope for the query, or `None` to span all archetypes.
    ///
    /// The visibility endpoints set this *after* compiling the user query
    /// (workflow endpoints → [`ArchetypeId::WORKFLOW`], activity endpoints → the
    /// activity archetype) so the shared index never leaks one archetype's rows
    /// into another's list/count, and a user query cannot escape the scope
    /// (Requirement 13.1). `None` (the default) is for store-mechanics tests.
    pub archetype: Option<ArchetypeId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    Default,
    StartTimeAsc,
    StartTimeDesc,
    CloseTimeAsc,
    CloseTimeDesc,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PageToken {
    pub close_time: Option<OffsetDateTime>,
    pub start_time: OffsetDateTime,
    pub run_key: RunKey,
    pub sort_order: SortOrder,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PageBounds {
    pub limit: usize,
    pub after: Option<PageToken>,
}

pub const MAX_PAGE_SIZE: usize = 1000;

#[derive(Serialize, Deserialize)]
struct PageTokenWire {
    ct: Option<i64>,
    st: i64,
    rk: String,
    so: SortOrder,
}

impl PageToken {
    pub fn encode(&self) -> Result<String> {
        let wire = PageTokenWire {
            ct: self.close_time.map(|v| v.unix_timestamp_nanos() as i64),
            st: self.start_time.unix_timestamp_nanos() as i64,
            rk: self.run_key.0.to_string(),
            so: self.sort_order,
        };
        let json = serde_json::to_vec(&wire)?;
        Ok(STANDARD.encode(json))
    }

    pub fn decode(s: &str) -> Result<Self> {
        let decoded = STANDARD
            .decode(s)
            .map_err(|e| anyhow!("malformed page token: {e}"))?;
        let wire: PageTokenWire =
            serde_json::from_slice(&decoded).map_err(|e| anyhow!("malformed page token: {e}"))?;
        let run_key = RunKey(
            Uuid::parse_str(&wire.rk).map_err(|e| anyhow!("malformed page token run key: {e}"))?,
        );
        Ok(Self {
            close_time: wire
                .ct
                .map(|v| OffsetDateTime::from_unix_timestamp_nanos(v as i128))
                .transpose()?,
            start_time: OffsetDateTime::from_unix_timestamp_nanos(wire.st as i128)?,
            run_key,
            sort_order: wire.so,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RollupDimension {
    ExecutionStatus,
    WorkflowType,
    TaskQueue,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("unknown rollup dimension database value {value}")]
pub struct RollupDimensionDecodeError {
    pub value: i16,
}

impl RollupDimension {
    pub fn to_db_smallint(self) -> i16 {
        match self {
            Self::ExecutionStatus => 0,
            Self::WorkflowType => 1,
            Self::TaskQueue => 2,
        }
    }
}

impl TryFrom<i16> for RollupDimension {
    type Error = RollupDimensionDecodeError;

    fn try_from(value: i16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ExecutionStatus),
            1 => Ok(Self::WorkflowType),
            2 => Ok(Self::TaskQueue),
            value => Err(RollupDimensionDecodeError { value }),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RollupDelta {
    pub namespace_id: NamespaceId,
    /// The archetype this count belongs to. Rollups are archetype-scoped so a
    /// `status = "Running"` count never mixes workflows and activities
    /// (Requirement 10.1; see reference/DECISION-visibility-dsql-schema.md).
    pub archetype_id: ArchetypeId,
    pub dimension: RollupDimension,
    pub value: String,
    /// Counter stripe = `hash(run_key) % ROLLUP_STRIPES`, so concurrent applies for
    /// different executions spread across counter rows (DSQL hot-key avoidance). A
    /// `+1`/`-1` pair for the same execution shares a stripe and cancels. The
    /// in-memory store ignores it (it aggregates per `(archetype, dimension, value)`);
    /// the DSQL store keys counter rows by it.
    pub stripe: i32,
    pub delta: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RollupCounter {
    pub value: String,
    pub count: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum GroupByField {
    System(SystemField),
    Custom {
        name: String,
        attr_id: AttrId,
        attr_type: SearchAttrType,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListResult {
    pub rows: Vec<ExecutionRow>,
    pub next_page_token: Option<PageToken>,
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CountResult {
    pub total_count: i64,
    pub groups: Vec<RollupCounter>,
}

pub fn search_attr_type_of(value: &SearchAttrValue) -> SearchAttrType {
    match value {
        SearchAttrValue::Keyword(_) => SearchAttrType::Keyword,
        SearchAttrValue::KeywordList(_) => SearchAttrType::KeywordList,
        SearchAttrValue::Int(_) => SearchAttrType::Int,
        SearchAttrValue::Bool(_) => SearchAttrType::Bool,
        SearchAttrValue::Double(_) => SearchAttrType::Double,
        SearchAttrValue::Datetime(_) => SearchAttrType::Datetime,
        SearchAttrValue::Text(_) => SearchAttrType::Text,
    }
}

pub(crate) fn text_search_tokens(text: &str) -> Vec<String> {
    let mut out = std::collections::HashSet::new();
    let mut token = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            token.push(ch.to_ascii_lowercase());
        } else if !token.is_empty() {
            out.insert(std::mem::take(&mut token));
        }
    }
    if !token.is_empty() {
        out.insert(token);
    }
    out.into_iter().collect()
}

pub fn beginning_cursor() -> ProjectionCursor {
    ProjectionCursor::beginning(0, 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn page_token_round_trips() {
        let token = PageToken {
            close_time: Some(OffsetDateTime::from_unix_timestamp(123).unwrap()),
            start_time: OffsetDateTime::from_unix_timestamp(45).unwrap(),
            run_key: RunKey(Uuid::from_u128(7)),
            sort_order: SortOrder::CloseTimeDesc,
        };

        let encoded = token.encode().unwrap();
        let decoded = PageToken::decode(&encoded).unwrap();

        assert_eq!(decoded, token);
    }

    #[test]
    fn page_token_decode_rejects_invalid_input() {
        let error = PageToken::decode("not-base64").unwrap_err();
        assert!(error.to_string().contains("malformed page token"));
    }

    #[test]
    fn search_attr_type_database_encoding_is_stable() {
        let cases = [
            (SearchAttrType::Keyword, 0),
            (SearchAttrType::KeywordList, 1),
            (SearchAttrType::Int, 2),
            (SearchAttrType::Bool, 3),
            (SearchAttrType::Double, 4),
            (SearchAttrType::Datetime, 5),
            (SearchAttrType::Text, 6),
        ];

        for (attr_type, value) in cases {
            assert_eq!(attr_type.to_db_smallint(), value);
            assert_eq!(SearchAttrType::try_from(value), Ok(attr_type));
        }
    }

    #[test]
    fn search_attr_type_rejects_unknown_database_values() {
        for value in [7, -1, 100] {
            assert_eq!(
                SearchAttrType::try_from(value),
                Err(SearchAttrTypeDecodeError { value })
            );
        }
    }

    #[test]
    fn rollup_dimension_database_encoding_is_stable() {
        let cases = [
            (RollupDimension::ExecutionStatus, 0),
            (RollupDimension::WorkflowType, 1),
            (RollupDimension::TaskQueue, 2),
        ];

        for (dimension, value) in cases {
            assert_eq!(dimension.to_db_smallint(), value);
            assert_eq!(RollupDimension::try_from(value), Ok(dimension));
        }
    }

    #[test]
    fn rollup_dimension_rejects_unknown_database_values() {
        for value in [3, -1, 100] {
            assert_eq!(
                RollupDimension::try_from(value),
                Err(RollupDimensionDecodeError { value })
            );
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn search_attr_type_database_encoding_round_trips(
            attr_type in prop_oneof![
                Just(SearchAttrType::Keyword),
                Just(SearchAttrType::KeywordList),
                Just(SearchAttrType::Int),
                Just(SearchAttrType::Bool),
                Just(SearchAttrType::Double),
                Just(SearchAttrType::Datetime),
                Just(SearchAttrType::Text),
            ],
        ) {
            prop_assert_eq!(
                SearchAttrType::try_from(attr_type.to_db_smallint()),
                Ok(attr_type)
            );
        }

        #[test]
        fn rollup_dimension_database_encoding_round_trips(
            dimension in prop_oneof![
                Just(RollupDimension::ExecutionStatus),
                Just(RollupDimension::WorkflowType),
                Just(RollupDimension::TaskQueue),
            ],
        ) {
            prop_assert_eq!(
                RollupDimension::try_from(dimension.to_db_smallint()),
                Ok(dimension)
            );
        }
    }
}
