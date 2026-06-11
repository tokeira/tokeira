//! Visibility data model for the projection plane.
//!
//! `ExecutionRow` is the materialised view of one workflow run, indexed for
//! list and count queries. `FilterExpr` is the compiled representation of a
//! user-supplied visibility query, and `SearchAttrType` defines the typed
//! dimensions that custom search attributes can occupy. Together these types
//! form the contract between the projection sink and the query path.

use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use tokeira_types::{
    ExecutionStatus, Memo, NamespaceId, ProjectionCursor, RunId, RunKey, SearchAttrValue,
    TaskQueueName, WorkflowId, WorkflowType,
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

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionRow {
    pub run_key: RunKey,
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub workflow_type: WorkflowType,
    pub task_queue: TaskQueueName,
    pub status: ExecutionStatus,
    pub start_time: OffsetDateTime,
    pub execution_time: Option<OffsetDateTime>,
    pub close_time: Option<OffsetDateTime>,
    pub history_length: i64,
    pub execution_duration: Option<i64>,
    pub state_transition_count: i64,
    pub history_size_bytes: i64,
    pub parent_workflow_id: Option<WorkflowId>,
    pub parent_run_id: Option<RunId>,
    pub root_workflow_id: WorkflowId,
    pub root_run_id: RunId,
    pub memo: Memo,
    pub search_attr_version: u64,
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
    Status(ExecutionStatus),
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CompiledFilter {
    pub expr: Option<FilterExpr>,
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
    pub dimension: RollupDimension,
    pub value: String,
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
