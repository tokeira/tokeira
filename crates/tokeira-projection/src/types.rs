use anyhow::{Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokeira_types::{
    ExecutionStatus, Memo, NamespaceId, ProjectionCursor, RunId, RunKey, SearchAttrValue,
    TaskQueueName, WorkflowId, WorkflowType,
};
use uuid::Uuid;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
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
    pub state_transition_count: i64,
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
    CloseTime,
    HistoryLength,
    StateTransitionCount,
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
        let wire: PageTokenWire = serde_json::from_slice(&decoded)
            .map_err(|e| anyhow!("malformed page token: {e}"))?;
        let run_key = RunKey(
            Uuid::parse_str(&wire.rk)
                .map_err(|e| anyhow!("malformed page token run key: {e}"))?,
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

pub fn beginning_cursor() -> ProjectionCursor {
    ProjectionCursor::beginning(0, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
