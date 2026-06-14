//! Filter compilation for visibility queries.
//!
//! Translates a user-supplied filter string (e.g. `WorkflowType = "Foo" AND
//! StartTime > ...`) into a typed `FilterExpr` tree. Field references are
//! resolved against the visibility store's attribute registry so that unknown
//! or type-mismatched filters are rejected at compile time rather than at
//! evaluation time.

use anyhow::{Result, anyhow};
use async_recursion::async_recursion;
use time::OffsetDateTime;
use tokeira_types::{NamespaceId, SearchAttrValue, SearchAttributes};

use crate::{
    store::VisibilityStore,
    types::{
        CompareOp, CompiledFilter, FieldRef, FilterExpr, FilterValue, SearchAttrType, SystemField,
    },
};

pub async fn compile_filter<S: VisibilityStore + ?Sized>(
    input: Option<&str>,
    namespace_id: NamespaceId,
    store: &S,
) -> Result<CompiledFilter> {
    let Some(input) = input.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(CompiledFilter { expr: None });
    };
    let expr = compile_expr(input, namespace_id, store).await?;
    Ok(CompiledFilter { expr: Some(expr) })
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleFilter {
    expr: ScheduleFilterExpr,
}

impl ScheduleFilter {
    pub fn matches(
        &self,
        schedule_id: &str,
        namespace_id: NamespaceId,
        paused: bool,
        notes: &str,
        search_attributes: &SearchAttributes,
    ) -> bool {
        self.expr
            .matches(schedule_id, namespace_id, paused, notes, search_attributes)
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ScheduleFilterExpr {
    Eq {
        field: ScheduleField,
        value: ScheduleFilterValue,
    },
    In {
        field: ScheduleField,
        values: Vec<ScheduleFilterValue>,
    },
}

impl ScheduleFilterExpr {
    fn matches(
        &self,
        schedule_id: &str,
        namespace_id: NamespaceId,
        paused: bool,
        notes: &str,
        search_attributes: &SearchAttributes,
    ) -> bool {
        match self {
            Self::Eq { field, value } => field
                .value(schedule_id, namespace_id, paused, notes, search_attributes)
                .is_some_and(|actual| actual == *value),
            Self::In { field, values } => field
                .value(schedule_id, namespace_id, paused, notes, search_attributes)
                .is_some_and(|actual| values.contains(&actual)),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScheduleField {
    ScheduleId,
    Namespace,
    Paused,
    Notes,
    SearchAttribute(String),
}

impl ScheduleField {
    fn parse(input: &str) -> Self {
        match input.trim() {
            "schedule_id" | "ScheduleId" => Self::ScheduleId,
            "namespace" | "Namespace" => Self::Namespace,
            "paused" | "Paused" => Self::Paused,
            "notes" | "Notes" => Self::Notes,
            other => Self::SearchAttribute(other.to_string()),
        }
    }

    fn value(
        &self,
        schedule_id: &str,
        namespace_id: NamespaceId,
        paused: bool,
        notes: &str,
        search_attributes: &SearchAttributes,
    ) -> Option<ScheduleFilterValue> {
        match self {
            Self::ScheduleId => Some(ScheduleFilterValue::String(schedule_id.to_string())),
            Self::Namespace => Some(ScheduleFilterValue::String(namespace_id.0.to_string())),
            Self::Paused => Some(ScheduleFilterValue::Bool(paused)),
            Self::Notes => Some(ScheduleFilterValue::String(notes.to_string())),
            Self::SearchAttribute(name) => {
                search_attributes.0.get(name).and_then(search_attr_value)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ScheduleFilterValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(f64),
}

fn search_attr_value(value: &SearchAttrValue) -> Option<ScheduleFilterValue> {
    match value {
        SearchAttrValue::Keyword(value) | SearchAttrValue::Text(value) => {
            Some(ScheduleFilterValue::String(value.clone()))
        }
        SearchAttrValue::Int(value) => Some(ScheduleFilterValue::Int(*value)),
        SearchAttrValue::Double(value) => Some(ScheduleFilterValue::Double(*value)),
        SearchAttrValue::Bool(value) => Some(ScheduleFilterValue::Bool(*value)),
        SearchAttrValue::Datetime(value) => Some(ScheduleFilterValue::String(value.to_string())),
        SearchAttrValue::KeywordList(_) => None,
    }
}

pub fn compile_schedule_filter(query: &str) -> Result<ScheduleFilter> {
    let input = query.trim();
    if input.is_empty() {
        return Err(anyhow!("unsupported schedule query"));
    }
    if let Some((field, values)) = parse_schedule_in(input) {
        return Ok(ScheduleFilter {
            expr: ScheduleFilterExpr::In {
                field: ScheduleField::parse(field),
                values: values
                    .into_iter()
                    .map(|value| parse_schedule_value(&value))
                    .collect(),
            },
        });
    }
    if let Some((field, value)) = input.split_once('=') {
        if field.contains('!') || value.contains('=') {
            return Err(anyhow!("unsupported schedule query"));
        }
        return Ok(ScheduleFilter {
            expr: ScheduleFilterExpr::Eq {
                field: ScheduleField::parse(field),
                value: parse_schedule_value(value),
            },
        });
    }
    Err(anyhow!("unsupported schedule query"))
}

fn parse_schedule_in(input: &str) -> Option<(&str, Vec<String>)> {
    let (field, rest) = input.split_once(" IN ")?;
    let body = rest.trim().strip_prefix('(')?.strip_suffix(')')?;
    Some((
        field.trim(),
        body.split(',')
            .map(|value| value.trim().to_string())
            .collect(),
    ))
}

fn parse_schedule_value(input: &str) -> ScheduleFilterValue {
    let value = input.trim().trim_matches('"').trim_matches('\'');
    if value.eq_ignore_ascii_case("true") {
        return ScheduleFilterValue::Bool(true);
    }
    if value.eq_ignore_ascii_case("false") {
        return ScheduleFilterValue::Bool(false);
    }
    if let Ok(parsed) = value.parse::<i64>() {
        return ScheduleFilterValue::Int(parsed);
    }
    if let Ok(parsed) = value.parse::<f64>() {
        return ScheduleFilterValue::Double(parsed);
    }
    ScheduleFilterValue::String(value.to_string())
}

#[async_recursion]
async fn compile_expr<S>(input: &str, namespace_id: NamespaceId, store: &S) -> Result<FilterExpr>
where
    S: VisibilityStore + ?Sized,
{
    if let Some((lhs, rhs)) = split_top_level(input, " AND ") {
        return Ok(FilterExpr::And(
            Box::new(compile_expr(lhs, namespace_id, store).await?),
            Box::new(compile_expr(rhs, namespace_id, store).await?),
        ));
    }
    if let Some((lhs, rhs)) = split_top_level(input, " OR ") {
        return Ok(FilterExpr::Or(
            Box::new(compile_expr(lhs, namespace_id, store).await?),
            Box::new(compile_expr(rhs, namespace_id, store).await?),
        ));
    }
    if let Some((field, low, high)) = parse_between(input) {
        let field = resolve_field(field, namespace_id, store).await?;
        let low = parse_value(&low);
        let high = parse_value(&high);
        ensure_value_type(&field, &low)?;
        ensure_value_type(&field, &high)?;
        return Ok(FilterExpr::Between { field, low, high });
    }
    if let Some((field, values)) = parse_in(input) {
        let field = resolve_field(field, namespace_id, store).await?;
        let values: Vec<_> = values.into_iter().map(|v| parse_value(&v)).collect();
        for value in &values {
            ensure_value_type(&field, value)?;
        }
        return Ok(FilterExpr::In { field, values });
    }
    if let Some((field, prefix)) = parse_starts_with(input) {
        let field = resolve_field(field, namespace_id, store).await?;
        ensure_starts_with_field(&field)?;
        return Ok(FilterExpr::StartsWith { field, prefix });
    }
    for (needle, op) in [
        ("!=", CompareOp::Ne),
        (">=", CompareOp::Ge),
        ("<=", CompareOp::Le),
        ("=", CompareOp::Eq),
        (">", CompareOp::Gt),
        ("<", CompareOp::Lt),
    ] {
        if let Some((field, value)) = input.split_once(needle) {
            let field = resolve_field(field.trim(), namespace_id, store).await?;
            let value = parse_value(value.trim());
            ensure_value_type(&field, &value)?;
            return Ok(FilterExpr::Compare { field, op, value });
        }
    }
    Err(anyhow!("unsupported filter expression: {input}"))
}

async fn resolve_field<S: VisibilityStore + ?Sized>(
    field: &str,
    namespace_id: NamespaceId,
    store: &S,
) -> Result<FieldRef> {
    let trimmed = field.trim();
    let system = match trimmed {
        "WorkflowId" => Some(SystemField::WorkflowId),
        "RunId" => Some(SystemField::RunId),
        "WorkflowType" => Some(SystemField::WorkflowType),
        "TaskQueue" => Some(SystemField::TaskQueue),
        "ExecutionStatus" => Some(SystemField::ExecutionStatus),
        "StartTime" => Some(SystemField::StartTime),
        "ExecutionTime" => Some(SystemField::ExecutionTime),
        "CloseTime" => Some(SystemField::CloseTime),
        "HistoryLength" => Some(SystemField::HistoryLength),
        "ExecutionDuration" => Some(SystemField::ExecutionDuration),
        "StateTransitionCount" => Some(SystemField::StateTransitionCount),
        "HistorySizeBytes" => Some(SystemField::HistorySizeBytes),
        "ParentWorkflowId" => Some(SystemField::ParentWorkflowId),
        "ParentRunId" => Some(SystemField::ParentRunId),
        "RootWorkflowId" => Some(SystemField::RootWorkflowId),
        "RootRunId" => Some(SystemField::RootRunId),
        _ => None,
    };
    if let Some(system) = system {
        return Ok(FieldRef::System(system));
    }
    let Some(attr) = store.resolve_attr(namespace_id, trimmed).await? else {
        return Err(anyhow!("unknown search attribute: {trimmed}"));
    };
    Ok(FieldRef::Custom {
        name: trimmed.to_string(),
        attr_id: attr.attr_id,
        attr_type: attr.attr_type,
    })
}

fn parse_value(input: &str) -> FilterValue {
    let trimmed = input.trim().trim_matches('"').trim_matches('\'');
    if trimmed.eq_ignore_ascii_case("true") {
        return FilterValue::Bool(true);
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return FilterValue::Bool(false);
    }
    // ExecutionStatus is no longer a distinct value type: it is queried as a
    // generic keyword string compared against the `status_keyword` column, so a
    // status name like "Running" parses as a plain string below (Requirement 10.5).
    if let Ok(value) = trimmed.parse::<i64>() {
        return FilterValue::Int(value);
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        return FilterValue::Float(value);
    }
    if let Ok(value) =
        OffsetDateTime::parse(trimmed, &time::format_description::well_known::Rfc3339)
    {
        return FilterValue::Datetime(value);
    }
    FilterValue::String(trimmed.to_string())
}

fn split_top_level<'a>(input: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    input.split_once(needle)
}

fn parse_between(input: &str) -> Option<(&str, String, String)> {
    let (field, rest) = input.split_once(" BETWEEN ")?;
    let (low, high) = rest.split_once(" AND ")?;
    Some((
        field.trim(),
        low.trim().to_string(),
        high.trim().to_string(),
    ))
}

fn parse_in(input: &str) -> Option<(&str, Vec<String>)> {
    let (field, rest) = input.split_once(" IN ")?;
    let body = rest.trim();
    let body = body.strip_prefix('(')?.strip_suffix(')')?;
    let values = body.split(',').map(|s| s.trim().to_string()).collect();
    Some((field.trim(), values))
}

fn parse_starts_with(input: &str) -> Option<(&str, String)> {
    let (field, rest) = input.split_once(" STARTS_WITH ")?;
    Some((
        field.trim(),
        rest.trim().trim_matches('"').trim_matches('\'').to_string(),
    ))
}

pub fn expected_type_for_field(field: &FieldRef) -> Option<SearchAttrType> {
    match field {
        FieldRef::Custom { attr_type, .. } => Some(*attr_type),
        _ => None,
    }
}

fn ensure_value_type(field: &FieldRef, value: &FilterValue) -> Result<()> {
    let matches = match field {
        FieldRef::System(SystemField::WorkflowId)
        | FieldRef::System(SystemField::RunId)
        | FieldRef::System(SystemField::WorkflowType)
        | FieldRef::System(SystemField::TaskQueue)
        | FieldRef::System(SystemField::ExecutionStatus)
        | FieldRef::System(SystemField::ParentWorkflowId)
        | FieldRef::System(SystemField::ParentRunId)
        | FieldRef::System(SystemField::RootWorkflowId)
        | FieldRef::System(SystemField::RootRunId) => {
            matches!(value, FilterValue::String(_))
        }
        FieldRef::System(SystemField::StartTime)
        | FieldRef::System(SystemField::ExecutionTime)
        | FieldRef::System(SystemField::CloseTime) => {
            matches!(value, FilterValue::Datetime(_))
        }
        FieldRef::System(SystemField::HistoryLength)
        | FieldRef::System(SystemField::ExecutionDuration)
        | FieldRef::System(SystemField::StateTransitionCount)
        | FieldRef::System(SystemField::HistorySizeBytes) => {
            matches!(value, FilterValue::Int(_))
        }
        FieldRef::Custom { attr_type, .. } => match attr_type {
            SearchAttrType::Keyword | SearchAttrType::KeywordList | SearchAttrType::Text => {
                matches!(value, FilterValue::String(_))
            }
            SearchAttrType::Int => matches!(value, FilterValue::Int(_)),
            SearchAttrType::Bool => matches!(value, FilterValue::Bool(_)),
            SearchAttrType::Double => matches!(value, FilterValue::Float(_)),
            SearchAttrType::Datetime => matches!(value, FilterValue::Datetime(_)),
        },
    };

    if matches {
        Ok(())
    } else {
        Err(anyhow!("type mismatch for filter field"))
    }
}

fn ensure_starts_with_field(field: &FieldRef) -> Result<()> {
    match field {
        FieldRef::System(SystemField::WorkflowId)
        | FieldRef::System(SystemField::RunId)
        | FieldRef::System(SystemField::WorkflowType)
        | FieldRef::System(SystemField::TaskQueue)
        | FieldRef::System(SystemField::ParentWorkflowId)
        | FieldRef::System(SystemField::ParentRunId)
        | FieldRef::System(SystemField::RootWorkflowId)
        | FieldRef::System(SystemField::RootRunId)
        | FieldRef::Custom {
            attr_type: SearchAttrType::Keyword | SearchAttrType::KeywordList | SearchAttrType::Text,
            ..
        } => Ok(()),
        _ => Err(anyhow!("type mismatch for filter field")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::InMemoryVisibilityStore;
    use proptest::prelude::*;

    fn arb_system_string_field() -> impl Strategy<Value = &'static str> {
        prop_oneof![Just("WorkflowId"), Just("WorkflowType"), Just("TaskQueue"),]
    }

    fn arb_compare_op() -> impl Strategy<Value = &'static str> {
        prop_oneof![Just("="), Just("!="),]
    }

    fn format_compare(field: &str, op: &str, value: &str) -> String {
        format!("{field} {op} \"{value}\"")
    }

    // Feature: projection-visibility, Property 6:
    // Filter Expression Round-Trip
    // **Validates: Requirements 3.1**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]
        #[test]
        fn prop_filter_round_trip(
            field in arb_system_string_field(),
            op in arb_compare_op(),
            value in "[a-z]{1,10}",
        ) {
            let rt =
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
            rt.block_on(async {
                let store =
                    InMemoryVisibilityStore::default();
                let ns =
                    NamespaceId(uuid::Uuid::from_u128(1));
                let input =
                    format_compare(field, op, &value);
                let compiled = compile_filter(
                    Some(&input),
                    ns,
                    &store,
                )
                .await
                .unwrap();
                let expr = compiled.expr.unwrap();
                match &expr {
                    FilterExpr::Compare {
                        field: f,
                        op: parsed_op,
                        value: v,
                    } => {
                        let expected_field =
                            match field {
                                "WorkflowId" => {
                                    SystemField::WorkflowId
                                }
                                "WorkflowType" => {
                                    SystemField::WorkflowType
                                }
                                "TaskQueue" => {
                                    SystemField::TaskQueue
                                }
                                _ => unreachable!(),
                            };
                        prop_assert_eq!(
                            f.clone(),
                            FieldRef::System(
                                expected_field
                            )
                        );
                        let expected_op = match op {
                            "=" => CompareOp::Eq,
                            "!=" => CompareOp::Ne,
                            _ => unreachable!(),
                        };
                        prop_assert_eq!(
                            *parsed_op,
                            expected_op
                        );
                        prop_assert_eq!(
                            v.clone(),
                            FilterValue::String(
                                value.clone()
                            )
                        );
                    }
                    _ => {
                        prop_assert!(
                            false,
                            "expected Compare"
                        );
                    }
                }
                Ok(())
            })?;
        }
    }

    #[tokio::test]
    async fn compile_filter_rejects_unknown_attribute() {
        let store = InMemoryVisibilityStore::default();
        let namespace_id = NamespaceId(uuid::Uuid::from_u128(1));
        let error = compile_filter(Some("CustomKeyword = \"x\""), namespace_id, &store)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unknown search attribute"));
    }

    #[tokio::test]
    async fn compile_filter_rejects_type_mismatch() {
        let store = InMemoryVisibilityStore::default();
        let namespace_id = NamespaceId(uuid::Uuid::from_u128(1));
        store
            .register_attr(namespace_id, "Attempts".to_string(), SearchAttrType::Int)
            .await
            .unwrap();

        let error = compile_filter(Some("Attempts = \"abc\""), namespace_id, &store)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("type mismatch"));
    }

    #[tokio::test]
    async fn compile_filter_resolves_paused_execution_status() {
        let store = InMemoryVisibilityStore::default();
        let namespace_id = NamespaceId(uuid::Uuid::from_u128(1));
        let compiled = compile_filter(Some("ExecutionStatus = \"Paused\""), namespace_id, &store)
            .await
            .unwrap();
        match compiled.expr.expect("expr") {
            FilterExpr::Compare { field, op, value } => {
                assert_eq!(field, FieldRef::System(SystemField::ExecutionStatus));
                assert_eq!(op, CompareOp::Eq);
                assert_eq!(value, FilterValue::String("Paused".to_string()));
            }
            other => panic!("expected Compare, got {other:?}"),
        }
    }
}
