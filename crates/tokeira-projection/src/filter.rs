use anyhow::{Result, anyhow};
use async_recursion::async_recursion;
use time::OffsetDateTime;
use tokeira_types::{ExecutionStatus, NamespaceId};

use crate::{
    store::VisibilityStore,
    types::{
        CompiledFilter, CompareOp, FieldRef, FilterExpr, FilterValue, SearchAttrType,
        SystemField,
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

#[async_recursion]
async fn compile_expr<S: VisibilityStore + ?Sized>(
    input: &str,
    namespace_id: NamespaceId,
    store: &S,
) -> Result<FilterExpr> {
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
        return Ok(FilterExpr::Between {
            field,
            low,
            high,
        });
    }
    if let Some((field, values)) = parse_in(input) {
        let field = resolve_field(field, namespace_id, store).await?;
        let values: Vec<_> = values.into_iter().map(|v| parse_value(&v)).collect();
        for value in &values {
            ensure_value_type(&field, value)?;
        }
        return Ok(FilterExpr::In {
            field,
            values,
        });
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
            return Ok(FilterExpr::Compare {
                field,
                op,
                value,
            });
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
        "CloseTime" => Some(SystemField::CloseTime),
        "HistoryLength" => Some(SystemField::HistoryLength),
        "StateTransitionCount" => Some(SystemField::StateTransitionCount),
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
    if let Ok(status) = parse_status(trimmed) {
        return FilterValue::Status(status);
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return FilterValue::Int(value);
    }
    if let Ok(value) = trimmed.parse::<f64>() {
        return FilterValue::Float(value);
    }
    if let Ok(value) = OffsetDateTime::parse(trimmed, &time::format_description::well_known::Rfc3339) {
        return FilterValue::Datetime(value);
    }
    FilterValue::String(trimmed.to_string())
}

fn parse_status(input: &str) -> Result<ExecutionStatus> {
    match input {
        "Running" => Ok(ExecutionStatus::Running),
        "Completed" => Ok(ExecutionStatus::Completed),
        "Failed" => Ok(ExecutionStatus::Failed),
        "Cancelled" => Ok(ExecutionStatus::Cancelled),
        "Terminated" => Ok(ExecutionStatus::Terminated),
        "ContinuedAsNew" => Ok(ExecutionStatus::ContinuedAsNew),
        "TimedOut" => Ok(ExecutionStatus::TimedOut),
        _ => Err(anyhow!("not a status")),
    }
}

fn split_top_level<'a>(input: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    input.split_once(needle)
}

fn parse_between(input: &str) -> Option<(&str, String, String)> {
    let (field, rest) = input.split_once(" BETWEEN ")?;
    let (low, high) = rest.split_once(" AND ")?;
    Some((field.trim(), low.trim().to_string(), high.trim().to_string()))
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
    Some((field.trim(), rest.trim().trim_matches('"').trim_matches('\'').to_string()))
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
        | FieldRef::System(SystemField::TaskQueue) => matches!(value, FilterValue::String(_)),
        FieldRef::System(SystemField::ExecutionStatus) => matches!(value, FilterValue::Status(_)),
        FieldRef::System(SystemField::StartTime)
        | FieldRef::System(SystemField::CloseTime) => matches!(value, FilterValue::Datetime(_)),
        FieldRef::System(SystemField::HistoryLength)
        | FieldRef::System(SystemField::StateTransitionCount) => matches!(value, FilterValue::Int(_)),
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

    fn arb_system_string_field(
    ) -> impl Strategy<Value = &'static str> {
        prop_oneof![
            Just("WorkflowId"),
            Just("WorkflowType"),
            Just("TaskQueue"),
        ]
    }

    fn arb_compare_op(
    ) -> impl Strategy<Value = &'static str> {
        prop_oneof![Just("="), Just("!="),]
    }

    fn format_compare(
        field: &str,
        op: &str,
        value: &str,
    ) -> String {
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
}
