//! Pure Workflow Rule matching over authoritative workflow and activity state.
//!
//! Storage supplies transport-neutral namespace records; lifecycle call sites supply the current
//! run and activity. This module has no I/O and returns a rule decision only, keeping persistence
//! and dispatch ordering in the surrounding runtime transition.

use anyhow::{Result, anyhow};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokeira_kernel::{ActivityPauseInfo, ActivityState, WorkflowState};
use tokeira_types::{ExecutionStatus, WorkflowRuleRecord, WorkflowRuleTrigger};

/// Return the first unexpired activity-pause rule whose predicates match.
pub(crate) fn matching_pause_rule<'a>(
    workflow: &WorkflowState,
    activity: &ActivityState,
    rules: &'a [WorkflowRuleRecord],
    now: OffsetDateTime,
    backoff_interval_seconds: Option<i64>,
) -> Option<&'a WorkflowRuleRecord> {
    rules.iter().find(|rule| {
        if !rule.is_unexpired_at(now) || !rule.pauses_activity() {
            return false;
        }
        match rule_matches(workflow, activity, rule, backoff_interval_seconds) {
            Ok(matches) => matches,
            Err(error) => {
                // Temporal treats one malformed rule as a non-match and continues evaluating the
                // namespace set (`ActivityMatchWorkflowRules`, mutable_state_impl.go:9232-9277
                // @ v1.31.0). A bad operator policy must not fail an activity transition.
                tracing::warn!(
                    rule_id = rule.id,
                    ?error,
                    "workflow rule predicate did not evaluate"
                );
                false
            }
        }
    })
}

/// Materialize durable activity-pause provenance from a matching rule.
pub(crate) fn pause_info_for_rule(
    rule: &WorkflowRuleRecord,
    pause_time: OffsetDateTime,
) -> ActivityPauseInfo {
    ActivityPauseInfo {
        pause_time,
        identity: rule.created_by_identity.clone(),
        reason: rule.description.clone(),
        rule_id: Some(rule.id.clone()),
    }
}

fn rule_matches(
    workflow: &WorkflowState,
    activity: &ActivityState,
    rule: &WorkflowRuleRecord,
    backoff_interval_seconds: Option<i64>,
) -> Result<bool> {
    if !rule.visibility_query.trim().is_empty()
        && !evaluate_expression(&rule.visibility_query, |field| {
            workflow_field(workflow, field)
        })?
    {
        return Ok(false);
    }
    let WorkflowRuleTrigger::ActivityStart { predicate } = &rule.trigger else {
        return Ok(false);
    };
    evaluate_expression(predicate, |field| {
        activity_field(activity, field, backoff_interval_seconds)
    })
}

#[derive(Clone, Debug, PartialEq)]
enum Value {
    String(String),
    Integer(i64),
    Time(OffsetDateTime),
}

fn workflow_field(workflow: &WorkflowState, field: &str) -> Option<Value> {
    match field {
        "WorkflowType" => Some(Value::String(workflow.workflow_type.0.clone())),
        "WorkflowId" => Some(Value::String(workflow.workflow_id.0.clone())),
        "StartTime" => Some(Value::Time(workflow.started_at)),
        "ExecutionStatus" => Some(Value::String(
            execution_status_name(workflow.status).to_string(),
        )),
        _ => None,
    }
}

fn activity_field(
    activity: &ActivityState,
    field: &str,
    backoff_interval_seconds: Option<i64>,
) -> Option<Value> {
    match field {
        "ActivityId" => Some(Value::String(activity.activity_id.clone())),
        "ActivityType" => Some(Value::String(activity.activity_type.clone())),
        "ActivityState" | "ActivityStatus" | "Status" => {
            Some(Value::String(activity_state_name(activity).to_string()))
        }
        "Attempts" | "ActivityAttempt" => Some(Value::Integer(i64::from(activity.attempt))),
        "BackoffInterval" => backoff_interval_seconds.map(Value::Integer),
        "LastFailure" => activity
            .last_failure
            .as_ref()
            .map(|failure| Value::String(String::from_utf8_lossy(&failure.data).into_owned())),
        "TaskQueue" => Some(Value::String(activity.task_queue.0.clone())),
        "StartedTime" => activity.started_at.map(Value::Time),
        _ => None,
    }
}

fn execution_status_name(status: ExecutionStatus) -> &'static str {
    match status {
        ExecutionStatus::Running => "Running",
        ExecutionStatus::Paused => "Paused",
        ExecutionStatus::Completed => "Completed",
        ExecutionStatus::Failed => "Failed",
        ExecutionStatus::Cancelled => "Canceled",
        ExecutionStatus::Terminated => "Terminated",
        ExecutionStatus::ContinuedAsNew => "ContinuedAsNew",
        ExecutionStatus::TimedOut => "TimedOut",
    }
}

fn activity_state_name(activity: &ActivityState) -> &'static str {
    if activity.pause_info.is_some() {
        "Paused"
    } else if activity.cancel_requested {
        "CancelRequested"
    } else if activity.started_event_id.is_some() {
        "Started"
    } else {
        "Scheduled"
    }
}

fn evaluate_expression(
    input: &str,
    mut resolve: impl FnMut(&str) -> Option<Value> + Copy,
) -> Result<bool> {
    let input = input.trim();
    let input = input
        .get(0..6)
        .filter(|prefix| prefix.eq_ignore_ascii_case("WHERE "))
        .map_or(input, |_| &input[6..]);
    evaluate_inner(input.trim(), &mut resolve)
}

fn evaluate_inner(
    input: &str,
    resolve: &mut (impl FnMut(&str) -> Option<Value> + Copy),
) -> Result<bool> {
    let input = strip_enclosing_parentheses(input.trim());
    if let Some((left, right)) = split_top_level(input, " OR ") {
        return Ok(evaluate_inner(left, resolve)? || evaluate_inner(right, resolve)?);
    }
    if let Some((left, right)) = split_top_level(input, " AND ") {
        return Ok(evaluate_inner(left, resolve)? && evaluate_inner(right, resolve)?);
    }
    if let Some((field, low, high)) = parse_between(input) {
        let actual = resolve(field).ok_or_else(|| anyhow!("unsupported rule field {field}"))?;
        let low = parse_literal(low, &actual)?;
        let high = parse_literal(high, &actual)?;
        return Ok(
            compare(&actual, &low, CompareOp::Ge)? && compare(&actual, &high, CompareOp::Le)?
        );
    }
    for (needle, op) in [
        (" NOT STARTS_WITH ", StringOp::NotStartsWith),
        (" STARTS_WITH ", StringOp::StartsWith),
        (" NOT CONTAINS ", StringOp::NotContains),
        (" CONTAINS ", StringOp::Contains),
    ] {
        if let Some((field, expected)) = split_once_ascii_case(input, needle) {
            let actual = resolve(field.trim())
                .ok_or_else(|| anyhow!("unsupported rule field {}", field.trim()))?;
            let Value::String(actual) = actual else {
                return Err(anyhow!("{needle} requires a string field"));
            };
            let expected = unquote(expected.trim());
            return Ok(match op {
                StringOp::StartsWith => actual.starts_with(expected),
                StringOp::NotStartsWith => !actual.starts_with(expected),
                StringOp::Contains => actual.contains(expected),
                StringOp::NotContains => !actual.contains(expected),
            });
        }
    }
    for (needle, op) in [
        ("!=", CompareOp::Ne),
        (">=", CompareOp::Ge),
        ("<=", CompareOp::Le),
        ("=", CompareOp::Eq),
        (">", CompareOp::Gt),
        ("<", CompareOp::Lt),
    ] {
        if let Some((field, expected)) = input.split_once(needle) {
            let field = field.trim();
            let actual = resolve(field).ok_or_else(|| anyhow!("unsupported rule field {field}"))?;
            let expected = parse_literal(expected, &actual)?;
            return compare(&actual, &expected, op);
        }
    }
    Err(anyhow!("unsupported rule expression {input}"))
}

#[derive(Clone, Copy)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, Copy)]
enum StringOp {
    StartsWith,
    NotStartsWith,
    Contains,
    NotContains,
}

fn compare(actual: &Value, expected: &Value, op: CompareOp) -> Result<bool> {
    macro_rules! ordered {
        ($left:expr, $right:expr) => {
            Ok(match op {
                CompareOp::Eq => $left == $right,
                CompareOp::Ne => $left != $right,
                CompareOp::Gt => $left > $right,
                CompareOp::Ge => $left >= $right,
                CompareOp::Lt => $left < $right,
                CompareOp::Le => $left <= $right,
            })
        };
    }
    match (actual, expected) {
        (Value::String(left), Value::String(right)) => ordered!(left, right),
        (Value::Integer(left), Value::Integer(right)) => ordered!(left, right),
        (Value::Time(left), Value::Time(right)) => ordered!(left, right),
        _ => Err(anyhow!("rule comparison type mismatch")),
    }
}

fn parse_literal(input: &str, actual: &Value) -> Result<Value> {
    let input = unquote(input.trim());
    match actual {
        Value::String(_) => Ok(Value::String(input.to_string())),
        Value::Integer(_) => input
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|error| anyhow!("invalid integer rule literal: {error}")),
        Value::Time(_) => OffsetDateTime::parse(input, &Rfc3339)
            .map(Value::Time)
            .map_err(|error| anyhow!("invalid time rule literal: {error}")),
    }
}

fn unquote(input: &str) -> &str {
    input
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
        .or_else(|| {
            input
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
        })
        .unwrap_or(input)
}

fn split_top_level<'a>(input: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    let mut quote = None;
    let mut depth = 0usize;
    let mut between_pending = false;
    let bytes = input.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        let character = bytes[index] as char;
        if matches!(character, '\'' | '"') {
            if quote == Some(character) {
                quote = None;
            } else if quote.is_none() {
                quote = Some(character);
            }
            index += 1;
            continue;
        }
        if quote.is_some() {
            index += 1;
            continue;
        }
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 {
            let rest = &input[index..];
            if rest
                .get(..9)
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(" BETWEEN "))
            {
                between_pending = true;
                index += 9;
                continue;
            }
            if rest
                .get(..needle.len())
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(needle))
            {
                if needle.eq_ignore_ascii_case(" AND ") && between_pending {
                    between_pending = false;
                    index += needle.len();
                    continue;
                }
                return Some((&input[..index], &input[index + needle.len()..]));
            }
        }
        index += 1;
    }
    None
}

fn strip_enclosing_parentheses(mut input: &str) -> &str {
    loop {
        let Some(inner) = input
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
        else {
            return input;
        };
        let mut depth = 0usize;
        let mut quote = None;
        let mut complete = true;
        for (index, character) in input.char_indices() {
            if matches!(character, '\'' | '"') {
                quote = if quote == Some(character) {
                    None
                } else if quote.is_none() {
                    Some(character)
                } else {
                    quote
                };
            } else if quote.is_none() {
                if character == '(' {
                    depth += 1;
                } else if character == ')' {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && index + 1 != input.len() {
                        complete = false;
                        break;
                    }
                }
            }
        }
        if !complete {
            return input;
        }
        input = inner.trim();
    }
}

fn parse_between(input: &str) -> Option<(&str, &str, &str)> {
    let (field, rest) = split_once_ascii_case(input, " BETWEEN ")?;
    let (low, high) = split_once_ascii_case(rest, " AND ")?;
    Some((field.trim(), low.trim(), high.trim()))
}

fn split_once_ascii_case<'a>(input: &'a str, needle: &str) -> Option<(&'a str, &'a str)> {
    input
        .as_bytes()
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle.as_bytes()))
        .map(|index| (&input[..index], &input[index + needle.len()..]))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::Duration;
    use tokeira_types::{Headers, Payloads, TaskQueueName, WorkflowRuleAction};

    use super::*;

    fn rule(predicate: String, expiration_time: Option<OffsetDateTime>) -> WorkflowRuleRecord {
        WorkflowRuleRecord {
            id: "rule".to_string(),
            create_time: OffsetDateTime::UNIX_EPOCH,
            created_by_identity: "creator".to_string(),
            description: "description".to_string(),
            trigger: WorkflowRuleTrigger::ActivityStart { predicate },
            visibility_query: String::new(),
            actions: vec![WorkflowRuleAction::ActivityPause],
            expiration_time,
        }
    }

    fn activity(activity_type: &str, attempt: u32) -> ActivityState {
        ActivityState {
            activity_id: "activity-id".to_string(),
            activity_type: activity_type.to_string(),
            schedule_event_id: 1,
            task_queue: TaskQueueName("queue".to_string()),
            deployment: None,
            build_id: None,
            input: Payloads::default(),
            header: Some(Headers::default()),
            attempt,
            retry_policy: None,
            schedule_to_close_timeout: None,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            scheduled_at: OffsetDateTime::UNIX_EPOCH,
            current_attempt_scheduled_at: None,
            started_at: None,
            started_event_id: None,
            last_failure: None,
            started_identity: None,
            retry_last_worker_identity: None,
            heartbeat_details: None,
            cancel_requested: false,
            pause_info: None,
            stamp: 0,
            priority: None,
            activity_reset: false,
            reset_heartbeats: false,
        }
    }

    #[test]
    fn boolean_activity_predicate_matches_supported_fields() {
        let activity = activity("demo", 3);
        assert!(
            evaluate_expression(
                "ActivityType STARTS_WITH 'de' AND Attempts >= 3 AND TaskQueue = 'queue'",
                |field| activity_field(&activity, field, Some(7)),
            )
            .expect("supported expression")
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        // Feature: workflow-rules, Property 3: rule evaluation reference model
        fn property_activity_type_equality_is_exact(
            activity_type in "[a-z]{1,16}",
            candidate in "[a-z]{1,16}",
        ) {
            let activity = activity(&activity_type, 1);
            let query = format!("ActivityType = '{candidate}'");
            let actual = evaluate_expression(&query, |field| {
                activity_field(&activity, field, None)
            }).expect("generated equality expression");
            prop_assert_eq!(actual, activity_type == candidate);
        }

        #[test]
        // Feature: workflow-rules, Property 7: expiration separates evaluation from retention
        fn property_expired_rules_never_match(delta_seconds in 0i64..1_000_000i64) {
            let now = OffsetDateTime::UNIX_EPOCH + Duration::seconds(delta_seconds + 1);
            let expired = rule(
                "ActivityType = 'demo'".to_string(),
                Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(delta_seconds)),
            );
            prop_assert!(!expired.is_unexpired_at(now));
        }

        #[test]
        // Feature: workflow-rules, Property 8: activity-start path equivalence
        fn property_backoff_and_start_views_share_pause_decision(
            attempt in 1u32..100u32,
            backoff in 0i64..10_000i64,
        ) {
            let activity = activity("demo", attempt);
            let rules = [rule(
                format!("ActivityType = 'demo' AND Attempts = {attempt}"),
                None,
            )];
            let predicate = match &rules[0].trigger {
                WorkflowRuleTrigger::ActivityStart { predicate } => predicate,
                WorkflowRuleTrigger::Unsupported => unreachable!("test rule uses activity start"),
            };
            let start = evaluate_expression(predicate, |field| {
                activity_field(&activity, field, None)
            }).expect("start expression");
            let retry = evaluate_expression(predicate, |field| {
                activity_field(&activity, field, Some(backoff))
            }).expect("retry expression");
            prop_assert_eq!(start, retry);
        }

        #[test]
        // Feature: workflow-rules, Property 4: rule-pause provenance
        fn property_rule_pause_provenance_retains_rule_identity(
            rule_id in "[a-z][a-z0-9-]{0,24}",
            identity in ".{0,32}",
            description in ".{0,64}",
            pause_seconds in -1_000_000i64..1_000_000i64,
        ) {
            let mut matching_rule = rule("ActivityType = 'demo'".to_string(), None);
            matching_rule.id = rule_id.clone();
            matching_rule.created_by_identity = identity.clone();
            matching_rule.description = description.clone();
            let pause_time = OffsetDateTime::from_unix_timestamp(pause_seconds)
                .expect("generated pause time");
            let pause = pause_info_for_rule(&matching_rule, pause_time);
            prop_assert_eq!(pause.rule_id, Some(rule_id));
            prop_assert_eq!(pause.identity, identity);
            prop_assert_eq!(pause.reason, description);
            prop_assert_eq!(pause.pause_time, pause_time);
        }
    }
}
