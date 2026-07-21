//! Worker-inventory filtering and pagination at the compatibility edge.
//!
//! The runtime owns only a process-local heartbeat observation store. This
//! module supplies the public `ListWorkers` read semantics: it evaluates the
//! bounded v1.31.0 worker-query grammar over decoded heartbeat protos, projects
//! list summaries, and applies a worker-key cursor. It performs no I/O and owns
//! no state.

use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokeira_proto::{
    enums::WorkerStatus,
    public::temporal::api::worker::v1::{WorkerHeartbeat, WorkerListInfo},
};
use tonic::Status;

#[derive(Clone, Debug, PartialEq)]
enum Filter {
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Predicate(Predicate),
}

#[derive(Clone, Debug, PartialEq)]
enum Predicate {
    Compare {
        field: Field,
        op: CompareOp,
        value: Value,
    },
    StartsWith {
        field: Field,
        prefix: String,
        negated: bool,
    },
    Between {
        field: Field,
        low: OffsetDateTime,
        high: OffsetDateTime,
        negated: bool,
    },
    IsNull {
        field: Field,
        negated: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Field {
    WorkerInstanceKey,
    WorkerIdentity,
    HostName,
    TaskQueue,
    DeploymentName,
    BuildId,
    SdkName,
    SdkVersion,
    StartTime,
    HeartbeatTime,
    WorkerStatus,
}

impl Field {
    fn parse(input: &str) -> Result<Self, Status> {
        match input.trim().trim_matches('`') {
            "WorkerInstanceKey" => Ok(Self::WorkerInstanceKey),
            "WorkerIdentity" => Ok(Self::WorkerIdentity),
            "HostName" => Ok(Self::HostName),
            "TaskQueue" => Ok(Self::TaskQueue),
            "DeploymentName" => Ok(Self::DeploymentName),
            "BuildId" => Ok(Self::BuildId),
            "SdkName" => Ok(Self::SdkName),
            "SdkVersion" => Ok(Self::SdkVersion),
            "StartTime" => Ok(Self::StartTime),
            "HeartbeatTime" => Ok(Self::HeartbeatTime),
            "WorkerStatus" | "Status" | "status" => Ok(Self::WorkerStatus),
            other => Err(Status::invalid_argument(format!(
                "unknown or unsupported worker heartbeat search field: {other}"
            ))),
        }
    }

    const fn is_time(self) -> bool {
        matches!(self, Self::StartTime | Self::HeartbeatTime)
    }

    fn string_value(self, heartbeat: &WorkerHeartbeat) -> String {
        match self {
            Self::WorkerInstanceKey => heartbeat.worker_instance_key.clone(),
            Self::WorkerIdentity => heartbeat.worker_identity.clone(),
            Self::HostName => heartbeat
                .host_info
                .as_ref()
                .map(|host| host.host_name.clone())
                .unwrap_or_default(),
            Self::TaskQueue => heartbeat.task_queue.clone(),
            Self::DeploymentName => heartbeat
                .deployment_version
                .as_ref()
                .map(|version| version.deployment_name.clone())
                .unwrap_or_default(),
            Self::BuildId => heartbeat
                .deployment_version
                .as_ref()
                .map(|version| version.build_id.clone())
                .unwrap_or_default(),
            Self::SdkName => heartbeat.sdk_name.clone(),
            Self::SdkVersion => heartbeat.sdk_version.clone(),
            Self::WorkerStatus => worker_status_name(heartbeat.status),
            Self::StartTime | Self::HeartbeatTime => String::new(),
        }
    }

    fn time_value(self, heartbeat: &WorkerHeartbeat) -> Option<OffsetDateTime> {
        let timestamp = match self {
            Self::StartTime => heartbeat.start_time.as_ref(),
            Self::HeartbeatTime => heartbeat.heartbeat_time.as_ref(),
            _ => None,
        }?;
        OffsetDateTime::from_unix_timestamp_nanos(
            i128::from(timestamp.seconds) * 1_000_000_000 + i128::from(timestamp.nanos),
        )
        .ok()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, Debug, PartialEq)]
enum Value {
    String(String),
    Time(OffsetDateTime),
}

impl Filter {
    fn parse(input: &str) -> Result<Self, Status> {
        let input = strip_enclosing_parentheses(input.trim());
        if input.is_empty()
            || starts_with_ascii_case(input, "WHERE ")
            || starts_with_ascii_case(input, "SELECT ")
        {
            return Err(Status::invalid_argument(format!("invalid filter: {input}")));
        }
        if let Some((left, right)) = split_boolean(input, " OR ", false) {
            return Ok(Self::Or(
                Box::new(Self::parse(left)?),
                Box::new(Self::parse(right)?),
            ));
        }
        if let Some((left, right)) = split_boolean(input, " AND ", true) {
            return Ok(Self::And(
                Box::new(Self::parse(left)?),
                Box::new(Self::parse(right)?),
            ));
        }
        Ok(Self::Predicate(Predicate::parse(input)?))
    }

    fn matches(&self, heartbeat: &WorkerHeartbeat) -> bool {
        match self {
            Self::And(left, right) => left.matches(heartbeat) && right.matches(heartbeat),
            Self::Or(left, right) => left.matches(heartbeat) || right.matches(heartbeat),
            Self::Predicate(predicate) => predicate.matches(heartbeat),
        }
    }
}

impl Predicate {
    fn parse(input: &str) -> Result<Self, Status> {
        if let Some(field) = strip_suffix_ascii_case(input, " IS NOT NULL") {
            return Ok(Self::IsNull {
                field: Field::parse(field)?,
                negated: true,
            });
        }
        if let Some(field) = strip_suffix_ascii_case(input, " IS NULL") {
            return Ok(Self::IsNull {
                field: Field::parse(field)?,
                negated: false,
            });
        }
        if let Some((field, value)) = split_top_level(input, " NOT STARTS_WITH ") {
            let field = Field::parse(field)?;
            ensure_string_field(field, "NOT STARTS_WITH")?;
            return Ok(Self::StartsWith {
                field,
                prefix: parse_non_empty_string(value)?,
                negated: true,
            });
        }
        if let Some((field, value)) = split_top_level(input, " STARTS_WITH ") {
            let field = Field::parse(field)?;
            ensure_string_field(field, "STARTS_WITH")?;
            return Ok(Self::StartsWith {
                field,
                prefix: parse_non_empty_string(value)?,
                negated: false,
            });
        }
        for (phrase, negated) in [(" NOT BETWEEN ", true), (" BETWEEN ", false)] {
            if let Some((field, bounds)) = split_top_level(input, phrase) {
                let field = Field::parse(field)?;
                if !field.is_time() {
                    return Err(Status::invalid_argument(format!(
                        "invalid expression: operation BETWEEN is not supported for {} column",
                        field_name(field)
                    )));
                }
                let (low, high) = split_top_level(bounds, " AND ").ok_or_else(|| {
                    Status::invalid_argument("invalid expression: BETWEEN requires two values")
                })?;
                return Ok(Self::Between {
                    field,
                    low: parse_time(low)?,
                    high: parse_time(high)?,
                    negated,
                });
            }
        }
        let (field, op, value) =
            split_comparison(input).ok_or_else(|| Status::invalid_argument("malformed query"))?;
        let field = Field::parse(field)?;
        let value = if field.is_time() {
            Value::Time(parse_time(value)?)
        } else {
            if !matches!(op, CompareOp::Eq | CompareOp::Ne) {
                return Err(Status::invalid_argument(format!(
                    "invalid expression: operation {} is not supported for {} column",
                    op_name(op),
                    field_name(field)
                )));
            }
            Value::String(parse_non_empty_string(value)?)
        };
        Ok(Self::Compare { field, op, value })
    }

    fn matches(&self, heartbeat: &WorkerHeartbeat) -> bool {
        match self {
            Self::Compare { field, op, value } => match value {
                Value::String(expected) => {
                    let actual = field.string_value(heartbeat);
                    match op {
                        CompareOp::Eq => actual == *expected,
                        CompareOp::Ne => actual != *expected,
                        _ => false,
                    }
                }
                Value::Time(expected) => field
                    .time_value(heartbeat)
                    .is_some_and(|actual| compare_time(actual, *expected, *op)),
            },
            Self::StartsWith {
                field,
                prefix,
                negated,
            } => field.string_value(heartbeat).starts_with(prefix) != *negated,
            Self::Between {
                field,
                low,
                high,
                negated,
            } => field
                .time_value(heartbeat)
                .is_some_and(|actual| (actual >= *low && actual <= *high) != *negated),
            Self::IsNull { field, negated } => {
                let is_null = if field.is_time() {
                    field.time_value(heartbeat).is_none()
                } else {
                    field.string_value(heartbeat).is_empty()
                };
                is_null != *negated
            }
        }
    }
}

/// Filter decoded heartbeats using Temporal's v1.31.0 worker-query grammar.
pub(crate) fn filter_workers(
    workers: Vec<WorkerHeartbeat>,
    query: &str,
) -> Result<Vec<WorkerHeartbeat>, Status> {
    let query = query.trim();
    if query.is_empty() {
        return Ok(workers);
    }
    let filter = Filter::parse(query)?;
    Ok(workers
        .into_iter()
        .filter(|worker| filter.matches(worker))
        .collect())
}

#[derive(Debug, Deserialize, Serialize)]
struct PageToken {
    l: String,
}

/// Cursor-paginate workers by instance key.
pub(crate) fn paginate_workers(
    mut workers: Vec<WorkerHeartbeat>,
    page_size: i32,
    next_page_token: &[u8],
) -> Result<(Vec<WorkerHeartbeat>, Vec<u8>), Status> {
    if workers.is_empty() {
        return Ok((workers, Vec::new()));
    }
    if page_size == 0 && next_page_token.is_empty() {
        return Ok((workers, Vec::new()));
    }

    workers.sort_by(|left, right| left.worker_instance_key.cmp(&right.worker_instance_key));
    let cursor = if next_page_token.is_empty() {
        String::new()
    } else {
        serde_json::from_slice::<PageToken>(next_page_token)
            .map_err(|_| Status::invalid_argument("invalid next_page_token"))?
            .l
    };
    let start = workers.partition_point(|worker| worker.worker_instance_key <= cursor);
    if start >= workers.len() {
        return Ok((Vec::new(), Vec::new()));
    }
    let end = if page_size > 0 {
        start.saturating_add(page_size as usize).min(workers.len())
    } else {
        workers.len()
    };
    let page = workers[start..end].to_vec();
    let token = if end < workers.len() {
        serde_json::to_vec(&PageToken {
            l: page
                .last()
                .map(|worker| worker.worker_instance_key.clone())
                .unwrap_or_default(),
        })
        .map_err(|error| Status::internal(format!("failed to encode worker page token: {error}")))?
    } else {
        Vec::new()
    };
    Ok((page, token))
}

/// Project the limited list shape from a complete heartbeat.
pub(crate) fn worker_list_info(heartbeat: &WorkerHeartbeat) -> WorkerListInfo {
    let host = heartbeat.host_info.as_ref();
    WorkerListInfo {
        worker_instance_key: heartbeat.worker_instance_key.clone(),
        worker_identity: heartbeat.worker_identity.clone(),
        task_queue: heartbeat.task_queue.clone(),
        deployment_version: heartbeat.deployment_version.clone(),
        sdk_name: heartbeat.sdk_name.clone(),
        sdk_version: heartbeat.sdk_version.clone(),
        status: heartbeat.status,
        start_time: heartbeat.start_time.clone(),
        host_name: host
            .map(|value| value.host_name.clone())
            .unwrap_or_default(),
        worker_grouping_key: host
            .map(|value| value.worker_grouping_key.clone())
            .unwrap_or_default(),
        process_id: host
            .map(|value| value.process_id.clone())
            .unwrap_or_default(),
        plugins: heartbeat.plugins.clone(),
        drivers: heartbeat.drivers.clone(),
    }
}

fn compare_time(actual: OffsetDateTime, expected: OffsetDateTime, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => actual == expected,
        CompareOp::Ne => actual != expected,
        CompareOp::Gt => actual > expected,
        CompareOp::Ge => actual >= expected,
        CompareOp::Lt => actual < expected,
        CompareOp::Le => actual <= expected,
    }
}

fn worker_status_name(status: i32) -> String {
    WorkerStatus::try_from(status)
        .map(|value| value.as_str_name().to_owned())
        // Protobuf's generated String method renders unknown enum values as
        // their decimal representation; retaining that behavior prevents an
        // unknown future value from matching UNSPECIFIED accidentally.
        .unwrap_or_else(|_| status.to_string())
}

fn ensure_string_field(field: Field, operator: &str) -> Result<(), Status> {
    if field.is_time() {
        return Err(Status::invalid_argument(format!(
            "invalid expression: operation {operator} is not supported for {} column",
            field_name(field)
        )));
    }
    Ok(())
}

fn field_name(field: Field) -> &'static str {
    match field {
        Field::WorkerInstanceKey => "WorkerInstanceKey",
        Field::WorkerIdentity => "WorkerIdentity",
        Field::HostName => "HostName",
        Field::TaskQueue => "TaskQueue",
        Field::DeploymentName => "DeploymentName",
        Field::BuildId => "BuildId",
        Field::SdkName => "SdkName",
        Field::SdkVersion => "SdkVersion",
        Field::StartTime => "StartTime",
        Field::HeartbeatTime => "HeartbeatTime",
        Field::WorkerStatus => "WorkerStatus",
    }
}

fn op_name(op: CompareOp) -> &'static str {
    match op {
        CompareOp::Eq => "=",
        CompareOp::Ne => "!=",
        CompareOp::Gt => ">",
        CompareOp::Ge => ">=",
        CompareOp::Lt => "<",
        CompareOp::Le => "<=",
    }
}

fn parse_non_empty_string(input: &str) -> Result<String, Status> {
    let input = input.trim();
    let Some(quote) = input
        .chars()
        .next()
        .filter(|value| matches!(value, '\'' | '"'))
    else {
        return Err(Status::invalid_argument(format!("invalid value: {input}")));
    };
    if input.len() < 2 || !input.ends_with(quote) {
        return Err(Status::invalid_argument(format!("invalid value: {input}")));
    }
    let value = &input[quote.len_utf8()..input.len() - quote.len_utf8()];
    if value.is_empty() {
        return Err(Status::invalid_argument("query value cannot be empty"));
    }
    Ok(value.replace("''", "'"))
}

fn parse_time(input: &str) -> Result<OffsetDateTime, Status> {
    let value = parse_non_empty_string(input)?;
    OffsetDateTime::parse(&value, &Rfc3339)
        .map_err(|error| Status::invalid_argument(format!("invalid time value: {error}")))
}

fn split_comparison(input: &str) -> Option<(&str, CompareOp, &str)> {
    for (operator, op) in [
        (">=", CompareOp::Ge),
        ("<=", CompareOp::Le),
        ("!=", CompareOp::Ne),
        ("=", CompareOp::Eq),
        (">", CompareOp::Gt),
        ("<", CompareOp::Lt),
    ] {
        if let Some((left, right)) = split_top_level(input, operator) {
            return Some((left.trim(), op, right.trim()));
        }
    }
    None
}

fn split_boolean<'a>(
    input: &'a str,
    phrase: &str,
    skip_between_and: bool,
) -> Option<(&'a str, &'a str)> {
    let mut quote = None;
    let mut depth = 0usize;
    let mut between_pending = false;
    for (index, ch) in input.char_indices() {
        if matches!(ch, '\'' | '"') {
            quote = if quote == Some(ch) {
                None
            } else if quote.is_none() {
                Some(ch)
            } else {
                quote
            };
            continue;
        }
        if quote.is_some() {
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth != 0 {
            continue;
        }
        let rest = &input[index..];
        if skip_between_and && starts_with_ascii_case(rest, " BETWEEN ") {
            between_pending = true;
            continue;
        }
        if starts_with_ascii_case(rest, phrase) {
            if skip_between_and && between_pending {
                between_pending = false;
                continue;
            }
            return Some((&input[..index], &input[index + phrase.len()..]));
        }
    }
    None
}

fn split_top_level<'a>(input: &'a str, phrase: &str) -> Option<(&'a str, &'a str)> {
    let mut quote = None;
    let mut depth = 0usize;
    for (index, ch) in input.char_indices() {
        if matches!(ch, '\'' | '"') {
            quote = if quote == Some(ch) {
                None
            } else if quote.is_none() {
                Some(ch)
            } else {
                quote
            };
            continue;
        }
        if quote.is_some() {
            continue;
        }
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0 && starts_with_ascii_case(&input[index..], phrase) {
            return Some((&input[..index], &input[index + phrase.len()..]));
        }
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
        let mut quote = None;
        let mut depth = 0usize;
        let mut encloses_all = true;
        for (index, ch) in input.char_indices() {
            if matches!(ch, '\'' | '"') {
                quote = if quote == Some(ch) {
                    None
                } else if quote.is_none() {
                    Some(ch)
                } else {
                    quote
                };
                continue;
            }
            if quote.is_some() {
                continue;
            }
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && index + ch.len_utf8() != input.len() {
                        encloses_all = false;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !encloses_all || depth != 0 || quote.is_some() {
            return input;
        }
        input = inner.trim();
    }
}

fn starts_with_ascii_case(input: &str, prefix: &str) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|value| value.eq_ignore_ascii_case(prefix))
}

fn strip_suffix_ascii_case<'a>(input: &'a str, suffix: &str) -> Option<&'a str> {
    input
        .get(input.len().checked_sub(suffix.len())?..)
        .filter(|value| value.eq_ignore_ascii_case(suffix))
        .map(|_| &input[..input.len() - suffix.len()])
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost_types::Timestamp;
    use tokeira_proto::public::temporal::api::{
        deployment::v1::WorkerDeploymentVersion,
        worker::v1::{WorkerHostInfo, WorkerInfo},
    };

    fn heartbeat(key: &str, queue: &str) -> WorkerHeartbeat {
        WorkerHeartbeat {
            worker_instance_key: key.to_owned(),
            worker_identity: format!("identity-{key}"),
            host_info: Some(WorkerHostInfo {
                host_name: format!("host-{key}"),
                worker_grouping_key: format!("group-{key}"),
                process_id: format!("process-{key}"),
                current_host_cpu_usage: 0.25,
                current_host_mem_usage: 0.5,
            }),
            task_queue: queue.to_owned(),
            deployment_version: Some(WorkerDeploymentVersion {
                deployment_name: "deployment".to_owned(),
                build_id: "build".to_owned(),
            }),
            sdk_name: "sdk".to_owned(),
            sdk_version: "1.0".to_owned(),
            status: 1,
            start_time: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            heartbeat_time: Some(Timestamp {
                seconds: 1_700_000_100,
                nanos: 0,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn compound_query_matches_all_supported_value_kinds() {
        let workers = vec![
            heartbeat("worker-a", "queue-a"),
            heartbeat("worker-b", "queue-b"),
        ];
        let result = filter_workers(
            workers,
            "(WorkerInstanceKey STARTS_WITH 'worker-' AND TaskQueue = 'queue-a') OR WorkerStatus = 'Shutdown'",
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].worker_instance_key, "worker-a");
    }

    #[test]
    fn time_range_and_null_queries_follow_worker_fields() {
        let mut missing = heartbeat("missing", "queue");
        missing.heartbeat_time = None;
        let workers = vec![heartbeat("present", "queue"), missing];
        let ranged = filter_workers(
            workers.clone(),
            "HeartbeatTime BETWEEN '2023-11-14T22:15:00Z' AND '2023-11-14T22:16:00Z'",
        )
        .unwrap();
        assert_eq!(ranged.len(), 1);
        assert_eq!(ranged[0].worker_instance_key, "present");
        let null = filter_workers(workers, "HeartbeatTime IS NULL").unwrap();
        assert_eq!(null.len(), 1);
        assert_eq!(null[0].worker_instance_key, "missing");
    }

    #[test]
    fn deleted_cursor_resumes_at_first_greater_key() {
        let token = serde_json::to_vec(&PageToken {
            l: "worker-b".to_owned(),
        })
        .unwrap();
        let workers = vec![heartbeat("worker-a", "q"), heartbeat("worker-c", "q")];
        let (page, next) = paginate_workers(workers, 2, &token).unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].worker_instance_key, "worker-c");
        assert!(next.is_empty());
    }

    #[test]
    fn list_projection_copies_v131_summary_fields() {
        let source = heartbeat("worker", "queue");
        let summary = worker_list_info(&source);
        assert_eq!(summary.worker_instance_key, source.worker_instance_key);
        assert_eq!(
            summary.host_name,
            source.host_info.as_ref().unwrap().host_name
        );
        assert_eq!(
            WorkerInfo {
                worker_heartbeat: Some(source.clone()),
            }
            .worker_heartbeat,
            Some(source)
        );
    }

    proptest! {
        #[test]
        fn pagination_is_ordered_and_duplicate_free(count in 1usize..80, page_size in 1i32..12) {
            // Feature: worker-heartbeat-observability, Property 11: cursor pagination.
            let all: Vec<_> = (0..count)
                .rev()
                .map(|index| heartbeat(&format!("worker-{index:03}"), "queue"))
                .collect();
            let mut token = Vec::new();
            let mut observed = Vec::new();
            loop {
                let (page, next) = paginate_workers(all.clone(), page_size, &token).unwrap();
                observed.extend(page.into_iter().map(|worker| worker.worker_instance_key));
                if next.is_empty() {
                    break;
                }
                token = next;
            }
            let expected: Vec<_> = (0..count).map(|index| format!("worker-{index:03}")).collect();
            prop_assert_eq!(observed, expected);
        }

        #[test]
        fn string_equality_filter_agrees_with_reference(
            actual in "[a-z]{1,12}",
            expected in "[a-z]{1,12}",
        ) {
            // Feature: worker-heartbeat-observability, Property 12: query agreement.
            let workers = vec![heartbeat("worker", &actual)];
            let query = format!("TaskQueue = '{expected}'");
            let matches = filter_workers(workers, &query).unwrap();
            prop_assert_eq!(!matches.is_empty(), actual == expected);
        }
    }
}
