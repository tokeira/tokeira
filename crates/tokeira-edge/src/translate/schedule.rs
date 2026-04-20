//! Translation for Temporal schedule protobufs.
//!
//! Schedule transport has a deliberately lossy boundary: Temporal accepts
//! several authoring forms (`CalendarSpec`, cron strings, timezone data) while
//! the runtime stores one normalized form. These helpers make that normalization
//! explicit and keep unsupported schedule-action fields from leaking into the
//! runtime with surprising defaults.

use prost_types::{Duration as ProtoDuration, Timestamp};
use time::{Duration, OffsetDateTime};
use tokeira_proto::{
    conversions::common::{
        memo_from_domain, memo_to_domain, payloads_from_domain, payloads_to_domain,
        search_attributes_from_domain, search_attributes_to_domain,
        task_queue_from_domain, to_proto_duration, to_proto_timestamp,
    },
    enums,
    public::temporal::api::{
        common::v1 as common, schedule::v1 as proto_schedule,
        schedule::v1::schedule_action, workflow::v1 as workflow,
    },
    workflowservice,
};
use tokeira_runtime::schedule as domain;
use tokeira_types::{
    Memo, NamespaceId, RetryPolicy, SearchAttributes, TaskQueueName, WorkflowId,
    WorkflowType,
};
use tonic::Status;

use crate::translate::to_internal::namespace_id_for;

pub fn create_schedule_request_to_edge(
    request: workflowservice::CreateScheduleRequest,
) -> Result<
    (
        NamespaceId,
        domain::ScheduleId,
        domain::ScheduleEntry,
        Option<domain::SchedulePatch>,
    ),
    Status,
> {
    if request.schedule_id.is_empty() {
        return Err(Status::invalid_argument("schedule_id must not be empty"));
    }
    let now = OffsetDateTime::now_utc();
    let namespace_id = namespace_id_for(&request.namespace);
    let schedule = request
        .schedule
        .ok_or_else(|| Status::invalid_argument("schedule is required"))?;
    let schedule_id = domain::ScheduleId(request.schedule_id);
    let (spec, action, policies, state) = schedule_to_domain(schedule)?;
    let entry = domain::ScheduleEntry {
        schedule_id: schedule_id.clone(),
        namespace_id,
        spec,
        action,
        policies,
        state,
        info: domain::ScheduleInfo::new(now),
        memo: request
            .memo
            .as_ref()
            .map(memo_to_domain)
            .unwrap_or_default(),
        search_attributes: match request.search_attributes.as_ref() {
            Some(attrs) => search_attributes_to_domain(attrs)
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
            None => SearchAttributes::default(),
        },
        conflict_token: Vec::new(),
    };
    Ok((
        namespace_id,
        schedule_id,
        entry,
        request
            .initial_patch
            .map(schedule_patch_to_domain)
            .transpose()?,
    ))
}

pub fn update_schedule_request_to_edge(
    request: workflowservice::UpdateScheduleRequest,
) -> Result<
    (
        NamespaceId,
        domain::ScheduleId,
        Vec<u8>,
        domain::ScheduleEntry,
    ),
    Status,
> {
    if request.schedule_id.is_empty() {
        return Err(Status::invalid_argument("schedule_id must not be empty"));
    }
    let now = OffsetDateTime::now_utc();
    let namespace_id = namespace_id_for(&request.namespace);
    let schedule = request
        .schedule
        .ok_or_else(|| Status::invalid_argument("schedule is required"))?;
    let schedule_id = domain::ScheduleId(request.schedule_id);
    let (spec, action, policies, state) = schedule_to_domain(schedule)?;
    let entry = domain::ScheduleEntry {
        schedule_id: schedule_id.clone(),
        namespace_id,
        spec,
        action,
        policies,
        state,
        info: domain::ScheduleInfo::new(now),
        memo: Memo::default(),
        search_attributes: match request.search_attributes.as_ref() {
            Some(attrs) => search_attributes_to_domain(attrs)
                .map_err(|err| Status::invalid_argument(err.to_string()))?,
            None => SearchAttributes::default(),
        },
        conflict_token: request.conflict_token.clone(),
    };
    Ok((namespace_id, schedule_id, request.conflict_token, entry))
}

pub fn patch_schedule_request_to_edge(
    request: workflowservice::PatchScheduleRequest,
) -> Result<(NamespaceId, domain::ScheduleId, domain::SchedulePatch), Status> {
    if request.schedule_id.is_empty() {
        return Err(Status::invalid_argument("schedule_id must not be empty"));
    }
    let patch = request
        .patch
        .ok_or_else(|| Status::invalid_argument("patch is required"))
        .and_then(schedule_patch_to_domain)?;
    Ok((
        namespace_id_for(&request.namespace),
        domain::ScheduleId(request.schedule_id),
        patch,
    ))
}

pub fn describe_schedule_response_to_proto(
    entry: &domain::ScheduleEntry,
) -> workflowservice::DescribeScheduleResponse {
    workflowservice::DescribeScheduleResponse {
        schedule: Some(schedule_to_proto(entry)),
        info: Some(schedule_info_to_proto(&entry.info)),
        memo: Some(memo_from_domain(&entry.memo)),
        search_attributes: Some(search_attributes_from_domain(&entry.search_attributes)),
        conflict_token: entry.conflict_token.clone(),
    }
}

pub fn list_schedules_response_to_proto(
    entries: Vec<domain::ScheduleEntry>,
    next_page_token: Option<Vec<u8>>,
) -> workflowservice::ListSchedulesResponse {
    workflowservice::ListSchedulesResponse {
        schedules: entries
            .iter()
            .map(|entry| proto_schedule::ScheduleListEntry {
                schedule_id: entry.schedule_id.0.clone(),
                memo: Some(memo_from_domain(&entry.memo)),
                search_attributes: Some(search_attributes_from_domain(
                    &entry.search_attributes,
                )),
                info: Some(proto_schedule::ScheduleListInfo {
                    spec: Some(schedule_spec_to_proto_without_timezone_data(&entry.spec)),
                    workflow_type: Some(common::WorkflowType {
                        name: entry.action.start_workflow.workflow_type.0.clone(),
                    }),
                    notes: entry.state.notes.clone(),
                    paused: entry.state.paused,
                    recent_actions: entry
                        .info
                        .recent_actions
                        .iter()
                        .map(schedule_action_result_to_proto)
                        .collect(),
                    future_action_times: entry
                        .info
                        .future_action_times
                        .iter()
                        .copied()
                        .map(to_proto_timestamp)
                        .collect(),
                }),
            })
            .collect(),
        next_page_token: next_page_token.unwrap_or_default(),
    }
}

pub fn matching_times_response_to_proto(
    times: Vec<OffsetDateTime>,
) -> workflowservice::ListScheduleMatchingTimesResponse {
    workflowservice::ListScheduleMatchingTimesResponse {
        start_time: times.into_iter().map(to_proto_timestamp).collect(),
    }
}

pub fn schedule_patch_to_domain(
    patch: proto_schedule::SchedulePatch,
) -> Result<domain::SchedulePatch, Status> {
    Ok(domain::SchedulePatch {
        trigger_immediately: patch.trigger_immediately.map(|trigger| {
            domain::TriggerImmediately {
                overlap_policy: overlap_policy_to_domain(trigger.overlap_policy),
            }
        }),
        backfill_request: patch
            .backfill_request
            .into_iter()
            .map(|backfill| {
                Ok(domain::BackfillRequest {
                    start_time: proto_timestamp_to_time(backfill.start_time.as_ref())
                        .ok_or_else(|| {
                            Status::invalid_argument("backfill start_time required")
                        })?,
                    end_time: proto_timestamp_to_time(backfill.end_time.as_ref())
                        .ok_or_else(|| {
                            Status::invalid_argument("backfill end_time required")
                        })?,
                    overlap_policy: overlap_policy_to_domain(backfill.overlap_policy),
                })
            })
            .collect::<Result<_, Status>>()?,
        pause: (!patch.pause.is_empty()).then_some(patch.pause),
        unpause: (!patch.unpause.is_empty()).then_some(patch.unpause),
    })
}

fn schedule_to_domain(
    schedule: proto_schedule::Schedule,
) -> Result<
    (
        domain::ScheduleSpec,
        domain::ScheduleAction,
        domain::SchedulePolicies,
        domain::ScheduleState,
    ),
    Status,
> {
    let spec = schedule
        .spec
        .ok_or_else(|| Status::invalid_argument("schedule.spec is required"))
        .and_then(schedule_spec_to_domain)?;
    let action = schedule
        .action
        .ok_or_else(|| Status::invalid_argument("schedule.action is required"))
        .and_then(schedule_action_to_domain)?;
    let policies = schedule
        .policies
        .map(schedule_policies_to_domain)
        .unwrap_or_default();
    let state = schedule
        .state
        .map(schedule_state_to_domain)
        .unwrap_or_default();
    Ok((spec, action, policies, state))
}

pub fn schedule_spec_to_domain(
    spec: proto_schedule::ScheduleSpec,
) -> Result<domain::ScheduleSpec, Status> {
    let mut structured_calendars = spec
        .structured_calendar
        .into_iter()
        .map(structured_calendar_to_domain)
        .collect::<Vec<_>>();
    for calendar in spec.calendar {
        structured_calendars.push(compile_calendar_spec(calendar)?);
    }
    for cron in spec.cron_string {
        structured_calendars.push(compile_cron_string(&cron)?);
    }
    let intervals = spec
        .interval
        .into_iter()
        .map(interval_to_domain)
        .collect::<Result<_, Status>>()?;
    #[allow(deprecated)]
    let legacy_exclude = spec.exclude_calendar;
    let exclude_calendars = spec
        .exclude_structured_calendar
        .into_iter()
        .map(structured_calendar_to_domain)
        .chain(
            legacy_exclude
                .into_iter()
                .map(compile_calendar_spec)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .collect();
    Ok(domain::ScheduleSpec {
        structured_calendars,
        intervals,
        exclude_calendars,
        start_time: proto_timestamp_to_time(spec.start_time.as_ref()),
        end_time: proto_timestamp_to_time(spec.end_time.as_ref()),
        jitter: proto_duration_to_time(spec.jitter.as_ref()),
        timezone_name: spec.timezone_name,
    })
}

pub fn schedule_spec_to_proto(
    spec: &domain::ScheduleSpec,
) -> proto_schedule::ScheduleSpec {
    schedule_spec_to_proto_inner(spec, Vec::new())
}

fn schedule_spec_to_proto_without_timezone_data(
    spec: &domain::ScheduleSpec,
) -> proto_schedule::ScheduleSpec {
    schedule_spec_to_proto_inner(spec, Vec::new())
}

fn schedule_spec_to_proto_inner(
    spec: &domain::ScheduleSpec,
    timezone_data: Vec<u8>,
) -> proto_schedule::ScheduleSpec {
    #[allow(deprecated)]
    let spec = proto_schedule::ScheduleSpec {
        structured_calendar: spec
            .structured_calendars
            .iter()
            .map(structured_calendar_to_proto)
            .collect(),
        cron_string: Vec::new(),
        calendar: Vec::new(),
        interval: spec.intervals.iter().map(interval_to_proto).collect(),
        exclude_calendar: Vec::new(),
        exclude_structured_calendar: spec
            .exclude_calendars
            .iter()
            .map(structured_calendar_to_proto)
            .collect(),
        start_time: spec.start_time.map(to_proto_timestamp),
        end_time: spec.end_time.map(to_proto_timestamp),
        jitter: spec.jitter.map(to_proto_duration),
        timezone_name: spec.timezone_name.clone(),
        timezone_data,
    };
    spec
}

pub fn schedule_action_to_domain(
    action: proto_schedule::ScheduleAction,
) -> Result<domain::ScheduleAction, Status> {
    let Some(schedule_action::Action::StartWorkflow(start)) = action.action else {
        return Err(Status::invalid_argument(
            "schedule action must start workflow",
        ));
    };
    if start.header.is_some() || start.user_metadata.is_some() {
        return Err(Status::invalid_argument(
            "schedule action header and user_metadata are not supported",
        ));
    }
    if start.versioning_override.is_some() {
        return Err(Status::invalid_argument(
            "schedule action versioning_override is not supported",
        ));
    }
    let workflow_type = start
        .workflow_type
        .ok_or_else(|| Status::invalid_argument("workflow_type is required"))?;
    let task_queue = start
        .task_queue
        .ok_or_else(|| Status::invalid_argument("task_queue is required"))?;
    Ok(domain::ScheduleAction {
        start_workflow: domain::StartWorkflowAction {
            workflow_id: WorkflowId(start.workflow_id),
            workflow_type: WorkflowType(workflow_type.name),
            task_queue: TaskQueueName(task_queue.name),
            input: start
                .input
                .as_ref()
                .map(payloads_to_domain)
                .unwrap_or_default(),
            workflow_execution_timeout: proto_duration_to_time(
                start.workflow_execution_timeout.as_ref(),
            ),
            workflow_run_timeout: proto_duration_to_time(
                start.workflow_run_timeout.as_ref(),
            ),
            workflow_task_timeout: proto_duration_to_time(
                start.workflow_task_timeout.as_ref(),
            ),
            retry_policy: start.retry_policy.as_ref().map(retry_policy_to_domain),
            memo: start.memo.as_ref().map(memo_to_domain).unwrap_or_default(),
            search_attributes: match start.search_attributes.as_ref() {
                Some(attrs) => search_attributes_to_domain(attrs)
                    .map_err(|err| Status::invalid_argument(err.to_string()))?,
                None => SearchAttributes::default(),
            },
        },
    })
}

pub fn schedule_action_to_proto(
    action: &domain::ScheduleAction,
) -> proto_schedule::ScheduleAction {
    let start = &action.start_workflow;
    proto_schedule::ScheduleAction {
        action: Some(schedule_action::Action::StartWorkflow(
            workflow::NewWorkflowExecutionInfo {
                workflow_id: start.workflow_id.0.clone(),
                workflow_type: Some(common::WorkflowType {
                    name: start.workflow_type.0.clone(),
                }),
                task_queue: Some(task_queue_from_domain(&start.task_queue)),
                input: Some(payloads_from_domain(&start.input)),
                workflow_execution_timeout: start
                    .workflow_execution_timeout
                    .map(to_proto_duration),
                workflow_run_timeout: start.workflow_run_timeout.map(to_proto_duration),
                workflow_task_timeout: start.workflow_task_timeout.map(to_proto_duration),
                workflow_id_reuse_policy: enums::WorkflowIdReusePolicy::AllowDuplicate
                    as i32,
                retry_policy: start.retry_policy.as_ref().map(retry_policy_from_domain),
                cron_schedule: String::new(),
                memo: Some(memo_from_domain(&start.memo)),
                search_attributes: Some(search_attributes_from_domain(
                    &start.search_attributes,
                )),
                header: None,
                user_metadata: None,
                versioning_override: None,
            },
        )),
    }
}

pub fn schedule_policies_to_domain(
    policies: proto_schedule::SchedulePolicies,
) -> domain::SchedulePolicies {
    domain::SchedulePolicies {
        overlap_policy: overlap_policy_to_domain(policies.overlap_policy),
        catchup_window: proto_duration_to_time(policies.catchup_window.as_ref())
            .unwrap_or(Duration::days(365)),
        pause_on_failure: policies.pause_on_failure,
        keep_original_workflow_id: policies.keep_original_workflow_id,
    }
}

pub fn schedule_policies_to_proto(
    policies: &domain::SchedulePolicies,
) -> proto_schedule::SchedulePolicies {
    proto_schedule::SchedulePolicies {
        overlap_policy: overlap_policy_to_proto(policies.overlap_policy),
        catchup_window: Some(to_proto_duration(policies.catchup_window)),
        pause_on_failure: policies.pause_on_failure,
        keep_original_workflow_id: policies.keep_original_workflow_id,
    }
}

pub fn schedule_state_to_domain(
    state: proto_schedule::ScheduleState,
) -> domain::ScheduleState {
    domain::ScheduleState {
        notes: state.notes,
        paused: state.paused,
        limited_actions: state.limited_actions,
        remaining_actions: state.remaining_actions,
    }
}

pub fn schedule_state_to_proto(
    state: &domain::ScheduleState,
) -> proto_schedule::ScheduleState {
    proto_schedule::ScheduleState {
        notes: state.notes.clone(),
        paused: state.paused,
        limited_actions: state.limited_actions,
        remaining_actions: state.remaining_actions,
    }
}

pub fn schedule_info_to_proto(
    info: &domain::ScheduleInfo,
) -> proto_schedule::ScheduleInfo {
    #[allow(deprecated)]
    let info = proto_schedule::ScheduleInfo {
        action_count: info.action_count,
        missed_catchup_window: info.missed_catchup_window,
        overlap_skipped: info.overlap_skipped,
        buffer_dropped: info.buffer_dropped,
        buffer_size: info.buffered_actions.len() as i64,
        running_workflows: info
            .running_workflows
            .iter()
            .map(workflow_execution_to_proto)
            .collect(),
        recent_actions: info
            .recent_actions
            .iter()
            .map(schedule_action_result_to_proto)
            .collect(),
        future_action_times: info
            .future_action_times
            .iter()
            .copied()
            .map(to_proto_timestamp)
            .collect(),
        create_time: Some(to_proto_timestamp(info.create_time)),
        update_time: Some(to_proto_timestamp(info.update_time)),
        invalid_schedule_error: String::new(),
    };
    info
}

fn schedule_to_proto(entry: &domain::ScheduleEntry) -> proto_schedule::Schedule {
    proto_schedule::Schedule {
        spec: Some(schedule_spec_to_proto(&entry.spec)),
        action: Some(schedule_action_to_proto(&entry.action)),
        policies: Some(schedule_policies_to_proto(&entry.policies)),
        state: Some(schedule_state_to_proto(&entry.state)),
    }
}

fn schedule_action_result_to_proto(
    result: &domain::ScheduleActionResult,
) -> proto_schedule::ScheduleActionResult {
    proto_schedule::ScheduleActionResult {
        schedule_time: Some(to_proto_timestamp(result.schedule_time)),
        actual_time: Some(to_proto_timestamp(result.actual_time)),
        start_workflow_result: result
            .start_workflow_result
            .as_ref()
            .map(workflow_execution_to_proto),
        start_workflow_status: workflow_status_to_proto(result.start_workflow_status),
    }
}

fn workflow_execution_to_proto(
    execution: &domain::WorkflowExecution,
) -> common::WorkflowExecution {
    common::WorkflowExecution {
        workflow_id: execution.workflow_id.0.clone(),
        run_id: execution.run_id.0.to_string(),
    }
}

fn workflow_status_to_proto(status: domain::WorkflowExecutionStatus) -> i32 {
    (match status {
        domain::WorkflowExecutionStatus::Running => {
            enums::WorkflowExecutionStatus::Running
        }
        domain::WorkflowExecutionStatus::Completed => {
            enums::WorkflowExecutionStatus::Completed
        }
        domain::WorkflowExecutionStatus::Failed => enums::WorkflowExecutionStatus::Failed,
        domain::WorkflowExecutionStatus::Cancelled => {
            enums::WorkflowExecutionStatus::Canceled
        }
        domain::WorkflowExecutionStatus::Terminated => {
            enums::WorkflowExecutionStatus::Terminated
        }
        domain::WorkflowExecutionStatus::ContinuedAsNew => {
            enums::WorkflowExecutionStatus::ContinuedAsNew
        }
        domain::WorkflowExecutionStatus::TimedOut => {
            enums::WorkflowExecutionStatus::TimedOut
        }
        domain::WorkflowExecutionStatus::StartFailed => {
            enums::WorkflowExecutionStatus::Failed
        }
    }) as i32
}

fn overlap_policy_to_domain(value: i32) -> domain::OverlapPolicy {
    match enums::ScheduleOverlapPolicy::try_from(value).ok() {
        Some(enums::ScheduleOverlapPolicy::BufferOne) => domain::OverlapPolicy::BufferOne,
        Some(enums::ScheduleOverlapPolicy::BufferAll) => domain::OverlapPolicy::BufferAll,
        Some(enums::ScheduleOverlapPolicy::CancelOther) => {
            domain::OverlapPolicy::CancelOther
        }
        Some(enums::ScheduleOverlapPolicy::TerminateOther) => {
            domain::OverlapPolicy::TerminateOther
        }
        Some(enums::ScheduleOverlapPolicy::AllowAll) => domain::OverlapPolicy::AllowAll,
        _ => domain::OverlapPolicy::Skip,
    }
}

fn overlap_policy_to_proto(value: domain::OverlapPolicy) -> i32 {
    (match value {
        domain::OverlapPolicy::Skip => enums::ScheduleOverlapPolicy::Skip,
        domain::OverlapPolicy::BufferOne => enums::ScheduleOverlapPolicy::BufferOne,
        domain::OverlapPolicy::BufferAll => enums::ScheduleOverlapPolicy::BufferAll,
        domain::OverlapPolicy::CancelOther => enums::ScheduleOverlapPolicy::CancelOther,
        domain::OverlapPolicy::TerminateOther => {
            enums::ScheduleOverlapPolicy::TerminateOther
        }
        domain::OverlapPolicy::AllowAll => enums::ScheduleOverlapPolicy::AllowAll,
    }) as i32
}

fn structured_calendar_to_domain(
    spec: proto_schedule::StructuredCalendarSpec,
) -> domain::StructuredCalendarSpec {
    domain::StructuredCalendarSpec {
        second: spec.second.into_iter().map(range_to_domain).collect(),
        minute: spec.minute.into_iter().map(range_to_domain).collect(),
        hour: spec.hour.into_iter().map(range_to_domain).collect(),
        day_of_month: spec.day_of_month.into_iter().map(range_to_domain).collect(),
        month: spec.month.into_iter().map(range_to_domain).collect(),
        year: spec.year.into_iter().map(range_to_domain).collect(),
        day_of_week: spec.day_of_week.into_iter().map(range_to_domain).collect(),
        comment: spec.comment,
    }
}

fn structured_calendar_to_proto(
    spec: &domain::StructuredCalendarSpec,
) -> proto_schedule::StructuredCalendarSpec {
    proto_schedule::StructuredCalendarSpec {
        second: spec.second.iter().copied().map(range_to_proto).collect(),
        minute: spec.minute.iter().copied().map(range_to_proto).collect(),
        hour: spec.hour.iter().copied().map(range_to_proto).collect(),
        day_of_month: spec
            .day_of_month
            .iter()
            .copied()
            .map(range_to_proto)
            .collect(),
        month: spec.month.iter().copied().map(range_to_proto).collect(),
        year: spec.year.iter().copied().map(range_to_proto).collect(),
        day_of_week: spec
            .day_of_week
            .iter()
            .copied()
            .map(range_to_proto)
            .collect(),
        comment: spec.comment.clone(),
    }
}

fn range_to_domain(range: proto_schedule::Range) -> domain::Range {
    domain::Range {
        start: range.start,
        end: if range.end < range.start {
            range.start
        } else {
            range.end
        },
        step: if range.step <= 0 { 1 } else { range.step },
    }
}

fn range_to_proto(range: domain::Range) -> proto_schedule::Range {
    proto_schedule::Range {
        start: range.start,
        end: range.end,
        step: range.step,
    }
}

fn interval_to_domain(
    interval: proto_schedule::IntervalSpec,
) -> Result<domain::IntervalSpec, Status> {
    let interval_duration = proto_duration_to_time(interval.interval.as_ref())
        .ok_or_else(|| Status::invalid_argument("interval.interval is required"))?;
    if interval_duration <= Duration::ZERO {
        return Err(Status::invalid_argument("interval must be positive"));
    }
    Ok(domain::IntervalSpec {
        interval: interval_duration,
        phase: proto_duration_to_time(interval.phase.as_ref()).unwrap_or(Duration::ZERO),
    })
}

fn interval_to_proto(interval: &domain::IntervalSpec) -> proto_schedule::IntervalSpec {
    proto_schedule::IntervalSpec {
        interval: Some(to_proto_duration(interval.interval)),
        phase: Some(to_proto_duration(interval.phase)),
    }
}

pub fn compile_calendar_spec(
    calendar: proto_schedule::CalendarSpec,
) -> Result<domain::StructuredCalendarSpec, Status> {
    Ok(domain::StructuredCalendarSpec {
        second: parse_calendar_field(&calendar.second, 0, 0, true)?,
        minute: parse_calendar_field(&calendar.minute, 0, 0, true)?,
        hour: parse_calendar_field(&calendar.hour, 0, 0, true)?,
        day_of_month: parse_calendar_field(&calendar.day_of_month, 1, 31, false)?,
        month: parse_calendar_field(&calendar.month, 1, 12, false)?,
        year: parse_calendar_field(&calendar.year, 1970, 9999, false)?,
        day_of_week: parse_calendar_field(&calendar.day_of_week, 0, 6, false)?,
        comment: calendar.comment,
    })
}

fn compile_cron_string(cron: &str) -> Result<domain::StructuredCalendarSpec, Status> {
    let fields: Vec<_> = cron
        .split('#')
        .next()
        .unwrap_or("")
        .split_whitespace()
        .collect();
    let fields = match fields.as_slice() {
        ["@hourly"] => vec!["0", "0", "*", "*", "*", "*", "*"],
        ["@daily"] => vec!["0", "0", "0", "*", "*", "*", "*"],
        ["@weekly"] => vec!["0", "0", "0", "*", "*", "0", "*"],
        ["@monthly"] => vec!["0", "0", "0", "1", "*", "*", "*"],
        ["@yearly"] | ["@annually"] => vec!["0", "0", "0", "1", "1", "*", "*"],
        [minute, hour, dom, month, dow] => {
            vec!["0", *minute, *hour, *dom, *month, *dow, "*"]
        }
        [minute, hour, dom, month, dow, year] => {
            vec!["0", *minute, *hour, *dom, *month, *dow, *year]
        }
        [second, minute, hour, dom, month, dow, year] => {
            vec![*second, *minute, *hour, *dom, *month, *dow, *year]
        }
        _ => return Err(Status::invalid_argument("unsupported cron string")),
    };
    Ok(domain::StructuredCalendarSpec {
        second: parse_calendar_field(fields[0], 0, 59, false)?,
        minute: parse_calendar_field(fields[1], 0, 59, false)?,
        hour: parse_calendar_field(fields[2], 0, 23, false)?,
        day_of_month: parse_calendar_field(fields[3], 1, 31, false)?,
        month: parse_calendar_field(fields[4], 1, 12, false)?,
        day_of_week: parse_calendar_field(fields[5], 0, 6, false)?,
        year: parse_calendar_field(fields[6], 1970, 9999, false)?,
        comment: cron.to_string(),
    })
}

fn parse_calendar_field(
    value: &str,
    default_start: i32,
    default_end: i32,
    zero_default: bool,
) -> Result<Vec<domain::Range>, Status> {
    if value.is_empty() {
        return Ok(if zero_default {
            vec![domain::Range {
                start: default_start,
                end: default_start,
                step: 1,
            }]
        } else {
            vec![domain::Range {
                start: default_start,
                end: default_end,
                step: 1,
            }]
        });
    }
    if value == "*" {
        return Ok(vec![domain::Range {
            start: default_start,
            end: default_end,
            step: 1,
        }]);
    }
    value
        .split(',')
        .map(|part| {
            let (base, step) = match part.split_once('/') {
                Some((base, step)) => (base, step.parse::<i32>().unwrap_or(1).max(1)),
                None => (part, 1),
            };
            let (start, end) = match base.split_once('-') {
                Some((start, end)) => (parse_named_int(start)?, parse_named_int(end)?),
                None => {
                    let value = parse_named_int(base)?;
                    (value, value)
                }
            };
            Ok(domain::Range { start, end, step })
        })
        .collect()
}

fn parse_named_int(value: &str) -> Result<i32, Status> {
    match value.to_ascii_lowercase().as_str() {
        "sun" | "sunday" => Ok(0),
        "mon" | "monday" => Ok(1),
        "tue" | "tuesday" => Ok(2),
        "wed" | "wednesday" => Ok(3),
        "thu" | "thursday" => Ok(4),
        "fri" | "friday" => Ok(5),
        "sat" | "saturday" => Ok(6),
        "jan" | "january" => Ok(1),
        "feb" | "february" => Ok(2),
        "mar" | "march" => Ok(3),
        "apr" | "april" => Ok(4),
        "may" => Ok(5),
        "jun" | "june" => Ok(6),
        "jul" | "july" => Ok(7),
        "aug" | "august" => Ok(8),
        "sep" | "september" => Ok(9),
        "oct" | "october" => Ok(10),
        "nov" | "november" => Ok(11),
        "dec" | "december" => Ok(12),
        other => other
            .parse()
            .map_err(|_| Status::invalid_argument("invalid calendar field")),
    }
}

fn proto_duration_to_time(value: Option<&ProtoDuration>) -> Option<Duration> {
    value.map(|duration| {
        Duration::seconds(duration.seconds)
            + Duration::nanoseconds(i64::from(duration.nanos))
    })
}

fn proto_timestamp_to_time(value: Option<&Timestamp>) -> Option<OffsetDateTime> {
    value.and_then(|timestamp| {
        OffsetDateTime::from_unix_timestamp(timestamp.seconds)
            .ok()
            .map(|time| time + Duration::nanoseconds(i64::from(timestamp.nanos)))
    })
}

fn retry_policy_to_domain(value: &common::RetryPolicy) -> RetryPolicy {
    RetryPolicy {
        initial_interval: proto_duration_to_time(value.initial_interval.as_ref())
            .unwrap_or(Duration::ZERO),
        backoff_coefficient: if value.backoff_coefficient > 0.0 {
            value.backoff_coefficient
        } else {
            1.0
        },
        maximum_interval: proto_duration_to_time(value.maximum_interval.as_ref()),
        maximum_attempts: value.maximum_attempts.max(0) as u32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
}

fn retry_policy_from_domain(value: &RetryPolicy) -> common::RetryPolicy {
    common::RetryPolicy {
        initial_interval: Some(to_proto_duration(value.initial_interval)),
        backoff_coefficient: value.backoff_coefficient,
        maximum_interval: value.maximum_interval.map(to_proto_duration),
        maximum_attempts: value.maximum_attempts as i32,
        non_retryable_error_types: value.non_retryable_error_types.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_proto::taskqueue;

    fn minimal_action() -> proto_schedule::ScheduleAction {
        proto_schedule::ScheduleAction {
            action: Some(schedule_action::Action::StartWorkflow(
                workflow::NewWorkflowExecutionInfo {
                    workflow_id: "wf".to_string(),
                    workflow_type: Some(common::WorkflowType {
                        name: "workflow".to_string(),
                    }),
                    task_queue: Some(taskqueue::TaskQueue {
                        name: "q".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )),
        }
    }

    fn minimal_schedule() -> proto_schedule::Schedule {
        proto_schedule::Schedule {
            spec: Some(proto_schedule::ScheduleSpec {
                interval: vec![proto_schedule::IntervalSpec {
                    interval: Some(ProtoDuration {
                        seconds: 60,
                        nanos: 0,
                    }),
                    phase: None,
                }],
                timezone_name: "UTC".to_string(),
                timezone_data: b"not retained by list views".to_vec(),
                ..Default::default()
            }),
            action: Some(minimal_action()),
            ..Default::default()
        }
    }

    #[test]
    fn empty_schedule_id_rejected() {
        let err =
            create_schedule_request_to_edge(workflowservice::CreateScheduleRequest {
                namespace: "default".to_string(),
                schedule_id: String::new(),
                schedule: Some(minimal_schedule()),
                ..Default::default()
            })
            .expect_err("empty schedule id should be invalid");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("schedule_id"));
    }

    #[test]
    fn missing_spec_rejected() {
        let err =
            create_schedule_request_to_edge(workflowservice::CreateScheduleRequest {
                namespace: "default".to_string(),
                schedule_id: "s1".to_string(),
                schedule: Some(proto_schedule::Schedule {
                    action: Some(minimal_action()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect_err("missing spec should be invalid");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("schedule.spec"));
    }

    #[test]
    fn invalid_proto_returns_error() {
        let err = schedule_spec_to_domain(proto_schedule::ScheduleSpec {
            interval: vec![proto_schedule::IntervalSpec {
                interval: Some(ProtoDuration {
                    seconds: -1,
                    nanos: 0,
                }),
                phase: None,
            }],
            ..Default::default()
        })
        .expect_err("negative interval should be invalid");

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("interval"));
    }

    #[test]
    fn list_empty_namespace() {
        let response = list_schedules_response_to_proto(Vec::new(), None);

        assert!(response.schedules.is_empty());
        assert!(response.next_page_token.is_empty());
    }

    #[test]
    fn list_info_drops_timezone_data() {
        let (_, _, entry, _) =
            create_schedule_request_to_edge(workflowservice::CreateScheduleRequest {
                namespace: "default".to_string(),
                schedule_id: "s1".to_string(),
                schedule: Some(minimal_schedule()),
                ..Default::default()
            })
            .expect("valid schedule");

        let response = list_schedules_response_to_proto(vec![entry], None);
        let list_spec = response.schedules[0]
            .info
            .as_ref()
            .and_then(|info| info.spec.as_ref())
            .expect("list spec");

        assert!(list_spec.timezone_data.is_empty());
        assert_eq!(list_spec.timezone_name, "UTC");
    }
}
