//! Runtime-owned schedule state and evaluation helpers.
//!
//! The Temporal schedule API is transport-heavy, but the durable semantics are
//! runtime concerns: optimistic schedule metadata, overlap bookkeeping, and the
//! conversion from a nominal scheduled time into a workflow start request. This
//! module deliberately stores codec-neutral domain values so the edge crate can
//! translate protobufs without owning schedule behavior.

use std::{
    collections::VecDeque,
    hash::{Hash, Hasher},
    sync::Arc,
};

use chrono::{DateTime, Offset as _, Utc};
use dashmap::DashMap;
use thiserror::Error;
use time::{Duration, Month, OffsetDateTime, UtcOffset, Weekday};
use tokeira_kernel::{
    CancelRequest, LoadedRun, StartRequest, TerminateRequest, WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy,
};
use tokeira_storage::RunRepository;
use tokeira_types::{
    BuildId, ExecutionRef, ExecutionStatus, Memo, NamespaceId, Payloads, RequestContext, RequestId,
    RunId, RunKey, SearchAttributes, TaskQueueName, WorkflowId, WorkflowType,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{StartWorkflowResult, TokeiraRuntime, VersioningRuleStore};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScheduleId(pub String);

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleEntry {
    pub schedule_id: ScheduleId,
    pub namespace_id: NamespaceId,
    pub spec: ScheduleSpec,
    pub action: ScheduleAction,
    pub policies: SchedulePolicies,
    pub state: ScheduleState,
    pub info: ScheduleInfo,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
    pub conflict_token: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScheduleSpec {
    pub structured_calendars: Vec<StructuredCalendarSpec>,
    pub intervals: Vec<IntervalSpec>,
    pub exclude_calendars: Vec<StructuredCalendarSpec>,
    pub start_time: Option<OffsetDateTime>,
    pub end_time: Option<OffsetDateTime>,
    pub jitter: Option<Duration>,
    pub timezone_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StructuredCalendarSpec {
    pub second: Vec<Range>,
    pub minute: Vec<Range>,
    pub hour: Vec<Range>,
    pub day_of_month: Vec<Range>,
    pub month: Vec<Range>,
    pub year: Vec<Range>,
    pub day_of_week: Vec<Range>,
    pub comment: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Range {
    pub start: i32,
    pub end: i32,
    pub step: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntervalSpec {
    pub interval: Duration,
    pub phase: Duration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleAction {
    pub start_workflow: StartWorkflowAction,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StartWorkflowAction {
    pub workflow_id: WorkflowId,
    pub workflow_type: WorkflowType,
    pub task_queue: TaskQueueName,
    pub input: Payloads,
    pub workflow_execution_timeout: Option<Duration>,
    pub workflow_run_timeout: Option<Duration>,
    pub workflow_task_timeout: Option<Duration>,
    pub retry_policy: Option<tokeira_types::RetryPolicy>,
    pub memo: Memo,
    pub search_attributes: SearchAttributes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchedulePolicies {
    pub overlap_policy: OverlapPolicy,
    pub catchup_window: Duration,
    pub pause_on_failure: bool,
    pub keep_original_workflow_id: bool,
}

impl Default for SchedulePolicies {
    fn default() -> Self {
        Self {
            overlap_policy: OverlapPolicy::Skip,
            catchup_window: Duration::days(365),
            pause_on_failure: false,
            keep_original_workflow_id: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverlapPolicy {
    Skip,
    BufferOne,
    BufferAll,
    CancelOther,
    TerminateOther,
    AllowAll,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScheduleState {
    pub notes: String,
    pub paused: bool,
    pub limited_actions: bool,
    pub remaining_actions: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduleInfo {
    pub action_count: i64,
    pub missed_catchup_window: i64,
    pub overlap_skipped: i64,
    pub buffer_dropped: i64,
    pub buffer_size: i64,
    pub buffered_actions: VecDeque<BufferedAction>,
    pub running_workflows: Vec<WorkflowExecution>,
    pub recent_actions: Vec<ScheduleActionResult>,
    pub future_action_times: Vec<OffsetDateTime>,
    pub create_time: OffsetDateTime,
    pub update_time: OffsetDateTime,
}

impl ScheduleInfo {
    pub fn new(now: OffsetDateTime) -> Self {
        Self {
            action_count: 0,
            missed_catchup_window: 0,
            overlap_skipped: 0,
            buffer_dropped: 0,
            buffer_size: 0,
            buffered_actions: VecDeque::new(),
            running_workflows: Vec::new(),
            recent_actions: Vec::new(),
            future_action_times: Vec::new(),
            create_time: now,
            update_time: now,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BufferedAction {
    pub nominal_time: OffsetDateTime,
    pub overlap_policy_override: Option<OverlapPolicy>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SchedulePatch {
    pub trigger_immediately: Option<TriggerImmediately>,
    pub backfill_request: Vec<BackfillRequest>,
    pub pause: Option<String>,
    pub unpause: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriggerImmediately {
    pub overlap_policy: OverlapPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackfillRequest {
    pub start_time: OffsetDateTime,
    pub end_time: OffsetDateTime,
    pub overlap_policy: OverlapPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowExecution {
    pub namespace_id: NamespaceId,
    pub workflow_id: WorkflowId,
    pub run_id: RunId,
    pub run_key: RunKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleActionResult {
    pub schedule_time: OffsetDateTime,
    pub actual_time: OffsetDateTime,
    pub start_workflow_result: Option<WorkflowExecution>,
    pub start_workflow_status: WorkflowExecutionStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkflowExecutionStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    Terminated,
    ContinuedAsNew,
    TimedOut,
    StartFailed,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("schedule already exists")]
    AlreadyExists,
    #[error("schedule not found")]
    NotFound,
    #[error("stale conflict token")]
    StaleConflictToken,
    #[error("invalid schedule argument: {0}")]
    InvalidArgument(String),
}

#[derive(Default)]
pub struct ScheduleStore {
    schedules: DashMap<(NamespaceId, ScheduleId), ScheduleEntry>,
}

impl ScheduleStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, mut entry: ScheduleEntry) -> Result<Vec<u8>, ScheduleError> {
        let key = (entry.namespace_id, entry.schedule_id.clone());
        entry.conflict_token = encode_token(1);
        match self.schedules.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(_) => Err(ScheduleError::AlreadyExists),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                let token = entry.conflict_token.clone();
                slot.insert(entry);
                Ok(token)
            }
        }
    }

    pub fn describe(
        &self,
        namespace_id: NamespaceId,
        schedule_id: &ScheduleId,
    ) -> Result<ScheduleEntry, ScheduleError> {
        self.schedules
            .get(&(namespace_id, schedule_id.clone()))
            .map(|entry| entry.clone())
            .ok_or(ScheduleError::NotFound)
    }

    pub fn update<F>(
        &self,
        namespace_id: NamespaceId,
        schedule_id: &ScheduleId,
        conflict_token: &[u8],
        updater: F,
    ) -> Result<ScheduleEntry, ScheduleError>
    where
        F: FnOnce(&mut ScheduleEntry),
    {
        let mut entry = self
            .schedules
            .get_mut(&(namespace_id, schedule_id.clone()))
            .ok_or(ScheduleError::NotFound)?;
        if !conflict_token.is_empty() && entry.conflict_token != conflict_token {
            return Err(ScheduleError::StaleConflictToken);
        }
        updater(&mut entry);
        entry.info.buffer_size = entry.info.buffered_actions.len() as i64;
        entry.conflict_token = increment_token(&entry.conflict_token);
        Ok(entry.clone())
    }

    pub fn delete(
        &self,
        namespace_id: NamespaceId,
        schedule_id: &ScheduleId,
    ) -> Result<(), ScheduleError> {
        self.schedules
            .remove(&(namespace_id, schedule_id.clone()))
            .map(|_| ())
            .ok_or(ScheduleError::NotFound)
    }

    pub fn list(
        &self,
        namespace_id: NamespaceId,
        page_size: usize,
        page_token: &[u8],
    ) -> (Vec<ScheduleEntry>, Option<Vec<u8>>) {
        let mut entries: Vec<_> = self
            .schedules
            .iter()
            .filter(|entry| entry.key().0 == namespace_id)
            .map(|entry| entry.value().clone())
            .collect();
        entries.sort_by(|a, b| a.schedule_id.cmp(&b.schedule_id));
        let start = (decode_page_token(page_token).unwrap_or(0) as usize).min(entries.len());
        let limit = page_size.max(1);
        let end = (start + limit).min(entries.len());
        let next = (end < entries.len()).then(|| encode_token(end as u64));
        (entries[start..end].to_vec(), next)
    }

    pub fn all_active_schedules(&self) -> Vec<ScheduleEntry> {
        self.schedules
            .iter()
            .filter(|entry| !entry.state.paused)
            .map(|entry| entry.value().clone())
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OverlapDecision {
    Allow,
    Skip,
    Buffer,
    CancelOther(Vec<WorkflowExecution>),
    TerminateOther(Vec<WorkflowExecution>),
}

pub fn decide_overlap(
    policy: OverlapPolicy,
    running_workflows: &[WorkflowExecution],
    current_buffer_size: usize,
) -> OverlapDecision {
    if running_workflows.is_empty() {
        return OverlapDecision::Allow;
    }
    match policy {
        OverlapPolicy::Skip => OverlapDecision::Skip,
        OverlapPolicy::BufferOne if current_buffer_size < 1 => OverlapDecision::Buffer,
        OverlapPolicy::BufferOne => OverlapDecision::Skip,
        OverlapPolicy::BufferAll => OverlapDecision::Buffer,
        OverlapPolicy::CancelOther => OverlapDecision::CancelOther(running_workflows.to_vec()),
        OverlapPolicy::TerminateOther => {
            OverlapDecision::TerminateOther(running_workflows.to_vec())
        }
        OverlapPolicy::AllowAll => OverlapDecision::Allow,
    }
}

pub fn schedule_workflow_id(
    base_workflow_id: &WorkflowId,
    nominal_time: OffsetDateTime,
    keep_original: bool,
) -> WorkflowId {
    if keep_original {
        return base_workflow_id.clone();
    }
    WorkflowId(format!(
        "{}-{}",
        base_workflow_id.0,
        nominal_time.unix_timestamp()
    ))
}

pub fn compute_next_times(
    spec: &ScheduleSpec,
    now: OffsetDateTime,
    count: usize,
    schedule_id: &ScheduleId,
) -> Vec<OffsetDateTime> {
    let mut out = Vec::new();
    let mut cursor = now;
    while out.len() < count && cursor < now + Duration::days(366) {
        let end = cursor + Duration::hours(24);
        out.extend(compute_matching_times(spec, cursor, end, schedule_id));
        out.sort();
        out.dedup();
        out.retain(|time| *time >= now);
        cursor = end + Duration::seconds(1);
    }
    out.truncate(count);
    out
}

pub fn compute_matching_times(
    spec: &ScheduleSpec,
    range_start: OffsetDateTime,
    range_end: OffsetDateTime,
    schedule_id: &ScheduleId,
) -> Vec<OffsetDateTime> {
    if range_start > range_end {
        return Vec::new();
    }
    let start = spec
        .start_time
        .map_or(range_start, |value| value.max(range_start));
    let end = spec
        .end_time
        .map_or(range_end, |value| value.min(range_end));
    if start > end {
        return Vec::new();
    }

    let mut times = interval_matches(spec, start, end);
    if !spec.structured_calendars.is_empty() {
        let mut cursor = truncate_to_second(start);
        while cursor <= end {
            let local = to_schedule_local_time(cursor, &spec.timezone_name);
            if spec
                .structured_calendars
                .iter()
                .any(|calendar| calendar_matches(calendar, local))
            {
                times.push(cursor);
            }
            cursor += Duration::seconds(1);
        }
    }

    times.sort();
    times.dedup();
    times.retain(|time| {
        let local = to_schedule_local_time(*time, &spec.timezone_name);
        !spec
            .exclude_calendars
            .iter()
            .any(|calendar| calendar_matches(calendar, local))
    });
    times
        .into_iter()
        .map(|time| apply_jitter(time, spec.jitter, schedule_id))
        .filter(|time| *time >= range_start && *time <= range_end)
        .collect()
}

fn interval_matches(
    spec: &ScheduleSpec,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Vec<OffsetDateTime> {
    let mut out = Vec::new();
    for interval in &spec.intervals {
        if interval.interval <= Duration::ZERO {
            continue;
        }
        let interval_ns = interval.interval.whole_nanoseconds();
        let phase_ns = interval.phase.whole_nanoseconds();
        let start_ns = i128::from(start.unix_timestamp_nanos()) - phase_ns;
        let mut n = div_floor(start_ns, interval_ns);
        loop {
            let candidate_ns = n * interval_ns + phase_ns;
            let Ok(candidate) = OffsetDateTime::from_unix_timestamp_nanos(candidate_ns) else {
                break;
            };
            if candidate < start {
                n += 1;
                continue;
            }
            if candidate > end {
                break;
            }
            out.push(candidate);
            n += 1;
        }
    }
    out
}

fn calendar_matches(spec: &StructuredCalendarSpec, time: OffsetDateTime) -> bool {
    ranges_match(&spec.second, time.second() as i32)
        && ranges_match(&spec.minute, time.minute() as i32)
        && ranges_match(&spec.hour, time.hour() as i32)
        && ranges_match(&spec.day_of_month, time.day() as i32)
        && ranges_match(&spec.month, month_number(time.month()))
        && ranges_match(&spec.year, time.year())
        && ranges_match(&spec.day_of_week, weekday_number(time.weekday()))
}

fn ranges_match(ranges: &[Range], value: i32) -> bool {
    ranges.is_empty()
        || ranges.iter().any(|range| {
            let step = range.step.max(1);
            value >= range.start && value <= range.end && (value - range.start) % step == 0
        })
}

fn to_schedule_local_time(time: OffsetDateTime, timezone_name: &str) -> OffsetDateTime {
    if timezone_name.is_empty() || timezone_name == "UTC" || timezone_name == "Etc/UTC" {
        return time;
    }
    let Ok(timezone) = timezone_name.parse::<chrono_tz::Tz>() else {
        return time;
    };
    let Some(utc) = DateTime::<Utc>::from_timestamp(time.unix_timestamp(), time.nanosecond())
    else {
        return time;
    };
    let local = utc.with_timezone(&timezone);
    let offset_seconds = local.offset().fix().local_minus_utc();
    let Ok(offset) = UtcOffset::from_whole_seconds(offset_seconds) else {
        return time;
    };
    time.to_offset(offset)
}

fn apply_jitter(
    time: OffsetDateTime,
    jitter: Option<Duration>,
    schedule_id: &ScheduleId,
) -> OffsetDateTime {
    let Some(jitter) = jitter else {
        return time;
    };
    if jitter <= Duration::ZERO {
        return time;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    schedule_id.hash(&mut hasher);
    time.unix_timestamp_nanos().hash(&mut hasher);
    let span = jitter.whole_nanoseconds().max(0) as u64;
    let offset = (hasher.finish() % (span + 1)) as i64;
    time + Duration::nanoseconds(offset)
}

fn truncate_to_second(time: OffsetDateTime) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(time.unix_timestamp()).unwrap_or(time)
}

fn month_number(month: Month) -> i32 {
    month as i32
}

fn weekday_number(weekday: Weekday) -> i32 {
    match weekday {
        Weekday::Sunday => 0,
        Weekday::Monday => 1,
        Weekday::Tuesday => 2,
        Weekday::Wednesday => 3,
        Weekday::Thursday => 4,
        Weekday::Friday => 5,
        Weekday::Saturday => 6,
    }
}

fn div_floor(lhs: i128, rhs: i128) -> i128 {
    let quotient = lhs / rhs;
    let remainder = lhs % rhs;
    if remainder != 0 && ((remainder > 0) != (rhs > 0)) {
        quotient - 1
    } else {
        quotient
    }
}

pub fn run_schedule_engine<R>(
    store: Arc<ScheduleStore>,
    runtime: Arc<TokeiraRuntime<R>>,
    versioning_rules: Arc<VersioningRuleStore>,
    config: ScheduleEngineConfig,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<()>
where
    R: RunRepository + 'static,
{
    tokio::spawn(async move {
        let mut last_tick = OffsetDateTime::now_utc();
        let mut ticker = tokio::time::interval(config.tick_interval);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let now = OffsetDateTime::now_utc();
                    evaluate_all_schedules(&store, runtime.as_ref(), versioning_rules.as_ref(), last_tick, now).await;
                    last_tick = now;
                }
            }
        }
    })
}

#[derive(Clone, Debug)]
pub struct ScheduleEngineConfig {
    pub tick_interval: std::time::Duration,
}

impl Default for ScheduleEngineConfig {
    fn default() -> Self {
        Self {
            tick_interval: std::time::Duration::from_secs(1),
        }
    }
}

pub async fn evaluate_all_schedules<R>(
    store: &ScheduleStore,
    runtime: &TokeiraRuntime<R>,
    versioning_rules: &VersioningRuleStore,
    last_tick: OffsetDateTime,
    now: OffsetDateTime,
) where
    R: RunRepository + 'static,
{
    reconcile_running_workflows(store, runtime).await;
    for entry in store.all_active_schedules() {
        let schedule_id = entry.schedule_id.clone();
        let times = compute_matching_times(&entry.spec, last_tick, now, &schedule_id);
        for nominal_time in times {
            handle_due_action(
                store,
                runtime,
                versioning_rules,
                entry.namespace_id,
                &schedule_id,
                nominal_time,
                None,
                now,
            )
            .await;
        }
    }
}

pub async fn handle_due_action<R>(
    store: &ScheduleStore,
    runtime: &TokeiraRuntime<R>,
    versioning_rules: &VersioningRuleStore,
    namespace_id: NamespaceId,
    schedule_id: &ScheduleId,
    nominal_time: OffsetDateTime,
    overlap_override: Option<OverlapPolicy>,
    now: OffsetDateTime,
) where
    R: RunRepository + 'static,
{
    let Ok(entry) = store.describe(namespace_id, schedule_id) else {
        return;
    };
    if entry.state.limited_actions && entry.state.remaining_actions <= 0 {
        return;
    }
    if now - nominal_time > entry.policies.catchup_window {
        let _ = store.update(namespace_id, schedule_id, &[], |current| {
            current.info.missed_catchup_window += 1;
        });
        return;
    }
    let policy = overlap_override.unwrap_or(entry.policies.overlap_policy);
    match decide_overlap(
        policy,
        &entry.info.running_workflows,
        entry.info.buffered_actions.len(),
    ) {
        OverlapDecision::Allow => {
            let _ = trigger_schedule_action(
                store,
                runtime,
                versioning_rules,
                namespace_id,
                schedule_id,
                nominal_time,
                now,
            )
            .await;
        }
        OverlapDecision::Skip => {
            let _ = store.update(namespace_id, schedule_id, &[], |current| {
                current.info.overlap_skipped += 1;
            });
        }
        OverlapDecision::Buffer => {
            let _ = store.update(namespace_id, schedule_id, &[], |current| {
                if policy == OverlapPolicy::BufferOne && current.info.buffered_actions.len() >= 1 {
                    current.info.buffered_actions.pop_front();
                    current.info.buffer_dropped += 1;
                }
                current.info.buffered_actions.push_back(BufferedAction {
                    nominal_time,
                    overlap_policy_override: overlap_override,
                });
            });
        }
        OverlapDecision::CancelOther(workflows) => {
            cancel_workflows(runtime, &workflows, schedule_request_context(now)).await;
            let _ = store.update(namespace_id, schedule_id, &[], |current| {
                current.info.running_workflows.clear();
            });
        }
        OverlapDecision::TerminateOther(workflows) => {
            terminate_workflows(runtime, &workflows, schedule_request_context(now)).await;
            let _ = store.update(namespace_id, schedule_id, &[], |current| {
                current.info.running_workflows.clear();
            });
            let _ = trigger_schedule_action(
                store,
                runtime,
                versioning_rules,
                namespace_id,
                schedule_id,
                nominal_time,
                now,
            )
            .await;
        }
    }
}

pub async fn reconcile_running_workflows<R>(store: &ScheduleStore, runtime: &TokeiraRuntime<R>)
where
    R: RunRepository + 'static,
{
    for entry in store.all_active_schedules() {
        if entry.info.running_workflows.is_empty() {
            continue;
        }
        let mut completed = false;
        let mut still_running = Vec::new();
        for workflow in &entry.info.running_workflows {
            let keep = match runtime.repo().load_run(workflow.run_key).await {
                Ok(LoadedRun::Existing(state)) => state.status == ExecutionStatus::Running,
                _ => false,
            };
            completed |= !keep;
            if keep {
                still_running.push(workflow.clone());
            }
        }
        let mut buffered_to_run = Vec::new();
        let _ = store.update(entry.namespace_id, &entry.schedule_id, &[], |current| {
            current.info.running_workflows = still_running;
            if completed && current.policies.pause_on_failure {
                current.state.paused = true;
                current.state.notes = "paused after scheduled workflow completed".to_string();
            }
            if completed && current.info.running_workflows.is_empty() && !current.state.paused {
                if let Some(buffered) = current.info.buffered_actions.pop_front() {
                    buffered_to_run.push(buffered);
                }
            }
        });
        for buffered in buffered_to_run {
            handle_due_action(
                store,
                runtime,
                &runtime.versioning_rule_store(),
                entry.namespace_id,
                &entry.schedule_id,
                buffered.nominal_time,
                buffered.overlap_policy_override,
                OffsetDateTime::now_utc(),
            )
            .await;
        }
    }
}

pub async fn trigger_schedule_action<R>(
    store: &ScheduleStore,
    runtime: &TokeiraRuntime<R>,
    versioning_rules: &VersioningRuleStore,
    namespace_id: NamespaceId,
    schedule_id: &ScheduleId,
    nominal_time: OffsetDateTime,
    actual_time: OffsetDateTime,
) -> Result<(), ScheduleError>
where
    R: RunRepository + 'static,
{
    let entry = store.describe(namespace_id, schedule_id)?;
    let workflow_id = schedule_workflow_id(
        &entry.action.start_workflow.workflow_id,
        nominal_time,
        entry.policies.keep_original_workflow_id,
    );
    let run_id = RunId::new();
    let run_key = RunKey::derive(namespace_id, &workflow_id, run_id);
    let build_id: Option<BuildId> = versioning_rules.evaluate_assignment(
        namespace_id,
        &entry.action.start_workflow.task_queue,
        &workflow_id,
    );
    let request = StartRequest {
        run_key,
        namespace_id,
        workflow_id: workflow_id.clone(),
        run_id,
        workflow_type: entry.action.start_workflow.workflow_type.clone(),
        task_queue: entry.action.start_workflow.task_queue.clone(),
        input: entry.action.start_workflow.input.clone(),
        memo: entry.action.start_workflow.memo.clone(),
        search_attributes: entry.action.start_workflow.search_attributes.clone(),
        workflow_execution_timeout: entry.action.start_workflow.workflow_execution_timeout,
        workflow_run_timeout: entry.action.start_workflow.workflow_run_timeout,
        workflow_task_timeout: entry
            .action
            .start_workflow
            .workflow_task_timeout
            .unwrap_or(Duration::seconds(10)),
        retry_policy: entry.action.start_workflow.retry_policy.clone(),
        conflict_policy: WorkflowIdConflictPolicy::Fail,
        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_initiated_event_id: 0,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: RequestContext {
            request_id: RequestId(Uuid::new_v4().to_string()),
            caller_identity: Some("schedule-engine".to_string()),
            received_at: actual_time,
        },
        now: actual_time,
        cron_schedule: Some(schedule_id.0.clone()),
    };

    let outcome = runtime.start_workflow_with_policy(request).await;
    let result = match outcome {
        Ok(StartWorkflowResult::Started { run_key, run_id }) => {
            let workflow = WorkflowExecution {
                namespace_id,
                workflow_id,
                run_id,
                run_key,
            };
            ScheduleActionResult {
                schedule_time: nominal_time,
                actual_time,
                start_workflow_result: Some(workflow),
                start_workflow_status: WorkflowExecutionStatus::Running,
            }
        }
        Ok(StartWorkflowResult::UsedExisting { run_key, run_id })
        | Ok(StartWorkflowResult::Rejected { run_key, run_id }) => ScheduleActionResult {
            schedule_time: nominal_time,
            actual_time,
            start_workflow_result: Some(WorkflowExecution {
                namespace_id,
                workflow_id,
                run_id,
                run_key,
            }),
            start_workflow_status: WorkflowExecutionStatus::StartFailed,
        },
        Err(_) => ScheduleActionResult {
            schedule_time: nominal_time,
            actual_time,
            start_workflow_result: None,
            start_workflow_status: WorkflowExecutionStatus::StartFailed,
        },
    };
    let _ = store.update(namespace_id, schedule_id, &[], |current| {
        if let Some(workflow) = result.start_workflow_result.clone()
            && result.start_workflow_status == WorkflowExecutionStatus::Running
        {
            current.info.running_workflows.push(workflow);
            if current.state.limited_actions {
                current.state.remaining_actions -= 1;
            }
        }
        current.info.action_count += 1;
        current.info.recent_actions.push(result);
        if current.info.recent_actions.len() > 10 {
            current.info.recent_actions.remove(0);
        }
        current.info.update_time = actual_time;
    });
    Ok(())
}

pub async fn cancel_workflows<R>(
    runtime: &TokeiraRuntime<R>,
    workflows: &[WorkflowExecution],
    request: RequestContext,
) where
    R: RunRepository + 'static,
{
    for workflow in workflows {
        let _ = runtime
            .cancel_workflow(
                ExecutionRef {
                    namespace_id: workflow.namespace_id,
                    workflow_id: workflow.workflow_id.clone(),
                    run_id: Some(workflow.run_id),
                },
                CancelRequest {
                    reason: "schedule overlap policy".to_string(),
                    external_initiator: None,
                    request: request.clone(),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await;
    }
}

pub async fn terminate_workflows<R>(
    runtime: &TokeiraRuntime<R>,
    workflows: &[WorkflowExecution],
    request: RequestContext,
) where
    R: RunRepository + 'static,
{
    for workflow in workflows {
        let _ = runtime
            .terminate_workflow(
                ExecutionRef {
                    namespace_id: workflow.namespace_id,
                    workflow_id: workflow.workflow_id.clone(),
                    run_id: Some(workflow.run_id),
                },
                TerminateRequest {
                    reason: "schedule overlap policy".to_string(),
                    details: Some(Payloads::default()),
                    identity: request.caller_identity.clone().unwrap_or_default(),
                    request: request.clone(),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await;
    }
}

fn schedule_request_context(now: OffsetDateTime) -> RequestContext {
    RequestContext {
        request_id: RequestId(Uuid::new_v4().to_string()),
        caller_identity: Some("schedule-engine".to_string()),
        received_at: now,
    }
}

fn encode_token(value: u64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn increment_token(token: &[u8]) -> Vec<u8> {
    encode_token(decode_page_token(token).unwrap_or(0).saturating_add(1))
}

fn decode_page_token(token: &[u8]) -> Option<u64> {
    if token.is_empty() {
        return Some(0);
    }
    let bytes: [u8; 8] = token.try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tokeira_storage::{InMemoryStore, RunRepository};

    fn make_runtime(store: Arc<InMemoryStore>) -> TokeiraRuntime<InMemoryStore> {
        TokeiraRuntime::new(
            store,
            1,
            crate::LaneConfig::default(),
            crate::TimerScannerConfig::default(),
            crate::WorkflowTimeoutScannerConfig::default(),
            crate::BacklogConfig::default(),
        )
    }

    async fn shutdown(runtime: &mut TokeiraRuntime<InMemoryStore>) {
        let _ = runtime.shutdown_timer_scanner().await;
        let _ = runtime.shutdown_workflow_timeout_scanner().await;
        let _ = runtime.shutdown_wft_timeout_scanner().await;
        let _ = runtime.shutdown_activity_timeout_scanner().await;
        let _ = runtime.shutdown_nexus_timeout_scanner().await;
        let _ = runtime.shutdown_grace_scanner().await;
        let _ = runtime.shutdown_drain_loop().await;
        let _ = runtime.shutdown_control_loop().await;
    }

    fn sample_entry(id: &str) -> ScheduleEntry {
        let now = OffsetDateTime::UNIX_EPOCH;
        ScheduleEntry {
            schedule_id: ScheduleId(id.to_string()),
            namespace_id: NamespaceId::new(),
            spec: ScheduleSpec {
                intervals: vec![IntervalSpec {
                    interval: Duration::seconds(60),
                    phase: Duration::ZERO,
                }],
                ..Default::default()
            },
            action: ScheduleAction {
                start_workflow: StartWorkflowAction {
                    workflow_id: WorkflowId("wf".to_string()),
                    workflow_type: WorkflowType("type".to_string()),
                    task_queue: TaskQueueName("q".to_string()),
                    input: Payloads::default(),
                    workflow_execution_timeout: None,
                    workflow_run_timeout: None,
                    workflow_task_timeout: None,
                    retry_policy: None,
                    memo: Memo::default(),
                    search_attributes: SearchAttributes::default(),
                },
            },
            policies: SchedulePolicies::default(),
            state: ScheduleState::default(),
            info: ScheduleInfo::new(now),
            memo: Memo::default(),
            search_attributes: SearchAttributes::default(),
            conflict_token: Vec::new(),
        }
    }

    #[test]
    fn create_initializes_info() {
        let store = ScheduleStore::new();
        let entry = sample_entry("created");
        let create_time = entry.info.create_time;
        let token = store.create(entry.clone()).expect("create");

        let stored = store
            .describe(entry.namespace_id, &entry.schedule_id)
            .expect("describe");
        assert_eq!(token, encode_token(1));
        assert_eq!(stored.info.create_time, create_time);
        assert_eq!(stored.info.update_time, create_time);
        assert_eq!(stored.info.action_count, 0);
        assert_eq!(stored.info.missed_catchup_window, 0);
        assert_eq!(stored.info.overlap_skipped, 0);
        assert_eq!(stored.info.buffer_size, 0);
        assert!(stored.info.running_workflows.is_empty());
        assert!(stored.info.recent_actions.is_empty());
    }

    #[test]
    fn create_default_state() {
        let entry = sample_entry("defaults");

        assert!(!entry.state.paused);
        assert!(!entry.state.limited_actions);
        assert_eq!(entry.state.remaining_actions, 0);
        assert_eq!(entry.policies.overlap_policy, OverlapPolicy::Skip);
        assert_eq!(entry.policies.catchup_window, Duration::days(365));
        assert!(!entry.policies.pause_on_failure);
    }

    #[test]
    fn update_sets_update_time() {
        let store = ScheduleStore::new();
        let entry = sample_entry("updated-at");
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        let token = store.create(entry).expect("create");
        let updated_at = OffsetDateTime::UNIX_EPOCH + Duration::seconds(42);

        let updated = store
            .update(namespace_id, &schedule_id, &token, |entry| {
                entry.info.update_time = updated_at;
            })
            .expect("update");

        assert_eq!(updated.info.update_time, updated_at);
    }

    #[test]
    fn describe_includes_future_times() {
        let mut entry = sample_entry("future");
        entry.info.future_action_times = compute_next_times(
            &entry.spec,
            OffsetDateTime::UNIX_EPOCH,
            10,
            &entry.schedule_id,
        );

        assert_eq!(entry.info.future_action_times.len(), 10);
        assert!(
            entry
                .info
                .future_action_times
                .windows(2)
                .all(|pair| pair[0] <= pair[1])
        );
    }

    #[test]
    fn schedule_store_tokens_are_monotonic() {
        let store = ScheduleStore::new();
        let mut entry = sample_entry("s1");
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        let first = store.create(entry.clone()).expect("create");
        entry.state.paused = true;
        let second = store
            .update(namespace_id, &schedule_id, &first, |stored| {
                stored.state.paused = true;
            })
            .expect("update")
            .conflict_token;
        assert!(second > first);
        assert_eq!(
            store
                .update(namespace_id, &schedule_id, &first, |_| {})
                .unwrap_err(),
            ScheduleError::StaleConflictToken
        );
    }

    #[test]
    fn matching_times_empty_for_inverted_range() {
        let spec = ScheduleSpec {
            intervals: vec![IntervalSpec {
                interval: Duration::seconds(10),
                phase: Duration::ZERO,
            }],
            ..Default::default()
        };

        let times = compute_matching_times(
            &spec,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
            &ScheduleId("inverted".to_string()),
        );

        assert!(times.is_empty());
    }

    #[test]
    fn timezone_calendar_matching() {
        let spec = ScheduleSpec {
            structured_calendars: vec![StructuredCalendarSpec {
                second: vec![Range {
                    start: 0,
                    end: 0,
                    step: 1,
                }],
                minute: vec![Range {
                    start: 0,
                    end: 0,
                    step: 1,
                }],
                hour: vec![Range {
                    start: 9,
                    end: 9,
                    step: 1,
                }],
                day_of_month: vec![Range {
                    start: 1,
                    end: 31,
                    step: 1,
                }],
                month: vec![Range {
                    start: 1,
                    end: 1,
                    step: 1,
                }],
                year: vec![Range {
                    start: 2026,
                    end: 2026,
                    step: 1,
                }],
                day_of_week: Vec::new(),
                comment: "ny business hour".to_string(),
            }],
            timezone_name: "America/New_York".to_string(),
            ..Default::default()
        };
        let start = OffsetDateTime::from_unix_timestamp(1_767_275_940).unwrap();
        let end = OffsetDateTime::from_unix_timestamp(1_767_276_060).unwrap();

        let times = compute_matching_times(&spec, start, end, &ScheduleId("tz".to_string()));

        assert_eq!(
            times,
            vec![OffsetDateTime::from_unix_timestamp(1_767_276_000).unwrap()]
        );
    }

    #[test]
    fn interval_matching_is_sorted_and_bounded() {
        let spec = ScheduleSpec {
            intervals: vec![IntervalSpec {
                interval: Duration::seconds(10),
                phase: Duration::ZERO,
            }],
            ..Default::default()
        };
        let start = OffsetDateTime::UNIX_EPOCH + Duration::seconds(5);
        let end = OffsetDateTime::UNIX_EPOCH + Duration::seconds(31);
        let times = compute_matching_times(&spec, start, end, &ScheduleId("s".to_string()));
        assert_eq!(
            times,
            vec![
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(20),
                OffsetDateTime::UNIX_EPOCH + Duration::seconds(30),
            ]
        );
    }

    #[test]
    fn overlap_policy_buffers_one_at_most() {
        let running = vec![WorkflowExecution {
            namespace_id: NamespaceId::new(),
            workflow_id: WorkflowId("wf".to_string()),
            run_id: RunId::new(),
            run_key: RunKey::new(),
        }];
        assert_eq!(
            decide_overlap(OverlapPolicy::BufferOne, &running, 0),
            OverlapDecision::Buffer
        );
        assert_eq!(
            decide_overlap(OverlapPolicy::BufferOne, &running, 1),
            OverlapDecision::Skip
        );
    }

    #[tokio::test]
    async fn catchup_window_skips_old() {
        let store = ScheduleStore::new();
        let mut entry = sample_entry("catchup-old");
        entry.policies.catchup_window = Duration::seconds(5);
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        store.create(entry).expect("create");
        let mut runtime = make_runtime(Arc::new(InMemoryStore::default()));

        handle_due_action(
            &store,
            &runtime,
            &VersioningRuleStore::default(),
            namespace_id,
            &schedule_id,
            OffsetDateTime::UNIX_EPOCH,
            None,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        )
        .await;

        let stored = store
            .describe(namespace_id, &schedule_id)
            .expect("describe");
        assert_eq!(stored.info.missed_catchup_window, 1);
        assert_eq!(stored.info.action_count, 0);
        shutdown(&mut runtime).await;
    }

    #[tokio::test]
    async fn catchup_window_triggers_recent() {
        let store = ScheduleStore::new();
        let mut entry = sample_entry("catchup-recent");
        entry.policies.catchup_window = Duration::seconds(30);
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        store.create(entry).expect("create");
        let mut runtime = make_runtime(Arc::new(InMemoryStore::default()));

        handle_due_action(
            &store,
            &runtime,
            &VersioningRuleStore::default(),
            namespace_id,
            &schedule_id,
            OffsetDateTime::UNIX_EPOCH,
            None,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(10),
        )
        .await;

        let stored = store
            .describe(namespace_id, &schedule_id)
            .expect("describe");
        assert_eq!(stored.info.missed_catchup_window, 0);
        assert_eq!(stored.info.action_count, 1);
        shutdown(&mut runtime).await;
    }

    #[tokio::test]
    async fn limited_actions_stops_at_zero() {
        let store = ScheduleStore::new();
        let mut entry = sample_entry("limited");
        entry.state.limited_actions = true;
        entry.state.remaining_actions = 0;
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        store.create(entry).expect("create");
        let mut runtime = make_runtime(Arc::new(InMemoryStore::default()));

        handle_due_action(
            &store,
            &runtime,
            &VersioningRuleStore::default(),
            namespace_id,
            &schedule_id,
            OffsetDateTime::UNIX_EPOCH,
            None,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await;

        let stored = store
            .describe(namespace_id, &schedule_id)
            .expect("describe");
        assert_eq!(stored.info.action_count, 0);
        assert!(stored.info.running_workflows.is_empty());
        shutdown(&mut runtime).await;
    }

    #[tokio::test]
    async fn pause_on_failure_pauses_after_running_workflow_closes() {
        let store = ScheduleStore::new();
        let mut entry = sample_entry("pause-on-failure");
        entry.policies.pause_on_failure = true;
        entry.info.running_workflows.push(WorkflowExecution {
            namespace_id: entry.namespace_id,
            workflow_id: WorkflowId("already-closed".to_string()),
            run_id: RunId::new(),
            run_key: RunKey::new(),
        });
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        store.create(entry).expect("create");
        let mut runtime = make_runtime(Arc::new(InMemoryStore::default()));

        reconcile_running_workflows(&store, &runtime).await;

        let stored = store
            .describe(namespace_id, &schedule_id)
            .expect("describe");
        assert!(stored.state.paused);
        assert!(stored.info.running_workflows.is_empty());
        shutdown(&mut runtime).await;
    }

    #[tokio::test]
    async fn engine_uses_start_workflow_path() {
        let store = ScheduleStore::new();
        let entry = sample_entry("engine-start");
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        store.create(entry).expect("create");
        let repo = Arc::new(InMemoryStore::default());
        let mut runtime = make_runtime(repo.clone());

        trigger_schedule_action(
            &store,
            &runtime,
            &VersioningRuleStore::default(),
            namespace_id,
            &schedule_id,
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::UNIX_EPOCH,
        )
        .await
        .expect("trigger");

        let stored = store
            .describe(namespace_id, &schedule_id)
            .expect("describe");
        assert_eq!(stored.info.action_count, 1);
        let workflow = stored
            .info
            .running_workflows
            .first()
            .expect("running workflow");
        let loaded = repo.load_run(workflow.run_key).await.expect("load run");
        assert!(matches!(loaded, LoadedRun::Existing(_)));
        shutdown(&mut runtime).await;
    }

    #[tokio::test]
    async fn delete_stops_engine_evaluation() {
        let store = ScheduleStore::new();
        let entry = sample_entry("deleted");
        let namespace_id = entry.namespace_id;
        let schedule_id = entry.schedule_id.clone();
        store.create(entry).expect("create");
        store.delete(namespace_id, &schedule_id).expect("delete");
        let mut runtime = make_runtime(Arc::new(InMemoryStore::default()));

        evaluate_all_schedules(
            &store,
            &runtime,
            &VersioningRuleStore::default(),
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::UNIX_EPOCH + Duration::seconds(60),
        )
        .await;

        assert!(store.describe(namespace_id, &schedule_id).is_err());
        shutdown(&mut runtime).await;
    }

    proptest! {
        // Feature: edge-schedule-transport, Property 2: Conflict token monotonicity
        #[test]
        fn prop_conflict_tokens_are_monotonic(updates in 1usize..25) {
            let store = ScheduleStore::new();
            let entry = sample_entry("token-prop");
            let namespace_id = entry.namespace_id;
            let schedule_id = entry.schedule_id.clone();
            let mut token = store.create(entry).expect("create");
            let mut previous = token.clone();
            for idx in 0..updates {
                let updated = store.update(namespace_id, &schedule_id, &token, |entry| {
                    entry.state.notes = format!("update-{idx}");
                }).expect("update");
                token = updated.conflict_token;
                prop_assert!(token > previous);
                previous = token.clone();
            }
            prop_assert_eq!(
                store.update(namespace_id, &schedule_id, &[0; 8], |_| {}).unwrap_err(),
                ScheduleError::StaleConflictToken
            );
        }

        // Feature: edge-schedule-transport, Property 3: Matching times range containment and monotonicity
        #[test]
        fn prop_interval_matches_are_bounded(
            interval_secs in 1i64..300,
            start_secs in 0i64..10_000,
            span_secs in 1i64..10_000,
        ) {
            let spec = ScheduleSpec {
                intervals: vec![IntervalSpec {
                    interval: Duration::seconds(interval_secs),
                    phase: Duration::ZERO,
                }],
                ..Default::default()
            };
            let start = OffsetDateTime::UNIX_EPOCH + Duration::seconds(start_secs);
            let end = start + Duration::seconds(span_secs);
            let times = compute_matching_times(&spec, start, end, &ScheduleId("bounded".into()));
            prop_assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
            prop_assert!(times.iter().all(|time| *time >= start && *time <= end));

            let sub_start = start + Duration::seconds(span_secs / 4);
            let sub_end = start + Duration::seconds(span_secs / 2);
            let sub_times = compute_matching_times(&spec, sub_start, sub_end, &ScheduleId("bounded".into()));
            prop_assert!(sub_times.iter().all(|time| times.contains(time)));
        }

        // Feature: edge-schedule-transport, Property 5: Jitter determinism
        #[test]
        fn prop_jitter_is_deterministic_and_bounded(
            jitter_secs in 1i64..600,
            nominal_secs in 0i64..10_000,
        ) {
            let spec = ScheduleSpec {
                intervals: vec![IntervalSpec {
                    interval: Duration::seconds(60),
                    phase: Duration::ZERO,
                }],
                jitter: Some(Duration::seconds(jitter_secs)),
                ..Default::default()
            };
            let start = OffsetDateTime::UNIX_EPOCH + Duration::seconds(nominal_secs);
            let end = start + Duration::seconds(3600 + jitter_secs);
            let a = compute_matching_times(&spec, start, end, &ScheduleId("jitter".into()));
            let b = compute_matching_times(&spec, start, end, &ScheduleId("jitter".into()));
            prop_assert_eq!(&a, &b);
            for time in a {
                let since_epoch = time.unix_timestamp();
                let nominal = since_epoch - (since_epoch % 60);
                prop_assert!(since_epoch - nominal <= jitter_secs);
            }
        }

        // Feature: edge-schedule-transport, Property 6: Overlap policy decision correctness
        #[test]
        fn prop_overlap_decisions(buffer_size in 0usize..5) {
            let running = vec![WorkflowExecution {
                namespace_id: NamespaceId::new(),
                workflow_id: WorkflowId("wf".into()),
                run_id: RunId::new(),
                run_key: RunKey::new(),
            }];
            prop_assert_eq!(decide_overlap(OverlapPolicy::Skip, &running, buffer_size), OverlapDecision::Skip);
            prop_assert_eq!(decide_overlap(OverlapPolicy::BufferAll, &running, buffer_size), OverlapDecision::Buffer);
            prop_assert_eq!(decide_overlap(OverlapPolicy::AllowAll, &running, buffer_size), OverlapDecision::Allow);
            prop_assert_eq!(
                decide_overlap(OverlapPolicy::BufferOne, &running, buffer_size),
                if buffer_size == 0 { OverlapDecision::Buffer } else { OverlapDecision::Skip }
            );
        }

        // Feature: edge-schedule-transport, Property 7: Workflow ID generation determinism
        #[test]
        fn prop_schedule_workflow_id_is_deterministic(base in "[a-z][a-z0-9]{0,12}", ts in 0i64..100_000) {
            let workflow_id = WorkflowId(base);
            let nominal = OffsetDateTime::UNIX_EPOCH + Duration::seconds(ts);
            prop_assert_eq!(
                schedule_workflow_id(&workflow_id, nominal, true),
                workflow_id.clone()
            );
            let generated = schedule_workflow_id(&workflow_id, nominal, false);
            prop_assert_eq!(generated.clone(), schedule_workflow_id(&workflow_id, nominal, false));
            prop_assert_ne!(generated, workflow_id);
        }
    }
}
