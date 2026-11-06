#![cfg(feature = "dsql-integration")]
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

//! The SQL a workflow's lifecycle sends to Aurora DSQL: the statements and round
//! trips of every transition's commit, the reads around it, the background
//! statements of an owning node, and the plan of every distinct statement. Runs
//! on an operator-owned ephemeral cluster; no connection occurs unless
//! `TOKEIRA_DSQL_TEST_DATABASE_URL` is set. The test migrates the database and
//! prepares eight homes, so it belongs on a database of its own
//! (`docs/testing/dsql-live-suites.md`).
//!
//! **Counting.** SQLx reports each statement it executes as a `sqlx::query`
//! tracing event when the statement finishes, COMMIT and ROLLBACK included.
//! `Connection::begin` sends BEGIN without one, so each transaction adds one
//! round trip to its statements. A layer attributes every event to the
//! innermost `dsql.*` span around it, and to the run and namespace the
//! enclosing spans name. A commit attempt is one `dsql.commit_transition` span
//! carrying `expected_seq` (the repository's trait-boundary span of the same
//! name carries none); its `expected_seq` places it in the lifecycle step whose
//! transitions it wrote, so effects that finish after their step returns (a
//! timer firing, a retry's dispatch) still count against the transition they
//! belong to. The budget pins committed attempts only: an attempt that loses a
//! race rolls back, and whether one does depends on timing.
//!
//! **The production metric.** The process's statement-accounting layer
//! (`tokeira_observability::statement_accounting`) runs beside the recorder,
//! and its round-trip histogram must agree with the recorder: one `committed`
//! sample per committed attempt, under the attempt's outermost `dsql.*` span,
//! with the round trips the recorder counted, and a cause other than `none`.
//!
//! Counted statements are not all of a commit's round trips: SQLx prepares a
//! statement its connection hasn't cached (one more round trip), and DSQL's
//! retry of an aborted commit repeats the whole transaction. The report keeps
//! each commit attempt apart, so a retry shows as a second attempt at the same
//! sequence.
//!
//! **Plans.** Every distinct statement is planned with
//! `EXPLAIN (GENERIC_PLAN, VERBOSE)`, which DSQL accepts for SQL with `$n`
//! parameters, so the suite plans exactly the text the engine sent without
//! rebuilding its arguments. A generic plan is the plan of unknown parameter
//! values; it shows whether the statement can seek an index at all.
//!
//! `TOKEIRA_DSQL_STATEMENT_REPORT`, when set, names a file for the Markdown
//! report of every count, commit time and plan. `TOKEIRA_DSQL_STATEMENT_REPEAT`,
//! when set, runs that many more lifecycles after the measured one and reports
//! their window, for the cluster's per-minute DPU metrics.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context as _, Result};
use metrics_util::{
    CompositeKey,
    debugging::{DebugValue, DebuggingRecorder, Snapshotter},
};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{
    LoadedRun, SignalRequest, StartRequest, WorkflowCommand, WorkflowTaskCompletedRequest,
};
use tokeira_runtime::{
    LaneConfig, NoopNexusHttpClient, StartedActivityTask, StartedWorkflowTask, TimerScannerConfig,
    TokeiraRuntime, WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{
    CommitResult, ConnectionDirector, DbClass, LeaseRepository, RunRepository,
    dsql::{
        DsqlPoolConfig, DsqlStore, MigrationConfig, ReservoirConfig, query_plan_for_tests,
        run_repository::DsqlRunRepository,
    },
};
use tokeira_types::{
    ExecutionRef, NamespaceId, Payload, Payloads, QueueKey, RequestContext, RequestId, RetryPolicy,
    RunKey, ShardId, TaskKind, TaskQueueName, WorkerIdentity, WorkflowId, WorkflowType,
};
use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id},
};
use tracing_subscriber::{
    Layer, filter::filter_fn, layer::Context, prelude::*, registry::LookupSpan,
};

const SHARD_COUNT: u32 = 8;
const OWNER: &str = "statement-budget";
const WORKFLOW_QUEUE: &str = "statement-budget-workflow";
const ACTIVITY_QUEUE: &str = "statement-budget-activity";
const COMMIT_SPAN: &str = "dsql.commit_transition";
/// Long enough for a 200 ms timer scan, a one-second retry backoff and a
/// one-second timer to come round on a loaded cluster.
const POLL: tokio::time::Duration = tokio::time::Duration::from_secs(30);

/// One lifecycle step: the API call, or the effect a call waits on.
const START: &str = "start";
const WFT_START: &str = "workflow-task-start";
const WFT_COMPLETE_SCHEDULE: &str = "workflow-task-complete/schedule-two-activities";
const ACTIVITY_START: &str = "activity-start";
const ACTIVITY_START_SECOND: &str = "activity-start/second";
const HEARTBEAT: &str = "activity-heartbeat";
const ACTIVITY_COMPLETE: &str = "activity-complete";
const ACTIVITY_FAIL_RETRY: &str = "activity-fail/retry";
const ACTIVITY_RETRY_START: &str = "activity-start/retry";
const ACTIVITY_COMPLETE_SECOND: &str = "activity-complete/second";
const SIGNAL: &str = "signal";
const WFT_START_SECOND: &str = "workflow-task-start/second";
const WFT_COMPLETE_TIMER: &str = "workflow-task-complete/start-timer";
const TIMER_FIRE_WFT_START: &str = "timer-fire+workflow-task-start";
const WFT_COMPLETE_CLOSE: &str = "workflow-task-complete/close";
/// Reads the test makes itself, outside every measurement.
const OBSERVE: &str = "observe";

/// Round trips of each committed commit a step writes, in sequence order: BEGIN,
/// the statements, COMMIT.
const EXPECTED_COMMIT_ROUND_TRIPS: &[(&str, &[usize])] = &[
    (START, &[18]),
    (WFT_START, &[11]),
    (WFT_COMPLETE_SCHEDULE, &[13]),
    (ACTIVITY_START, &[12]),
    (ACTIVITY_START_SECOND, &[11]),
    (HEARTBEAT, &[10]),
    (ACTIVITY_COMPLETE, &[11]),
    (ACTIVITY_FAIL_RETRY, &[11]),
    (ACTIVITY_RETRY_START, &[11]),
    (ACTIVITY_COMPLETE_SECOND, &[11]),
    (SIGNAL, &[13]),
    (WFT_START_SECOND, &[11]),
    (WFT_COMPLETE_TIMER, &[12]),
    (TIMER_FIRE_WFT_START, &[12, 11]),
    (WFT_COMPLETE_CLOSE, &[13]),
];

// ---------------------------------------------------------------------------
// Recorder
// ---------------------------------------------------------------------------

/// The current step and every statement observed under it.
#[derive(Debug, Default)]
struct Recorder {
    step: Mutex<&'static str>,
    statements: Mutex<Vec<Statement>>,
    attempts: AtomicU64,
    /// Each commit attempt's span time: from the span's opening, before the
    /// commit waits for a connection, to its close after COMMIT.
    commit_times: Mutex<BTreeMap<u64, std::time::Duration>>,
}

impl Recorder {
    fn enter(&self, step: &'static str) {
        *self.step.lock().unwrap() = step;
    }

    fn statements(&self) -> Vec<Statement> {
        self.statements.lock().unwrap().clone()
    }
}

#[derive(Clone, Debug)]
struct Statement {
    step: &'static str,
    /// The innermost `dsql.*` span, or `unscoped`.
    operation: &'static str,
    run_key: Option<String>,
    namespace_id: Option<String>,
    commit: Option<Attempt>,
    /// The outermost `dsql.*` span, the operation the production metric
    /// charges the statement to.
    charged_to: &'static str,
    sql: String,
}

/// One `dsql.commit_transition` span. Span ids are reused once a span closes,
/// so attempts carry their own number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Attempt {
    expected_seq: u64,
    number: u64,
}

#[derive(Debug, Default)]
struct SpanFields {
    run_key: Option<String>,
    namespace_id: Option<String>,
    expected_seq: Option<u64>,
    attempt: Option<u64>,
    opened: Option<std::time::Instant>,
}

impl Visit for SpanFields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "expected_seq" {
            self.expected_seq = Some(value);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "run_key" => self.run_key = Some(value.to_owned()),
            "namespace_id" => self.namespace_id = Some(value.to_owned()),
            _ => {}
        }
    }

    // `%`-formatted fields arrive here; their Debug is the Display text.
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "run_key" => self.run_key = Some(format!("{value:?}")),
            "namespace_id" => self.namespace_id = Some(format!("{value:?}")),
            _ => {}
        }
    }
}

/// SQLx's statement event: a four-word `summary`, and the full text in
/// `db.statement` when the statement is longer than its summary.
#[derive(Debug, Default)]
struct QueryFields {
    summary: String,
    statement: String,
}

impl QueryFields {
    fn sql(&self) -> String {
        let text = if self.statement.trim().is_empty() {
            self.summary.as_str()
        } else {
            self.statement.as_str()
        };
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }
}

impl Visit for QueryFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "summary" => self.summary = value.to_owned(),
            "db.statement" => self.statement = value.to_owned(),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Strings recorded through Debug arrive quoted and escaped.
        let text = || {
            let quoted = format!("{value:?}");
            quoted
                .strip_prefix('"')
                .and_then(|text| text.strip_suffix('"'))
                .map(|text| text.replace("\\n", "\n").replace("\\\"", "\""))
                .unwrap_or(quoted)
        };
        match field.name() {
            "summary" => self.summary = text(),
            "db.statement" => self.statement = text(),
            _ => {}
        }
    }
}

struct StatementLayer(Arc<Recorder>);

impl<S> Layer<S> for StatementLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = SpanFields::default();
        attrs.record(&mut fields);
        if attrs.metadata().name() == COMMIT_SPAN && fields.expected_seq.is_some() {
            fields.attempt = Some(self.0.attempts.fetch_add(1, Ordering::Relaxed));
            fields.opened = Some(std::time::Instant::now());
        }
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(fields);
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        if let Some(SpanFields {
            attempt: Some(attempt),
            opened: Some(opened),
            ..
        }) = span.extensions().get::<SpanFields>()
        {
            self.0
                .commit_times
                .lock()
                .unwrap()
                .insert(*attempt, opened.elapsed());
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        let mut query = QueryFields::default();
        event.record(&mut query);
        let mut statement = Statement {
            step: *self.0.step.lock().unwrap(),
            operation: "unscoped",
            run_key: None,
            namespace_id: None,
            commit: None,
            charged_to: "unscoped",
            sql: query.sql(),
        };
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                let name = span.metadata().name();
                if name.starts_with("dsql.") {
                    if statement.operation == "unscoped" {
                        statement.operation = name;
                    }
                    statement.charged_to = name;
                }
                let extensions = span.extensions();
                let Some(fields) = extensions.get::<SpanFields>() else {
                    continue;
                };
                if statement.run_key.is_none() {
                    statement.run_key.clone_from(&fields.run_key);
                }
                if statement.namespace_id.is_none() {
                    statement.namespace_id.clone_from(&fields.namespace_id);
                }
                if statement.commit.is_none()
                    && let (Some(number), Some(expected_seq)) =
                        (fields.attempt, fields.expected_seq)
                {
                    statement.commit = Some(Attempt {
                        expected_seq,
                        number,
                    });
                }
            }
        }
        self.0.statements.lock().unwrap().push(statement);
    }
}

/// Install the recorder and the production statement accounting for this
/// process, with a metrics recorder to read the accounting back. Nextest runs
/// each test in its own process, which is why this file holds one test.
fn install() -> (Arc<Recorder>, Snapshotter) {
    let recorder = Arc::new(Recorder::default());
    // Every span, so event scopes are whole; SQLx's statement events at the
    // level it emits them by default.
    let layer = StatementLayer(recorder.clone()).with_filter(filter_fn(|meta| {
        meta.is_span() || meta.target() == "sqlx::query"
    }));
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(layer)
            .with(tokeira_observability::statement_accounting_layer()),
    )
    .expect("one statement recorder per process");
    let metrics = DebuggingRecorder::new();
    let snapshotter = metrics.snapshotter();
    metrics::set_global_recorder(metrics).expect("one metrics recorder per process");
    (recorder, snapshotter)
}

/// The production metric's round-trip samples since the last snapshot, keyed
/// by operation, cause and transaction outcome (the snapshot drains them), and
/// the unattributed-statement count since the process began.
#[derive(Debug, Default)]
struct Accounted {
    samples: BTreeMap<(String, String, String), Vec<u64>>,
    unattributed: u64,
}

impl Accounted {
    fn take(snapshotter: &Snapshotter) -> Self {
        let mut accounted = Self::default();
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            let key: CompositeKey = key;
            let label = |name: &str| {
                key.key()
                    .labels()
                    .find(|label| label.key() == name)
                    .map(|label| label.value().to_owned())
                    .unwrap_or_default()
            };
            match (key.key().name(), value) {
                (name, DebugValue::Histogram(values))
                    if name == tokeira_observability::DSQL_OPERATION_ROUND_TRIPS =>
                {
                    let samples = accounted
                        .samples
                        .entry((
                            label("operation"),
                            label("command_type"),
                            label("transaction"),
                        ))
                        .or_default();
                    // Round trips are whole numbers recorded as f64.
                    samples.extend(values.iter().map(|value| value.into_inner() as u64));
                    samples.sort_unstable();
                }
                (name, DebugValue::Counter(count))
                    if name == tokeira_observability::DSQL_UNATTRIBUTED_STATEMENTS_TOTAL =>
                {
                    accounted.unattributed = count;
                }
                _ => {}
            }
        }
        accounted
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id.to_owned()),
        caller_identity: None,
        principal: None,
        received_at: OffsetDateTime::now_utc(),
    }
}

fn queue(namespace_id: NamespaceId, name: &str, task_kind: TaskKind) -> QueueKey {
    QueueKey {
        namespace_id,
        task_queue: TaskQueueName(name.to_owned()),
        task_kind,
        deployment: None,
        build_id: None,
    }
}

fn start_request(namespace_id: NamespaceId, workflow_id: &WorkflowId) -> StartRequest {
    let run_id = tokeira_types::RunId::new();
    StartRequest {
        advice_policy: tokeira_kernel::ContinueAsNewAdvicePolicy::V1_31_0,
        initiator: None,
        run_key: RunKey::derive(namespace_id, workflow_id, run_id),
        namespace_id,
        workflow_id: workflow_id.clone(),
        run_id,
        workflow_type: WorkflowType("statement-budget".to_owned()),
        task_queue: TaskQueueName(WORKFLOW_QUEUE.to_owned()),
        input: Payloads(vec![Payload::new(b"input".to_vec())]),
        header: None,
        memo: Default::default(),
        search_attributes: Default::default(),
        workflow_execution_timeout: None,
        workflow_run_timeout: None,
        workflow_task_timeout: Duration::seconds(10),
        retry_policy: None,
        conflict_policy: tokeira_kernel::WorkflowIdConflictPolicy::Fail,
        reuse_policy: tokeira_kernel::WorkflowIdReusePolicy::AllowDuplicate,
        deployment: None,
        build_id: None,
        versioning_override: None,
        workflow_start_delay: None,
        client_cron_schedule: None,
        completion_callbacks: Vec::new(),
        user_metadata: None,
        links: Vec::new(),
        on_conflict_options: None,
        priority: None,
        attempt: 1,
        continued_execution_run_id: None,
        first_execution_run_id: Some(run_id),
        parent_run_key: None,
        parent_workflow_id: None,
        parent_run_id: None,
        parent_namespace_id: None,
        parent_namespace_name: None,
        parent_initiated_event_id: 0,
        root_workflow_id: None,
        root_run_id: None,
        original_execution_run_id: Some(run_id),
        continued_failure: None,
        last_completion_result: None,
        first_run_started_at: None,
        request: context("start"),
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

fn schedule(index: usize, retry_policy: Option<RetryPolicy>) -> WorkflowCommand {
    WorkflowCommand::ScheduleActivity {
        activity_id: format!("activity-{index}"),
        activity_type: "work".to_owned(),
        task_queue: TaskQueueName(ACTIVITY_QUEUE.to_owned()),
        input: Payloads(vec![Payload::new(format!("input-{index}").into_bytes())]),
        header: None,
        request_eager_execution: false,
        use_workflow_build_id: false,
        retry_policy,
        deployment: None,
        build_id: None,
        schedule_to_close_timeout: None,
        schedule_to_start_timeout: None,
        start_to_close_timeout: Some(Duration::minutes(1)),
        heartbeat_timeout: None,
        priority: None,
    }
}

/// One retry after a second, so the retry's dispatch is observed promptly.
fn one_retry() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Duration::seconds(1),
        backoff_coefficient: 1.0,
        maximum_interval: None,
        maximum_attempts: 2,
        non_retryable_error_types: Vec::new(),
    }
}

struct Lifecycle {
    runtime: TokeiraRuntime<DsqlRunRepository>,
    repo: Arc<DsqlRunRepository>,
    recorder: Arc<Recorder>,
    namespace_id: NamespaceId,
    workflow_id: WorkflowId,
    run_key: Option<RunKey>,
    /// Each step and the sequences its transitions were written at.
    steps: Vec<(&'static str, std::ops::Range<u64>)>,
}

impl Lifecycle {
    async fn seq(&self) -> Result<u64> {
        self.recorder.enter(OBSERVE);
        let Some(run_key) = self.run_key else {
            return Ok(0);
        };
        match self.repo.load_run(run_key).await? {
            LoadedRun::Existing(state) => Ok(state.transition_seq.0),
            LoadedRun::Absent => Ok(0),
        }
    }

    /// Run `call` as `step`, and record the sequences its transitions took.
    async fn step<T>(
        &mut self,
        step: &'static str,
        call: impl AsyncFnOnce(&Self) -> Result<T>,
    ) -> Result<T> {
        let before = self.seq().await?;
        self.recorder.enter(step);
        let value = call(self).await.with_context(|| format!("step {step}"))?;
        let after = self.seq().await?;
        anyhow::ensure!(after > before, "step {step} wrote no transition");
        self.steps.push((step, before..after));
        Ok(value)
    }

    async fn poll_workflow_task(&self) -> Result<StartedWorkflowTask> {
        self.runtime
            .poll_workflow_task(
                queue(self.namespace_id, WORKFLOW_QUEUE, TaskKind::Workflow),
                WorkerIdentity("worker".to_owned()),
                POLL,
            )
            .await?
            .context("a workflow task")
    }

    async fn poll_activity_task(&self) -> Result<StartedActivityTask> {
        self.runtime
            .poll_activity_task(
                queue(self.namespace_id, ACTIVITY_QUEUE, TaskKind::Activity),
                WorkerIdentity("worker".to_owned()),
                POLL,
            )
            .await?
            .context("an activity task")
    }

    async fn complete_workflow_task(
        &self,
        task: StartedWorkflowTask,
        commands: Vec<WorkflowCommand>,
    ) -> Result<()> {
        self.runtime
            .complete_workflow_task(WorkflowTaskCompletedRequest {
                client_discards_speculative_with_events: false,
                token: task.token,
                identity: WorkerIdentity("worker".to_owned()),
                sdk_metadata: None,
                metering_metadata: None,
                worker_version: None,
                versioning_behavior: tokeira_kernel::VersioningBehavior::Unspecified,
                deployment_version: None,
                worker_deployment_name: None,
                sticky: None,
                commands,
                eager_activities: Default::default(),
                inline_workflow_task: None,
                heartbeat: Default::default(),
                force_new_workflow_task: false,
                limits: Default::default(),
                delivered_update_ids: Vec::new(),
                request: RequestContext::unattributed(OffsetDateTime::UNIX_EPOCH),
                now: OffsetDateTime::now_utc(),
                command_sizes: Vec::new(),
            })
            .await?;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        let start = start_request(self.namespace_id, &self.workflow_id);
        // The run's key first, so the step reads the sequence its start wrote.
        self.run_key = Some(start.run_key);
        self.step(START, async |this| {
            match this.runtime.start_workflow(start).await? {
                CommitResult::Applied { .. } => Ok(()),
                other => anyhow::bail!("start answered {other:?}"),
            }
        })
        .await?;

        let task = self
            .step(WFT_START, async |this| this.poll_workflow_task().await)
            .await?;
        self.step(WFT_COMPLETE_SCHEDULE, async |this| {
            this.complete_workflow_task(
                task,
                vec![schedule(0, None), schedule(1, Some(one_retry()))],
            )
            .await
        })
        .await?;

        let first = self
            .step(ACTIVITY_START, async |this| this.poll_activity_task().await)
            .await?;
        let second = self
            .step(ACTIVITY_START_SECOND, async |this| {
                this.poll_activity_task().await
            })
            .await?;
        let (plain, retried) = if first.activity_id == "activity-0" {
            (first, second)
        } else {
            (second, first)
        };

        self.step(HEARTBEAT, async |this| {
            this.runtime
                .record_activity_heartbeat(
                    plain.token.clone(),
                    Some(Payloads(vec![Payload::new(b"progress".to_vec())])),
                    Some(WorkerIdentity("worker".to_owned())),
                )
                .await?;
            Ok(())
        })
        .await?;
        self.step(ACTIVITY_COMPLETE, async |this| {
            this.runtime
                .complete_activity_task(
                    plain.token,
                    Payloads(vec![Payload::new(b"done".to_vec())]),
                    Some(WorkerIdentity("worker".to_owned())),
                    context("complete-0"),
                )
                .await?;
            Ok(())
        })
        .await?;
        self.step(ACTIVITY_FAIL_RETRY, async |this| {
            this.runtime
                .fail_activity_task(
                    retried.token,
                    Payload::new(b"transient".to_vec()),
                    Some("Transient".to_owned()),
                    false,
                    None,
                    Some(WorkerIdentity("worker".to_owned())),
                    context("fail-1"),
                )
                .await
        })
        .await?;
        let retry = self
            .step(ACTIVITY_RETRY_START, async |this| {
                this.poll_activity_task().await
            })
            .await?;
        anyhow::ensure!(retry.attempt == 2, "the retry is attempt {}", retry.attempt);
        self.step(ACTIVITY_COMPLETE_SECOND, async |this| {
            this.runtime
                .complete_activity_task(
                    retry.token,
                    Payloads(vec![Payload::new(b"done".to_vec())]),
                    Some(WorkerIdentity("worker".to_owned())),
                    context("complete-1"),
                )
                .await?;
            Ok(())
        })
        .await?;

        let namespace_id = self.namespace_id;
        let workflow_id = self.workflow_id.clone();
        self.step(SIGNAL, async |this| {
            this.runtime
                .signal_workflow(
                    ExecutionRef {
                        namespace_id,
                        workflow_id,
                        run_id: None,
                    },
                    SignalRequest {
                        signal_name: "nudge".to_owned(),
                        input: Payloads(vec![Payload::new(b"signal".to_vec())]),
                        header: None,
                        links: Vec::new(),
                        request: context("signal"),
                        now: OffsetDateTime::now_utc(),
                    },
                )
                .await?;
            Ok(())
        })
        .await?;

        let task = self
            .step(WFT_START_SECOND, async |this| {
                this.poll_workflow_task().await
            })
            .await?;
        self.step(WFT_COMPLETE_TIMER, async |this| {
            let fire_at = OffsetDateTime::now_utc() + Duration::seconds(1);
            this.complete_workflow_task(
                task,
                vec![WorkflowCommand::StartTimer {
                    timer_id: "wake".to_owned(),
                    fire_at,
                }],
            )
            .await
        })
        .await?;
        let task = self
            .step(TIMER_FIRE_WFT_START, async |this| {
                this.poll_workflow_task().await
            })
            .await?;
        self.step(WFT_COMPLETE_CLOSE, async |this| {
            this.complete_workflow_task(
                task,
                vec![WorkflowCommand::CompleteWorkflow {
                    result: Payloads(vec![Payload::new(b"result".to_vec())]),
                }],
            )
            .await
        })
        .await?;
        self.recorder.enter(OBSERVE);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// A step's commit attempts: each attempt's sequence and statements.
fn commits_of(
    statements: &[Statement],
    run_key: &str,
    sequences: &std::ops::Range<u64>,
) -> BTreeMap<Attempt, Vec<String>> {
    let mut commits = BTreeMap::<Attempt, Vec<String>>::new();
    for statement in statements {
        if let Some(attempt) = statement.commit
            && statement.run_key.as_deref() == Some(run_key)
            && sequences.contains(&attempt.expected_seq)
        {
            commits
                .entry(attempt)
                .or_default()
                .push(statement.sql.clone());
        }
    }
    commits
}

#[derive(Debug)]
struct Plan {
    lines: Vec<String>,
    error: Option<String>,
}

impl Plan {
    fn any(&self, needle: &str) -> bool {
        self.lines.iter().any(|line| line.contains(needle))
    }

    fn full_scan(&self) -> bool {
        self.any("Full Scan")
    }

    fn join(&self) -> bool {
        self.any("Nested Loop") || self.any("Hash Join") || self.any("Merge Join")
    }

    /// A `Filter:` above storage runs after rows cross the network; storage's
    /// own pushdown is spelled `Filters:`.
    fn filter_after_transfer(&self) -> bool {
        self.lines
            .iter()
            .any(|line| line.trim_start().starts_with("Filter:"))
    }

    fn flags(&self) -> String {
        if let Some(error) = &self.error {
            return format!("unplanned: {error}");
        }
        let mut flags = Vec::new();
        if self.full_scan() {
            flags.push("FULL SCAN");
        }
        if self.join() {
            flags.push("JOIN");
        }
        if self.filter_after_transfer() {
            flags.push("filter after transfer");
        }
        if flags.is_empty() {
            "seek".to_owned()
        } else {
            flags.join(", ")
        }
    }
}

fn plannable(sql: &str) -> bool {
    let verb = sql
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    matches!(
        verb.as_str(),
        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "WITH"
    )
}

fn report(
    lifecycle: &Lifecycle,
    statements: &[Statement],
    plans: &BTreeMap<String, Plan>,
    elapsed: std::time::Duration,
) -> String {
    let run_key = lifecycle
        .run_key
        .map(|key| key.0.to_string())
        .unwrap_or_default();
    let namespace_id = lifecycle.namespace_id.0.to_string();
    let mut out = String::new();
    let _ = writeln!(out, "# DSQL statement budget\n");
    let _ = writeln!(out, "## Commits per step\n");
    let _ = writeln!(
        out,
        "| Step | Commit round trips (BEGIN + statements) | Reads outside the commit |"
    );
    let _ = writeln!(out, "|---|---|---|");
    for (step, sequences) in &lifecycle.steps {
        let commits = commits_of(statements, &run_key, sequences);
        let trips = commits
            .iter()
            .map(|(attempt, sql)| {
                let time = lifecycle
                    .recorder
                    .commit_times
                    .lock()
                    .unwrap()
                    .get(&attempt.number)
                    .map(|time| format!(", {:.1} ms", time.as_secs_f64() * 1000.0))
                    .unwrap_or_default();
                format!("seq {}: {}{time}", attempt.expected_seq, sql.len() + 1)
            })
            .collect::<Vec<_>>()
            .join("; ");
        let mut reads = BTreeMap::<&str, usize>::new();
        for statement in statements.iter().filter(|statement| {
            statement.step == *step
                && statement.commit.is_none()
                && (statement.run_key.as_deref() == Some(&run_key)
                    || statement.namespace_id.as_deref() == Some(&namespace_id))
        }) {
            *reads.entry(statement.operation).or_default() += 1;
        }
        let reads = reads
            .iter()
            .map(|(operation, count)| format!("{operation} ×{count}"))
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(out, "| {step} | {trips} | {reads} |");
    }

    let _ = writeln!(out, "\n## Statements of each commit\n");
    for (step, sequences) in &lifecycle.steps {
        for (attempt, sql) in commits_of(statements, &run_key, sequences) {
            let _ = writeln!(
                out,
                "### {step} — seq {} (attempt {})\n",
                attempt.expected_seq, attempt.number
            );
            let _ = writeln!(out, "1. BEGIN");
            for text in sql {
                let flags = plans.get(&text).map(Plan::flags).unwrap_or_default();
                let _ = writeln!(out, "1. `{}` — {flags}", truncate(&text, 160));
            }
            let _ = writeln!(out);
        }
    }

    let _ = writeln!(out, "## Background statements\n");
    let _ = writeln!(
        out,
        "Statements outside every commit of the run and every read of its namespace, over {:.1} s.\n",
        elapsed.as_secs_f64()
    );
    let mut background = BTreeMap::<&str, usize>::new();
    for statement in statements.iter().filter(|statement| {
        statement.step != OBSERVE
            && statement.run_key.as_deref() != Some(&run_key)
            && statement.namespace_id.as_deref() != Some(&namespace_id)
    }) {
        *background.entry(statement.operation).or_default() += 1;
    }
    let _ = writeln!(out, "| Operation | Statements | Per second |");
    let _ = writeln!(out, "|---|---|---|");
    for (operation, count) in &background {
        let _ = writeln!(
            out,
            "| {operation} | {count} | {:.2} |",
            *count as f64 / elapsed.as_secs_f64()
        );
    }

    let _ = writeln!(out, "\n## Plans\n");
    let mut operations = BTreeMap::<&str, BTreeSet<&str>>::new();
    for statement in statements {
        operations
            .entry(statement.sql.as_str())
            .or_default()
            .insert(statement.operation);
    }
    for (sql, plan) in plans {
        let used_by = operations
            .get(sql.as_str())
            .map(|set| set.iter().copied().collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        let _ = writeln!(out, "### {} — {}\n", plan.flags(), used_by);
        let _ = writeln!(out, "```sql\n{sql}\n```\n");
        if !plan.lines.is_empty() {
            let _ = writeln!(out, "```\n{}\n```\n", plan.lines.join("\n"));
        }
    }
    out
}

fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_owned()
    } else {
        format!("{}…", text.chars().take(limit).collect::<String>())
    }
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// One workflow's lifecycle on a fenced, controller-placed node: every commit's
/// round trips match the pinned budget, and every statement is planned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dsql_statement_budget_of_a_workflow_lifecycle() -> Result<()> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        return Ok(());
    };
    let (recorder, snapshotter) = install();
    recorder.enter(OBSERVE);
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: 8,
                inflight_limit: 2,
                ..ReservoirConfig::default()
            },
            shard_count: SHARD_COUNT,
            ..DsqlPoolConfig::default()
        },
    )
    .await?;
    {
        let mut permit =
            ConnectionDirector::acquire(store.connection_director(), DbClass::Maintenance).await?;
        store
            .migration_runner()
            .apply_connection(permit.connection()?)
            .await?;
    }
    tokeira_storage::prepare_execution_placement(store.run_repository()).await?;
    let (director, repo, _, _, _) = store.into_parts();
    let repo = Arc::new(repo);

    // Controller-managed placement fences every write with the home's lease,
    // as a deployed node's writes are.
    let runtime = TokeiraRuntime::new_with_nexus_and_shards(
        repo.clone(),
        2,
        LaneConfig {
            controller_managed_placement: true,
            ..LaneConfig::default()
        },
        TimerScannerConfig::default(),
        WorkflowTimeoutScannerConfig::default(),
        Default::default(),
        Default::default(),
        Default::default(),
        Arc::new(NoopNexusHttpClient),
        Default::default(),
        SHARD_COUNT,
        OWNER.to_owned(),
        false,
    );
    let mut epochs = Vec::new();
    for home in 0..SHARD_COUNT {
        epochs.push((ShardId(home), runtime.acquire_shard(ShardId(home)).await?));
    }

    let mut lifecycle = Lifecycle {
        runtime,
        repo: repo.clone(),
        recorder: recorder.clone(),
        namespace_id: NamespaceId::new(),
        workflow_id: WorkflowId(format!("statement-budget-{}", uuid::Uuid::new_v4())),
        run_key: None,
        steps: Vec::new(),
    };
    let before = Accounted::take(&snapshotter);
    let started = std::time::Instant::now();
    let outcome = lifecycle.run().await;
    let elapsed = started.elapsed();
    let statements = recorder.statements();
    let accounted = Accounted::take(&snapshotter);
    let unattributed = accounted.unattributed - before.unattributed;

    // Repeat the lifecycle for a cluster-metrics window: DSQL reports DPU per
    // minute, per cluster, so a baseline divides a window's DPU by the
    // lifecycles in it. The repeats are outside every count above.
    let mut repeated = String::new();
    let repeats = std::env::var("TOKEIRA_DSQL_STATEMENT_REPEAT")
        .ok()
        .map(|count| count.parse::<usize>())
        .transpose()?
        .unwrap_or(0);
    if outcome.is_ok() && repeats > 0 {
        let measured = (lifecycle.run_key, std::mem::take(&mut lifecycle.steps));
        let from = OffsetDateTime::now_utc();
        for index in 0..repeats {
            lifecycle.workflow_id =
                WorkflowId(format!("statement-budget-repeat-{}", uuid::Uuid::new_v4()));
            lifecycle.run_key = None;
            lifecycle
                .run()
                .await
                .with_context(|| format!("repeat {index}"))?;
            lifecycle.steps.clear();
        }
        let to = OffsetDateTime::now_utc();
        (lifecycle.run_key, lifecycle.steps) = measured;
        repeated = format!("\n## Repeats\n\n{repeats} more lifecycles ran from {from} to {to}.\n");
    }

    // Plan every distinct statement on a maintenance connection. The counts
    // above are already taken, so planning adds nothing to them.
    let mut plans = BTreeMap::new();
    {
        let mut permit =
            ConnectionDirector::acquire(director.as_ref(), DbClass::Maintenance).await?;
        for sql in statements
            .iter()
            .map(|statement| statement.sql.clone())
            .filter(|sql| plannable(sql))
            .collect::<BTreeSet<_>>()
        {
            let plan = match query_plan_for_tests::generic_plan(permit.connection()?, &sql).await {
                Ok(lines) => Plan { lines, error: None },
                Err(error) => Plan {
                    lines: Vec::new(),
                    error: Some(error.to_string()),
                },
            };
            plans.insert(sql, plan);
        }
    }

    let text = report(&lifecycle, &statements, &plans, elapsed)
        + &accounted_report(&accounted, unattributed)
        + &repeated;
    if let Ok(path) = std::env::var("TOKEIRA_DSQL_STATEMENT_REPORT") {
        std::fs::write(&path, &text).with_context(|| format!("writing {path}"))?;
    }

    let shutdown = lifecycle.runtime.shutdown_handle();
    shutdown.begin_shutdown();
    let _ = shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await;
    for (home, epoch) in epochs {
        let _ = repo.relinquish_bundle(home, OWNER.to_owned(), epoch).await;
    }
    director.shutdown().await?;
    outcome?;

    let run_key = lifecycle
        .run_key
        .map(|key| key.0.to_string())
        .unwrap_or_default();
    let measured = lifecycle
        .steps
        .iter()
        .map(|(step, sequences)| {
            let trips = commits_of(&statements, &run_key, sequences)
                .values()
                .filter(|sql| committed(sql))
                .map(|sql| sql.len() + 1)
                .collect::<Vec<_>>();
            (*step, trips)
        })
        .collect::<Vec<_>>();
    let expected = EXPECTED_COMMIT_ROUND_TRIPS
        .iter()
        .map(|(step, trips)| (*step, trips.to_vec()))
        .collect::<Vec<_>>();
    assert_eq!(measured, expected, "\n{text}");

    // The production metric agrees with the recorder: each committed attempt
    // is one `committed` sample of the operation it was charged to.
    let mut attempts = BTreeMap::<Attempt, (&str, usize, bool)>::new();
    for statement in &statements {
        if let Some(attempt) = statement.commit {
            let entry = attempts
                .entry(attempt)
                .or_insert((statement.charged_to, 1, false));
            entry.1 += 1;
            entry.2 = statement.sql == "COMMIT";
        }
    }
    let mut recorded = BTreeMap::<String, Vec<u64>>::new();
    for (operation, trips, _) in attempts.values().filter(|(_, _, done)| *done) {
        let operation = operation.trim_start_matches("dsql.").to_owned();
        recorded.entry(operation).or_default().push(*trips as u64);
    }
    let mut metric = BTreeMap::<String, Vec<u64>>::new();
    for ((operation, cause, transaction), samples) in &accounted.samples {
        if transaction == "committed" && recorded.contains_key(operation) {
            assert_ne!(
                cause, "none",
                "a commit of {operation} named no cause\n{text}"
            );
            metric.entry(operation.clone()).or_default().extend(samples);
        }
    }
    for samples in recorded.values_mut().chain(metric.values_mut()) {
        samples.sort_unstable();
    }
    assert_eq!(metric, recorded, "\n{text}");
    Ok(())
}

/// Whether a commit attempt's statements end with its COMMIT.
fn committed(sql: &[String]) -> bool {
    sql.last().is_some_and(|text| text == "COMMIT")
}

fn accounted_report(accounted: &Accounted, unattributed: u64) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "\n## Production metric\n");
    let _ = writeln!(
        out,
        "`tokeira_dsql_operation_round_trips` over the lifecycle; {unattributed} statements unattributed.\n"
    );
    let _ = writeln!(out, "| Operation | Cause | Transaction | Samples |");
    let _ = writeln!(out, "|---|---|---|---|");
    for ((operation, cause, transaction), samples) in &accounted.samples {
        // A series recorded before the lifecycle keeps its key, emptied.
        if samples.is_empty() {
            continue;
        }
        let samples = samples
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let _ = writeln!(out, "| {operation} | {cause} | {transaction} | {samples} |");
    }
    out
}
