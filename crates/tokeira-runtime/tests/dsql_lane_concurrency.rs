#![cfg(feature = "dsql-integration")]
// Integration test: unwrap is idiomatic in test code (root AGENTS.md §1).
#![allow(clippy::unwrap_used)]

//! Different runs on one lane commit to Aurora DSQL concurrently, and never
//! more at once than the `Commit` class has permits. Runs on an operator-owned
//! ephemeral cluster; no connection occurs unless
//! `TOKEIRA_DSQL_TEST_DATABASE_URL` is set. The test migrates the database and
//! prepares eight homes, so it belongs on a database of its own
//! (`docs/testing/dsql-live-suites.md`).
//!
//! **The setup.** The runtime has one lane, so every run shares it, and its
//! reservoir target gives the `Commit` class a small allocation above one. A
//! lane runs up to `N = max(lane_count, ceiling)` activations of different
//! runs at once, where the ceiling is that allocation; with one lane, `N` is
//! the allocation. The test starts `3 * N` runs and signals each of them
//! several times, every run's work concurrently with every other's.
//!
//! **What bounds the permits.** The `dsql.commit_transition` span opens before
//! the commit waits for its `Commit` permit, so open spans include commits
//! still queued for one and can't bound the permits held. The window measured
//! instead is narrower: from the first SQLx statement event inside an attempt's
//! span to its last. SQLx reports each statement as a `sqlx::query` event when
//! the statement finishes, and the first one (the fence read) runs on the
//! connection the permit lent, after BEGIN; the last (COMMIT, or ROLLBACK for
//! an attempt that lost) runs before the permit is dropped. Every window
//! therefore sits inside a held permit, and the peak number of windows open at
//! once is at most the number of permits held then. Only the repository's
//! attempt span counts (the one carrying `expected_seq`; the trait-boundary
//! span of the same name carries none). Other operations of the `Commit` class
//! share the permits without being counted, which only lowers the peak, so
//! the upper bound stays sound; a peak above one shows same-lane commits
//! really overlapped.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::{Context as _, Result};
use time::{Duration, OffsetDateTime};
use tokeira_kernel::{HistoryEventKind, SignalRequest, StartRequest};
use tokeira_runtime::{
    DeleteWorkflowRequest, LaneConfig, NoopNexusHttpClient, TimerScannerConfig, TokeiraRuntime,
    WorkflowTimeoutScannerConfig,
};
use tokeira_storage::{
    CommitResult, ConnectionDirector, DbClass, LeaseRepository, RunRepository,
    dsql::{
        DsqlPoolConfig, DsqlStore, MIN_TARGET, MigrationConfig, ReservoirConfig,
        run_repository::DsqlRunRepository,
    },
};
use tokeira_types::{
    ExecutionRef, NamespaceId, Payload, Payloads, RequestContext, RequestId, RunKey, ShardId,
    TaskQueueName, WorkflowId, WorkflowType,
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
const OWNER: &str = "lane-concurrency";
const WORKFLOW_QUEUE: &str = "lane-concurrency-workflow";
const COMMIT_SPAN: &str = "dsql.commit_transition";
const LANE_COUNT: usize = 1;
/// The reservoir's configured target. `default_allocations` in
/// `tokeira-storage`'s connection director gives every class one permit and
/// the `Commit` class half of the rest: `1 + (9 - MIN_TARGET) / 2 = 3`. So
/// `N = max(1, 3) = 3`, and a lane can run more activations than one at once
/// while the cluster sees only a few concurrent commits.
const RESERVOIR_TARGET: usize = 9;
const SIGNALS_PER_RUN: usize = 8;

/// The `Commit` allocation `default_allocations` gives `RESERVOIR_TARGET`,
/// which `RunRepository::commit_concurrency_ceiling` reports for DSQL.
const fn commit_allocation() -> usize {
    1 + (RESERVOIR_TARGET - MIN_TARGET) / 2
}

// ---------------------------------------------------------------------------
// Recorder
// ---------------------------------------------------------------------------

/// The statement window of every commit attempt that sent a statement.
#[derive(Debug, Default)]
struct Recorder {
    windows: Mutex<Vec<Window>>,
}

#[derive(Clone, Copy, Debug)]
struct Window {
    first: Instant,
    last: Instant,
    committed: bool,
}

/// An attempt span's statement window so far.
#[derive(Debug, Default)]
struct AttemptStatements {
    first: Option<Instant>,
    last: Option<Instant>,
    committed: bool,
}

#[derive(Debug, Default)]
struct ExpectedSeq(bool);

impl Visit for ExpectedSeq {
    fn record_u64(&mut self, field: &Field, _: u64) {
        if field.name() == "expected_seq" {
            self.0 = true;
        }
    }

    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

/// SQLx's statement event, as far as telling COMMIT apart.
#[derive(Debug, Default)]
struct Summary(String);

impl Visit for Summary {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "summary" {
            value.trim().clone_into(&mut self.0);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "summary" {
            self.0 = format!("{value:?}").trim_matches('"').trim().to_owned();
        }
    }
}

struct WindowLayer(Arc<Recorder>);

impl<S> Layer<S> for WindowLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() != COMMIT_SPAN {
            return;
        }
        let mut seq = ExpectedSeq::default();
        attrs.record(&mut seq);
        if seq.0
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(AttemptStatements::default());
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        let now = Instant::now();
        let Some(scope) = ctx.event_scope(event) else {
            return;
        };
        for span in scope {
            let mut extensions = span.extensions_mut();
            if let Some(attempt) = extensions.get_mut::<AttemptStatements>() {
                let mut summary = Summary::default();
                event.record(&mut summary);
                attempt.first.get_or_insert(now);
                attempt.last = Some(now);
                attempt.committed = summary.0.eq_ignore_ascii_case("COMMIT");
                return;
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        if let Some(AttemptStatements {
            first: Some(first),
            last: Some(last),
            committed,
        }) = span.extensions().get::<AttemptStatements>()
        {
            self.0.windows.lock().unwrap().push(Window {
                first: *first,
                last: *last,
                committed: *committed,
            });
        }
    }
}

/// Install the window recorder for this process. Nextest runs each test in its
/// own process, which is why this file holds one test.
fn install() -> Arc<Recorder> {
    let recorder = Arc::new(Recorder::default());
    // Every span, so event scopes are whole; SQLx's statement events at the
    // level it emits them by default.
    let layer = WindowLayer(recorder.clone()).with_filter(filter_fn(|meta| {
        meta.is_span() || meta.target() == "sqlx::query"
    }));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))
        .expect("one window recorder per process");
    recorder
}

/// The most windows open at one instant. At a shared instant a close counts
/// before an open, so two windows that only touch never count as overlapping.
fn peak_concurrency(windows: &[Window]) -> usize {
    let mut edges = windows
        .iter()
        .flat_map(|window| [(window.first, 1_i8), (window.last, -1_i8)])
        .collect::<Vec<_>>();
    edges.sort_unstable();
    let mut open = 0_i64;
    let mut peak = 0_i64;
    for (_, delta) in edges {
        open += i64::from(delta);
        peak = peak.max(open);
    }
    usize::try_from(peak).unwrap()
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

fn context(request_id: String) -> RequestContext {
    RequestContext {
        request_id: RequestId(request_id),
        caller_identity: None,
        principal: None,
        received_at: OffsetDateTime::now_utc(),
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
        workflow_type: WorkflowType("lane-concurrency".to_owned()),
        task_queue: TaskQueueName(WORKFLOW_QUEUE.to_owned()),
        input: Payloads::default(),
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
        request: context(format!("start-{}", workflow_id.0)),
        now: OffsetDateTime::now_utc(),
        cron_schedule: None,
        eager_execution_accepted: false,
        reserved_poller_identity: None,
        inherited_versioning_info: None,
    }
}

fn signal_input(run: usize, signal: usize) -> Payloads {
    Payloads(vec![Payload::new(format!("{run}/{signal}").into_bytes())])
}

/// Start one run and signal it `SIGNALS_PER_RUN` times, each signal after the
/// last one's reply, so the run's history has one order to check. Returns the
/// run's key and how many submits were applied.
async fn drive(
    runtime: Arc<TokeiraRuntime<DsqlRunRepository>>,
    namespace_id: NamespaceId,
    run: usize,
) -> Result<(RunKey, usize)> {
    let workflow_id = WorkflowId(format!("lane-concurrency-{run}-{}", uuid::Uuid::new_v4()));
    let CommitResult::Applied { new_state } = runtime
        .start_workflow(start_request(namespace_id, &workflow_id))
        .await
        .with_context(|| format!("run {run}: start"))?
    else {
        anyhow::bail!("run {run}: start not applied");
    };
    let mut applied = 1;
    for signal in 0..SIGNALS_PER_RUN {
        let outcome = runtime
            .signal_workflow(
                ExecutionRef {
                    namespace_id,
                    workflow_id: workflow_id.clone(),
                    run_id: Some(new_state.run_id),
                },
                SignalRequest {
                    signal_name: "nudge".to_owned(),
                    input: signal_input(run, signal),
                    header: None,
                    links: Vec::new(),
                    request: context(format!("signal-{run}-{signal}-{}", uuid::Uuid::new_v4())),
                    now: OffsetDateTime::now_utc(),
                },
            )
            .await
            .with_context(|| format!("run {run}: signal {signal}"))?;
        anyhow::ensure!(
            matches!(outcome, CommitResult::Applied { .. }),
            "run {run}: signal {signal} not applied: {outcome:?}"
        );
        applied += 1;
    }
    Ok((new_state.run_key, applied))
}

/// A run's start event, in either encoding: the engine records new starts as
/// `WorkflowExecutionStartedV2`, and the count and first-event checks must
/// agree on what a start is.
fn is_started(kind: &HistoryEventKind) -> bool {
    matches!(
        kind,
        HistoryEventKind::WorkflowExecutionStarted { .. }
            | HistoryEventKind::WorkflowExecutionStartedV2 { .. }
    )
}

/// Each run's history holds its start and every signal exactly once, in the
/// order they were sent.
async fn check_histories(repo: &DsqlRunRepository, runs: &BTreeMap<usize, RunKey>) -> Result<()> {
    for (&run, &run_key) in runs {
        let events = repo.read_history_to_end(run_key, 0).await?;
        let starts = events
            .iter()
            .filter(|event| is_started(&event.kind))
            .count();
        anyhow::ensure!(
            starts == 1 && events.first().is_some_and(|event| is_started(&event.kind)),
            "run {run}: {starts} started events, first {:?}",
            events.first().map(|event| &event.kind)
        );
        let signals = events
            .iter()
            .filter_map(|event| match &event.kind {
                HistoryEventKind::WorkflowExecutionSignaled { input, .. } => Some(input.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let expected = (0..SIGNALS_PER_RUN)
            .map(|signal| signal_input(run, signal))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            signals == expected,
            "run {run}: signals out of order, missing or repeated: {} of {SIGNALS_PER_RUN}",
            signals.len()
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The test
// ---------------------------------------------------------------------------

/// `3 * N` runs share one lane on a fenced, controller-placed node. Every
/// submit is answered, every history is whole and ordered, same-lane commits
/// overlap, and no more of them overlap than the `Commit` class has permits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dsql_lane_concurrent_commits_stay_within_commit_permits() -> Result<()> {
    let Ok(url) = std::env::var("TOKEIRA_DSQL_TEST_DATABASE_URL") else {
        // The suite's convention: without a database there is nothing to run.
        // The runbook requires the URL, so a run without one is no live check.
        #[allow(clippy::print_stderr)] // Visible with `--no-capture`.
        {
            eprintln!("dsql_lane_concurrency: TOKEIRA_DSQL_TEST_DATABASE_URL unset; not run");
        }
        return Ok(());
    };
    let recorder = install();
    let store = DsqlStore::from_database_url_for_tests(
        url,
        DsqlPoolConfig {
            migration: MigrationConfig {
                migrations_dir: Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../tokeira-storage/migrations"),
            },
            reservoir: ReservoirConfig {
                target_ready: RESERVOIR_TARGET,
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

    let allocation = commit_allocation();
    assert_eq!(
        repo.commit_concurrency_ceiling(),
        Some(allocation),
        "the repository's commit ceiling is the Commit allocation of the reservoir target"
    );
    // The lane's activation bound, as `node_activation_bound` computes it.
    let slots = LANE_COUNT.max(allocation);
    assert!(slots >= 2, "N = {slots} lets no two runs overlap");

    // Controller-managed placement fences every write with the home's lease,
    // as a deployed node's writes are.
    let runtime = Arc::new(TokeiraRuntime::new_with_nexus_and_shards(
        repo.clone(),
        LANE_COUNT,
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
    ));
    let mut epochs = Vec::new();
    for home in 0..SHARD_COUNT {
        epochs.push((ShardId(home), runtime.acquire_shard(ShardId(home)).await?));
    }

    let run_count = 3 * slots;
    let namespace_id = NamespaceId::new();
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for run in 0..run_count {
        let runtime = runtime.clone();
        tasks.spawn(async move { (run, drive(runtime, namespace_id, run).await) });
    }
    let mut runs = BTreeMap::new();
    let mut replies = 0;
    let mut failures = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        let (run, outcome) = joined?;
        match outcome {
            Ok((run_key, applied)) => {
                runs.insert(run, run_key);
                replies += applied;
            }
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    let elapsed = started.elapsed();
    // Take the windows before cleanup, whose deletions commit too.
    let windows = recorder.windows.lock().unwrap().clone();
    let histories = check_histories(&repo, &runs).await;

    for (run, &run_key) in &runs {
        let now = OffsetDateTime::now_utc();
        let _ = runtime
            .delete_workflow(
                run_key,
                DeleteWorkflowRequest {
                    request: context(format!("cleanup-{run}")),
                    now,
                    purge_inline: true,
                },
            )
            .await;
    }
    let shutdown = runtime.shutdown_handle();
    shutdown.begin_shutdown();
    let _ = shutdown
        .wait(std::time::Instant::now() + std::time::Duration::from_secs(10))
        .await;
    for (home, epoch) in epochs {
        let _ = repo.relinquish_bundle(home, OWNER.to_owned(), epoch).await;
    }
    director.shutdown().await?;

    let committed = windows.iter().filter(|window| window.committed).count();
    let peak = peak_concurrency(&windows);
    #[allow(clippy::print_stdout)] // The run's summary, visible with `--no-capture`.
    {
        println!(
            "lane concurrency: runs={run_count} signals={} N={slots} commit_allocation={allocation} \
             peak_concurrent_commits={peak} attempts={} committed={committed} elapsed={elapsed:?}",
            run_count * SIGNALS_PER_RUN,
            windows.len(),
        );
    }

    anyhow::ensure!(failures.is_empty(), "submits failed: {failures:#?}");
    assert_eq!(
        replies,
        run_count * (1 + SIGNALS_PER_RUN),
        "every start and signal is answered"
    );
    histories?;
    assert!(
        peak > 1,
        "same-lane commits never overlapped: peak {peak} over {} attempts",
        windows.len()
    );
    assert!(
        peak <= allocation,
        "{peak} commits held a connection at once; the Commit class has {allocation} permits"
    );
    Ok(())
}
