//! Continue-as-new advice as the pinned Temporal Rust SDK worker sees it.
//!
//! A workflow absorbs signals until the server's advice flips, then continues
//! as new carrying its count; the continued run returns that count. One
//! embedded engine serves the scenario over both client transports it offers:
//! the in-process `service_override` and a host-attached listener. The
//! engine pins the v1.31.0 thresholds, so the flip comes from the event count
//! (`limit.historyCount.suggestContinueAsNew`,
//! `common/dynamicconfig/constants.go:412-417 @ v1.31.0`); empty signals keep
//! the size threshold out of reach.

use std::{
    net::TcpListener,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context as _, Result, anyhow, ensure};
use futures::{TryStreamExt as _, future::join_all};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowDescribeOptions,
    WorkflowExecutionStatus, WorkflowFetchHistoryOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::{
    enums::v1::{EventType, SuggestContinueAsNewReason},
    history::v1::history_event::Attributes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ContinueAsNewOptions, Runtime, SyncWorkflowContext, Worker, WorkerOptions, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};
use tokeira_engine::{Engine, TokeiraConfig};

const EMBEDDED_URL: &str = "http://tokeira-engine.invalid:7233";
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(180);
/// The pinned count threshold; the flip cannot come earlier than this many events.
const COUNT_THRESHOLD: usize = 4 * 1024;
/// Signals in flight per batch; the handle is `&self`, so they overlap.
const SIGNAL_BATCH: usize = 16;
/// Well past what the flip needs; reaching it means the advice never arrived.
const SIGNAL_CAP: u64 = 3 * COUNT_THRESHOLD as u64;

#[workflow]
struct AdviceProbeWorkflow {
    carried: u64,
    seen: u64,
}

#[workflow_methods]
impl AdviceProbeWorkflow {
    #[init]
    fn new(_ctx: &WorkflowContextView, carried: u64) -> Self {
        Self { carried, seen: 0 }
    }

    /// Run one absorbs signals until the server suggests continuing and carries
    /// the count over; run two returns what was carried.
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<u64> {
        let carried = ctx.state(|state| state.carried);
        if carried > 0 {
            return Ok(carried);
        }
        loop {
            let seen = ctx.state(|state| state.seen);
            ctx.wait_condition(|state| state.seen > seen).await?;
            // The flag belongs to the activation, so it is consulted after
            // every wake. The SDK asks that handlers be quiescent before a
            // continue-as-new; the sync handler leaves nothing in flight.
            if ctx.continue_as_new_suggested() && ctx.all_handlers_finished() {
                let total = ctx.state(|state| state.seen);
                let never = ctx.continue_as_new(total, ContinueAsNewOptions::default())?;
                match never {}
            }
        }
    }

    #[signal(name = "bump")]
    fn bump(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: u64) {
        self.seen += 1;
    }
}

#[derive(Clone, Copy, Debug)]
enum Transport {
    InProcess,
    Listener,
}

fn unique_execution_names(transport: Transport) -> Result<(String, String)> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock must be after the Unix epoch")?
        .as_nanos();
    let name = format!(
        "sdk-continue-as-new-advice-{transport:?}-{}-{timestamp}",
        std::process::id()
    )
    .to_lowercase();
    Ok((name.clone(), name))
}

async fn probe(transport: Transport) -> Result<()> {
    // Sentinels keep the clean spike's proof that the engine opens no accidental
    // Temporal or Nexus listener of its own.
    let grpc_guard = TcpListener::bind("127.0.0.1:0").context("reserve gRPC sentinel port")?;
    let metrics_guard =
        TcpListener::bind("127.0.0.1:0").context("reserve metrics sentinel port")?;
    let nexus_guard = TcpListener::bind("127.0.0.1:0").context("reserve Nexus sentinel port")?;
    let mut config = TokeiraConfig::default();
    config.infrastructure.network.grpc_addr = grpc_guard.local_addr()?.to_string();
    config.infrastructure.network.metrics_addr = metrics_guard.local_addr()?.to_string();
    config.policy.nexus_completion.http_addr = nexus_guard.local_addr()?.to_string();
    let engine = Engine::start_with_config(config)
        .await
        .context("start zero-listener in-memory Tokeira engine")?;

    let listener = match transport {
        Transport::Listener => Some(
            engine
                .listen("127.0.0.1:0".parse()?)
                .await
                .context("attach a listener to the engine")?,
        ),
        Transport::InProcess => None,
    };
    let connection = match &listener {
        Some(listener) => Connection::connect(
            ConnectionOptions::new(Url::parse(&format!("http://{}", listener.bound_addr()))?)
                .build(),
        )
        .await
        .context("connect Temporal Rust SDK 1.0.0 over the listener")?,
        None => Connection::connect(
            ConnectionOptions::new(Url::parse(EMBEDDED_URL)?)
                .service_override(engine.service_override())
                .dns_load_balancing(None)
                .build(),
        )
        .await
        .context("connect Temporal Rust SDK 1.0.0 through service_override")?,
    };
    let client = Client::new(connection, ClientOptions::new("default".to_owned()).build())?;

    let runtime = Runtime::from_current_tokio(Default::default())?;
    let (task_queue, workflow_id) = unique_execution_names(transport)?;
    let options = WorkerOptions::new(task_queue.clone())
        .register_workflow::<AdviceProbeWorkflow>()?
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), options)?;
    let shutdown_worker = worker.shutdown_handle();

    let scenario = async {
        let handle = client
            .start_workflow(
                AdviceProbeWorkflow::run,
                0u64,
                WorkflowStartOptions::new(task_queue.clone(), workflow_id.clone()).build(),
            )
            .await
            .context("start the advice probe workflow")?;
        let first_run_id = handle
            .run_id()
            .context("the start handle carries the first run id")?
            .to_owned();

        // Signal run one until it closes. The handle is pinned to that run, so
        // a signal after the continue-as-new is refused rather than delivered to
        // run two; the describe check is the backstop if a refusal never comes.
        let mut accepted = 0u64;
        let mut batches = 0u64;
        loop {
            let outcomes = join_all((0..SIGNAL_BATCH).map(|_| {
                handle.signal(
                    AdviceProbeWorkflow::bump,
                    1u64,
                    WorkflowSignalOptions::default(),
                )
            }))
            .await;
            let refused = outcomes.iter().any(Result::is_err);
            accepted += outcomes.iter().filter(|outcome| outcome.is_ok()).count() as u64;
            batches += 1;
            if refused {
                break;
            }
            if batches % 4 == 0 {
                let description = handle.describe(WorkflowDescribeOptions::default()).await?;
                if description.status() != WorkflowExecutionStatus::Running {
                    break;
                }
            }
            ensure!(
                accepted <= SIGNAL_CAP,
                "run one absorbed {accepted} signals without continuing as new"
            );
        }

        // Run one closed by continuing as new; run two returned the carried state.
        let description = handle.describe(WorkflowDescribeOptions::default()).await?;
        ensure!(
            description.status() == WorkflowExecutionStatus::ContinuedAsNew,
            "run one ended {:?} after {accepted} accepted signals",
            description.status()
        );
        let carried: u64 = handle
            .get_result(WorkflowGetResultOptions::default())
            .await
            .context("follow the chain to the continued run's result")?;

        // The carried state is exactly the signals run one recorded.
        let history: Vec<_> = handle
            .fetch_history(WorkflowFetchHistoryOptions::default())
            .try_collect()
            .await
            .context("fetch run one's history")?;
        let signaled = history
            .iter()
            .filter(|event| event.event_type == EventType::WorkflowExecutionSignaled as i32)
            .count() as u64;
        ensure!(signaled > 0, "run one recorded no signals");
        ensure!(
            carried == signaled,
            "run two returned {carried} but run one recorded {signaled} signals"
        );
        ensure!(
            signaled <= accepted,
            "run one recorded {signaled} signals but only {accepted} were accepted"
        );
        let last = history.last().context("run one's history is empty")?;
        ensure!(
            last.event_type == EventType::WorkflowExecutionContinuedAsNew as i32,
            "run one's last event is {}",
            last.event_type
        );
        ensure!(
            history.len() >= COUNT_THRESHOLD,
            "the flip preceded the threshold: {} events",
            history.len()
        );

        // The flip the worker acted on is on the last started event, with the
        // count reason and the persisted size the server accounted.
        let advised = history
            .iter()
            .rev()
            .find_map(|event| match &event.attributes {
                Some(Attributes::WorkflowTaskStartedEventAttributes(attrs)) => Some(attrs.clone()),
                _ => None,
            })
            .context("run one has a started event")?;
        ensure!(
            advised.suggest_continue_as_new,
            "the last started event was not advised"
        );
        ensure!(
            advised.suggest_continue_as_new_reasons
                == vec![SuggestContinueAsNewReason::TooManyHistoryEvents as i32],
            "reasons {:?}",
            advised.suggest_continue_as_new_reasons
        );
        ensure!(
            advised.history_size_bytes > 0,
            "the started event carries no history size"
        );

        // Same workflow id, a new run id, and the namespace of the same client.
        let current = client
            .get_workflow_handle::<AdviceProbeWorkflow>(workflow_id.clone())
            .describe(WorkflowDescribeOptions::default())
            .await
            .context("describe the current run")?;
        ensure!(
            current.run_id() != first_run_id,
            "the continued run kept the run id"
        );
        ensure!(
            current.status() == WorkflowExecutionStatus::Completed,
            "the continued run ended {:?}",
            current.status()
        );
        let current_workflow_id = current
            .raw()
            .workflow_execution_info
            .as_ref()
            .and_then(|info| info.execution.as_ref())
            .map(|execution| execution.workflow_id.as_str());
        ensure!(
            current_workflow_id == Some(workflow_id.as_str()),
            "the continued run changed the workflow id: {current_workflow_id:?}"
        );

        shutdown_worker();
        Ok::<(), anyhow::Error>(())
    };
    let worker_run = worker.run();
    tokio::pin!(scenario);
    tokio::pin!(worker_run);
    tokio::select! {
        outcome = &mut scenario => outcome?,
        outcome = &mut worker_run => {
            return Err(match outcome {
                Ok(()) => anyhow!("SDK worker stopped before the scenario finished"),
                Err(error) => anyhow!(error).context("SDK worker failed mid-scenario"),
            });
        }
    }
    worker_run
        .await
        .context("SDK worker should shut down cleanly after the continue-as-new")?;
    drop(listener);
    engine
        .shutdown()
        .await
        .context("shut down embedded Tokeira engine")?;
    Ok(())
}

#[tokio::test]
async fn sdk_worker_continues_as_new_on_the_advice_in_process() -> Result<()> {
    tokio::time::timeout(SCENARIO_TIMEOUT, probe(Transport::InProcess))
        .await
        .context("in-process continue-as-new advice probe exceeded its timeout")??;
    Ok(())
}

#[tokio::test]
async fn sdk_worker_continues_as_new_on_the_advice_over_a_listener() -> Result<()> {
    tokio::time::timeout(SCENARIO_TIMEOUT, probe(Transport::Listener))
        .await
        .context("listener continue-as-new advice probe exceeded its timeout")??;
    Ok(())
}
