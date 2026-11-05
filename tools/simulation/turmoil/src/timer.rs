//! Step three: a workflow timer that fires in simulated time.
//!
//! Two simulated hosts:
//!
//! - `node` runs one listener-backed engine node with no controller, on a
//!   clock that reads turmoil's simulated time ([`SimClock`](crate::SimClock)). With no
//!   controller, the node owns its one bundle itself.
//! - `worker` starts a workflow, answers its first workflow task with a timer
//!   [`TIMER`] long, waits for the workflow task the timer's firing causes,
//!   completes the workflow, and reads the history.
//!
//! The node stamps the timer's deadline, judges it in its timer scanner and
//! stamps every history event on simulated time. So the timer fires after
//! [`TIMER`] of simulated time and no real time, and the event times in the
//! history are the same on every run of one seed. The trace records them as
//! milliseconds since the run's [`epoch`], the axis the
//! worker's own observations use, and the worker checks that the two agree.

use std::{error::Error, sync::Mutex, time::Duration};

use anyhow::{Context as _, anyhow, ensure};
use tokeira_engine::TokeiraConfig;
use tokeira_proto::{
    common::{WorkflowExecution, WorkflowType},
    enums::{CommandType, EventType},
    public::temporal::api::command::v1::{
        Command, CompleteWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
        command::Attributes,
    },
    workflowservice::{
        GetWorkflowExecutionHistoryRequest, PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse, StartWorkflowExecutionRequest,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_storage::InMemoryStore;
use tonic::transport::{Channel, Endpoint};

use crate::{
    EPOCH_UNIX_SECONDS, IDENTITY, NAMESPACE, NODE, PORT, Trace, WORKER, cluster::serve_piped_node,
    epoch, observe, respond_workflow_task, task_queue, text_payloads, turmoil_connector,
};

/// How long the workflow's timer runs, in simulated time.
pub const TIMER: Duration = Duration::from_secs(60);

const WORKFLOW_ID: &str = "turmoil-timer";
/// Room for the timer and the work around it. Simulated time costs real time
/// only in the ticks turmoil steps through, so this stays close to what the
/// run needs.
const SIMULATION_DURATION: Duration = Duration::from_secs(180);

/// Run one engine node and one scripted worker through a workflow that waits
/// on a [`TIMER`]-long timer, with turmoil's RNG seeded from `seed`.
///
/// Fails if the simulation fails, if the worker saw something it did not
/// expect, or if the history's times show the timer firing early or the
/// node's clock running apart from the simulation's.
pub fn run_timer_workflow(seed: u64) -> anyhow::Result<Trace> {
    let trace = std::sync::Arc::new(Mutex::new(Trace::default()));
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .epoch(epoch())
        .simulation_duration(SIMULATION_DURATION);
    let mut sim = builder.build();
    sim.host(NODE, serve_standalone_node);
    let observed = std::sync::Arc::clone(&trace);
    sim.client(WORKER, async move {
        // The whole chain, so a failing run says why a call failed and not
        // only which call it was.
        run_timer_script(&observed)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok(())
    });
    sim.run()
        .map_err(|error| anyhow!("simulation failed: {error}"))?;
    let trace = trace.lock().expect("trace lock is never poisoned").clone();
    Ok(trace)
}

/// The node host's software: a piped node on its own store, with no
/// controller. The store lives inside the host, so a crash or bounce would
/// start the node afresh.
async fn serve_standalone_node() -> Result<(), Box<dyn Error>> {
    let mut config = TokeiraConfig::default();
    let placement = &mut config.infrastructure.placement;
    placement.node_host = NODE.to_owned();
    placement.node_port = Some(PORT);
    // The default placement is one shard in one bundle, which a node with no
    // controller seeds and owns itself. The default store homes every run
    // there.
    serve_piped_node(InMemoryStore::default(), config, None, || {}).await
}

/// The scripted worker: start the workflow, start a timer, wait for the
/// workflow task its firing causes, complete the workflow, then record and
/// check the history.
async fn run_timer_script(trace: &Mutex<Trace>) -> anyhow::Result<()> {
    // Lazy, so the first call, not the connect, waits for the node to start
    // serving.
    let mut client = WorkflowServiceClient::new(
        Endpoint::from_shared(format!("http://{NODE}:{PORT}"))?
            .connect_with_connector_lazy(turmoil_connector()),
    );

    client
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: WORKFLOW_ID.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "one-timer".to_owned(),
            }),
            task_queue: task_queue(),
            request_id: "turmoil-timer-start".to_owned(),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        })
        .await
        .context("StartWorkflowExecution")?;
    observe(trace, "started the workflow");

    let first = next_workflow_task(&mut client).await?;
    observe(trace, format!("workflow task {} received", first.attempt));
    respond_workflow_task(
        &mut client,
        first.task_token,
        Command {
            command_type: CommandType::StartTimer as i32,
            user_metadata: None,
            attributes: Some(Attributes::StartTimerCommandAttributes(
                StartTimerCommandAttributes {
                    timer_id: "wait".to_owned(),
                    start_to_fire_timeout: Some(prost_types::Duration {
                        seconds: i64::try_from(TIMER.as_secs()).expect("the timer fits an i64"),
                        nanos: 0,
                    }),
                },
            )),
        },
    )
    .await?;
    observe(trace, format!("started a {}-second timer", TIMER.as_secs()));

    let second = next_workflow_task(&mut client).await?;
    let second_seen_ms = elapsed_ms();
    observe(trace, format!("workflow task {} received", second.attempt));
    respond_workflow_task(
        &mut client,
        second.task_token,
        Command {
            command_type: CommandType::CompleteWorkflowExecution as i32,
            user_metadata: None,
            attributes: Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
                CompleteWorkflowExecutionCommandAttributes {
                    result: Some(text_payloads("finished")),
                },
            )),
        },
    )
    .await?;
    observe(trace, "completed the workflow");

    let history = client
        .get_workflow_execution_history(GetWorkflowExecutionHistoryRequest {
            namespace: NAMESPACE.to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: WORKFLOW_ID.to_owned(),
                run_id: String::new(),
            }),
            ..Default::default()
        })
        .await
        .context("GetWorkflowExecutionHistory")?
        .into_inner();
    let mut timer_started_ms = None;
    let mut timer_fired_ms = None;
    for event in history.history.unwrap_or_default().events {
        let kind = EventType::try_from(event.event_type).ok();
        let name = kind.map_or("EVENT_TYPE_UNKNOWN", |kind| kind.as_str_name());
        let stamped_ms = event
            .event_time
            .as_ref()
            .map(|time| {
                (time.seconds - epoch_seconds()) * 1_000 + i64::from(time.nanos) / 1_000_000
            })
            .with_context(|| format!("history event {} carries no time", event.event_id))?;
        match kind {
            Some(EventType::TimerStarted) => timer_started_ms = Some(stamped_ms),
            Some(EventType::TimerFired) => timer_fired_ms = Some(stamped_ms),
            _ => {}
        }
        observe(
            trace,
            format!(
                "history event {} stamped {stamped_ms} ms: {name}",
                event.event_id
            ),
        );
    }

    let started = timer_started_ms.context("the history has no TimerStarted event")?;
    let fired = timer_fired_ms.context("the history has no TimerFired event")?;
    let timer_ms = i64::try_from(TIMER.as_millis()).expect("the timer fits an i64");
    ensure!(
        fired - started >= timer_ms,
        "the timer fired {} ms after it started, sooner than its {timer_ms} ms",
        fired - started
    );
    // The node stamps events on its clock and the worker observes on the
    // simulation's. Both count from the epoch, so a firing stamped after the
    // worker already held the task it caused means the node's clock is not
    // the simulation's.
    ensure!(
        fired <= second_seen_ms,
        "the node stamped the timer fired at {fired} ms, after the worker received the task \
         it caused at {second_seen_ms} ms: the node's clock is not the simulation's"
    );
    Ok(())
}

/// Poll the workflow task queue until a task arrives. An empty reply is a
/// long poll that lapsed, in simulated time, with nothing to hand out.
async fn next_workflow_task(
    client: &mut WorkflowServiceClient<Channel>,
) -> anyhow::Result<PollWorkflowTaskQueueResponse> {
    loop {
        let task = client
            .poll_workflow_task_queue(PollWorkflowTaskQueueRequest {
                namespace: NAMESPACE.to_owned(),
                task_queue: task_queue(),
                identity: IDENTITY.to_owned(),
                ..Default::default()
            })
            .await
            .context("PollWorkflowTaskQueue")?
            .into_inner();
        if !task.task_token.is_empty() {
            return Ok(task);
        }
    }
}

fn epoch_seconds() -> i64 {
    i64::try_from(EPOCH_UNIX_SECONDS).expect("the epoch fits an i64")
}

/// Simulated milliseconds since the run's epoch, as the worker sees them.
fn elapsed_ms() -> i64 {
    let elapsed = turmoil::sim_elapsed().unwrap_or_default();
    i64::try_from(elapsed.as_millis()).expect("a run's length fits an i64")
}
