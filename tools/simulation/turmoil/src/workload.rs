//! What the worker does, through the public gRPC API.
//!
//! [`Worker`] is the worker host's client. It speaks the raw Temporal
//! protocol rather than an SDK, so every call is one the trace can name and
//! nothing runs outside the simulation. Like a client behind a load balancer,
//! it offers each call to one node and, when that node refuses it, to the
//! next.
//!
//! The scripts below are the workloads. Each runs workflows through the API,
//! records what the worker saw, and records each workflow's history in the
//! trace under a role (`"workflow"`, `"parent"`, `"child"`), for the oracles
//! to check. A script that runs out of patience on a call records that it
//! gave up and returns `Ok`, so the trace, not an error, shows how far the
//! run got.

use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context as _, bail, ensure};
use tokeira_edge::{
    CacheBackedRouter, EdgeRouter as _, RouteTarget, RoutingCache, subscribe_routing_once,
    translate::to_internal::namespace_id_for,
};
use tokeira_proto::{
    common::{ActivityType, RetryPolicy, WorkflowExecution, WorkflowType},
    enums::{CommandType, EventType},
    public::temporal::api::command::v1::{
        Command, CompleteWorkflowExecutionCommandAttributes, ScheduleActivityTaskCommandAttributes,
        StartChildWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
        command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        GetWorkflowExecutionHistoryRequest, PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
        RespondActivityTaskCompletedRequest, RespondWorkflowTaskCompletedRequest,
        StartWorkflowExecutionRequest, workflow_service_client::WorkflowServiceClient,
    },
};
use tokeira_types::{
    IncarnationId, RoutingSnapshot, ShardId, TaskKind, WorkflowId, bundle_for_partition,
    workflow_queue_partition,
};
use tonic::{
    Status,
    transport::{Channel, Endpoint},
};

use crate::{
    IDENTITY, NAMESPACE, PORT, Trace, elapsed, observe, record_history, text_payloads,
    topology::{
        BUNDLES, LEASE, NODE, NODES, PARTITIONS, controller_connection, controller_endpoint,
        placement_config,
    },
    turmoil_connector,
};

/// How long the timer in [`timer`] runs, in simulated time.
pub const TIMER: Duration = Duration::from_secs(60);
/// The start-to-close timeout of the activity in [`late_completion`].
pub const ACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);
/// The step [`late_completion`] records when the first attempt of its
/// activity reaches the worker, for a fault schedule to trigger on.
pub const FIRST_ATTEMPT_RECEIVED: &str = "activity attempt 1 received";

const TASK_QUEUE: &str = "turmoil-queue";
/// The node [`work_a_lease_later`] homes its workflow on, for a fault
/// schedule to cut off.
pub const CUT_NODE: &str = NODES[0];
/// The step [`work_a_lease_later`] records once it has chosen a workflow
/// homed on [`CUT_NODE`], for a fault schedule to trigger on.
pub const HOME_CHOSEN: &str = "the workflow will be homed on";
/// How long [`work_a_lease_later`] waits, after [`HOME_CHOSEN`], before it
/// starts its workflow: a lease, and a margin for the renewal the node made
/// just before a fault cut it off.
const LAPSE_WAIT: Duration = Duration::from_secs(LEASE.as_secs() + 2);
/// The step [`child_across_homes`] records just before it answers the
/// parent's first task by starting the child, for a fault schedule to
/// trigger on before the parent's home starts delivering the child.
pub const STARTING_THE_CHILD: &str = "parent task: starting the child";
/// The step [`child_across_homes`] records just before it completes the
/// child, for a fault schedule to trigger on before the child's home starts
/// delivering its result.
pub const COMPLETING_THE_CHILD: &str = "child task: completing the child";
/// How long the activity in [`late_completion`] works on each attempt.
const ACTIVITY_WORK: Duration = Duration::from_secs(2);
/// How long the worker waits for both nodes of a cluster to own bundles.
const SETTLE_PATIENCE: Duration = Duration::from_secs(120);
/// How long the worker keeps offering one call to the nodes before it gives
/// up and records that no node served it.
const CALL_PATIENCE: Duration = Duration::from_secs(120);
/// The worker's pause between rounds of offering a refused call. It is the
/// simulated worker's own backoff, in simulated time, not a test waiting on
/// real time.
const RETRY_DELAY: Duration = Duration::from_millis(500);
/// How long one poll may wait on a node for a task, when there is another
/// node to try.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the worker waits on one node's reply, when there is another node
/// to try, before it counts the call as refused. It outlasts a poll, so it
/// cuts off only a node that will never answer, such as a crashed one, as a
/// real client's own deadline would.
const CALL_TIMEOUT: Duration = Duration::from_secs(POLL_TIMEOUT.as_secs() + 5);

/// Where a scenario puts its task queue, relative to the two places that
/// decide an activity queue's home.
///
/// The edge sends an activity poll to the node that owns the bundle it
/// computes from the namespace *name* and the Activity kind. The runtime
/// serves the poll only at the queue's home, which it computes from the
/// namespace *id* and the Workflow kind for every queue. Whether the two
/// bundles have one owner depends on the queue name and on placement, so the
/// worker searches for a queue name that gives the scenario it was asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueuePlacement {
    /// The edge's choice and the runtime's home are the same node.
    Agreeing,
    /// The edge's choice and the runtime's home are different nodes.
    Disagreeing,
}

/// One activity on the single node.
pub async fn one_activity(trace: Arc<Mutex<Trace>>) -> anyhow::Result<()> {
    let worker = Worker::new(&[NODE], trace)?;
    run_one_activity(&worker, TASK_QUEUE, "turmoil-one-activity").await
}

/// One activity on the cluster. The workflow is homed on the node that is
/// not its task queue's home, so its tasks start across nodes, and the queue
/// is placed as `placement` asks.
pub async fn one_activity_across_nodes(
    trace: Arc<Mutex<Trace>>,
    placement: QueuePlacement,
) -> anyhow::Result<()> {
    let worker = Worker::new(&NODES, trace)?;
    let routing = settled_placement().await?;
    let snapshot = routing.snapshot();
    worker.observe(describe_placement(&snapshot));
    let (queue, edge_host, home_host) = pick_queue(&routing, &snapshot, placement, "").await?;
    worker.observe(format!(
        "task queue {queue}: the edge sends activity polls to {edge_host}, the runtime serves \
         them on {home_host}"
    ));
    let (workflow_id, workflow_host) = pick_workflow(&routing, "turmoil-workflow", &home_host)?;
    worker.observe(format!(
        "workflow {workflow_id} is homed on {workflow_host}"
    ));
    run_one_activity(&worker, &queue, &workflow_id).await
}

/// One workflow on the cluster, started a lease after the worker chose it.
///
/// The workflow is homed on [`CUT_NODE`], and its task queue on the other
/// node. The worker records [`HOME_CHOSEN`], then waits a lease and a margin,
/// so that a fault applied on that step has had a full lease to take effect:
/// a stall of the node's renewals or a crash has lapsed its leases, and a
/// restart of the node or of the controller has had time to settle. Then it
/// starts the workflow and completes it on its first task. Whether it gets
/// that far shows whether some node serves the home.
pub async fn work_a_lease_later(trace: Arc<Mutex<Trace>>) -> anyhow::Result<()> {
    let worker = Worker::new(&NODES, trace)?;
    let routing = settled_placement().await?;
    let snapshot = routing.snapshot();
    worker.observe(describe_placement(&snapshot));
    let (queue, _, queue_host) =
        pick_queue(&routing, &snapshot, QueuePlacement::Agreeing, CUT_NODE).await?;
    let other = NODES
        .into_iter()
        .find(|host| *host != CUT_NODE)
        .expect("the cluster has a second node");
    let (workflow_id, home) = pick_workflow(&routing, "turmoil-lapse", other)?;
    worker.observe(format!(
        "{HOME_CHOSEN} {home}: {workflow_id}, with task queue {queue} on {queue_host}"
    ));
    tokio::time::sleep(LAPSE_WAIT).await;
    worker.observe(format!(
        "waited {} s for the lease on {home} to lapse",
        LAPSE_WAIT.as_secs()
    ));
    if !worker.start(&workflow_id, "lapse", &queue).await {
        return Ok(());
    }
    let Some(task) = worker.poll_workflow_task(&queue).await else {
        return Ok(());
    };
    if !worker
        .complete_workflow_task(task.task_token, vec![complete_workflow("finished")])
        .await
    {
        return Ok(());
    }
    worker.record_history("workflow", &workflow_id).await;
    Ok(())
}

async fn run_one_activity(worker: &Worker, queue: &str, workflow_id: &str) -> anyhow::Result<()> {
    if !worker.start(workflow_id, "one-activity", queue).await {
        return Ok(());
    }
    let Some(first) = worker.poll_workflow_task(queue).await else {
        return Ok(());
    };
    let schedule = schedule_activity(queue, None);
    if !worker
        .complete_workflow_task(first.task_token, vec![schedule])
        .await
    {
        return Ok(());
    }
    let Some(activity) = worker.poll_activity_task(queue).await else {
        return Ok(());
    };
    if !worker.complete_activity_task(activity.task_token).await {
        return Ok(());
    }
    let Some(second) = worker.poll_workflow_task(queue).await else {
        return Ok(());
    };
    if !worker
        .complete_workflow_task(second.task_token, vec![complete_workflow("finished")])
        .await
    {
        return Ok(());
    }
    worker.record_history("workflow", workflow_id).await;
    Ok(())
}

/// A workflow on the single node whose first task starts a [`TIMER`]-long
/// timer and whose second task, caused by the timer firing, completes it.
///
/// Besides the history, it checks that the node's clock is the simulation's:
/// the node stamps the firing on its clock and the worker receives the task
/// it caused on the simulation's. Both count from the epoch, so a firing
/// stamped after the worker already held that task means they differ.
pub async fn timer(trace: Arc<Mutex<Trace>>) -> anyhow::Result<()> {
    const WORKFLOW_ID: &str = "turmoil-timer";
    let worker = Worker::new(&[NODE], Arc::clone(&trace))?;
    if !worker.start(WORKFLOW_ID, "one-timer", TASK_QUEUE).await {
        return Ok(());
    }
    let Some(first) = worker.poll_workflow_task(TASK_QUEUE).await else {
        return Ok(());
    };
    let start_timer = Command {
        command_type: CommandType::StartTimer as i32,
        user_metadata: None,
        attributes: Some(Attributes::StartTimerCommandAttributes(
            StartTimerCommandAttributes {
                timer_id: "wait".to_owned(),
                start_to_fire_timeout: Some(proto_duration(TIMER)),
            },
        )),
    };
    if !worker
        .complete_workflow_task(first.task_token, vec![start_timer])
        .await
    {
        return Ok(());
    }
    worker.observe(format!("started a {}-second timer", TIMER.as_secs()));
    let Some(second) = worker.poll_workflow_task(TASK_QUEUE).await else {
        return Ok(());
    };
    let second_seen_ms = i64::try_from(elapsed().as_millis()).expect("a run's length fits an i64");
    if !worker
        .complete_workflow_task(second.task_token, vec![complete_workflow("finished")])
        .await
    {
        return Ok(());
    }
    worker.record_history("workflow", WORKFLOW_ID).await;

    let trace = trace.lock().expect("trace lock is never poisoned");
    let fired = trace
        .history("workflow")
        .iter()
        .find(|event| event.kind == EventType::TimerFired)
        .context("the history has no TimerFired event")?
        .stamped_ms;
    ensure!(
        fired <= second_seen_ms,
        "the node stamped the timer fired at {fired} ms, after the worker received the task \
         it caused at {second_seen_ms} ms: the node's clock is not the simulation's"
    );
    Ok(())
}

/// One activity on the single node, with a [`ACTIVITY_TIMEOUT`]
/// start-to-close timeout and retries a second apart. Each attempt works for
/// two seconds of simulated time before the worker completes it.
///
/// If the node refuses the first attempt's completion, the worker records
/// the refusal, takes the next attempt and completes that. It records
/// [`FIRST_ATTEMPT_RECEIVED`] when the first attempt arrives, so a fault
/// schedule can delay that completion.
pub async fn late_completion(trace: Arc<Mutex<Trace>>) -> anyhow::Result<()> {
    const WORKFLOW_ID: &str = "turmoil-late-completion";
    let worker = Worker::new(&[NODE], trace)?;
    if !worker
        .start(WORKFLOW_ID, "late-completion", TASK_QUEUE)
        .await
    {
        return Ok(());
    }
    let Some(first) = worker.poll_workflow_task(TASK_QUEUE).await else {
        return Ok(());
    };
    let retry = RetryPolicy {
        initial_interval: Some(proto_duration(Duration::from_secs(1))),
        backoff_coefficient: 1.0,
        maximum_attempts: 3,
        ..Default::default()
    };
    if !worker
        .complete_workflow_task(
            first.task_token,
            vec![schedule_activity(TASK_QUEUE, Some(retry))],
        )
        .await
    {
        return Ok(());
    }

    let attempt = loop {
        let Some(activity) = worker.poll_activity_task(TASK_QUEUE).await else {
            return Ok(());
        };
        let attempt = activity.attempt;
        worker.observe(format!("activity attempt {attempt} received"));
        // The activity's own work, in simulated time.
        tokio::time::sleep(ACTIVITY_WORK).await;
        match worker.try_complete_activity_task(activity.task_token).await {
            Ok(()) => {
                worker.observe(format!("completion of attempt {attempt} accepted"));
                break attempt;
            }
            Err(status) => worker.observe(format!(
                "completion of attempt {attempt} refused: {:?}: {}",
                status.code(),
                status.message()
            )),
        }
        if attempt >= 3 {
            bail!("every attempt's completion was refused");
        }
    };
    worker.observe(format!("the activity completed on attempt {attempt}"));

    let Some(second) = worker.poll_workflow_task(TASK_QUEUE).await else {
        return Ok(());
    };
    if !worker
        .complete_workflow_task(second.task_token, vec![complete_workflow("finished")])
        .await
    {
        return Ok(());
    }
    worker.record_history("workflow", WORKFLOW_ID).await;
    Ok(())
}

/// A parent workflow on the cluster that starts one child homed on the other
/// node and completes when the child's result reaches it.
///
/// The worker answers workflow tasks as they come, by workflow type, until
/// the parent completes: a child task completes the child; a parent task
/// starts the child if it has not, completes the parent once the child's
/// result is in its history, and otherwise answers with no commands.
pub async fn child_across_homes(trace: Arc<Mutex<Trace>>) -> anyhow::Result<()> {
    let worker = Worker::new(&NODES, trace)?;
    let routing = settled_placement().await?;
    let snapshot = routing.snapshot();
    worker.observe(describe_placement(&snapshot));
    let (queue, _, _) = pick_queue(&routing, &snapshot, QueuePlacement::Agreeing, "").await?;
    let (parent_id, parent_host) = pick_workflow(&routing, "turmoil-parent", "")?;
    let (child_id, child_host) = pick_workflow(&routing, "turmoil-child", &parent_host)?;
    worker.observe(format!(
        "parent {parent_id} is homed on {parent_host}, child {child_id} on {child_host}"
    ));
    if !worker.start(&parent_id, "parent", &queue).await {
        return Ok(());
    }

    loop {
        let Some(task) = worker.poll_workflow_task(&queue).await else {
            return Ok(());
        };
        let workflow_type = task
            .workflow_type
            .as_ref()
            .map(|kind| kind.name.as_str())
            .unwrap_or_default()
            .to_owned();
        let seen: BTreeSet<i32> = task
            .history
            .as_ref()
            .map(|history| {
                history
                    .events
                    .iter()
                    .map(|event| event.event_type)
                    .collect()
            })
            .unwrap_or_default();
        let has = |kind: EventType| seen.contains(&(kind as i32));
        let (commands, what, parent_done) = match workflow_type.as_str() {
            "child" => (
                vec![complete_workflow("child finished")],
                COMPLETING_THE_CHILD,
                false,
            ),
            "parent" if has(EventType::ChildWorkflowExecutionCompleted) => (
                vec![complete_workflow("parent finished")],
                "parent task: the child's result is in, completing the parent",
                true,
            ),
            "parent" if !has(EventType::StartChildWorkflowExecutionInitiated) => (
                vec![start_child(&child_id, &queue)],
                STARTING_THE_CHILD,
                false,
            ),
            "parent" => (Vec::new(), "parent task: waiting for the child", false),
            other => bail!("a task arrived for an unknown workflow type {other:?}"),
        };
        // Recorded before the answer is sent, so a fault triggered on it
        // lands before the node commits the answer and delivers its effect.
        worker.observe(what);
        if !worker
            .complete_workflow_task(task.task_token, commands)
            .await
        {
            return Ok(());
        }
        if parent_done {
            break;
        }
    }
    worker.record_history("parent", &parent_id).await;
    worker.record_history("child", &child_id).await;
    Ok(())
}

/// The worker host's client: one connection per node, and the trace.
#[derive(Debug)]
pub struct Worker {
    nodes: Vec<(&'static str, WorkflowServiceClient<Channel>)>,
    trace: Arc<Mutex<Trace>>,
}

impl Worker {
    /// A client for the nodes on `hosts`, offered calls in that order.
    ///
    /// Connections are lazy, so the first call, not this, waits for a node
    /// to start serving.
    pub fn new(hosts: &[&'static str], trace: Arc<Mutex<Trace>>) -> anyhow::Result<Self> {
        let nodes = hosts
            .iter()
            .map(|host| {
                let channel = Endpoint::from_shared(format!("http://{host}:{PORT}"))?
                    .connect_with_connector_lazy(turmoil_connector());
                Ok((*host, WorkflowServiceClient::new(channel)))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Self { nodes, trace })
    }

    /// Record that the worker saw `what`.
    pub fn observe(&self, what: impl Into<String>) {
        observe(&self.trace, what);
    }

    /// Offer one call to each node in turn until a node's reply satisfies
    /// `served`.
    ///
    /// Each distinct refusal is traced once, so the trace shows how nodes
    /// refuse without one line per retry. With more than one node, a node
    /// that doesn't answer within fifteen simulated seconds counts as
    /// refusing. Returns `None`, after tracing that it gave up, if no node
    /// served the call within two simulated minutes.
    pub async fn call<T, F, Fut>(
        &self,
        what: &str,
        served: impl Fn(&T) -> bool,
        mut call: F,
    ) -> Option<T>
    where
        F: FnMut(WorkflowServiceClient<Channel>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, Status>>,
    {
        let give_up_at = elapsed() + CALL_PATIENCE;
        let mut refusals = BTreeSet::new();
        let mut attempts = 0_u32;
        loop {
            let mut refused = false;
            for (host, client) in &self.nodes {
                attempts += 1;
                let reply = if self.nodes.len() > 1 {
                    tokio::time::timeout(CALL_TIMEOUT, call(client.clone()))
                        .await
                        .unwrap_or_else(|_| {
                            Err(Status::deadline_exceeded(format!(
                                "no reply within {} s",
                                CALL_TIMEOUT.as_secs()
                            )))
                        })
                } else {
                    call(client.clone()).await
                };
                match reply {
                    Ok(reply) => {
                        let reply = reply.into_inner();
                        if served(&reply) {
                            self.observe(format!("{what} served by {host}"));
                            return Some(reply);
                        }
                    }
                    Err(status) => {
                        refused = true;
                        let refusal = format!(
                            "{what} refused by {host}: {:?}: {}",
                            status.code(),
                            status.message()
                        );
                        if refusals.insert(refusal.clone()) {
                            self.observe(refusal);
                        }
                    }
                }
            }
            if elapsed() >= give_up_at {
                self.observe(format!(
                    "gave up on {what} after {attempts} attempts across {} node(s)",
                    self.nodes.len()
                ));
                return None;
            }
            // A reply that served nothing is a long poll that lapsed: it has
            // already waited, so ask again at once, as a real worker does.
            // Only a refusal earns the backoff.
            if refused {
                tokio::time::sleep(RETRY_DELAY).await;
            }
        }
    }

    /// Start a workflow. Returns whether a node accepted it.
    pub async fn start(&self, workflow_id: &str, workflow_type: &str, queue: &str) -> bool {
        let request = StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: workflow_id.to_owned(),
            workflow_type: Some(WorkflowType {
                name: workflow_type.to_owned(),
            }),
            task_queue: task_queue(queue),
            request_id: format!("{workflow_id}-start"),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        };
        self.call(
            "StartWorkflowExecution",
            |_| true,
            |mut client| {
                let request = request.clone();
                async move { client.start_workflow_execution(request).await }
            },
        )
        .await
        .is_some()
    }

    /// Poll `queue` until a node hands out a workflow task.
    pub async fn poll_workflow_task(&self, queue: &str) -> Option<PollWorkflowTaskQueueResponse> {
        let request = PollWorkflowTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(queue),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        };
        self.call(
            "PollWorkflowTaskQueue",
            |task: &PollWorkflowTaskQueueResponse| !task.task_token.is_empty(),
            |mut client| {
                let request = self.poll_request(request.clone());
                async move { client.poll_workflow_task_queue(request).await }
            },
        )
        .await
    }

    /// Complete a workflow task with `commands`. Returns whether a node
    /// accepted it.
    pub async fn complete_workflow_task(
        &self,
        task_token: Vec<u8>,
        commands: Vec<Command>,
    ) -> bool {
        let request = RespondWorkflowTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token,
            commands,
            identity: IDENTITY.to_owned(),
            ..Default::default()
        };
        self.call(
            "RespondWorkflowTaskCompleted",
            |_| true,
            |mut client| {
                let request = request.clone();
                async move { client.respond_workflow_task_completed(request).await }
            },
        )
        .await
        .is_some()
    }

    /// Poll `queue` until a node hands out an activity task.
    pub async fn poll_activity_task(&self, queue: &str) -> Option<PollActivityTaskQueueResponse> {
        let request = PollActivityTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(queue),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        };
        self.call(
            "PollActivityTaskQueue",
            |task: &PollActivityTaskQueueResponse| !task.task_token.is_empty(),
            |mut client| {
                let request = self.poll_request(request.clone());
                async move { client.poll_activity_task_queue(request).await }
            },
        )
        .await
    }

    /// Complete an activity task. Returns whether a node accepted it.
    pub async fn complete_activity_task(&self, task_token: Vec<u8>) -> bool {
        let request = activity_completed(task_token);
        self.call(
            "RespondActivityTaskCompleted",
            |_| true,
            |mut client| {
                let request = request.clone();
                async move { client.respond_activity_task_completed(request).await }
            },
        )
        .await
        .is_some()
    }

    /// Offer an activity task's completion to the first node once, and
    /// return its answer: a refusal here is an outcome to record, not a
    /// reason to try again.
    pub async fn try_complete_activity_task(&self, task_token: Vec<u8>) -> Result<(), Status> {
        let (_, client) = self
            .nodes
            .first()
            .expect("a worker is built with at least one node");
        client
            .clone()
            .respond_activity_task_completed(activity_completed(task_token))
            .await
            .map(|_| ())
    }

    /// Read a workflow's history and record it under `role`. Returns whether
    /// a node served it.
    pub async fn record_history(&self, role: &str, workflow_id: &str) -> bool {
        let request = GetWorkflowExecutionHistoryRequest {
            namespace: NAMESPACE.to_owned(),
            execution: Some(WorkflowExecution {
                workflow_id: workflow_id.to_owned(),
                run_id: String::new(),
            }),
            ..Default::default()
        };
        let Some(history) = self
            .call(
                "GetWorkflowExecutionHistory",
                |_| true,
                |mut client| {
                    let request = request.clone();
                    async move { client.get_workflow_execution_history(request).await }
                },
            )
            .await
        else {
            return false;
        };
        record_history(
            &self.trace,
            role,
            history.history.unwrap_or_default().events,
        );
        true
    }

    /// A poll that, when there is another node to try, gives up on this one
    /// after [`POLL_TIMEOUT`]. With one node, the node's own long-poll
    /// timeout ends an empty poll.
    fn poll_request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        if self.nodes.len() > 1 {
            request.set_timeout(POLL_TIMEOUT);
        }
        request
    }
}

fn task_queue(name: &str) -> Option<TaskQueue> {
    Some(TaskQueue {
        name: name.to_owned(),
        ..Default::default()
    })
}

fn proto_duration(duration: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: i64::try_from(duration.as_secs()).expect("a scenario's duration fits an i64"),
        nanos: 0,
    }
}

fn schedule_activity(queue: &str, retry_policy: Option<RetryPolicy>) -> Command {
    Command {
        command_type: CommandType::ScheduleActivityTask as i32,
        user_metadata: None,
        attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
            ScheduleActivityTaskCommandAttributes {
                activity_id: "work-1".to_owned(),
                activity_type: Some(ActivityType {
                    name: "work".to_owned(),
                }),
                task_queue: task_queue(queue),
                start_to_close_timeout: Some(proto_duration(ACTIVITY_TIMEOUT)),
                retry_policy,
                ..Default::default()
            },
        )),
    }
}

fn start_child(workflow_id: &str, queue: &str) -> Command {
    Command {
        command_type: CommandType::StartChildWorkflowExecution as i32,
        user_metadata: None,
        attributes: Some(Attributes::StartChildWorkflowExecutionCommandAttributes(
            StartChildWorkflowExecutionCommandAttributes {
                workflow_id: workflow_id.to_owned(),
                workflow_type: Some(WorkflowType {
                    name: "child".to_owned(),
                }),
                task_queue: task_queue(queue),
                ..Default::default()
            },
        )),
    }
}

fn complete_workflow(result: &str) -> Command {
    Command {
        command_type: CommandType::CompleteWorkflowExecution as i32,
        user_metadata: None,
        attributes: Some(Attributes::CompleteWorkflowExecutionCommandAttributes(
            CompleteWorkflowExecutionCommandAttributes {
                result: Some(text_payloads(result)),
            },
        )),
    }
}

fn activity_completed(task_token: Vec<u8>) -> RespondActivityTaskCompletedRequest {
    RespondActivityTaskCompletedRequest {
        namespace: NAMESPACE.to_owned(),
        task_token,
        result: Some(text_payloads("done")),
        identity: IDENTITY.to_owned(),
        ..Default::default()
    }
}

/// Ask the controller for routing until every bundle has an owner and both
/// nodes own at least one, using the edge's own subscription code.
async fn settled_placement() -> anyhow::Result<Arc<RoutingCache>> {
    let connection = controller_connection();
    let mut last = String::from("no snapshot yet");
    while elapsed() < SETTLE_PATIENCE {
        let routing = Arc::new(RoutingCache::new(placement_config()));
        match subscribe_routing_once(
            Arc::clone(&routing),
            controller_endpoint(),
            Some(connection.clone()),
        )
        .await
        {
            Ok(()) => {
                let snapshot = routing.snapshot();
                let owners: BTreeSet<&str> = (0..BUNDLES)
                    .filter_map(|bundle| owner_host(&snapshot, ShardId(bundle)))
                    .collect();
                let every_bundle_owned =
                    (0..BUNDLES).all(|bundle| owner_host(&snapshot, ShardId(bundle)).is_some());
                let only_nodes_own = owners.iter().all(|host| NODES.contains(host));
                if every_bundle_owned && only_nodes_own && owners.len() == NODES.len() {
                    return Ok(routing);
                }
                last = describe_placement(&snapshot);
            }
            Err(error) => last = format!("{error:#}"),
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
    bail!("placement did not settle within {SETTLE_PATIENCE:?}; last seen: {last}")
}

/// The host that owns `bundle` in `snapshot`, if any.
fn owner_host(snapshot: &RoutingSnapshot, bundle: ShardId) -> Option<&str> {
    let owner = snapshot.lookup_bundle_owner(bundle)?;
    Some(snapshot.lookup_node_endpoint(owner.node_id)?.host.as_str())
}

fn describe_placement(snapshot: &RoutingSnapshot) -> String {
    let owners: Vec<String> = (0..BUNDLES)
        .map(|bundle| {
            format!(
                "bundle {bundle} on {}",
                owner_host(snapshot, ShardId(bundle)).unwrap_or("nobody")
            )
        })
        .collect();
    format!("placement settled: {}", owners.join(", "))
}

/// Find a queue name whose activity polls the edge sends to the runtime's
/// queue home (`Agreeing`) or away from it (`Disagreeing`), and whose home is
/// not on `avoid_host`. Returns the queue, the host the edge sends its
/// activity polls to, and the host the runtime serves them on.
///
/// The edge's side is asked of the edge's own router. The runtime's side uses
/// the function the runtime's queue-home provider calls
/// (`tokeira_runtime::discovery::PlacementQueueHomes::local_home`).
async fn pick_queue(
    routing: &Arc<RoutingCache>,
    snapshot: &RoutingSnapshot,
    placement: QueuePlacement,
    avoid_host: &str,
) -> anyhow::Result<(String, String, String)> {
    // An id no node has, so every route names the node it goes to.
    let router = CacheBackedRouter::new(Arc::clone(routing), IncarnationId::new());
    for index in 0..256 {
        let queue = format!("turmoil-queue-{index}");
        let RouteTarget::Remote { target } = router
            .route_task_queue(NAMESPACE, &queue, TaskKind::Activity)
            .await?
        else {
            bail!("the edge routed {queue} to an outside node id");
        };
        let edge_host = target.split(':').next().unwrap_or_default().to_owned();
        let home = bundle_for_partition(
            workflow_queue_partition(namespace_id_for(NAMESPACE), &queue, PARTITIONS),
            BUNDLES,
        );
        let home_host = owner_host(snapshot, home)
            .context("a settled placement owns every bundle")?
            .to_owned();
        if (edge_host == home_host) == (placement == QueuePlacement::Agreeing)
            && home_host != avoid_host
        {
            return Ok((queue, edge_host, home_host));
        }
    }
    bail!("no queue name among 256 gives {placement:?} placement away from {avoid_host:?}")
}

/// Find a workflow id starting `prefix` that is homed away from
/// `avoid_host`, and the host it is homed on. An empty `avoid_host` takes the
/// first id.
fn pick_workflow(
    routing: &RoutingCache,
    prefix: &str,
    avoid_host: &str,
) -> anyhow::Result<(String, String)> {
    for index in 0..256 {
        let workflow_id = format!("{prefix}-{index}");
        let (_, endpoint, _) = routing
            .resolve_execution_home(
                namespace_id_for(NAMESPACE),
                &WorkflowId(workflow_id.clone()),
            )
            .context("a settled placement routes every workflow")?;
        if endpoint.host != avoid_host {
            return Ok((workflow_id, endpoint.host));
        }
    }
    bail!("every workflow id among 256 starting {prefix} is homed on {avoid_host}")
}
