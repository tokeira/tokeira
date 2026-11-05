//! Tokeira's real engine code on turmoil's simulated hosts and network.
//!
//! Step one, in this module: one simulated host runs the embedded in-memory
//! [`Engine`] and serves its Temporal endpoint over turmoil's simulated TCP. A
//! simulated client host drives a one-activity workflow through the public
//! gRPC API with a scripted worker, then reads the workflow's history.
//! [`run_one_activity_workflow`] returns what the client observed as a
//! [`Trace`], so a test can compare two runs from one seed.
//!
//! The bridge in this module frames gRPC by hand and hands each call to
//! [`TemporalEndpoint::call`], the same in-process entry point the engine's own
//! integration tests drive. Everything between that call and the store is the
//! code that runs in production.
//!
//! Step two, in [`cluster`]: a placement controller and two listener-backed
//! nodes, with every connection between them on turmoil's network.
//!
//! Step three, in [`timer`]: a workflow timer that fires in simulated time.
//! The listener-backed nodes of steps two and three read the time from
//! [`SimClock`], so the deadlines they stamp and the scanners that judge them
//! follow the simulation. Step one's embedded [`Engine`] has no clock to
//! supply and still reads the host's clock, so it runs no timers.

pub mod cluster;
pub mod timer;

use std::{
    convert::Infallible,
    error::Error,
    future::Future,
    net::{IpAddr, Ipv4Addr},
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use anyhow::{Context as _, anyhow, ensure};
use bytes::{BufMut as _, Bytes, BytesMut};
use http::{HeaderMap, HeaderValue, Request, Response, Uri, header::CONTENT_TYPE};
use http_body_util::{BodyExt as _, Full, combinators::UnsyncBoxBody};
use hyper::{body::Incoming, server::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_engine::{Engine, InProcessGrpcRequest, InProcessGrpcResponse, TemporalEndpoint};
use tokeira_proto::{
    common::{ActivityType, Payload, Payloads, WorkflowExecution, WorkflowType},
    enums::{CommandType, EventType},
    public::temporal::api::command::v1::{
        Command, CompleteWorkflowExecutionCommandAttributes, ScheduleActivityTaskCommandAttributes,
        command::Attributes,
    },
    taskqueue::TaskQueue,
    workflowservice::{
        GetWorkflowExecutionHistoryRequest, PollActivityTaskQueueRequest,
        PollWorkflowTaskQueueRequest, RespondActivityTaskCompletedRequest,
        RespondWorkflowTaskCompletedRequest, StartWorkflowExecutionRequest,
        workflow_service_client::WorkflowServiceClient,
    },
};
use tonic::{
    Status,
    transport::{Channel, Endpoint},
};
use turmoil::net::{TcpListener, TcpStream};

/// Simulated host name of the engine node.
pub const NODE: &str = "node";
/// Simulated host name of the scripted worker.
pub const WORKER: &str = "worker";
/// Port the engine node serves gRPC on, inside the simulated network.
pub const PORT: u16 = 7233;

const NAMESPACE: &str = "default";
const TASK_QUEUE: &str = "turmoil-queue";
const WORKFLOW_ID: &str = "turmoil-one-activity";
const IDENTITY: &str = "turmoil-worker";
/// Simulated time the run may take. Each tick is turmoil's default 1 ms, and
/// computation takes no simulated time, so this bounds waiting, not work.
const SIMULATION_DURATION: Duration = Duration::from_secs(120);

/// The wall-clock time every simulated host starts at: 2026-01-01T00:00:00Z.
///
/// turmoil starts its clocks at the real time by default, so a run would
/// stamp different times on every run. Fixing the start keeps the times the
/// engine stamps, and with them a run's trace, the same for one seed.
pub const EPOCH_UNIX_SECONDS: u64 = 1_767_225_600;

/// [`EPOCH_UNIX_SECONDS`] as the start time turmoil's builder takes.
pub fn epoch() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(EPOCH_UNIX_SECONDS)
}

/// The engine's clock on a simulated host: turmoil's simulated time, counted
/// from the run's [`epoch`].
///
/// Every node in a run, and every store handle a node or the controller holds,
/// gets this clock, so a deadline one of them stamps is judged against the same
/// time by all of them. Each host reads its own simulated clock, and turmoil
/// moves every host's clock together.
///
/// # Panics
///
/// When read off a simulated host, where there is no simulated time to read.
/// The engine reads its clock from tasks on the host's runtime, so a panic
/// here means some engine code read the clock from a thread turmoil does not
/// drive.
#[derive(Clone, Copy, Debug, Default)]
pub struct SimClock;

impl Clock for SimClock {
    fn now(&self) -> OffsetDateTime {
        let since_epoch =
            turmoil::since_epoch().expect("the engine reads its clock only on a simulated host");
        OffsetDateTime::UNIX_EPOCH + since_epoch
    }
}

/// What the client host observed during one run, in order.
///
/// Each step carries the simulated time at which the client saw it. The
/// trace leaves out values that differ between runs by design, such as run
/// ids and the wall-clock event times the engine stamps, so two runs from one
/// seed can be compared with `==`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    /// One line per observation: simulated milliseconds, then what was seen.
    pub steps: Vec<String>,
}

impl Trace {
    /// Whether the run's history ends with the workflow completing.
    pub fn completed(&self) -> bool {
        self.steps
            .last()
            .is_some_and(|step| step.ends_with(EventType::WorkflowExecutionCompleted.as_str_name()))
    }
}

/// Run one engine node and one scripted worker through a one-activity
/// workflow, with turmoil's RNG seeded from `seed`.
///
/// Fails if the simulation fails (a host or the client returned an error, or
/// simulated time ran out) or if the worker saw something it did not expect.
pub fn run_one_activity_workflow(seed: u64) -> anyhow::Result<Trace> {
    let trace = Arc::new(Mutex::new(Trace::default()));
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .epoch(epoch())
        .simulation_duration(SIMULATION_DURATION);
    let mut sim = builder.build();
    sim.host(NODE, serve_engine);
    let observed = Arc::clone(&trace);
    sim.client(WORKER, async move {
        let mut client = connect().await?;
        run_worker_script(&mut client, &observed).await?;
        Ok(())
    });
    sim.run()
        .map_err(|error| anyhow!("simulation failed: {error}"))?;
    let trace = trace.lock().expect("trace lock is never poisoned").clone();
    Ok(trace)
}

/// The engine node's software: start the embedded engine and serve its
/// Temporal endpoint on [`PORT`] until the simulation ends.
///
/// The engine lives for as long as this future does. A crash or bounce drops
/// the future and with it the engine and its in-memory store, so state does not
/// survive a restart in this step.
async fn serve_engine() -> Result<(), Box<dyn Error>> {
    let engine = Engine::start().await?;
    let endpoint = engine.endpoint();
    let listener = TcpListener::bind((IpAddr::V4(Ipv4Addr::UNSPECIFIED), PORT)).await?;
    loop {
        let (stream, peer) = listener.accept().await?;
        let endpoint = endpoint.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| handle(endpoint.clone(), request));
            if let Err(error) = http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::warn!(%peer, %error, "gRPC connection to the engine node ended with an error");
            }
        });
    }
}

type ReplyBody = UnsyncBoxBody<Bytes, Infallible>;

/// Bridge one gRPC request to the engine's in-process endpoint.
///
/// The endpoint takes the service name, the method name, request metadata and
/// unframed protobuf, which is exactly what a gRPC request carries once its
/// path is split and its five-byte length prefix removed.
async fn handle(
    endpoint: TemporalEndpoint,
    request: Request<Incoming>,
) -> Result<Response<ReplyBody>, Infallible> {
    let (parts, body) = request.into_parts();
    let reply = match route(&parts.uri) {
        Some((service, rpc)) => match body.collect().await {
            Ok(collected) => match unframe(collected.to_bytes()) {
                Ok(proto) => {
                    endpoint
                        .call(InProcessGrpcRequest {
                            service,
                            rpc,
                            headers: forwarded_metadata(&parts.headers),
                            proto,
                        })
                        .await
                }
                Err(status) => Err(status),
            },
            Err(error) => Err(Status::internal(format!(
                "could not read the request body: {error}"
            ))),
        },
        None => Err(Status::unimplemented(format!(
            "no gRPC service and method in path {}",
            parts.uri.path()
        ))),
    };
    Ok(match reply {
        Ok(response) => ok_reply(response),
        Err(status) => status.into_http(),
    })
}

/// Split `/package.Service/Method` into its service and method.
fn route(uri: &Uri) -> Option<(String, String)> {
    let (service, rpc) = uri.path().strip_prefix('/')?.split_once('/')?;
    Some((service.to_owned(), rpc.to_owned()))
}

/// Request metadata minus the fields the HTTP/2 and gRPC transport own. The
/// in-process endpoint frames its own request, so passing these through would
/// describe a transport the call never used.
fn forwarded_metadata(headers: &HeaderMap) -> HeaderMap {
    headers
        .iter()
        .filter(|(name, _)| {
            let name = name.as_str();
            !(name.starts_with("grpc-")
                || matches!(
                    name,
                    "content-type" | "content-length" | "te" | "host" | "user-agent"
                ))
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Remove the gRPC length prefix from a single uncompressed message.
fn unframe(body: Bytes) -> Result<Bytes, Status> {
    let Some(prefix) = body.get(..5) else {
        return Err(Status::invalid_argument(
            "gRPC message is shorter than its prefix",
        ));
    };
    if prefix[0] != 0 {
        // The scripted worker never compresses; a compressed message here
        // means a client this bridge was not built for.
        return Err(Status::unimplemented(
            "compressed gRPC messages are not bridged",
        ));
    }
    let length = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]) as usize;
    if body.len() != 5 + length {
        return Err(Status::invalid_argument(
            "gRPC request must carry exactly one message",
        ));
    }
    Ok(body.slice(5..))
}

fn ok_reply(response: InProcessGrpcResponse) -> Response<ReplyBody> {
    let mut framed = BytesMut::with_capacity(5 + response.proto.len());
    framed.put_u8(0);
    framed.put_u32(
        u32::try_from(response.proto.len()).expect("a unary gRPC response fits a u32 length"),
    );
    framed.extend_from_slice(&response.proto);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let body = Full::new(framed.freeze())
        .with_trailers(async move { Some(Ok::<_, Infallible>(trailers)) })
        .boxed_unsync();
    let mut reply = Response::new(body);
    reply.headers_mut().extend(response.headers);
    reply
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
    reply
}

type ConnectFuture = Pin<Box<dyn Future<Output = std::io::Result<TokioIo<TcpStream>>> + Send>>;

/// Dials the host and port a URI names over turmoil's simulated TCP. tonic
/// and connect-rust both accept it as a connector.
fn turmoil_connector() -> impl tower::Service<
    Uri,
    Response = TokioIo<TcpStream>,
    Error = std::io::Error,
    Future = ConnectFuture,
> + Clone
+ Send
+ 'static {
    tower::service_fn(|uri: Uri| {
        Box::pin(async move {
            let host = uri.host().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "the URI names no host")
            })?;
            let port = uri.port_u16().unwrap_or(80);
            let stream = TcpStream::connect(format!("{host}:{port}")).await?;
            Ok(TokioIo::new(stream))
        }) as ConnectFuture
    })
}

/// A tonic channel to the engine node over turmoil's simulated TCP.
async fn connect() -> anyhow::Result<WorkflowServiceClient<Channel>> {
    let channel = Endpoint::from_shared(format!("http://{NODE}:{PORT}"))?
        .connect_with_connector(turmoil_connector())
        .await
        .context("could not connect to the engine node")?;
    Ok(WorkflowServiceClient::new(channel))
}

/// Record that the client saw `what`, stamped with the simulated time.
fn observe(trace: &Mutex<Trace>, what: impl Into<String>) {
    let at = turmoil::sim_elapsed().unwrap_or_default().as_millis();
    let what = what.into();
    trace
        .lock()
        .expect("trace lock is never poisoned")
        .steps
        .push(format!("{at:>7} ms  {what}"));
}

/// The scripted worker: start the workflow, schedule one activity, run it,
/// complete the workflow, then record the history the engine kept.
///
/// It speaks the raw Temporal protocol rather than an SDK, so every call is
/// one the trace can name and nothing runs outside the simulation.
async fn run_worker_script(
    client: &mut WorkflowServiceClient<Channel>,
    trace: &Mutex<Trace>,
) -> anyhow::Result<()> {
    let observe = |what: String| observe(trace, what);

    client
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_owned(),
            workflow_id: WORKFLOW_ID.to_owned(),
            workflow_type: Some(WorkflowType {
                name: "one-activity".to_owned(),
            }),
            task_queue: task_queue(),
            request_id: "turmoil-start".to_owned(),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        })
        .await
        .context("StartWorkflowExecution")?;
    observe("started the workflow".to_owned());

    let first = poll_workflow_task(client).await?;
    observe(format!("workflow task {} received", first.0));
    respond_workflow_task(
        client,
        first.1,
        Command {
            command_type: CommandType::ScheduleActivityTask as i32,
            user_metadata: None,
            attributes: Some(Attributes::ScheduleActivityTaskCommandAttributes(
                ScheduleActivityTaskCommandAttributes {
                    activity_id: "work-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: "work".to_owned(),
                    }),
                    task_queue: task_queue(),
                    start_to_close_timeout: Some(prost_types::Duration {
                        seconds: 10,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            )),
        },
    )
    .await?;
    observe("scheduled the activity".to_owned());

    let activity = client
        .poll_activity_task_queue(PollActivityTaskQueueRequest {
            namespace: NAMESPACE.to_owned(),
            task_queue: task_queue(),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        })
        .await
        .context("PollActivityTaskQueue")?
        .into_inner();
    ensure!(
        !activity.task_token.is_empty(),
        "the activity poll returned no task"
    );
    observe(format!("activity task {} received", activity.attempt));
    client
        .respond_activity_task_completed(RespondActivityTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token: activity.task_token,
            result: Some(text_payloads("done")),
            identity: IDENTITY.to_owned(),
            ..Default::default()
        })
        .await
        .context("RespondActivityTaskCompleted")?;
    observe("completed the activity".to_owned());

    let second = poll_workflow_task(client).await?;
    observe(format!("workflow task {} received", second.0));
    respond_workflow_task(
        client,
        second.1,
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
    observe("completed the workflow".to_owned());

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
    for event in history.history.unwrap_or_default().events {
        let name = EventType::try_from(event.event_type)
            .map_or("EVENT_TYPE_UNKNOWN", |kind| kind.as_str_name());
        observe(format!("history event {} {name}", event.event_id));
    }
    Ok(())
}

/// Poll the workflow task queue once; returns the attempt and the task token.
async fn poll_workflow_task(
    client: &mut WorkflowServiceClient<Channel>,
) -> anyhow::Result<(i32, Vec<u8>)> {
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
    ensure!(
        !task.task_token.is_empty(),
        "the workflow task poll returned no task"
    );
    Ok((task.attempt, task.task_token))
}

async fn respond_workflow_task(
    client: &mut WorkflowServiceClient<Channel>,
    task_token: Vec<u8>,
    command: Command,
) -> anyhow::Result<()> {
    client
        .respond_workflow_task_completed(RespondWorkflowTaskCompletedRequest {
            namespace: NAMESPACE.to_owned(),
            task_token,
            commands: vec![command],
            identity: IDENTITY.to_owned(),
            ..Default::default()
        })
        .await
        .context("RespondWorkflowTaskCompleted")?;
    Ok(())
}

fn task_queue() -> Option<TaskQueue> {
    Some(TaskQueue {
        name: TASK_QUEUE.to_owned(),
        ..Default::default()
    })
}

fn text_payloads(text: &str) -> Payloads {
    Payloads {
        payloads: vec![Payload {
            metadata: [("encoding".to_owned(), b"json/plain".to_vec())].into(),
            data: format!("\"{text}\"").into_bytes(),
            ..Default::default()
        }],
    }
}
