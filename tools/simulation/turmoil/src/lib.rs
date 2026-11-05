//! Tokeira's real engine code on turmoil's simulated hosts and network.
//!
//! A scenario runs listener-backed engine nodes as turmoil hosts, drives them
//! through the public gRPC API from a scripted worker host, and checks what
//! happened. Every scenario has four parts, one module each:
//!
//! - [`topology`]: who runs. One node, or a placement controller and two
//!   nodes on a shared store. [`topology::Run`] holds the simulation and
//!   drives it.
//! - [`workload`]: what the worker does, through the public API, and the
//!   histories it reads back.
//! - [`faults`]: what goes wrong, and when. Faults are applied between
//!   simulation steps, triggered by a simulated time or by something the
//!   worker has seen.
//! - [`oracle`]: what must hold. Some checks run on every tick against the
//!   shared store; others run on the histories after the run.
//!
//! The scenarios themselves are the tests in `tests/scenarios/`, one module
//! per area of the engine.
//!
//! Every node reads the time from [`SimClock`], so the deadlines it stamps
//! and the scanners that judge them follow the simulation, and a run returns
//! a [`Trace`] that is the same for one seed on every run.

pub mod faults;
pub mod oracle;
pub mod topology;
pub mod workload;

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Mutex,
    time::{Duration, SystemTime},
};

use http::Uri;
use hyper_util::rt::TokioIo;
use time::OffsetDateTime;
use tokeira_clock::Clock;
use tokeira_proto::{
    common::{Payload, Payloads},
    enums::EventType,
    history::HistoryEvent,
};
use turmoil::net::TcpStream;

/// Simulated host name of the scripted worker.
pub const WORKER: &str = "worker";
/// Port every engine node serves gRPC on, inside the simulated network.
pub const PORT: u16 = 7233;
/// The namespace every scenario's workflows run in.
pub const NAMESPACE: &str = "default";
const IDENTITY: &str = "turmoil-worker";

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
/// Every node in a run, and every store handle a node, the controller or an
/// oracle holds, gets this clock, so a deadline one of them stamps is judged
/// against the same time by all of them. Each host reads its own simulated
/// clock, and turmoil moves every host's clock together.
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

/// What a run observed, in order, and the histories its workflows ended with.
///
/// Every value in it is simulated: times count from the [`epoch`], and run
/// ids and node incarnation ids are left out. So two runs from one seed can
/// be compared with `==`, as long as the topology itself replays (see
/// [`topology::cluster`] for where it does not yet).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Trace {
    /// One line per observation: simulated milliseconds, then what was seen.
    pub steps: Vec<String>,
    /// Each workflow's history, by the role the workload gave it, such as
    /// `"workflow"`, `"parent"` or `"child"`.
    pub histories: BTreeMap<String, Vec<Event>>,
}

/// One history event, as a scenario checks it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    /// The event's id within its history.
    pub id: i64,
    /// What happened.
    pub kind: EventType,
    /// When the node stamped the event, in simulated milliseconds since the
    /// [`epoch`].
    pub stamped_ms: i64,
}

impl Trace {
    /// The history the workload recorded for `role`; empty if it recorded
    /// none, for instance because the run gave up before reading it.
    pub fn history(&self, role: &str) -> &[Event] {
        self.histories.get(role).map_or(&[], Vec::as_slice)
    }

    /// Whether the history recorded for `role` ends with its workflow
    /// completing.
    pub fn completed(&self, role: &str) -> bool {
        self.history(role)
            .last()
            .is_some_and(|event| event.kind == EventType::WorkflowExecutionCompleted)
    }

    /// Whether any step contains `text`.
    pub fn saw(&self, text: &str) -> bool {
        self.steps.iter().any(|step| step.contains(text))
    }
}

impl fmt::Display for Trace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for step in &self.steps {
            writeln!(formatter, "{step}")?;
        }
        Ok(())
    }
}

/// Record that a host saw `what`, stamped with the simulated time. Call it
/// only from a simulated host.
fn observe(trace: &Mutex<Trace>, what: impl Into<String>) {
    record(trace, elapsed(), what);
}

/// Record `what` at simulated time `at`, for code outside the hosts, such as
/// the loop that applies faults between steps.
fn record(trace: &Mutex<Trace>, at: Duration, what: impl Into<String>) {
    let what = what.into();
    trace
        .lock()
        .expect("trace lock is never poisoned")
        .steps
        .push(format!("{:>7} ms  {what}", at.as_millis()));
}

/// Record a workflow's history under `role`, one step per event.
fn record_history(trace: &Mutex<Trace>, role: &str, events: Vec<HistoryEvent>) {
    let epoch_ms = i64::try_from(EPOCH_UNIX_SECONDS).expect("the epoch fits an i64") * 1_000;
    let events: Vec<Event> = events
        .into_iter()
        .map(|event| Event {
            id: event.event_id,
            kind: EventType::try_from(event.event_type).unwrap_or(EventType::Unspecified),
            stamped_ms: event.event_time.map_or(0, |time| {
                time.seconds * 1_000 + i64::from(time.nanos) / 1_000_000 - epoch_ms
            }),
        })
        .collect();
    for event in &events {
        observe(
            trace,
            format!(
                "{role} event {} stamped {} ms: {}",
                event.id,
                event.stamped_ms,
                event.kind.as_str_name()
            ),
        );
    }
    trace
        .lock()
        .expect("trace lock is never poisoned")
        .histories
        .insert(role.to_owned(), events);
}

/// Simulated time since the run started, as the current host sees it.
fn elapsed() -> Duration {
    turmoil::sim_elapsed().unwrap_or_default()
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

fn text_payloads(text: &str) -> Payloads {
    Payloads {
        payloads: vec![Payload {
            metadata: [("encoding".to_owned(), b"json/plain".to_vec())].into(),
            data: format!("\"{text}\"").into_bytes(),
            ..Default::default()
        }],
    }
}
