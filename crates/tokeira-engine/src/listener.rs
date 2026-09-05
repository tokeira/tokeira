//! Host-attached TCP listener over a running embedded [`Engine`].
//!
//! The in-process endpoint dispatches into a tonic `Routes` value assembled
//! once per engine from the Workflow, Operator, and Admin services. `Routes`
//! is cheaply cloneable, so a listener is a second consumer of that same
//! value: bind, mount a clone, serve. No service, interceptor, runtime, or
//! storage object is constructed a second time, which is what makes network
//! and in-process clients observe one engine by construction
//! (spec `.kiro/specs/embedded-engine-listener/`).
//!
//! Invariants this module owns:
//!
//! - Every start path stays zero-listener; only [`Engine::listen`] binds.
//! - A listener's stop token is a child of the engine's cancellation token, so
//!   no listener can outlive engine shutdown or drop.
//! - Stopping a listener resets its in-flight calls (see [`ResetOnStop`])
//!   rather than waiting for long polls to expire; the engine's shutdown
//!   deadline is therefore never spent on a parked poller.
//! - Bind happens before any engine state changes, so a failed bind leaves the
//!   engine exactly as it was.

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use anyhow::{Context as _, Result};
use hyper_legacy::{Body, Request, Response};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Status, body::BoxBody, transport::Server};
use tower::{Layer, Service};

use crate::{EmbeddedShutdownFailure, Engine};

/// Bound on how long an explicit [`EngineListener::shutdown`] waits for the
/// server task after resetting its in-flight calls. Matches the engine's own
/// shutdown deadline so the two paths report the same patience.
const LISTENER_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

/// Why [`Engine::listen`] could not attach a listener.
#[derive(Debug)]
pub enum EngineListenError {
    /// The requested address could not be bound. The engine is unchanged.
    Bind {
        /// Address the host asked for.
        addr: SocketAddr,
        /// Operating-system failure from `bind` or `local_addr`.
        source: std::io::Error,
    },
    /// Engine shutdown had already begun, so no listener may attach.
    ShutDown,
}

impl std::fmt::Display for EngineListenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bind { addr, source } => write!(
                formatter,
                "failed to bind embedded engine listener on {addr}: {source}"
            ),
            Self::ShutDown => formatter.write_str("embedded Tokeira engine is shutting down"),
        }
    }
}

impl std::error::Error for EngineListenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind { source, .. } => Some(source),
            Self::ShutDown => None,
        }
    }
}

/// Why an explicit [`EngineListener::shutdown`] did not complete cleanly.
#[derive(Debug)]
pub enum EngineListenerShutdownError {
    /// The server task did not exit within the listener deadline.
    DrainTimeout {
        /// Address the listener was bound to.
        addr: SocketAddr,
    },
    /// The server task failed or panicked.
    Task {
        /// Address the listener was bound to.
        addr: SocketAddr,
        /// Failure description from the server task.
        reason: String,
    },
}

impl std::fmt::Display for EngineListenerShutdownError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DrainTimeout { addr } => {
                write!(
                    formatter,
                    "timed out draining embedded engine listener on {addr}"
                )
            }
            Self::Task { addr, reason } => write!(
                formatter,
                "embedded engine listener task on {addr} failed: {reason}"
            ),
        }
    }
}

impl std::error::Error for EngineListenerShutdownError {}

/// One attached listener.
///
/// Dropping the handle signals the listener to stop without awaiting it; the
/// engine keeps serving its in-process endpoint and any other listener. Call
/// [`Self::shutdown`] to stop, drain, and join deterministically.
#[derive(Debug)]
pub struct EngineListener {
    slot: ListenerSlot,
    registry: ListenerRegistry,
}

impl EngineListener {
    /// The address the operating system bound.
    ///
    /// An unspecified request (`0.0.0.0` or `[::]`) is reported unspecified,
    /// with the concrete port. Only the host knows which interface other
    /// processes can reach, so the engine never substitutes one.
    pub fn bound_addr(&self) -> SocketAddr {
        self.slot.bound_addr
    }

    /// Stop accepting connections, reset in-flight calls, and join the server
    /// task within the listener deadline.
    ///
    /// The engine and its in-process endpoint keep serving afterwards.
    pub async fn shutdown(self) -> Result<(), EngineListenerShutdownError> {
        let deadline = Instant::now() + LISTENER_SHUTDOWN_DEADLINE;
        self.slot.stop.cancel();
        let outcome = self.slot.join(deadline).await;
        self.registry.remove(&self.slot);
        let addr = self.slot.bound_addr;
        match outcome {
            JoinOutcome::Completed => Ok(()),
            JoinOutcome::DeadlineExceeded => {
                Err(EngineListenerShutdownError::DrainTimeout { addr })
            }
            JoinOutcome::Failed(reason) => Err(EngineListenerShutdownError::Task { addr, reason }),
        }
    }
}

impl Drop for EngineListener {
    fn drop(&mut self) {
        // Signal only: an async join cannot run in `Drop`. The task exits on
        // its own once the accept loop closes and in-flight calls are reset;
        // deregistering here keeps the engine from owning a handle to work it
        // will never await.
        self.slot.stop.cancel();
        self.registry.remove(&self.slot);
    }
}

/// Registry entry shared between the handle and the engine.
///
/// The join handle sits in a shared slot so whichever side drains first
/// (explicit handle shutdown or engine shutdown) takes it, and the other side
/// observes an already-joined listener.
#[derive(Clone, Debug)]
pub(crate) struct ListenerSlot {
    bound_addr: SocketAddr,
    /// Child of the engine's cancellation token: stops the accept loop and
    /// resets in-flight calls.
    stop: CancellationToken,
    task: Arc<tokio::sync::Mutex<Option<JoinHandle<Result<()>>>>>,
}

enum JoinOutcome {
    Completed,
    DeadlineExceeded,
    Failed(String),
}

impl ListenerSlot {
    async fn join(&self, deadline: Instant) -> JoinOutcome {
        let Some(task) = self.task.lock().await.take() else {
            return JoinOutcome::Completed;
        };
        let now = Instant::now();
        if now >= deadline {
            task.abort();
            return JoinOutcome::DeadlineExceeded;
        }
        match tokio::time::timeout(deadline.saturating_duration_since(now), task).await {
            Ok(Ok(Ok(()))) => JoinOutcome::Completed,
            Ok(Ok(Err(error))) => JoinOutcome::Failed(format!("{error:#}")),
            Ok(Err(join_error)) => JoinOutcome::Failed(join_error.to_string()),
            Err(_elapsed) => {
                // The accept loop is closed and every in-flight call was reset,
                // so a task still alive here is stuck in transport teardown.
                // Abandon it rather than hold the engine's deadline hostage;
                // the reported failure says the drain was not clean.
                JoinOutcome::DeadlineExceeded
            }
        }
    }
}

/// Listeners attached to one engine, in attachment order.
#[derive(Clone, Debug, Default)]
pub(crate) struct ListenerRegistry {
    slots: Arc<std::sync::Mutex<Vec<ListenerSlot>>>,
}

impl ListenerRegistry {
    fn register(&self, slot: ListenerSlot) {
        self.slots
            .lock()
            .expect("listener registry poisoned")
            .push(slot);
    }

    fn remove(&self, slot: &ListenerSlot) {
        self.slots
            .lock()
            .expect("listener registry poisoned")
            .retain(|registered| !Arc::ptr_eq(&registered.task, &slot.task));
    }

    /// Number of listeners still registered.
    #[cfg(test)]
    pub(crate) fn attached(&self) -> usize {
        self.slots.lock().expect("listener registry poisoned").len()
    }

    /// Stop every attached listener and join it within `deadline`.
    ///
    /// Called by `Engine::shutdown` after the coordinator has closed
    /// in-process admission and signalled the runtime, and before the
    /// in-process drain: once this returns no network handler can still be
    /// admitted, so the existing drain, task join, lease release, ownership
    /// release, and storage close see the same world they did before
    /// listeners existed.
    pub(crate) async fn stop_all(
        &self,
        deadline: Instant,
        failures: &mut Vec<EmbeddedShutdownFailure>,
    ) {
        let slots = std::mem::take(&mut *self.slots.lock().expect("listener registry poisoned"));
        for slot in &slots {
            slot.stop.cancel();
        }
        for slot in slots {
            if !matches!(slot.join(deadline).await, JoinOutcome::Completed) {
                failures.push(EmbeddedShutdownFailure::ListenerDrain);
            }
        }
    }
}

impl Engine {
    /// Bind `addr` and serve this engine's Temporal gRPC services on it.
    ///
    /// Port `0` requests an ephemeral port; [`EngineListener::bound_addr`]
    /// reports the concrete one. The listener serves a clone of the engine's
    /// own routes, so authentication, authorization, namespaces, task queues,
    /// and storage are exactly those of the in-process endpoint. Attaching a
    /// listener touches no storage or ownership state and reads no
    /// configuration field.
    ///
    /// A failed bind returns [`EngineListenError::Bind`] with the engine
    /// unchanged. The reflection service is mounted alongside the Temporal
    /// services, as `tokeirad` mounts it.
    pub async fn listen(&self, addr: SocketAddr) -> Result<EngineListener, EngineListenError> {
        if self.background_cancel.is_cancelled() {
            return Err(EngineListenError::ShutDown);
        }
        // Bind before touching anything else: a refused address must leave no
        // task, token, or registration behind (Property 3).
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|source| EngineListenError::Bind { addr, source })?;
        let bound_addr = listener
            .local_addr()
            .map_err(|source| EngineListenError::Bind { addr, source })?;
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(tokeira_proto::public::FILE_DESCRIPTOR_SET)
            .build()
            .expect("the pinned Temporal file descriptor set builds a reflection service");

        let service = &self.endpoint.service;
        let routes = service.routes().await;
        let stop = self.background_cancel.child_token();
        let reset = stop.clone();
        let signal = stop.clone();
        // Spawn on the engine-host runtime, never the caller's: handler
        // futures must share the executor lifetime of the in-process path.
        let task = service.handler_runtime().spawn(async move {
            Server::builder()
                .layer(ResetOnStopLayer { stop: reset })
                .add_routes(routes)
                .add_service(reflection)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    signal.cancelled_owned(),
                )
                .await
                .with_context(|| {
                    format!("failed to serve embedded engine listener on {bound_addr}")
                })
        });
        let slot = ListenerSlot {
            bound_addr,
            stop,
            task: Arc::new(tokio::sync::Mutex::new(Some(task))),
        };
        self.listeners.register(slot.clone());
        tracing::info!(%bound_addr, "embedded engine listener bound");
        Ok(EngineListener {
            slot,
            registry: self.listeners.clone(),
        })
    }
}

/// Tower layer that resets in-flight calls when the listener stops.
///
/// tonic's graceful shutdown closes the accept loop and then waits for every
/// in-flight request to finish. A parked long poll finishes only when a task
/// arrives or its timeout (60 s by default) elapses, which would spend the
/// engine's whole shutdown deadline on one idle worker. The in-process
/// endpoint already treats caller cancellation as a server-side reset of the
/// handler (`AbortOnDropHandler` in `tokeira-edge`); this layer gives the
/// network path the same semantics on stop: the handler future is dropped,
/// its admission and RAII state are released, and the caller receives
/// `UNAVAILABLE`, exactly what an h2 reset would produce. Every engine
/// mutation the dropped handler started is fenced and deduplicated by the
/// runtime, so the reset is as safe as a worker dying mid-call.
#[derive(Clone)]
struct ResetOnStopLayer {
    stop: CancellationToken,
}

impl<S> Layer<S> for ResetOnStopLayer {
    type Service = ResetOnStop<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ResetOnStop {
            inner,
            stop: self.stop.clone(),
        }
    }
}

/// Service produced by [`ResetOnStopLayer`].
#[derive(Clone)]
struct ResetOnStop<S> {
    inner: S,
    stop: CancellationToken,
}

impl<S> Service<Request<Body>> for ResetOnStop<S>
where
    S: Service<Request<Body>, Response = Response<BoxBody>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let stop = self.stop.clone();
        let future = self.inner.call(request);
        Box::pin(async move {
            tokio::select! {
                biased;
                _ = stop.cancelled() => Ok(Status::unavailable(
                    "embedded Tokeira engine listener is stopping",
                )
                .to_http()),
                response = future => response,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tonic::codegen::http::HeaderValue;

    use super::*;

    /// Inner service that never resolves, standing in for a parked long poll.
    #[derive(Clone)]
    struct ParkedService;

    impl Service<Request<Body>> for ParkedService {
        type Response = Response<BoxBody>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Request<Body>) -> Self::Future {
            Box::pin(std::future::pending())
        }
    }

    #[tokio::test]
    async fn stop_resets_a_parked_call_with_unavailable() {
        let stop = CancellationToken::new();
        let mut service = ResetOnStopLayer { stop: stop.clone() }.layer(ParkedService);
        let request = Request::builder()
            .uri("/temporal.api.workflowservice.v1.WorkflowService/PollWorkflowTaskQueue")
            .body(Body::empty())
            .expect("static request builds");
        let call = tokio::spawn(service.call(request));
        stop.cancel();
        let response = call
            .await
            .expect("reset future completes")
            .expect("reset never errors");
        assert_eq!(
            response.headers().get("grpc-status"),
            Some(&HeaderValue::from_static("14")),
            "a reset call must surface UNAVAILABLE"
        );
    }

    #[tokio::test]
    async fn stop_all_on_an_empty_registry_records_nothing() {
        let registry = ListenerRegistry::default();
        let mut failures = Vec::new();
        registry
            .stop_all(Instant::now() + Duration::from_millis(10), &mut failures)
            .await;
        assert!(failures.is_empty());
        assert_eq!(registry.attached(), 0);
    }
}
