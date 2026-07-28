//! `tokeira-edge` — the **compatibility edge plane**: the Temporal-compatible
//! public API surface, and nothing more.
//!
//! Tokeira is organised into three planes (`docs/architecture/000-overview.md`).
//! This crate is the first one a client request touches:
//!
//! - **Compatibility edge** (this crate, `tokeira-proto`, `tokeira-types`) — admits,
//!   authenticates, validates, and translates requests; routes them to the owning
//!   shard/node; and shapes responses back onto the wire.
//! - **Authoritative runtime + storage** (`tokeira-kernel`, `tokeira-runtime`,
//!   `tokeira-storage`) — owns correctness: durable transitions, dispatch, timers.
//! - **Projection plane** (`tokeira-projection`) — derived read models (visibility).
//!
//! ## The load-bearing rule: the edge is thin
//!
//! The edge admits and translates; it does **not** implement durable workflow
//! semantics. If a change would alter workflow history ordering, retry semantics,
//! timer behaviour, or task durability, it belongs in `tokeira-kernel`,
//! `tokeira-runtime`, or `tokeira-storage` — not here. The edge's own logic is
//! confined to transport concerns: request validation, authorization, long-poll
//! blocking, routing to the right node, namespace resolution, and proto↔domain
//! translation. Workflow *meaning* lives below the edge boundary
//! (`crates/tokeira-edge/AGENTS.md`).
//!
//! ## Public-API behaviour is ground-truthed, not invented
//!
//! Every observable behaviour — field semantics, error code/message mapping,
//! defaulting, validation, lifecycle ordering — is pinned to the targeted Temporal
//! release (`TEMPORAL_SERVER_COMPAT`, currently `v1.31.0`; `AGENTS §8`). Where a
//! handler reproduces Temporal behaviour it cites the verifying source inline
//! (a proto path, or a server source path + tag). A handler whose justification is
//! "it makes the test pass" rather than "v1.31.0 does X, verified at `<path>@tag`"
//! is incomplete. Wire shape is ground-truthed against `proto/upstream/`, never the
//! generated artifacts under `target/`.
//!
//! ## Two layers: transport adapters over domain services
//!
//! Each public service is split in two so the gRPC/HTTP wiring stays separable from
//! the edge logic it drives:
//!
//! - **Domain services** — [`workflow_service::WorkflowService`] and
//!   [`operator_service::OperatorService`] hold the edge logic: they take already-
//!   decoded edge/domain inputs, run the interceptor (authz) + validation, call the
//!   runtime, and return [`errors::EdgeError`] on failure.
//! - **Transport adapters** — [`grpc`] wraps each domain service in a `tonic`
//!   server impl: it decodes the proto request, calls the domain service, and maps
//!   the result back to a proto response. [`http_api`] derives Temporal's
//!   annotated HTTP/JSON surface from the same public descriptors and dispatches
//!   through those existing gRPC adapters.
//!
//! The pivot between the two is [`errors::EdgeError`]: domain services speak it, and
//! `From<EdgeError> for tonic::Status` (`grpc::errors`) is the single place edge
//! errors become gRPC status codes (`BadRequest → InvalidArgument`,
//! `NotFound → NotFound`, `AlreadyExists → AlreadyExists`,
//! `FailedPrecondition → FailedPrecondition`, …). Keeping that mapping in one place
//! is what lets handlers carry the exact v1.31.0 message in the error and trust the
//! code is assigned consistently.
//!
//! ## Cross-cutting contracts a reader must respect
//!
//! - **Authorization** — every state-changing or reading RPC begins by calling
//!   [`interceptors::EdgeInterceptors::begin`] with the request headers, the target
//!   namespace (or `None` for cluster-global resources), and an
//!   [`interceptors::Action`]. A new RPC that skips this bypasses authz; route it
//!   through its domain service like the others.
//! - **Long-poll** — blocking RPCs (poll, `GetWorkflowExecutionHistory`,
//!   activity/Nexus describe) return *empty slightly before* the caller's deadline,
//!   leaving room to resubmit. [`long_poll`] caps concurrent long polls and
//!   [`history_wait`] supplies the wait primitive; the runtime is never asked to
//!   block.
//! - **Routing** — in a multi-node deployment a request may arrive at a node that
//!   does not own the target shard. [`routing`] + [`routing_cache`] resolve the
//!   owning node from a controller-fed snapshot and forward; ownership is
//!   authoritative below the edge, so the edge only *routes*, never decides.
//! - **Namespace resolution** — names are resolved to ids through
//!   [`namespace_cache`]; the deterministic id mapping ([`translate::to_internal`])
//!   is what keeps a name addressing the same execution lineage everywhere.
//!
//! ## Boundaries to the other planes
//!
//! - **CHASM components** — first-class (standalone) activities run on the CHASM
//!   engine in `tokeira-runtime`, not the workflow lane model.
//!   [`chasm_activity::ActivityBridge`] is the thin seam that translates the
//!   `*ActivityExecution` RPCs to CHASM engine calls; it owns no activity semantics
//!   (those are in `tokeira-chasm-activity`). Gated off by default — ahead of the
//!   v1.31.0 baseline (`AGENTS`).
//! - **Nexus endpoints** — [`nexus_endpoint`] is the OperatorService endpoint-admin
//!   registry CRUD; it reads/writes the neutral `NexusEndpointStore` that runtime
//!   dispatch also resolves against. Endpoint *operation execution* / task transport
//!   is a separate, larger surface owned elsewhere.
//! - **Projection** — visibility list/count/describe read models come from
//!   `tokeira-projection`; the edge re-exposes them but does not own them.
//!
//! ## Module map
//!
//! Request lifecycle: [`grpc`]/[`http_api`] (transport) → [`interceptors`] (authz)
//! → [`workflow_service`]/[`operator_service`] (edge logic) → [`routing`] (forward
//! if not local) → runtime, with [`translate`] converting at the wire boundary and
//! [`errors`] shaping every failure.
//!
//! - [`workflow_service`] / [`grpc`] — the WorkflowService surface and its adapter.
//! - [`operator_service`] / [`nexus_endpoint`] — OperatorService: search-attribute
//!   admin, cluster info, and Nexus endpoint CRUD.
//! - [`chasm_activity`] — the standalone-activity bridge over the CHASM engine.
//! - [`batch_engine`] — the edge-side driver loop for batch operations (visibility
//!   discovery + per-execution fan-out; the runtime owns persisted progress).
//! - [`translate`] — edge DTOs and proto↔domain conversion (`to_internal` /
//!   `from_internal` / `nexus` / `schedule` / `history_serializer`).
//! - [`errors`] — [`errors::EdgeError`], the edge's failure vocabulary.
//! - [`interceptors`] — the authz/observability seam (`begin`, [`interceptors::Action`]).
//! - [`long_poll`] / [`history_wait`] — long-poll admission and wait primitives.
//! - [`routing`] / [`routing_cache`] — shard-ownership-aware request forwarding.
//! - [`namespace_cache`] — name↔id resolution and namespace metadata.
//! - [`pending_queries`] / [`poller_registry`] — consistent-query completion and
//!   per-task-queue poller bookkeeping.
//! - [`request_id`] — canonical request-id extraction/generation.
//! - [`conformance`] — Tier-2 functional-conformance data models (the report shapes
//!   for replaying Temporal's Go suite against `tokeirad`).
//! - [`metrics`] — edge request metrics.
//! - [`health_service`] — the gRPC health surface.

// Advisory clippy lints accepted across this proto-translation crate:
// - `too_many_arguments`: request translators thread many wire fields by design.
// - `result_large_err`: fallible translators return tonic `Status`; boxing every
//   `Result` is churn without a measured win.
// - `type_complexity`: tower/tonic middleware and interceptor signatures are
//   inherently nested.
// - `needless_update`: `..Default::default()` on prost messages is deliberate
//   forward-compat — an upstream proto field addition cannot then break the build.
#![allow(
    clippy::too_many_arguments,
    clippy::result_large_err,
    clippy::type_complexity,
    clippy::needless_update
)]

pub mod batch_engine;
pub mod chasm_activity;
pub mod conformance;
pub mod errors;
pub mod grpc;
pub mod health_service;
pub mod history_wait;
pub mod http_api;
pub mod interceptors;
pub mod long_poll;
pub mod metrics;
pub mod namespace_cache;
pub mod nexus_callback;
pub mod nexus_endpoint;
pub mod nexus_http;
pub mod operator_service;
pub mod pending_queries;
pub mod poller_registry;
pub mod request_id;
pub mod routing;
pub mod routing_cache;
pub mod scoped_worker_session;
mod task_token;
pub mod translate;
mod worker_inventory;
pub mod workflow_rules;
pub mod workflow_service;

pub use batch_engine::*;
pub use conformance::*;
pub use errors::*;
pub use grpc::*;
pub use health_service::*;
pub use history_wait::*;
pub use http_api::*;
pub use interceptors::*;
pub use long_poll::*;
pub use metrics::*;
pub use namespace_cache::*;
pub use nexus_callback::*;
pub use operator_service::*;
pub use pending_queries::*;
pub use poller_registry::*;
pub use request_id::*;
pub use routing::*;
pub use routing_cache::*;
pub use scoped_worker_session::*;
pub use translate::*;
pub use workflow_service::*;
