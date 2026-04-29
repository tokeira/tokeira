//! Shared durable-domain value types.
//!
//! This crate exists to keep the rest of the workspace honest.
//! The moment a crate starts inventing its own notion of
//! `RunKey`, `LogicalTaskSeq`, or `QueueKey`, the architecture
//! begins to drift and cross-crate contracts get fuzzy. The
//! right level for those terms is a small shared crate.
//!
//! The types here intentionally avoid storage-driver details
//! and transport details. They should be usable from the
//! kernel, runtime, storage, and future edge/proto crates
//! without carrying an accidental dependency on any one of
//! them.
//!
//! # Design principles
//!
//! 1. **No behaviour** — types are data, not services. No I/O,
//!    no side effects.
//! 2. **No storage details** — types don't know about DSQL
//!    columns or table names.
//! 3. **No transport details** — types don't know about
//!    protobuf or gRPC.
//! 4. **Strong typing** — newtypes prevent mixing up `RunId`
//!    with `WorkflowId` or `NamespaceId`.
//! 5. **Minimal dependencies** — only `serde`, `time`, and
//!    `uuid`.
//!
//! See `docs/crates/types.md` for the full module map and
//! feature-coverage matrix.

/// Execution lifecycle types: status, refs, and summaries.
pub mod execution;
/// Core identity newtypes: `RunKey`, `RunId`, `ShardEpoch`, etc.
pub mod ids;
/// Shared observability naming and metric conventions.
pub mod observability;
/// Codec-neutral payload, header, and memo containers.
pub mod payload;
/// Edge-to-core request context for idempotency.
pub mod request;
/// Retry policy configuration carried on workflows/activities.
pub mod retry;
/// Typed search-attribute values for SQL-native visibility.
pub mod search_attributes;
/// Task-queue naming, queue keys, and sticky affinity.
pub mod task_queue;
/// Opaque task tokens used for fencing stale completions.
pub mod tokens;
/// Projection cursors and visibility-plane helpers.
pub mod visibility;

pub use execution::*;
pub use ids::*;
pub use observability::*;
pub use payload::*;
pub use request::*;
pub use retry::*;
pub use search_attributes::*;
pub use task_queue::*;
pub use tokens::*;
pub use visibility::*;
