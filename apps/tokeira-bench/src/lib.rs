//! Client-side benchmark for a local `tokeirad`.
//!
//! Exposes a trivial `EchoWorkflow` shared between the worker binary and the
//! starter binary. Keeping the workflow in a library ensures both binaries
//! register the same type with the same name — a mismatch there shows up as
//! workflow-task failures and skews throughput numbers.

pub mod workflows;

pub use workflows::EchoWorkflow;

/// Default task queue name used by the bench-worker and the bench-starter.
///
/// A dedicated task queue keeps the bench isolated from other workloads on the
/// same `tokeirad` instance.
pub const BENCH_TASK_QUEUE: &str = "tokeira-bench";
