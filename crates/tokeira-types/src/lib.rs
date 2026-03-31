//! Shared durable-domain value types.
//!
//! This crate exists to keep the rest of the workspace honest. The moment a
//! crate starts inventing its own notion of `RunKey`, `LogicalTaskSeq`, or
//! `QueueKey`, the architecture begins to drift and cross-crate contracts get
//! fuzzy. The right level for those terms is a small shared crate.
//!
//! The types here intentionally avoid storage-driver details and transport
//! details. They should be usable from the kernel, runtime, storage, and future
//! edge/proto crates without carrying an accidental dependency on any one of
//! them.

pub mod execution;
pub mod ids;
pub mod payload;
pub mod request;
pub mod search_attributes;
pub mod task_queue;
pub mod tokens;
pub mod visibility;

pub use execution::*;
pub use ids::*;
pub use payload::*;
pub use request::*;
pub use search_attributes::*;
pub use task_queue::*;
pub use tokens::*;
pub use visibility::*;
