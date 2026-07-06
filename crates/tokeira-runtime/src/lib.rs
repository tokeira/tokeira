//! Lane-based runtime orchestration.
//!
//! The runtime is where pure semantics meet scheduling and delivery. It should
//! stay much thinner than a full server process: its job is to serialize commands
//! for a run, persist transitions, and publish derived effects.
//!
//! The runtime deliberately does not assume that all runs are hot, that all work
//! is poll-driven, or that a lane owns a run forever.

// Advisory clippy lints accepted in the runtime: orchestration entry points thread
// many context handles (stores, registries, clocks, keys) by design
// (`too_many_arguments`); lane/publisher signatures are inherently nested
// (`type_complexity`); and the command/effect enums carry one large authoritative
// variant whose boxing is a deliberate hot-path perf decision, not a lint fix
// (`large_enum_variant`, `enum_variant_names`).
#![allow(
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::large_enum_variant,
    clippy::enum_variant_names
)]

pub mod activity_timeout;
pub mod backlog;
pub mod batch;
pub mod broker;
pub mod buffered_queries;
#[cfg(test)]
mod bug_condition_exploration_tests;
pub mod chasm;
pub mod deployment_registry;
pub mod drain;
pub mod errors;
pub mod fairness;
pub mod heartbeat;
pub mod lane;
pub mod membership;
pub mod metrics;
pub mod nexus;
pub mod nexus_http;
pub mod publisher;
pub mod query;
pub mod query_consistency_model;
pub mod recovery;
pub mod retry;
pub mod runtime;
pub mod scanner;
pub mod schedule;
pub mod shard;
pub mod speculative_timer;
pub mod task_queue_config;
pub mod timeout;
pub mod update;
pub mod versioning;
pub mod wft_timeout;
pub mod worker_registry;

pub use activity_timeout::*;
pub use backlog::*;
pub use batch::*;
pub use broker::*;
pub use buffered_queries::*;
pub use deployment_registry::*;
pub use drain::*;
pub use errors::*;
pub use fairness::*;
pub use heartbeat::*;
pub use lane::*;
pub use membership::*;
pub use metrics::*;
pub use nexus::*;
pub use nexus_http::*;
pub use publisher::*;
pub use query::*;
pub use recovery::*;
pub use retry::*;
pub use runtime::*;
pub use scanner::*;
pub use schedule::*;
pub use shard::*;
pub use task_queue_config::*;
pub use timeout::*;
pub use update::*;
pub use versioning::*;
pub use wft_timeout::*;
pub use worker_registry::*;
