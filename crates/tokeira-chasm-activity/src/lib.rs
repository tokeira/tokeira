//! The standalone-activity archetype — CHASM component #1.
//!
//! This crate is the first Application State Machine on the CHASM substrate, and
//! the template every future archetype follows: it is "one ASM among many"
//! (foundation §4). It is a **pure** library — the activity state enum, the legal
//! transition table, the task definitions and their validators, request
//! validation, and the config — depending only on [`tokeira_chasm`] (the
//! substrate), the derive macro, and value/wire types. It contains **no engine
//! internals** (Requirement 1.4); the runtime engine drives it.
//!
//! Ground truth: Temporal `v1.31.0`
//! (`chasm/lib/activity/{activity.go,statemachine.go,validator.go,frontend.go,
//! config.go,library.go} @ v1.31.0`), per `AGENTS §8`. Where the design's sketch
//! and the targeted release differ, the release wins and the deviation is noted at
//! the call site.
//!
//! ## MVP materialization note
//!
//! The runtime engine materializes a component from a single root data node (Layer
//! 2 boundary), so the activity is modelled as **one `#[chasm(data)]` field**
//! carrying the whole [`ActivityState`] proto, rather than
//! the several `Field` children the design's low-level sketch shows. The full
//! multi-node field tree is a substrate enhancement deferred until a component
//! needs it; the activity's state fits one proto.
//!
//! ## Module map
//!
//! - [`state`] — the [`ActivityState`] proto, the
//!   [`ActivityStatus`] enum, and the lifecycle mapping.
//! - [`statemachine`] — the legal transition table and stamp-fenced `apply`.
//! - [`backoff`] — exponential retry-interval computation (pure, proto-free).
//! - [`retry`] — the [`retry_decision`] (`shouldRetry` +
//!   `hasEnoughTimeForRetry`).
//! - [`timeouts`] — pure derivation of the due/next timeout from `ActivityState`.
//! - [`tasks`] — the `dispatch` side-effect task and the pure timer tasks, with
//!   their validators.
//! - [`validator`] — request validation and timeout normalization.
//! - [`config`] — the config-as-constant activity configuration.
//! - [`component`] — the [`ActivityExecution`] root
//!   component and the `activity` library registration.

pub mod backoff;
pub mod component;
pub mod config;
pub mod retry;
pub mod state;
pub mod statemachine;
pub mod tasks;
pub mod timeouts;
pub mod validator;

pub use backoff::exponential_retry_interval;
pub use component::{ActivityExecution, ActivityLibrary, rebuild_visibility_snapshot};
pub use config::ActivityConfig;
pub use retry::{RetryOutcome, retry_decision};
pub use state::{ActivityState, ActivityStatus, lifecycle_for};
pub use statemachine::{ActivityEvent, TimeoutType, legal_target};
pub use tasks::{
    DISPATCH_TASK_ID, DispatchTask, HEARTBEAT_TASK_ID, HeartbeatTimer, SCHEDULE_TO_CLOSE_TASK_ID,
    SCHEDULE_TO_START_TASK_ID, START_TO_CLOSE_TASK_ID, ScheduleToCloseTimer, ScheduleToStartTimer,
    StartToCloseTimer,
};
pub use timeouts::{due_timeout, next_timeout_deadline};
pub use validator::{ActivityRequest, NormalizedTimeouts, validate_and_normalize};
