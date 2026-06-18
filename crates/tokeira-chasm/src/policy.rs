//! Business-id reuse/conflict policy for `StartExecution`.
//!
//! Mirrors the targeted release's `chasm.BusinessIDReusePolicy` /
//! `chasm.BusinessIDConflictPolicy` and the `WithBusinessIDPolicy(reuse, conflict)`
//! start option (`chasm` package + `service/history/chasm_engine.go @ v1.31.0`).
//! These are pure value types — the enforcement matrix that consumes them lives in
//! the runtime engine, not here (the kernel/chasm crate stays free of I/O and
//! engine logic).
//!
//! Semantics (enforced by the engine against the current run for a business id):
//! - **Conflict policy** decides the outcome when the current run is **live**.
//! - **Reuse policy** decides the outcome when the current run is **terminal**.
//!
//! Defaults are `AllowDuplicate` / `Fail`, matching `defaultTransitionOptions`
//! (`chasm_engine.go:65 @ v1.31.0`) and the activity validator's normalization of
//! an unspecified policy (`chasm/lib/activity/validator.go:210 @ v1.31.0`).

/// Policy applied when a Start collides with a **terminal** current run for the
/// same business id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BusinessIdReusePolicy {
    /// Always allow a new run (the default).
    #[default]
    AllowDuplicate,
    /// Allow a new run only if the terminal current run did **not** complete
    /// successfully (failed / canceled / terminated / timed out) — otherwise
    /// reject (`chasm_engine.go:1070 @ v1.31.0`).
    AllowDuplicateFailedOnly,
    /// Reject any new run once a run for this id has reached a terminal state
    /// (`chasm_engine.go:1084 @ v1.31.0`).
    RejectDuplicate,
}

/// Policy applied when a Start collides with a **live** current run for the same
/// business id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BusinessIdConflictPolicy {
    /// Reject the Start with an already-started error naming the current run (the
    /// default — `chasm_engine.go:1018 @ v1.31.0`).
    #[default]
    Fail,
    /// Return the existing live run rather than starting a new one
    /// (`chasm_engine.go:1041 @ v1.31.0`).
    UseExisting,
    /// Terminate the existing run and start a new one. Unsupported in the targeted
    /// release's CHASM engine (`chasm_engine.go:1029-1041 @ v1.31.0` answers
    /// `Unimplemented`); the activity edge never maps a request to this variant.
    TerminateExisting,
}

/// The pair of policies carried on a Start, mirroring `WithBusinessIDPolicy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BusinessIdPolicy {
    /// Applied against a terminal current run.
    pub reuse: BusinessIdReusePolicy,
    /// Applied against a live current run.
    pub conflict: BusinessIdConflictPolicy,
}
