//! The deliberately-injectable placement bug.
//!
//! A simulator that can never fail proves nothing. This bug makes the model
//! violate a named safety invariant on demand, so both verification modes can
//! demonstrate real falsifying power — the placement analog of the broker
//! simulator's `--bug` defects. With no bug selected, all invariants pass.
//!
//! There is one defect because the placement design has one canonical
//! protocol-shape mistake worth guarding against: conflating queue-home (a
//! dispatch hint) with execution-home (the correctness boundary). It is the
//! original `placement-sim`'s `--buggy-start-routing` made selectable.

/// A selectable injected defect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InjectedBug {
    /// Route `StartWorkflow` by the workflow's advisory queue-home instead of
    /// its execution-home. Because the two are independent hashes, the start
    /// (almost always) commits on the wrong bundle, violating I1.
    BuggyStartRouting,
}

impl InjectedBug {
    /// Parse the `--bug=<name>` flag value. Returns `None` for an unknown name
    /// so the caller can report the supported set.
    pub fn from_flag(value: &str) -> Option<Self> {
        match value {
            "buggy-start-routing" => Some(Self::BuggyStartRouting),
            _ => None,
        }
    }

    /// The invariant this bug is expected to falsify, for the report line.
    pub fn expected_violation(self) -> &'static str {
        match self {
            Self::BuggyStartRouting => "I1 (execution-home boundary)",
        }
    }
}
