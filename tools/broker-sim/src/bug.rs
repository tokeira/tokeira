//! Deliberately-injectable known bugs (spec Requirement 35).
//!
//! Each variant makes the broker model violate one named safety invariant, so
//! the verification modes — especially the bounded-exhaustive checker — can
//! demonstrate real falsifying power, the broker analog of `placement-sim`'s
//! `--buggy-start-routing`. With no bug selected, all safety invariants pass.

/// A selectable broker defect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InjectedBug {
    /// Hand the worker a token before the start transaction commits.
    /// Violates S3 (reservation⇄commit coupling) and enables S2 double-start.
    TokenBeforeCommit,
    /// Drop an expired sticky claim instead of promoting it to general.
    /// Violates S7 (sticky safety) and causes L1 loss.
    DropExpiredSticky,
    /// Skip the dedup check on a re-published logical task.
    /// Violates S6 (duplicate publication safety) and can drive S2.
    NoDedupOnRepublish,
}

impl InjectedBug {
    /// Parse a `--bug=<name>` value into a bug variant.
    pub fn from_flag(value: &str) -> Option<Self> {
        match value {
            "token-before-commit" => Some(InjectedBug::TokenBeforeCommit),
            "drop-expired-sticky" => Some(InjectedBug::DropExpiredSticky),
            "no-dedup-on-republish" => Some(InjectedBug::NoDedupOnRepublish),
            _ => None,
        }
    }

    /// The safety invariant this bug is expected to violate (for reporting).
    pub fn expected_violation(&self) -> &'static str {
        match self {
            InjectedBug::TokenBeforeCommit => "S3",
            InjectedBug::DropExpiredSticky => "S7",
            InjectedBug::NoDedupOnRepublish => "S6",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_known_flags() {
        assert_eq!(
            InjectedBug::from_flag("token-before-commit"),
            Some(InjectedBug::TokenBeforeCommit)
        );
        assert_eq!(
            InjectedBug::from_flag("drop-expired-sticky"),
            Some(InjectedBug::DropExpiredSticky)
        );
        assert_eq!(InjectedBug::from_flag("nope"), None);
    }
}
