//! The bounded-exhaustive `BrokerActionModel`.
//!
//! A tiny, `Hash + Eq` model (one run, one queue, two workers, at most one WFT)
//! over which `run_bounded_exhaustive` enumerates every interleaving of
//! publish / reserve / commit / complete / crash / lease-expire / sticky-expire.
//! `check` evaluates the broker safety invariants at each state, so a protocol-
//! shape bug — including an injected one — surfaces at shallow depth. This is
//! the broker analog of placement-sim's mini exhaustive checker.

use sim_engine::ExhaustiveModel;

use crate::bug::InjectedBug;

/// Reservation/delivery status of the single modelled WFT.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Status {
    /// No task published.
    Absent,
    /// Published, in the sticky tier, awaiting its preferred worker.
    StickyReady,
    /// Published, general-deliverable.
    GeneralReady,
    /// Reserved by a worker; start transaction not yet committed.
    Reserved,
    /// Token committed and held (the only legitimate "held" state).
    CommittedHeld,
    /// Terminally completed.
    Completed,
}

/// The tiny exhaustive state. `bug` is part of the state so the checker explores
/// the buggy variant's reachable space.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BrokerActionModel {
    status: Status,
    /// Whether a token is currently held by a worker.
    token_held: bool,
    /// Whether the held token's start transaction has committed.
    committed: bool,
    /// Live delivery count for the single task (for S2).
    live: u8,
    /// Whether the task still exists in authoritative pending state.
    pending: bool,
    bug: Option<InjectedBug>,
}

/// The transition alphabet for the tiny model.
#[derive(Clone, Debug)]
pub enum BrokerAction {
    /// Publish the WFT (sticky-preferred).
    PublishSticky,
    /// A poll reserves the task and begins the start transaction.
    Reserve,
    /// The start transaction commits.
    Commit,
    /// The current delivery completes.
    Complete,
    /// The broker crashes (ephemeral state discarded; sweeper rebuilds).
    Crash,
    /// The delivery lease expires (redelivery; old completion staled).
    LeaseExpire,
    /// The sticky TTL expires (promote to general, unless the bug drops it).
    StickyExpire,
}

impl BrokerActionModel {
    /// Initial state parameterised by the optional injected bug.
    pub fn with_bug(bug: Option<InjectedBug>) -> Self {
        BrokerActionModel {
            status: Status::Absent,
            token_held: false,
            committed: false,
            live: 0,
            pending: false,
            bug,
        }
    }
}

impl ExhaustiveModel for BrokerActionModel {
    type Action = BrokerAction;

    fn initial() -> Self {
        // Default exploration uses the correct broker (no bug).
        BrokerActionModel::with_bug(None)
    }

    fn actions() -> Vec<BrokerAction> {
        vec![
            BrokerAction::PublishSticky,
            BrokerAction::Reserve,
            BrokerAction::Commit,
            BrokerAction::Complete,
            BrokerAction::Crash,
            BrokerAction::LeaseExpire,
            BrokerAction::StickyExpire,
        ]
    }

    fn apply(&mut self, action: &BrokerAction) -> Result<(), String> {
        match action {
            BrokerAction::PublishSticky => {
                if self.status == Status::Absent {
                    self.status = Status::StickyReady;
                    self.pending = true;
                }
            }
            BrokerAction::Reserve => {
                if matches!(self.status, Status::StickyReady | Status::GeneralReady) {
                    self.status = Status::Reserved;
                    // BUG: token-before-commit hands the token out now.
                    if matches!(self.bug, Some(InjectedBug::TokenBeforeCommit)) {
                        self.token_held = true;
                        self.committed = false;
                        self.live += 1;
                    }
                }
            }
            BrokerAction::Commit => {
                if self.status == Status::Reserved {
                    self.status = Status::CommittedHeld;
                    if !self.token_held {
                        self.token_held = true;
                        self.live += 1;
                    }
                    self.committed = true;
                }
            }
            BrokerAction::Complete => {
                if self.status == Status::CommittedHeld {
                    self.status = Status::Completed;
                    self.token_held = false;
                    self.committed = false;
                    self.live = self.live.saturating_sub(1);
                    self.pending = false;
                }
            }
            BrokerAction::Crash => {
                // Discard ephemeral state. Authoritative `pending` survives; a
                // pending task is re-made general-deliverable by the sweeper.
                self.token_held = false;
                self.committed = false;
                self.live = 0;
                if self.pending && self.status != Status::Completed {
                    self.status = Status::GeneralReady;
                }
            }
            BrokerAction::LeaseExpire => {
                if self.status == Status::CommittedHeld {
                    // Redeliver: token released, task general again, old delivery stale.
                    self.token_held = false;
                    self.committed = false;
                    self.live = self.live.saturating_sub(1);
                    self.status = Status::GeneralReady;
                }
            }
            BrokerAction::StickyExpire => {
                if self.status == Status::StickyReady {
                    if matches!(self.bug, Some(InjectedBug::DropExpiredSticky)) {
                        // BUG: drop the claim — task lost while still pending.
                        self.status = Status::Absent;
                        // pending stays true with no deliverable task: the loss.
                    } else {
                        self.status = Status::GeneralReady;
                    }
                }
            }
        }
        Ok(())
    }

    fn check(&self) -> Option<String> {
        // S3: a held token implies committed.
        if self.token_held && !self.committed {
            return Some("S3: token held without committed start transaction".into());
        }
        // S2: at most one live delivery.
        if self.live > 1 {
            return Some(format!("S2: {} concurrent live deliveries", self.live));
        }
        // S7/S5 (loss): a pending task must be deliverable, held, or completed —
        // never absent while still pending.
        if self.pending && self.status == Status::Absent {
            return Some("S7: pending task dropped (sticky claim lost)".into());
        }
        None
    }
}

/// Run the bounded-exhaustive checker with a specific bug seeded into the
/// initial state. (The trait's `initial()` is always bug-free; this helper lets
/// the bug tests explore the buggy state space via newtype wrappers whose
/// `initial()` carries the bug.)
pub fn run_with_bug(
    bug: Option<InjectedBug>,
    max_depth: usize,
) -> Result<sim_engine::EnumReport, sim_engine::Counterexample<BrokerAction>> {
    match bug {
        None => sim_engine::run_bounded_exhaustive::<BrokerActionModel>(max_depth),
        Some(InjectedBug::TokenBeforeCommit) => {
            sim_engine::run_bounded_exhaustive::<BuggyTokenModel>(max_depth)
        }
        Some(InjectedBug::DropExpiredSticky) => {
            sim_engine::run_bounded_exhaustive::<BuggyStickyModel>(max_depth)
        }
        Some(InjectedBug::NoDedupOnRepublish) => {
            // The dedup bug is a stress-mode concern (republish path); the tiny
            // model does not republish, so exploration completes cleanly here.
            sim_engine::run_bounded_exhaustive::<BrokerActionModel>(max_depth)
        }
    }
}

macro_rules! buggy_model {
    ($name:ident, $bug:expr) => {
        #[derive(Clone, PartialEq, Eq, Hash)]
        pub struct $name(BrokerActionModel);
        impl ExhaustiveModel for $name {
            type Action = BrokerAction;
            fn initial() -> Self {
                $name(BrokerActionModel::with_bug(Some($bug)))
            }
            fn actions() -> Vec<BrokerAction> {
                BrokerActionModel::actions()
            }
            fn apply(&mut self, action: &BrokerAction) -> Result<(), String> {
                self.0.apply(action)
            }
            fn check(&self) -> Option<String> {
                self.0.check()
            }
        }
    };
}

buggy_model!(BuggyTokenModel, InjectedBug::TokenBeforeCommit);
buggy_model!(BuggyStickyModel, InjectedBug::DropExpiredSticky);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_model_passes() {
        let report = run_with_bug(None, 12).expect("no violation for correct broker");
        assert!(report.states_explored > 0);
    }

    #[test]
    fn token_before_commit_is_caught() {
        let ce = run_with_bug(Some(InjectedBug::TokenBeforeCommit), 12)
            .expect_err("token-before-commit must be falsified");
        assert!(
            ce.message.contains("S3"),
            "expected S3 violation, got: {}",
            ce.message
        );
        // Shallow: Publish -> Reserve reaches the held-uncommitted state.
        assert!(
            ce.depth <= 3,
            "expected shallow counterexample, got depth {}",
            ce.depth
        );
    }

    #[test]
    fn drop_expired_sticky_is_caught() {
        let ce = run_with_bug(Some(InjectedBug::DropExpiredSticky), 12)
            .expect_err("drop-expired-sticky must be falsified");
        assert!(
            ce.message.contains("S7"),
            "expected S7 violation, got: {}",
            ce.message
        );
    }
}
