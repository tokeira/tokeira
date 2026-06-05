//! Model-level property tests for the broker model's pure transitions
//! (delivery-broker-simulator spec tasks 7.3 / 7.4).
//!
//! These complement the two simulation modes by asserting invariants of the
//! tiny exhaustive transition model directly under randomised action sequences:
//! dedup/no-double-start (Property 6/2), sticky promotion and reservation⇄commit
//! coupling (Property 7/3). Each runs >= 100 iterations.

use broker_sim::{
    bug::InjectedBug,
    exhaustive::{BrokerAction, BrokerActionModel},
};
use proptest::prelude::*;
use sim_harness::ExhaustiveModel;

fn arb_action() -> impl Strategy<Value = BrokerAction> {
    prop_oneof![
        Just(BrokerAction::PublishSticky),
        Just(BrokerAction::Reserve),
        Just(BrokerAction::Commit),
        Just(BrokerAction::Complete),
        Just(BrokerAction::Crash),
        Just(BrokerAction::LeaseExpire),
        Just(BrokerAction::StickyExpire),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// Property 3 (S3): in the correct broker, no reachable state ever holds a
    /// token without a committed start transaction, for any action sequence.
    #[test]
    fn correct_broker_never_holds_uncommitted_token(actions in prop::collection::vec(arb_action(), 0..40)) {
        let mut m = BrokerActionModel::with_bug(None);
        for a in &actions {
            m.apply(a).unwrap();
            // S3 component of check(): a violation here would mention S3.
            if let Some(reason) = m.check() {
                prop_assert!(!reason.contains("S3"), "correct broker violated S3: {reason}");
            }
        }
    }

    /// Property 2/6: in the correct broker, no reachable state has more than one
    /// concurrent live delivery (no double start) for any action sequence.
    #[test]
    fn correct_broker_never_double_starts(actions in prop::collection::vec(arb_action(), 0..40)) {
        let mut m = BrokerActionModel::with_bug(None);
        for a in &actions {
            m.apply(a).unwrap();
            if let Some(reason) = m.check() {
                prop_assert!(!reason.contains("S2"), "correct broker violated S2: {reason}");
            }
        }
    }

    /// Property 7 (S7): the drop-expired-sticky bug, when its StickyExpire is
    /// applied to a sticky-ready pending task, drives the model into the lost
    /// state — i.e. the bug is reliably reachable and detectable.
    #[test]
    fn drop_expired_sticky_bug_loses_task(_n in 0u8..100) {
        let mut m = BrokerActionModel::with_bug(Some(InjectedBug::DropExpiredSticky));
        m.apply(&BrokerAction::PublishSticky).unwrap();
        m.apply(&BrokerAction::StickyExpire).unwrap();
        let reason = m.check().expect("drop-expired-sticky must violate a loss invariant");
        prop_assert!(reason.contains("S7"), "expected S7 loss, got: {reason}");
    }

    /// Property 3 (S3): the token-before-commit bug holds a token immediately
    /// after Reserve, before any Commit — the S3 violation.
    #[test]
    fn token_before_commit_bug_holds_uncommitted(_n in 0u8..100) {
        let mut m = BrokerActionModel::with_bug(Some(InjectedBug::TokenBeforeCommit));
        m.apply(&BrokerAction::PublishSticky).unwrap();
        m.apply(&BrokerAction::Reserve).unwrap();
        let reason = m.check().expect("token-before-commit must violate S3");
        prop_assert!(reason.contains("S3"), "expected S3, got: {reason}");
    }
}
