//! Model-level property tests for the placement model's exhaustive kernel.
//!
//! These complement the two simulation modes by asserting invariants of the
//! tiny exhaustive transition model directly under randomised action sequences:
//! the execution-home boundary (I1) and durable single-apply of a signal (I2)
//! hold for the correct model, while the injected buggy-routing variant is
//! reliably falsified. Each property runs >= 100 iterations.

use placement_sim::{
    bug::InjectedBug,
    exhaustive::{run_with_bug, MiniAction, MiniModel},
};
use proptest::prelude::*;
use sim_engine::ExhaustiveModel;

fn arb_action() -> impl Strategy<Value = MiniAction> {
    prop_oneof![
        (0u8..2).prop_map(MiniAction::ObserveBundle),
        (0u8..2, 0u8..2).prop_map(|(runtime, bundle)| MiniAction::Acquire { runtime, bundle }),
        (0u8..2).prop_map(MiniAction::ExpireBundle),
        (0u8..2, 0u8..2).prop_map(|(runtime, bundle)| MiniAction::Relinquish { runtime, bundle }),
        (0u8..2).prop_map(MiniAction::CrashRuntime),
        Just(MiniAction::StartWorkflow),
        Just(MiniAction::SignalWorkflow),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    /// Property (I1): in the correct model, no reachable state ever commits a
    /// workflow off its execution-home, for any action sequence.
    #[test]
    fn correct_model_keeps_execution_home(actions in prop::collection::vec(arb_action(), 0..40)) {
        let mut m = <MiniModel<false>>::initial();
        for a in &actions {
            m.apply(a).expect("correct model never errors a transition");
            if let Some(reason) = m.check() {
                prop_assert!(!reason.contains("I1"), "correct model violated I1: {reason}");
            }
        }
    }

    /// Property (I2): in the correct model, a signal request never applies more
    /// than once regardless of how the schedule interleaves.
    #[test]
    fn correct_model_signals_at_most_once(actions in prop::collection::vec(arb_action(), 0..40)) {
        let mut m = <MiniModel<false>>::initial();
        for a in &actions {
            m.apply(a).expect("correct model never errors a transition");
            if let Some(reason) = m.check() {
                prop_assert!(!reason.contains("I2"), "correct model violated I2: {reason}");
            }
        }
    }

    /// Property: the buggy-start-routing model is reliably falsified by the
    /// bounded checker — the simulator has real catching power.
    #[test]
    fn buggy_routing_is_always_caught(depth in 4usize..14) {
        let result = run_with_bug(Some(InjectedBug::BuggyStartRouting), depth);
        let ce = result.expect_err("buggy routing must produce a counterexample");
        prop_assert!(ce.message.contains("queue-home") || ce.message.contains("I1"));
    }
}
