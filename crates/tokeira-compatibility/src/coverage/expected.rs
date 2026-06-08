//! Expected-outcome projection over the `dispatch_rpc` policy.
//!
//! This submodule owns the projection from a `FeatureEntry` (plus a dynamic-config reader) to the
//! [`ExpectedOutcome`](super::ExpectedOutcome) its state implies, so a coverage consumer can judge
//! an observed status code as agreeing with or contradicting the matrix claim. It is a thin view
//! over the existing `dispatch.rs` policy — one policy, two views — so the expected outcome and
//! the runtime dispatch decision can never silently diverge.

use crate::{
    FeatureEntry, FeatureState,
    dispatch::{DisabledReason, DispatchOutcome, DynamicConfigReader, dispatch_outcome_for},
};

use super::ExpectedOutcome;

/// Project a feature entry to the wire outcome its matrix state implies for one of its RPCs.
///
/// This is the coverage-side view of the *same* policy that [`dispatch_rpc`](crate::dispatch::dispatch_rpc)
/// applies at the edge: rather than re-deciding state→outcome here (which would let the two drift
/// apart silently), it derives from the very [`DispatchOutcome`] the dispatch path would produce
/// for `entry`, then refines that two-valued runtime decision into the four-valued reporting
/// vocabulary a coverage report needs. Because the underlying decision is shared with the edge by
/// construction (both call `dispatch_outcome_for`), the expected outcome and the actual dispatch
/// decision agree for every input — the "single policy, two views" guarantee.
///
/// The one piece of information `DispatchOutcome` deliberately discards but the report wants back is
/// *why* a call would proceed: the edge does not care whether a `Proceed` came from an unconditional
/// `Implemented` feature or from an `Experimental` feature whose gate happens to be on, but a report
/// does (so it can show that a success is contingent on a gate). That refinement is recovered here
/// from `entry.state`, which is why this takes the entry rather than only the `DispatchOutcome`.
///
/// `dynamic_config` and `namespace` are passed through unchanged to the shared decision; this
/// function reads no configuration of its own (mirroring `dispatch_rpc`, which also takes the
/// reader rather than owning one).
///
/// # Outcome mapping
///
/// - `Implemented` / `Partial` ⟹ [`ExpectedOutcome::Ok`]. `Partial` is treated as
///   `Implemented`-equivalent for *verdict* purposes (Requirement 6.1): the matrix's five states
///   collapse onto the harness's coarser axis here so a single projection serves both conformance
///   tiers. The `Partial` label is not erased — it survives as the `state` field on
///   [`RpcClassification::Known`](super::RpcClassification::Known) reporting detail (Requirement
///   6.2) — but a `Partial` feature is expected to answer its RPCs successfully, so for the
///   pass/fail decision it is indistinguishable from `Implemented`.
/// - `Experimental` with its dynamic-config gate enabled for `namespace` ⟹
///   [`ExpectedOutcome::OkWhenEnabled`]; gate disabled, or no gate key at all ⟹
///   [`ExpectedOutcome::DisabledPrecondition`].
/// - `Stubbed` / `Unsupported` ⟹ [`ExpectedOutcome::Unimplemented`].
pub fn expected_outcome(
    entry: &FeatureEntry,
    dynamic_config: &dyn DynamicConfigReader,
    namespace: Option<&str>,
) -> ExpectedOutcome {
    match dispatch_outcome_for(entry, dynamic_config, namespace) {
        // `Proceed` is two-valued at the edge but two-faced in a report: an unconditional success
        // (`Implemented`/`Partial`) versus a gate-contingent one (`Experimental` enabled). Recover
        // the distinction from the state the dispatch decision folded away.
        DispatchOutcome::Proceed => match entry.state {
            FeatureState::Experimental => ExpectedOutcome::OkWhenEnabled,
            _ => ExpectedOutcome::Ok,
        },
        DispatchOutcome::Disabled {
            reason: DisabledReason::ExperimentalDisabled,
            ..
        } => ExpectedOutcome::DisabledPrecondition,
        DispatchOutcome::Disabled {
            reason: DisabledReason::Stubbed | DisabledReason::Unsupported,
            ..
        } => ExpectedOutcome::Unimplemented,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dispatch::StaticDynamicConfig, matrix::FEATURE_MATRIX};

    // Feature: temporal-compatibility-surface, Property 3
    //
    // Expected-outcome agrees with dispatch policy ("single policy, two views"): for every real
    // matrix entry, under both dynamic-config states, the four-valued `ExpectedOutcome` the report
    // projection yields is exactly the refinement of the two-valued `DispatchOutcome` the edge would
    // produce for the same inputs. Iterating the real `FEATURE_MATRIX` exhaustively (rather than a
    // synthetic entry generator) is the strongest form: it proves the projection cannot drift from
    // the dispatch decision for any feature the matrix actually declares, including every `state` /
    // `dynamic_config_key` combination present in production data.
    #[test]
    fn expected_outcome_agrees_with_dispatch_outcome_for_every_entry() {
        for entry in FEATURE_MATRIX {
            for cfg in [
                StaticDynamicConfig::enabled(),
                StaticDynamicConfig::disabled(),
            ] {
                let expected = expected_outcome(entry, &cfg, Some("default"));
                let dispatched = dispatch_outcome_for(entry, &cfg, Some("default"));

                match (dispatched, &expected) {
                    (DispatchOutcome::Proceed, _) => match entry.state {
                        FeatureState::Experimental => assert_eq!(
                            expected,
                            ExpectedOutcome::OkWhenEnabled,
                            "Proceed on an Experimental entry ({}) must project to OkWhenEnabled",
                            entry.id
                        ),
                        _ => assert_eq!(
                            expected,
                            ExpectedOutcome::Ok,
                            "Proceed on a non-Experimental entry ({}) must project to Ok",
                            entry.id
                        ),
                    },
                    (
                        DispatchOutcome::Disabled {
                            reason: DisabledReason::ExperimentalDisabled,
                            ..
                        },
                        _,
                    ) => assert_eq!(
                        expected,
                        ExpectedOutcome::DisabledPrecondition,
                        "ExperimentalDisabled ({}) must project to DisabledPrecondition",
                        entry.id
                    ),
                    (
                        DispatchOutcome::Disabled {
                            reason: DisabledReason::Stubbed | DisabledReason::Unsupported,
                            ..
                        },
                        _,
                    ) => assert_eq!(
                        expected,
                        ExpectedOutcome::Unimplemented,
                        "Stubbed/Unsupported ({}) must project to Unimplemented",
                        entry.id
                    ),
                }
            }
        }
    }
}
