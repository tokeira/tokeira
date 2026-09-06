//! The continue-as-new advice rule.
//!
//! Temporal tells a workflow it should continue as new soon by stamping three
//! values on every `WorkflowTaskStarted` event: a flag, the reasons behind it,
//! and the run's persisted history size. The kernel derives them with the one
//! pure function here at every site that emits a started event, records them on
//! the pending task, and copies them unchanged wherever a virtual task's started
//! event is materialized or synthesized later. Nothing in the engine acts on the
//! advice; it is information for the workflow.
//!
//! Ground truth: `service/history/workflow/workflow_task_state_machine.go:487-491,
//! 1440-1466` and `service/history/workflow/update/registry.go:182-184, 496-505
//! @ v1.31.0`.

use crate::{
    command::ContinueAsNewAdvicePolicy, event::SuggestContinueAsNewReason, state::RecordedAdvice,
};

/// Derive the Advice for a workflow task that is about to start.
///
/// `next_event_id` is the id the next history event would receive at the moment
/// of the decision (`GetNextEventID()` in v1.31.0): the started event's own id
/// for a normal task, and the virtual scheduled id for a transient or
/// speculative one whose events are not persisted. `in_flight_updates` counts
/// updates admitted or accepted but not yet resolved; `completed_update_count`
/// is the run's durable count of completed outcomes.
///
/// Every comparison is `>=`, the reasons come out in enum order, and a zero
/// update threshold disables that reason. The same operands always yield the
/// same result, which is what lets replay and every delivery path agree.
pub fn continue_as_new_advice(
    history_size_bytes: i64,
    next_event_id: i64,
    in_flight_updates: usize,
    completed_update_count: u32,
    policy: ContinueAsNewAdvicePolicy,
) -> RecordedAdvice {
    let mut reasons = Vec::new();
    if history_size_bytes >= policy.history_size_threshold_bytes {
        reasons.push(SuggestContinueAsNewReason::HistorySizeTooLarge);
    }
    if next_event_id >= policy.history_count_threshold {
        reasons.push(SuggestContinueAsNewReason::TooManyHistoryEvents);
    }
    // `registry.go:496-505 @ v1.31.0`: `inFlight + completed >= ceil(max ×
    // threshold)`, with a zero threshold meaning the reason is off. The sum is
    // widened so a pathological in-flight count cannot wrap.
    if policy.total_updates_suggest_threshold > 0 {
        let outstanding = u64::try_from(in_flight_updates)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::from(completed_update_count));
        if outstanding >= u64::from(policy.total_updates_suggest_threshold) {
            reasons.push(SuggestContinueAsNewReason::TooManyUpdates);
        }
    }
    RecordedAdvice {
        history_size_bytes,
        suggest_continue_as_new: !reasons.is_empty(),
        suggest_continue_as_new_reasons: reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> ContinueAsNewAdvicePolicy {
        ContinueAsNewAdvicePolicy {
            history_size_threshold_bytes: 4 * 1024 * 1024,
            history_count_threshold: 4096,
            total_updates_suggest_threshold: 1800,
        }
    }

    #[test]
    fn thresholds_are_inclusive_and_reasons_are_ordered() {
        let below = continue_as_new_advice(4 * 1024 * 1024 - 1, 4095, 0, 1799, policy());
        assert!(!below.suggest_continue_as_new);
        assert!(below.suggest_continue_as_new_reasons.is_empty());

        let at = continue_as_new_advice(4 * 1024 * 1024, 4096, 900, 900, policy());
        assert!(at.suggest_continue_as_new);
        assert_eq!(
            at.suggest_continue_as_new_reasons,
            vec![
                SuggestContinueAsNewReason::HistorySizeTooLarge,
                SuggestContinueAsNewReason::TooManyHistoryEvents,
                SuggestContinueAsNewReason::TooManyUpdates,
            ]
        );
        assert_eq!(at.history_size_bytes, 4 * 1024 * 1024);
    }

    #[test]
    fn zero_update_threshold_disables_the_update_reason() {
        let disabled = ContinueAsNewAdvicePolicy {
            total_updates_suggest_threshold: 0,
            ..policy()
        };
        let advice = continue_as_new_advice(0, 3, usize::MAX, u32::MAX, disabled);
        assert!(!advice.suggest_continue_as_new);
        assert!(advice.suggest_continue_as_new_reasons.is_empty());
    }
}
