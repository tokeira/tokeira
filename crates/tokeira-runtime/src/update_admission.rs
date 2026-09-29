//! Update admission against the lane's authoritative run snapshot.
//!
//! The transport registry holds worker payloads and waiters, never the update
//! budget. Admission is checked again after every OCC reload, so concurrent
//! callers cannot reserve the same remaining slot.

use std::collections::BTreeSet;

use anyhow::Result;
use tokeira_kernel::{Command, HistoryEventKind, LoadedRun};
use tokeira_storage::RunRepository;

/// A new update would exceed the run's distinct-update budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "The limit on the total number of distinct updates in this workflow has been reached ({limit}). Make sure any duplicate updates share an Update ID so the server can deduplicate them, and consider rejecting updates that you aren't going to process. You can also Continue-as-New to avoid this; we recommend you check Continue-as-New Suggested in your Workflow."
)]
pub struct UpdateLimitExceeded {
    /// Effective limit at this admission attempt; zero disables enforcement.
    pub limit: i64,
}

/// Read the stock limit, with a live override only in conformance builds.
pub(crate) fn total_updates_limit() -> i64 {
    #[cfg(feature = "conformance")]
    if let Some(limit) = crate::conformance::reads().get_i64("history.maxTotalUpdates") {
        return limit;
    }
    2_000
}

fn check_limit(limit: i64, count: u64) -> Result<(), UpdateLimitExceeded> {
    // Zero alone disables enforcement, including for an already-populated run
    // (service/history/workflow/update/registry.go:438-449 @ v1.32.0).
    if limit != 0 && i128::from(count) >= i128::from(limit) {
        Err(UpdateLimitExceeded { limit })
    } else {
        Ok(())
    }
}

/// Return true for an already-completed id, or reject a new id over budget.
///
/// The caller runs this inside the load/apply/commit retry loop. History reads
/// stop at the loaded snapshot's boundary; the ensuing commit fences that same
/// snapshot. Accepted/admitted duplicates keep the kernel's existing handling.
pub(crate) async fn check_admission<R: RunRepository + ?Sized>(
    repo: &R,
    loaded: &LoadedRun,
    command: &Command,
    limit: i64,
) -> Result<bool> {
    let Command::Update(request) = command else {
        // Fresh StartAndUpdate has an empty budget. A positive limit admits its
        // first id and zero disables the limit (service/history/api/multioperation/api.go @ v1.32.0).
        if matches!(command, Command::StartAndUpdate(_)) && matches!(loaded, LoadedRun::Absent) {
            check_limit(limit, 0)?;
        }
        return Ok(false);
    };
    let LoadedRun::Existing(state) = loaded else {
        return Ok(false);
    };
    if !state.is_open()
        || state.admitted_updates.contains(&request.update_id)
        || state.pending_updates.contains_key(&request.update_id)
    {
        return Ok(false);
    }

    // Completed ids live in history, not the volatile registry. In particular,
    // cold replay and legacy completion commands do not populate the advice-only
    // completed_update_count. Use distinct history ids for admission correctness,
    // including after a reset, and stop at the snapshot fenced by this commit
    // (service/history/workflow/update/registry.go:438-491 @ v1.32.0).
    let mut completed = BTreeSet::new();
    let mut cursor = 0;
    loop {
        let events = repo.read_history(state.run_key, cursor, 512).await?;
        let Some(last) = events.last() else { break };
        let next_cursor = last.event_id;
        for event in events
            .iter()
            .take_while(|event| event.event_id <= state.last_event_id)
        {
            if let HistoryEventKind::WorkflowExecutionUpdateCompleted { update_id, .. }
            | HistoryEventKind::WorkflowExecutionUpdateCompletedV2 { update_id, .. } =
                &event.kind
            {
                completed.insert(update_id.clone());
            }
        }
        if next_cursor >= state.last_event_id || next_cursor <= cursor {
            break;
        }
        cursor = next_cursor;
    }
    if completed.contains(&request.update_id) {
        return Ok(true);
    }
    completed.extend(state.admitted_updates.iter().cloned());
    completed.extend(state.pending_updates.keys().cloned());
    check_limit(limit, completed.len() as u64)?;
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        // Feature: v132-lifecycle-fidelity, Property 3: total-updates limit at admission
        // Zero disables the limit; compare the complete formatted error contract
        // (service/history/workflow/update/registry.go:438-449 @ v1.32.0).
        #[test]
        fn total_updates_limit_at_admission(limit in -1i64..100, count in 0u64..200) {
            let result = check_limit(limit, count);
            prop_assert_eq!(result.is_err(), limit != 0 && i128::from(count) >= i128::from(limit));
            if let Err(error) = result {
                prop_assert_eq!(error.to_string(), format!(
                    "The limit on the total number of distinct updates in this workflow has been reached ({}). \
                     Make sure any duplicate updates share an Update ID so the server can deduplicate them, and consider rejecting updates that you aren't going to process. \
                     You can also Continue-as-New to avoid this; we recommend you check Continue-as-New Suggested in your Workflow.", limit));
            }
        }
    }
}
