//! Signal request-id projections committed with the signal's fenced transition.
//!
//! The conformance override controls recording, never response links. Buffered
//! signals retain the admission decision until their real event ids are assigned.
//! Reset reconstructs the copied prefix before recording its reapplied tail.

use anyhow::Result;
use tokeira_kernel::{
    Command, HistoryEventKind, LoadedRun, RequestIdInfo, Transition, WorkflowTaskFailedCause,
};
use tokeira_storage::RunRepository;

fn enabled() -> bool {
    #[cfg(feature = "conformance")]
    return crate::conformance::reads()
        .get_bool("history.enableCHASMSignalBacklinks")
        .unwrap_or(false);
    #[cfg(not(feature = "conformance"))]
    false
}

fn signal_request_id(kind: &HistoryEventKind) -> Option<&str> {
    match kind {
        HistoryEventKind::WorkflowExecutionSignaled { request_id, .. }
            if !request_id.is_empty() =>
        {
            Some(request_id)
        }
        _ => None,
    }
}

fn signal_info(event_id: i64, buffered: bool) -> RequestIdInfo {
    RequestIdInfo {
        event_id,
        event_type: tokeira_proto::enums::EventType::WorkflowExecutionSignaled as i32,
        buffered,
    }
}

/// Augment the next state before the lane commits; never write a volatile side map.
pub(crate) async fn record<R: RunRepository + ?Sized>(
    repo: &R,
    previous: &LoadedRun,
    command: &Command,
    transition: &mut Transition,
) -> Result<()> {
    record_with_mode(repo, previous, command, transition, enabled()).await
}

/// Apply an already-resolved recording policy to the fenced snapshot.
/// Keeping the policy explicit also permits deterministic lifecycle tests.
pub(crate) async fn record_with_mode<R: RunRepository + ?Sized>(
    repo: &R,
    previous: &LoadedRun,
    command: &Command,
    transition: &mut Transition,
    enabled: bool,
) -> Result<()> {
    // Copied signals precede the reset transition's newly emitted events, so
    // rebuild them from the successor's actual history prefix
    // (service/history/workflow/mutable_state_impl.go:6156-6186 @ v1.32.0).
    if enabled
        && matches!(command, Command::WorkflowTaskFailed(request)
        if request.failure_cause == WorkflowTaskFailedCause::ResetWorkflow)
        && let LoadedRun::Existing(state) = previous
    {
        let mut cursor = 0;
        loop {
            let page = repo.read_history(state.run_key, cursor, 512).await?;
            let Some(last) = page.last() else { break };
            let next = last.event_id;
            for event in page
                .iter()
                .take_while(|event| event.event_id <= state.last_event_id)
            {
                if let Some(id) = signal_request_id(&event.kind) {
                    transition
                        .next_state
                        .request_id_infos
                        .insert(id.to_owned(), signal_info(event.event_id, false));
                }
            }
            if next >= state.last_event_id || next <= cursor {
                break;
            }
            cursor = next;
        }
    }
    record_transition(previous, transition, enabled);
    Ok(())
}

fn record_transition(previous: &LoadedRun, transition: &mut Transition, enabled: bool) {
    let was_buffered = |id: &str| match previous {
        LoadedRun::Existing(state) => state
            .buffered_events
            .iter()
            .any(|event| signal_request_id(&event.kind) == Some(id)),
        LoadedRun::Absent => false,
    };
    for event in &transition.history_events {
        if let Some(id) = signal_request_id(&event.kind) {
            let tracked = transition
                .next_state
                .request_id_infos
                .get(id)
                .is_some_and(|info| {
                    info.event_type
                        == tokeira_proto::enums::EventType::WorkflowExecutionSignaled as i32
                });
            // A flush resolves an already-recorded backlink even if the override
            // is now off; turning it on cannot retroactively record an old buffer
            // (service/history/workflow/mutable_state_impl.go:6174-6186 @ v1.32.0).
            if tracked || (enabled && !was_buffered(id)) {
                transition
                    .next_state
                    .request_id_infos
                    .insert(id.to_owned(), signal_info(event.event_id, false));
            }
        }
    }
    if enabled {
        for event in &transition.next_state.buffered_events {
            if let Some(id) = signal_request_id(&event.kind)
                && !was_buffered(id)
            {
                transition
                    .next_state
                    .request_id_infos
                    .insert(id.to_owned(), signal_info(0, true));
            }
        }
    }
}
