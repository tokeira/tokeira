//! Drain coordination state.
//!
//! Records which nodes are under drain and the latest drain state each has
//! reported. Controller intent (an accepted `MarkNodeDraining`) and runtime
//! progress (the heartbeat `drain_state`) meet here, and `DescribeNodeDrain`
//! reads the result so an autoscaler can gate termination on the runtime's own
//! `SafeToTerminate` verdict rather than on a timer.

use std::collections::HashMap;

use tokeira_types::{IncarnationId, ShardId};

use crate::membership::NodeDrainState;

/// Tracks nodes under controller-initiated or self-reported drain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainCoordinator {
    drain_states: HashMap<IncarnationId, NodeDrainState>,
}

impl DrainCoordinator {
    /// Record controller intent to drain `node_id`.
    ///
    /// Idempotent, and never demotes a recorded `SafeToTerminate`: a repeated
    /// mark (a retry, or a poll loop misusing the RPC) must not send a node
    /// that already finished draining back to `Draining`.
    pub(crate) fn mark_draining(&mut self, node_id: IncarnationId) {
        self.drain_states
            .entry(node_id)
            .or_insert(NodeDrainState::Draining);
    }

    pub(crate) fn is_draining(&self, node_id: IncarnationId) -> bool {
        self.drain_states.contains_key(&node_id)
    }

    /// Fold a heartbeat's drain state into the record.
    ///
    /// `Active` from a node under controller-initiated drain means the
    /// directive has not been applied yet (still in flight on the stream, or
    /// not yet processed by the runtime); the intent stands, so the record
    /// keeps `Draining`. `Active` from any other node carries no drain
    /// information. `Draining` and `SafeToTerminate` are recorded as reported:
    /// the runtime is the authority on its own progress.
    pub(crate) fn record_progress(&mut self, node_id: IncarnationId, state: NodeDrainState) {
        match state {
            NodeDrainState::Active => {}
            NodeDrainState::Draining | NodeDrainState::SafeToTerminate => {
                self.drain_states.insert(node_id, state);
            }
        }
    }

    pub fn drain_state(&self, node_id: IncarnationId) -> Option<NodeDrainState> {
        self.drain_states.get(&node_id).copied()
    }

    /// Nodes still draining, excluding those already safe to terminate.
    pub(crate) fn active_count(&self) -> usize {
        self.drain_states
            .values()
            .filter(|state| **state == NodeDrainState::Draining)
            .count()
    }

    pub fn filter_new_work_owners(
        &self,
        owners: impl IntoIterator<Item = (ShardId, IncarnationId)>,
    ) -> Vec<ShardId> {
        owners
            .into_iter()
            .filter_map(|(bundle_id, node_id)| (!self.is_draining(node_id)).then_some(bundle_id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn re_marking_never_demotes_a_safe_verdict() {
        let mut coordinator = DrainCoordinator::default();
        let node_id = IncarnationId::new();

        coordinator.mark_draining(node_id);
        assert_eq!(coordinator.active_count(), 1);
        coordinator.record_progress(node_id, NodeDrainState::SafeToTerminate);
        coordinator.mark_draining(node_id);

        assert_eq!(
            coordinator.drain_state(node_id),
            Some(NodeDrainState::SafeToTerminate)
        );
        assert_eq!(coordinator.active_count(), 0);
    }

    #[test]
    fn an_active_heartbeat_does_not_erase_controller_intent() {
        let mut coordinator = DrainCoordinator::default();
        let marked = IncarnationId::new();
        let unmarked = IncarnationId::new();

        coordinator.mark_draining(marked);
        coordinator.record_progress(marked, NodeDrainState::Active);
        coordinator.record_progress(unmarked, NodeDrainState::Active);

        assert_eq!(
            coordinator.drain_state(marked),
            Some(NodeDrainState::Draining)
        );
        assert_eq!(coordinator.drain_state(unmarked), None);
        assert_eq!(
            coordinator.filter_new_work_owners([(ShardId(0), marked), (ShardId(1), unmarked)]),
            vec![ShardId(1)]
        );
    }
}
