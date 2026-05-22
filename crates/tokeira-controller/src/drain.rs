//! Drain coordination state.

use std::collections::{HashMap, HashSet};

use tokeira_types::{IncarnationId, ShardId};

use crate::membership::NodeDrainState;

/// Tracks nodes currently under controller-initiated drain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainCoordinator {
    draining_nodes: HashSet<IncarnationId>,
    drain_states: HashMap<IncarnationId, NodeDrainState>,
}

impl DrainCoordinator {
    pub fn mark_draining(&mut self, node_id: IncarnationId) {
        self.draining_nodes.insert(node_id);
        self.drain_states.insert(node_id, NodeDrainState::Draining);
    }

    pub fn clear(&mut self, node_id: IncarnationId) {
        self.draining_nodes.remove(&node_id);
        self.drain_states.remove(&node_id);
    }

    pub fn is_draining(&self, node_id: IncarnationId) -> bool {
        self.draining_nodes.contains(&node_id)
    }

    pub fn record_progress(&mut self, node_id: IncarnationId, state: NodeDrainState) {
        match state {
            NodeDrainState::Active => self.clear(node_id),
            NodeDrainState::Draining => self.mark_draining(node_id),
            NodeDrainState::SafeToTerminate => {
                self.draining_nodes.insert(node_id);
                self.drain_states.insert(node_id, state);
            }
        }
    }

    pub fn drain_state(&self, node_id: IncarnationId) -> Option<NodeDrainState> {
        self.drain_states.get(&node_id).copied()
    }

    pub fn active_count(&self) -> usize {
        self.draining_nodes.len()
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
