//! Live runtime membership state observed by one controller instance.

use std::{
    collections::HashMap,
    time::{Duration as StdDuration, Instant},
};

use serde::{Deserialize, Serialize};
use tokeira_types::{IncarnationId, NodeReachability, ShardId};
use tokio::sync::mpsc;

/// Runtime registration sent as the first membership-stream message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeRegistration {
    pub(crate) node_id: IncarnationId,
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) zone: Option<String>,
    pub(crate) version: String,
    pub(crate) build_id: String,
}

/// Per-lane pressure metric reported by runtimes.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LanePressure {
    pub(crate) lane_id: u32,
    pub(crate) runnable_depth: u64,
    pub(crate) active_actors: u64,
    pub(crate) utilization: f32,
}

/// Runtime drain state reported in heartbeats.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeDrainState {
    Active,
    Draining,
    SafeToTerminate,
}

/// Periodic runtime pressure and ownership report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeHeartbeat {
    pub(crate) owned_bundle_count: u32,
    pub(crate) owned_bundles: Vec<ShardId>,
    pub(crate) runnable_transitions: u64,
    pub(crate) active_actor_count: u64,
    pub(crate) backlog_depth: u64,
    pub(crate) available_connections: u32,
    pub(crate) connection_rate_headroom: f32,
    pub(crate) drain_state: NodeDrainState,
    pub(crate) lane_pressures: Vec<LanePressure>,
}

/// Controller directive placeholder used by membership stream state.
#[derive(Clone, Debug, PartialEq)]
pub enum ControllerDirective {
    Drain,
}

/// Local stream state for one runtime node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeMembershipState {
    Active,
    Draining,
    GracePeriod,
    Unavailable,
}

/// Live node state tracked by a controller instance.
#[derive(Debug)]
pub struct LiveNode {
    pub(crate) node_id: IncarnationId,
    pub registration: RuntimeRegistration,
    pub(crate) last_heartbeat: Instant,
    pub(crate) heartbeat: RuntimeHeartbeat,
    pub(crate) membership_state: NodeMembershipState,
    pub(crate) reachability: NodeReachability,
    pub directive_tx: Option<mpsc::Sender<ControllerDirective>>,
}

/// Controller-local membership map.
#[derive(Debug, Default)]
pub struct LiveMembership {
    nodes: HashMap<IncarnationId, LiveNode>,
}

/// A node nominated for scale-in retirement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScaleInCandidate {
    pub(crate) node_id: IncarnationId,
    pub(crate) owned_bundle_count: u32,
    pub(crate) runnable_transitions: u64,
    pub(crate) active_actor_count: u64,
    pub(crate) backlog_depth: u64,
}

impl LiveMembership {
    pub(crate) fn register_node(
        &mut self,
        registration: RuntimeRegistration,
        heartbeat: RuntimeHeartbeat,
        directive_tx: Option<mpsc::Sender<ControllerDirective>>,
    ) {
        let node_id = registration.node_id;
        self.nodes.insert(
            node_id,
            LiveNode {
                node_id,
                registration,
                last_heartbeat: Instant::now(),
                heartbeat,
                membership_state: NodeMembershipState::Active,
                reachability: NodeReachability::Healthy,
                directive_tx,
            },
        );
    }

    pub(crate) fn update_heartbeat(&mut self, node_id: IncarnationId, heartbeat: RuntimeHeartbeat) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.last_heartbeat = Instant::now();
            node.heartbeat = heartbeat;
            if node.membership_state == NodeMembershipState::GracePeriod {
                node.membership_state = NodeMembershipState::Active;
            }
            node.reachability = NodeReachability::Healthy;
        }
    }

    pub(crate) fn mark_grace_period(&mut self, node_id: IncarnationId) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.membership_state = NodeMembershipState::GracePeriod;
            node.reachability = NodeReachability::Suspect;
        }
    }

    pub fn mark_unavailable(&mut self, node_id: IncarnationId) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.membership_state = NodeMembershipState::Unavailable;
            node.reachability = NodeReachability::Unavailable;
        }
    }

    pub(crate) fn mark_draining(&mut self, node_id: IncarnationId) -> bool {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.membership_state = NodeMembershipState::Draining;
            true
        } else {
            false
        }
    }

    pub fn remove_node(&mut self, node_id: IncarnationId) -> Option<LiveNode> {
        self.nodes.remove(&node_id)
    }

    pub(crate) fn active_nodes(&self) -> impl Iterator<Item = &LiveNode> {
        self.nodes
            .values()
            .filter(|node| node.membership_state == NodeMembershipState::Active)
    }

    pub(crate) fn nodes(&self) -> impl Iterator<Item = &LiveNode> {
        self.nodes.values()
    }

    pub fn get(&self, node_id: IncarnationId) -> Option<&LiveNode> {
        self.nodes.get(&node_id)
    }

    pub(crate) fn active_node_ids_sorted(&self) -> Vec<IncarnationId> {
        let mut ids = self
            .active_nodes()
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        ids.sort_by_key(|id| id.0);
        ids
    }

    pub fn expire_grace_periods(&mut self, grace_interval: StdDuration) {
        let now = Instant::now();
        for node in self.nodes.values_mut() {
            if node.membership_state == NodeMembershipState::GracePeriod
                && now.duration_since(node.last_heartbeat) >= grace_interval
            {
                node.membership_state = NodeMembershipState::Unavailable;
                node.reachability = NodeReachability::Unavailable;
            }
        }
    }

    /// Rank active nodes by bundle count and pressure for scale-in nomination.
    /// Excludes draining nodes. Returns up to `limit` candidates.
    pub(crate) fn nominate_scale_in(&self, limit: u32) -> Vec<ScaleInCandidate> {
        let mut candidates: Vec<_> = self
            .nodes
            .values()
            .filter(|n| n.membership_state == NodeMembershipState::Active)
            .map(|n| ScaleInCandidate {
                node_id: n.node_id,
                owned_bundle_count: n.heartbeat.owned_bundle_count,
                runnable_transitions: n.heartbeat.runnable_transitions,
                active_actor_count: n.heartbeat.active_actor_count,
                backlog_depth: n.heartbeat.backlog_depth,
            })
            .collect();
        // Prefer nodes with fewest bundles, then lowest pressure.
        candidates.sort_by_key(|c| (c.owned_bundle_count, c.runnable_transitions));
        candidates.truncate(limit as usize);
        candidates
    }

    /// Aggregate connection headroom across all active nodes.
    pub(crate) fn aggregate_headroom(&self) -> (u32, f32) {
        let mut total_connections: u32 = 0;
        let mut total_rate: f32 = 0.0;
        for node in self.active_nodes() {
            total_connections += node.heartbeat.available_connections;
            total_rate += node.heartbeat.connection_rate_headroom;
        }
        (total_connections, total_rate)
    }
}

impl RuntimeHeartbeat {
    pub(crate) fn empty() -> Self {
        Self {
            owned_bundle_count: 0,
            owned_bundles: Vec::new(),
            runnable_transitions: 0,
            active_actor_count: 0,
            backlog_depth: 0,
            available_connections: 0,
            connection_rate_headroom: 0.0,
            drain_state: NodeDrainState::Active,
            lane_pressures: Vec::new(),
        }
    }
}
