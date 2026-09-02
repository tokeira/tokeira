//! Connect-rust client for the placement controller.
//!
//! Loop C reaches the controller through [`PlacementControl`], the
//! counterpart of [`Actuator`](crate::actuator::Actuator) on the platform
//! side, so the retirement state machine can be proven with recording doubles
//! while the served client stays a thin transport wrapper over three RPCs:
//! - `NominateScaleInCandidates` — which runtime nodes are safe to retire
//! - `MarkNodeDraining` — record the drain on the controller, which sends the
//!   drain directive to the node
//! - `DescribeNodeDrain` — the node's drain progress as its heartbeat reports it

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use buffa::EnumValue;
use connectrpc::client::{ClientConfig, HttpClient};
use tokeira_proto::connect::tokeira::internal::controller::v1::{
    DescribeNodeDrainRequest, MarkDrainingRequest, NodeDrainState, NominateRequest,
    PlacementControllerClient,
};

use crate::loop_c::RetirementCandidate;

/// The controller's autoscaler-facing surface.
#[async_trait]
pub trait PlacementControl: Send + Sync + std::fmt::Debug {
    /// Ask the controller which runtime nodes are safe to retire, ranked.
    async fn nominate_scale_in_candidates(&self, limit: u32) -> Result<NominationResult>;

    /// Ask the controller to drain a node. `false` means the controller does
    /// not know the node, so nothing was marked and no drain was sent.
    async fn mark_node_draining(&self, node_id: &str) -> Result<bool>;

    /// The node's drain progress as last reported by its heartbeat, or `None`
    /// when the controller has no record of the node.
    async fn describe_node_drain(&self, node_id: &str) -> Result<Option<NodeDrainState>>;
}

/// Autoscaler's view of the placement controller.
pub struct ControllerClient {
    client: PlacementControllerClient<HttpClient>,
}

// Manual impl: the generated RPC client carries no `Debug`.
impl std::fmt::Debug for ControllerClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControllerClient").finish_non_exhaustive()
    }
}

/// Response from the controller's scale-in nomination RPC.
#[derive(Debug, Clone)]
pub struct NominationResult {
    pub candidates: Vec<RetirementCandidate>,
    pub aggregate_available_connections: u32,
    pub aggregate_connection_rate_headroom: f32,
}

impl ControllerClient {
    /// Construct a client for the given controller endpoint.
    pub fn new(endpoint: &str) -> Result<Self> {
        let http = HttpClient::plaintext();
        let config = ClientConfig::new(
            endpoint
                .parse()
                .map_err(|e| anyhow!("invalid controller endpoint: {e}"))?,
        );
        Ok(Self {
            client: PlacementControllerClient::new(http, config),
        })
    }
}

#[async_trait]
impl PlacementControl for ControllerClient {
    async fn nominate_scale_in_candidates(&self, limit: u32) -> Result<NominationResult> {
        let response = self
            .client
            .nominate_scale_in_candidates(NominateRequest {
                limit,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("nominate_scale_in_candidates failed: {e}"))?;

        let owned = response.into_owned();
        let candidates = owned
            .candidates
            .into_iter()
            .map(|c| RetirementCandidate { node_id: c.node_id })
            .collect();

        Ok(NominationResult {
            candidates,
            aggregate_available_connections: owned.aggregate_available_connections,
            aggregate_connection_rate_headroom: owned.aggregate_connection_rate_headroom,
        })
    }

    async fn mark_node_draining(&self, node_id: &str) -> Result<bool> {
        let response = self
            .client
            .mark_node_draining(MarkDrainingRequest {
                node_id: node_id.to_owned(),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("mark_node_draining failed: {e}"))?;

        Ok(response.into_owned().accepted)
    }

    async fn describe_node_drain(&self, node_id: &str) -> Result<Option<NodeDrainState>> {
        let response = self
            .client
            .describe_node_drain(DescribeNodeDrainRequest {
                node_id: node_id.to_owned(),
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow!("describe_node_drain failed: {e}"))?;

        let owned = response.into_owned();
        if !owned.known {
            return Ok(None);
        }
        match owned.state {
            EnumValue::Known(state) => Ok(Some(state)),
            // A newer controller may add states; an unrecognised one must
            // hold the retirement rather than be mistaken for any known phase.
            EnumValue::Unknown(raw) => bail!("controller reported an unknown drain state {raw}"),
        }
    }
}
