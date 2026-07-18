//! Connect-rust client for the placement controller.
//!
//! Provides typed access to the controller's autoscaler-facing RPCs:
//! - `NominateScaleInCandidates` — ask which runtime hosts to retire
//! - `MarkNodeDraining` — tell the controller to stop assigning bundles

use anyhow::{Result, anyhow};
use connectrpc::client::{ClientConfig, HttpClient};
use tokeira_proto::connect::tokeira::internal::controller::v1::{
    MarkDrainingRequest, NominateRequest, PlacementControllerClient,
};

use crate::loop_c::RetirementCandidate;

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

    /// Ask the controller which runtime hosts are safe to retire.
    pub async fn nominate_scale_in_candidates(&self, limit: u32) -> Result<NominationResult> {
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
            .map(|c| RetirementCandidate {
                instance_id: c.node_id,
            })
            .collect();

        Ok(NominationResult {
            candidates,
            aggregate_available_connections: owned.aggregate_available_connections,
            aggregate_connection_rate_headroom: owned.aggregate_connection_rate_headroom,
        })
    }

    /// Tell the controller to stop assigning bundles to a node and begin drain.
    pub async fn mark_node_draining(&self, node_id: &str) -> Result<bool> {
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
}
