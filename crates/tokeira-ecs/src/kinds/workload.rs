//! Typed author input for one ECS deploy-plane workload.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::{EcsConfig, services::EcsWorkload};

/// Author-visible name of the realized service type.
pub const TYPE: &str = "EcsWorkload";

/// Reusable author input for one ECS workload, selected by canonical
/// service name from the platform's derived workload set.
///
/// The full deployment configuration rides along because workload shape —
/// container wiring, sidecars, Service Connect ports, capacity assignment —
/// is derived by the platform's builders from the whole model, exactly as
/// the legacy path derives it. The definition owns that model; this kind
/// carries it to the builders without re-deriving anything.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Workload {
    /// Canonical workload name (e.g. `tokeira-runtime`, `tokeira-grafana`).
    pub service: String,
    /// The deployment configuration the workload set derives from.
    pub config: EcsConfig,
}

impl Kind<EcsWorkload> for Workload {
    fn realize(&self, _placement: &PlacementContext) -> Result<EcsWorkload, KindError> {
        let mut workloads = EcsWorkload::build_all(&self.config);
        workloads.extend(EcsWorkload::build_observability(&self.config));
        match workloads
            .iter()
            .position(|workload| workload.name == self.service)
        {
            Some(index) => Ok(workloads.swap_remove(index)),
            None => {
                let names: Vec<&str> = workloads
                    .iter()
                    .map(|workload| workload.name.as_str())
                    .collect();
                Err(KindError::new(format!(
                    "unknown ECS workload `{}`; the platform builds: {}",
                    self.service,
                    names.join(", ")
                )))
            }
        }
    }
}
