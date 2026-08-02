//! Deployment-directory routing metadata shared by `tkr` and a bound provisioner.
//!
//! The complete `metadata.json` document remains a `tkr` registry record. This
//! module owns only the identity fields a deployment-local provisioner must
//! verify before reading its definition or touching state/provider seams.

use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId, PlatformLaunchClass};
use tokeira_platform::definition::RelativeDefinitionPath;

/// Definition identity recorded for a deployment-root source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordedDefinition {
    /// Definition frontend format selected when the deployment was created.
    pub format: DefinitionFormatId,
    /// Canonical deployment-relative path of the sole live definition source.
    pub path: RelativeDefinitionPath,
}

/// Provisioner-relevant subset of `metadata.json`.
///
/// Unknown registry fields are intentionally accepted: status, timestamps,
/// storage hints, and display names belong to `tkr`, not to definition routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentBindingMetadata {
    /// Operator-visible deployment name used as the stable platform naming input.
    pub name: String,
    /// Stable deployment UUID recorded at creation.
    pub id: uuid::Uuid,
    /// Open platform identity selected at deployment creation.
    pub platform: PlatformId,
    /// Generic launch mechanism selected by the trusted platform descriptor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_class: Option<PlatformLaunchClass>,
    /// Recorded definition identity for bound-provisioner deployments.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition: Option<RecordedDefinition>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_provisioner_subset_from_complete_registry_metadata() {
        let decoded: DeploymentBindingMetadata = serde_json::from_value(serde_json::json!({
            "name": "demo",
            "id": "7698ae09-197e-4325-9f77-256dac98f23a",
            "platform": "compose",
            "launch_class": "bound-provisioner",
            "definition": {
                "format": "tkd",
                "path": "definition.tkd"
            },
            "storage": "in-memory",
            "status": "created",
            "created_at": "2026-08-02T00:00:00Z",
            "updated_at": "2026-08-02T00:00:00Z"
        }))
        .expect("registry extras are outside the bound subset");

        assert_eq!(decoded.platform.as_str(), "compose");
        assert_eq!(
            decoded.definition.expect("definition").path.as_str(),
            "definition.tkd"
        );
    }

    #[test]
    fn unsafe_definition_paths_are_rejected_during_metadata_admission() {
        let error = serde_json::from_value::<DeploymentBindingMetadata>(serde_json::json!({
            "name": "demo",
            "id": "7698ae09-197e-4325-9f77-256dac98f23a",
            "platform": "compose",
            "launch_class": "bound-provisioner",
            "definition": { "format": "tkd", "path": "../definition.tkd" }
        }))
        .expect_err("escaping path must not reach filesystem access");
        assert!(error.to_string().contains("not canonical"));
    }
}
