//! The Deployment Claim: one signed statement binding a publication's two
//! halves (deployment-repository spec, §Deployment Claim Contract).
//!
//! The claim rides as `custom["tokeira:deployment"]` on the definition
//! root's target, inside signed targets metadata — so it enjoys the targets
//! role's signature and, transitively, snapshot + timestamp. It carries the
//! two things TUF does not define: the served-companion order (with the
//! product's composite configuration identity over it) and the binding to
//! the exact engine (identity digest + manifest target + build authority).

use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::definition::ConfigurationIdentity;
use uuid::Uuid;

use crate::DeploymentStateLocation;

/// Custom-metadata key carrying the claim on the definition root's target.
pub(crate) const CLAIM_KEY: &str = "tokeira:deployment";
/// Custom-metadata key marking definition companion targets.
pub(crate) const COMPANION_KEY: &str = "tokeira:definition-companion";
/// Custom-metadata key marking engine binary targets.
pub(crate) const ENGINE_ARTIFACT_KEY: &str = "tokeira:engine-artifact";
/// Custom-metadata key marking platform config-tree targets.
pub(crate) const CONFIG_KEY: &str = "tokeira:config";

/// The committed transition a publication captures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transition {
    /// The birth publication, written as part of `deployment create`.
    Create,
    /// A committed `apply`.
    Apply,
    /// A committed `upgrade`.
    Upgrade,
    /// A committed `revert` — a NEW, higher publication whose content is the
    /// reverted-to state; repository versions never move backward.
    Revert,
}

/// The named Deployment this lineage belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentRef {
    /// Deployment name (the repository's subject).
    pub name: String,
    /// Deployment id from `metadata.json`.
    pub id: Uuid,
}

/// The definition half: root, served order, composite identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DefinitionSection {
    /// The definition root's target name (the claim's carrier).
    pub root: String,
    /// Bare companion names in first-request (served) order — may be a
    /// strict subset of the published authored set.
    pub companions: Vec<String>,
    /// The product's composite configuration identity over (root, served
    /// companions) — recomputed at verification with the sole
    /// implementation, `tokeira_platform`'s.
    pub identity: ConfigurationIdentity,
}

/// The engine half: identity binding and the manifest that enumerates the
/// binaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineSection {
    /// Hex digest of the bundle manifest's `EngineIdentity`.
    pub identity_digest: String,
    /// Human-facing version label (never a key).
    pub provisioner_version: String,
    /// The bundle-manifest target name.
    pub manifest: String,
    /// Build-authority tier label — surfaces dev engines on inspect.
    pub build_authority: String,
}

/// The signed statement binding one publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentClaim {
    /// The named Deployment.
    pub deployment: DeploymentRef,
    /// The deployment's platform id.
    pub platform: PlatformId,
    /// Durable placement shared by every authoritative state document and
    /// operation lease. Older claims omit this field and therefore resolve to
    /// local state; credentials are deliberately never part of the claim.
    #[serde(default)]
    pub state: DeploymentStateLocation,
    /// The format selected at create — one format per Deployment, for life.
    pub format: DefinitionFormatId,
    /// The definition half.
    pub definition: DefinitionSection,
    /// The engine half.
    pub engine: EngineSection,
    /// The committed transition this publication captures.
    pub transition: Transition,
    /// The post-transition configuration revision.
    pub config_revision: u64,
}

impl DeploymentClaim {
    /// The `<name>.<format>` target name a claimed companion resolves to.
    pub(crate) fn companion_target(&self, companion: &str) -> String {
        format!("{companion}.{}", self.format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim() -> DeploymentClaim {
        DeploymentClaim {
            deployment: DeploymentRef {
                name: "dev".to_string(),
                id: Uuid::nil(),
            },
            platform: PlatformId::new("compose").unwrap(),
            state: DeploymentStateLocation::Local,
            format: DefinitionFormatId::new("tkd").unwrap(),
            definition: DefinitionSection {
                root: "deployment.tkd".to_string(),
                companions: vec!["platform".to_string(), "observability".to_string()],
                identity: ConfigurationIdentity::compute(
                    &DefinitionFormatId::new("tkd").unwrap(),
                    b"root",
                ),
            },
            engine: EngineSection {
                identity_digest: "ab".repeat(32),
                provisioner_version: "0.1.0".to_string(),
                manifest: "tkp.manifest.json".to_string(),
                build_authority: "hermetic".to_string(),
            },
            transition: Transition::Create,
            config_revision: 0,
        }
    }

    #[test]
    fn claim_serde_round_trips_with_snake_case_transitions() {
        let value = serde_json::to_value(claim()).unwrap();
        assert_eq!(value["transition"], "create");
        assert_eq!(value["state"]["backend"], "local");
        assert_eq!(value["definition"]["identity"]["algorithm"], "sha256-v1");
        let back: DeploymentClaim = serde_json::from_value(value).unwrap();
        assert_eq!(back, claim());

        // Unknown fields refuse: a claim is a contract, not a bag.
        let mut loose = serde_json::to_value(claim()).unwrap();
        loose["surprise"] = serde_json::json!(1);
        assert!(serde_json::from_value::<DeploymentClaim>(loose).is_err());
    }

    #[test]
    fn claims_without_a_state_locator_remain_local() {
        let mut value = serde_json::to_value(claim()).unwrap();
        value.as_object_mut().unwrap().remove("state");

        let decoded: DeploymentClaim = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.state, DeploymentStateLocation::Local);
    }

    #[test]
    fn companion_targets_carry_the_selected_format() {
        assert_eq!(claim().companion_target("platform"), "platform.tkd");
    }
}
