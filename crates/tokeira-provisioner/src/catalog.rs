//! Published platform/frontend catalog vocabulary and admitted artifact locators.
//!
//! These serializable values are the release pipeline's language-neutral
//! counterpart to source-workspace Cargo metadata. Deserialization validates
//! open identifiers and safe definition-source conventions, but it does not
//! grant execution authority: an installed `tkr` accepts this vocabulary only
//! after the enclosing release/bundle artifact has passed its existing
//! authority and integrity admission policy.

use serde::{Deserialize, Serialize};
use tokeira_orchestrator::{
    DefinitionFormatId, DefinitionSourceExtension, PlatformId, PlatformLaunchClass,
    RelativeDefinitionPath,
};

use crate::EngineIdentity;

/// Provider-neutral platform descriptor emitted by a trusted release pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedPlatformDescriptor {
    /// Open platform identity.
    pub id: PlatformId,
    /// Whether this entry is the distribution's unique default.
    pub is_default: bool,
    /// Generic launch mechanism, independent of platform identity.
    pub launch_class: PlatformLaunchClass,
    /// Private Platform Binding contract used to assemble its provisioners.
    pub binding_contract: u32,
}

/// Language-neutral definition-frontend descriptor emitted by a trusted release pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedDefinitionFrontendDescriptor {
    /// Open Definition Format identity.
    pub format: DefinitionFormatId,
    /// Private Definition Frontend contract used to assemble provisioners.
    pub frontend_contract: u32,
    /// Canonical source extension without a leading dot.
    pub source_extension: DefinitionSourceExtension,
    /// Safe default definition path relative to a deployment directory.
    pub default_relative_path: RelativeDefinitionPath,
}

/// Admitted external locations for one platform/format/engine build.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedProvisionerLocator {
    /// Platform statically assembled into the located provisioner.
    pub platform: PlatformId,
    /// Definition frontend statically assembled into the located provisioner.
    pub format: DefinitionFormatId,
    /// Closure-scoped executable identity named by the bundle.
    pub engine_identity: EngineIdentity,
    /// Opaque authority-owned reference to the create-time Definition Seed.
    pub definition_seed_ref: String,
    /// Opaque authority-owned reference to the provisioner bundle.
    pub bundle_ref: String,
}

/// One authority-admitted published inventory.
///
/// Platform and frontend descriptors remain independent. Locators are the
/// explicit join table selecting a released pair; neither descriptor names or
/// constrains the other dimension.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishedProvisionerCatalog {
    /// Published platform inventory.
    pub platforms: Vec<PublishedPlatformDescriptor>,
    /// Published definition-frontend inventory.
    pub frontends: Vec<PublishedDefinitionFrontendDescriptor>,
    /// Released platform/format/engine artifact locations.
    pub locators: Vec<PublishedProvisionerLocator>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::{BuildProfile, Sha256Digest};

    fn catalog() -> PublishedProvisionerCatalog {
        let platform = PlatformId::new("compose").expect("platform");
        let format = DefinitionFormatId::new("tkd").expect("format");
        PublishedProvisionerCatalog {
            platforms: vec![PublishedPlatformDescriptor {
                id: platform.clone(),
                is_default: true,
                launch_class: PlatformLaunchClass::BoundProvisioner,
                binding_contract: 1,
            }],
            frontends: vec![PublishedDefinitionFrontendDescriptor {
                format: format.clone(),
                frontend_contract: 1,
                source_extension: DefinitionSourceExtension::new("tkd").expect("extension"),
                default_relative_path: RelativeDefinitionPath::new("definition.tkd").expect("path"),
            }],
            locators: vec![PublishedProvisionerLocator {
                platform,
                format,
                engine_identity: EngineIdentity {
                    source_closure: Sha256Digest::from_bytes(b"source"),
                    lock_closure: Sha256Digest::from_bytes(b"lock"),
                    toolchain: "rustc test".to_string(),
                    build_container: None,
                    features: BTreeSet::new(),
                    profile: BuildProfile::Dist,
                },
                definition_seed_ref: "seeds/compose/tkd".to_string(),
                bundle_ref: "bundles/compose/tkd".to_string(),
            }],
        }
    }

    #[test]
    fn published_vocabulary_round_trips_without_weakening_validated_fields() {
        let catalog = catalog();
        let encoded = serde_json::to_value(&catalog).expect("serialize");
        let decoded: PublishedProvisionerCatalog =
            serde_json::from_value(encoded.clone()).expect("deserialize");
        assert_eq!(decoded, catalog);

        let mut invalid_path = encoded.clone();
        invalid_path["frontends"][0]["default_relative_path"] =
            serde_json::Value::String("../definition.tkd".to_string());
        assert!(serde_json::from_value::<PublishedProvisionerCatalog>(invalid_path).is_err());

        let mut unknown_field = encoded;
        unknown_field["platforms"][0]["crate_path"] =
            serde_json::Value::String("arbitrary::binding".to_string());
        assert!(serde_json::from_value::<PublishedProvisionerCatalog>(unknown_field).is_err());
    }
}
