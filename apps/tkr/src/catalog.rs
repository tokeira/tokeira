//! Trusted platform and definition-frontend catalog admission for operator commands.
//!
//! Source catalog construction consumes only descriptors decoded from the
//! recognized workspace by `tokeira-build`. External legacy entries are
//! injected as data, so launch routing depends on [`PlatformLaunchClass`]
//! rather than a platform-name match arm.

use std::path::Path;

use async_trait::async_trait;
use thiserror::Error;
use tokeira_build::{
    DEFINITION_FRONTEND_CONTRACT, DefinitionFrontendPackageDescriptor, DiscoveryError,
    PLATFORM_BINDING_CONTRACT, PackageCoordinates, PlatformLaunchClass, PlatformPackageDescriptor,
    discover_workspace_descriptors,
};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};

/// Coordinates owned by the active trusted catalog source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlatformDescriptorOrigin {
    /// A conventional library found in recognized workspace Cargo metadata.
    Workspace(PackageCoordinates),
    /// A separately admitted legacy entry with no source-build coordinates.
    External,
}

/// Provider-neutral platform selection descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformDescriptor {
    /// Open platform identity.
    pub id: PlatformId,
    /// Exactly one admitted entry is the catalog default.
    pub is_default: bool,
    /// Generic launch mechanism.
    pub launch_class: PlatformLaunchClass,
    /// Private platform binding contract.
    pub binding_contract: u32,
    /// Coordinates supplied by the selected trusted source family.
    pub origin: PlatformDescriptorOrigin,
}

impl PlatformDescriptor {
    /// Construct a separately admitted entry, including the retained Local adapter.
    pub fn external(
        id: PlatformId,
        is_default: bool,
        launch_class: PlatformLaunchClass,
        binding_contract: u32,
    ) -> Self {
        Self {
            id,
            is_default,
            launch_class,
            binding_contract,
            origin: PlatformDescriptorOrigin::External,
        }
    }
}

impl From<PlatformPackageDescriptor> for PlatformDescriptor {
    fn from(value: PlatformPackageDescriptor) -> Self {
        Self {
            id: value.id,
            is_default: value.is_default,
            launch_class: value.launch_class,
            binding_contract: value.binding_contract,
            origin: PlatformDescriptorOrigin::Workspace(value.package),
        }
    }
}

/// Language-neutral definition-frontend selection descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionFrontendDescriptor {
    /// Trusted workspace package metadata and conventional library coordinates.
    pub package: DefinitionFrontendPackageDescriptor,
}

impl DefinitionFrontendDescriptor {
    /// Borrow the descriptor's open format identity.
    pub fn format(&self) -> &DefinitionFormatId {
        &self.package.format
    }
}

/// Refusal from platform catalog admission or resolution.
#[derive(Debug, Error)]
pub enum PlatformCatalogError {
    /// Recognized workspace metadata could not be decoded.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The complete selected source family is not internally consistent.
    #[error("invalid platform catalog: {0}")]
    InvalidCatalog(String),
    /// An explicit platform id is not present in the admitted catalog.
    #[error("unknown platform `{requested}`; supported platforms: {supported}")]
    NotFound {
        /// Requested canonical platform id.
        requested: PlatformId,
        /// Deterministic comma-separated supported inventory.
        supported: String,
    },
}

/// Refusal from definition-frontend catalog admission or resolution.
#[derive(Debug, Error)]
pub enum DefinitionFrontendCatalogError {
    /// Recognized workspace metadata could not be decoded.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The complete selected source family is not internally consistent.
    #[error("invalid definition-frontend catalog: {0}")]
    InvalidCatalog(String),
    /// An explicit format id is not present in the admitted catalog.
    #[error("unknown definition format `{requested}`; supported formats: {supported}")]
    NotFound {
        /// Requested canonical definition-format id.
        requested: DefinitionFormatId,
        /// Deterministic comma-separated supported inventory.
        supported: String,
    },
}

/// Failure to construct both independent catalogs from one metadata snapshot.
#[derive(Debug, Error)]
pub enum WorkspaceCatalogError {
    /// Cargo metadata discovery failed before either catalog was admitted.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The platform inventory was invalid.
    #[error(transparent)]
    Platform(#[from] PlatformCatalogError),
    /// The frontend inventory was invalid.
    #[error(transparent)]
    Frontend(#[from] DefinitionFrontendCatalogError),
}

/// Platform inventory selected from exactly one trusted source family.
#[async_trait]
pub trait PlatformCatalog: Send + Sync {
    /// Resolve the unique catalog default.
    async fn default(&self) -> Result<PlatformDescriptor, PlatformCatalogError>;

    /// Resolve one explicit canonical id.
    async fn resolve(&self, id: &PlatformId) -> Result<PlatformDescriptor, PlatformCatalogError>;

    /// Return the deterministic admitted inventory.
    async fn list(&self) -> Result<Vec<PlatformDescriptor>, PlatformCatalogError>;
}

/// Definition-frontend inventory selected independently from platform identity.
#[async_trait]
pub trait DefinitionFrontendCatalog: Send + Sync {
    /// Resolve one explicit canonical format id.
    async fn resolve(
        &self,
        format: &DefinitionFormatId,
    ) -> Result<DefinitionFrontendDescriptor, DefinitionFrontendCatalogError>;

    /// Return the deterministic admitted format inventory.
    async fn list(
        &self,
    ) -> Result<Vec<DefinitionFrontendDescriptor>, DefinitionFrontendCatalogError>;
}

/// Admitted source-workspace platform inventory.
#[derive(Debug, Clone)]
pub struct WorkspacePlatformCatalog {
    entries: Vec<PlatformDescriptor>,
    default_index: usize,
}

impl WorkspacePlatformCatalog {
    /// Admit decoded workspace packages plus separately trusted legacy entries.
    pub fn new(
        workspace: Vec<PlatformPackageDescriptor>,
        external: Vec<PlatformDescriptor>,
    ) -> Result<Self, PlatformCatalogError> {
        let mut entries = workspace
            .into_iter()
            .map(PlatformDescriptor::from)
            .chain(external)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.id.cmp(&right.id));

        for entry in &entries {
            if entry.binding_contract != PLATFORM_BINDING_CONTRACT {
                return Err(PlatformCatalogError::InvalidCatalog(format!(
                    "platform `{}` uses binding contract {}; supported contract is {}",
                    entry.id, entry.binding_contract, PLATFORM_BINDING_CONTRACT
                )));
            }
        }
        for pair in entries.windows(2) {
            if pair[0].id == pair[1].id {
                return Err(PlatformCatalogError::InvalidCatalog(format!(
                    "duplicate platform id `{}` across active catalog entries",
                    pair[0].id
                )));
            }
        }
        let defaults = entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| entry.is_default.then_some(index))
            .collect::<Vec<_>>();
        let [default_index] = defaults.as_slice() else {
            return Err(PlatformCatalogError::InvalidCatalog(format!(
                "expected exactly one default platform; found {}",
                defaults.len()
            )));
        };

        Ok(Self {
            entries,
            default_index: *default_index,
        })
    }

    fn supported(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[async_trait]
impl PlatformCatalog for WorkspacePlatformCatalog {
    async fn default(&self) -> Result<PlatformDescriptor, PlatformCatalogError> {
        Ok(self
            .entries
            .get(self.default_index)
            .expect("catalog admission records an in-bounds unique default")
            .clone())
    }

    async fn resolve(&self, id: &PlatformId) -> Result<PlatformDescriptor, PlatformCatalogError> {
        self.entries
            .binary_search_by(|entry| entry.id.cmp(id))
            .map(|index| self.entries[index].clone())
            .map_err(|_| PlatformCatalogError::NotFound {
                requested: id.clone(),
                supported: self.supported(),
            })
    }

    async fn list(&self) -> Result<Vec<PlatformDescriptor>, PlatformCatalogError> {
        Ok(self.entries.clone())
    }
}

/// Admitted source-workspace definition-frontend inventory.
#[derive(Debug, Clone)]
pub struct WorkspaceDefinitionFrontendCatalog {
    entries: Vec<DefinitionFrontendDescriptor>,
}

impl WorkspaceDefinitionFrontendCatalog {
    /// Admit independently decoded workspace frontend packages.
    pub fn new(
        workspace: Vec<DefinitionFrontendPackageDescriptor>,
    ) -> Result<Self, DefinitionFrontendCatalogError> {
        let mut entries = workspace
            .into_iter()
            .map(|package| DefinitionFrontendDescriptor { package })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.format().cmp(right.format()));
        if entries.is_empty() {
            return Err(DefinitionFrontendCatalogError::InvalidCatalog(
                "expected at least one definition frontend".to_string(),
            ));
        }
        for entry in &entries {
            if entry.package.frontend_contract != DEFINITION_FRONTEND_CONTRACT {
                return Err(DefinitionFrontendCatalogError::InvalidCatalog(format!(
                    "format `{}` uses frontend contract {}; supported contract is {}",
                    entry.format(),
                    entry.package.frontend_contract,
                    DEFINITION_FRONTEND_CONTRACT
                )));
            }
        }
        for pair in entries.windows(2) {
            if pair[0].format() == pair[1].format() {
                return Err(DefinitionFrontendCatalogError::InvalidCatalog(format!(
                    "duplicate definition format `{}` across active catalog entries",
                    pair[0].format()
                )));
            }
        }
        Ok(Self { entries })
    }

    fn supported(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.format().as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[async_trait]
impl DefinitionFrontendCatalog for WorkspaceDefinitionFrontendCatalog {
    async fn resolve(
        &self,
        format: &DefinitionFormatId,
    ) -> Result<DefinitionFrontendDescriptor, DefinitionFrontendCatalogError> {
        self.entries
            .binary_search_by(|entry| entry.format().cmp(format))
            .map(|index| self.entries[index].clone())
            .map_err(|_| DefinitionFrontendCatalogError::NotFound {
                requested: format.clone(),
                supported: self.supported(),
            })
    }

    async fn list(
        &self,
    ) -> Result<Vec<DefinitionFrontendDescriptor>, DefinitionFrontendCatalogError> {
        Ok(self.entries.clone())
    }
}

/// Both independent source catalogs decoded from one Cargo metadata snapshot.
#[derive(Debug, Clone)]
pub struct WorkspaceCatalogs {
    /// Platform identities and launch classes.
    pub platforms: WorkspacePlatformCatalog,
    /// Definition formats and conventional frontend libraries.
    pub frontends: WorkspaceDefinitionFrontendCatalog,
}

impl WorkspaceCatalogs {
    /// Discover a recognized workspace once and admit its independent catalogs.
    pub fn discover(
        workspace_root: &Path,
        external_platforms: Vec<PlatformDescriptor>,
    ) -> Result<Self, WorkspaceCatalogError> {
        let descriptors = discover_workspace_descriptors(workspace_root)?;
        Ok(Self {
            platforms: WorkspacePlatformCatalog::new(descriptors.platforms, external_platforms)?,
            frontends: WorkspaceDefinitionFrontendCatalog::new(descriptors.frontends)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> PlatformId {
        PlatformId::new(value).expect("canonical test platform id")
    }

    fn external(value: &str, is_default: bool) -> PlatformDescriptor {
        PlatformDescriptor::external(
            id(value),
            is_default,
            PlatformLaunchClass::LegacyInProcess,
            PLATFORM_BINDING_CONTRACT,
        )
    }

    #[tokio::test]
    async fn current_workspace_resolves_the_independent_tkd_catalog() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let catalogs = WorkspaceCatalogs::discover(&root, vec![external("local", true)])
            .expect("discover workspace catalogs");

        assert_eq!(
            catalogs
                .platforms
                .default()
                .await
                .expect("default platform")
                .id
                .as_str(),
            "local"
        );
        let frontend = catalogs
            .frontends
            .resolve(&DefinitionFormatId::new("tkd").expect("canonical format"))
            .await
            .expect("resolve tkd");
        assert_eq!(frontend.package.package.package_name, "tokeira-tkd");
    }

    #[test]
    fn platform_catalog_requires_one_default_and_unique_ids() {
        let no_default = WorkspacePlatformCatalog::new(Vec::new(), vec![external("local", false)])
            .expect_err("zero defaults must be rejected");
        assert!(no_default.to_string().contains("exactly one default"));

        let duplicate = WorkspacePlatformCatalog::new(
            Vec::new(),
            vec![external("local", true), external("local", false)],
        )
        .expect_err("duplicates must be rejected");
        assert!(duplicate.to_string().contains("duplicate platform id"));

        let multiple_defaults = WorkspacePlatformCatalog::new(
            Vec::new(),
            vec![external("local", true), external("compose", true)],
        )
        .expect_err("multiple defaults must be rejected");
        assert!(
            multiple_defaults
                .to_string()
                .contains("exactly one default")
        );
    }

    #[tokio::test]
    async fn unsupported_name_reports_sorted_admitted_inventory() {
        let catalog = WorkspacePlatformCatalog::new(
            Vec::new(),
            vec![external("zeta", false), external("alpha", true)],
        )
        .expect("valid catalog");
        let error = catalog
            .resolve(&id("missing"))
            .await
            .expect_err("unknown id must be rejected");
        assert!(error.to_string().contains("alpha, zeta"));
    }
}
