//! Trusted platform and definition-frontend catalog admission for operator commands.
//!
//! Source catalog construction consumes only descriptors decoded from the
//! recognized workspace by `tokeira-build`. External legacy entries are
//! injected as data, so launch routing depends on [`PlatformLaunchClass`]
//! rather than a platform-name match arm.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use async_trait::async_trait;
use thiserror::Error;
use tokeira_build::{
    DEFINITION_FRONTEND_CONTRACT, DefinitionFrontendPackageDescriptor, DiscoveryError,
    PLATFORM_BINDING_CONTRACT, PackageCoordinates, PlatformPackageDescriptor,
    discover_workspace_descriptors,
};
use tokeira_orchestrator::{
    DefinitionFormatId, DefinitionSourceExtension, PlatformId, PlatformLaunchClass,
    RelativeDefinitionPath,
};
use tokeira_provisioner::{PublishedProvisionerCatalog, PublishedProvisionerLocator};

/// Coordinates owned by the active trusted catalog source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlatformDescriptorOrigin {
    /// A conventional library found in recognized workspace Cargo metadata.
    Workspace(PackageCoordinates),
    /// Authority-admitted seed/bundle locations for installed operation.
    Published(Vec<PublishedProvisionerLocator>),
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

/// Coordinates owned by the active trusted definition-frontend source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefinitionFrontendDescriptorOrigin {
    /// A conventional library found in recognized workspace Cargo metadata.
    Workspace(PackageCoordinates),
    /// Authority-admitted seed/bundle locations for installed operation.
    Published(Vec<PublishedProvisionerLocator>),
}

/// Language-neutral definition-frontend selection descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionFrontendDescriptor {
    /// Open Definition Format identity.
    pub format: DefinitionFormatId,
    /// Private Definition Frontend contract.
    pub frontend_contract: u32,
    /// Canonical source extension without a leading dot.
    pub source_extension: DefinitionSourceExtension,
    /// Safe default definition path relative to a deployment directory.
    pub default_relative_path: RelativeDefinitionPath,
    /// Coordinates supplied by the selected trusted source family.
    pub origin: DefinitionFrontendDescriptorOrigin,
}

impl DefinitionFrontendDescriptor {
    /// Borrow the descriptor's open format identity.
    pub fn format(&self) -> &DefinitionFormatId {
        &self.format
    }
}

impl From<DefinitionFrontendPackageDescriptor> for DefinitionFrontendDescriptor {
    fn from(value: DefinitionFrontendPackageDescriptor) -> Self {
        Self {
            format: value.format,
            frontend_contract: value.frontend_contract,
            source_extension: value.source_extension,
            default_relative_path: value.default_relative_path,
            origin: DefinitionFrontendDescriptorOrigin::Workspace(value.package),
        }
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

fn admit_platform_entries(
    mut entries: Vec<PlatformDescriptor>,
) -> Result<(Vec<PlatformDescriptor>, usize), PlatformCatalogError> {
    entries.sort_by(|left, right| left.id.cmp(&right.id));
    for entry in &entries {
        if entry.launch_class == PlatformLaunchClass::LegacyInProcess
            && !matches!(entry.origin, PlatformDescriptorOrigin::External)
        {
            return Err(PlatformCatalogError::InvalidCatalog(format!(
                "platform `{}` uses the legacy in-process launch class outside the external catalog",
                entry.id
            )));
        }
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
    Ok((entries, *default_index))
}

fn admit_frontend_entries(
    mut entries: Vec<DefinitionFrontendDescriptor>,
) -> Result<Vec<DefinitionFrontendDescriptor>, DefinitionFrontendCatalogError> {
    entries.sort_by(|left, right| left.format().cmp(right.format()));
    if entries.is_empty() {
        return Err(DefinitionFrontendCatalogError::InvalidCatalog(
            "expected at least one definition frontend".to_string(),
        ));
    }
    for entry in &entries {
        if entry.frontend_contract != DEFINITION_FRONTEND_CONTRACT {
            return Err(DefinitionFrontendCatalogError::InvalidCatalog(format!(
                "format `{}` uses frontend contract {}; supported contract is {}",
                entry.format(),
                entry.frontend_contract,
                DEFINITION_FRONTEND_CONTRACT
            )));
        }
        let path_extension = entry
            .default_relative_path
            .as_path()
            .extension()
            .and_then(|extension| extension.to_str());
        if path_extension != Some(entry.source_extension.as_str()) {
            return Err(DefinitionFrontendCatalogError::InvalidCatalog(format!(
                "default path `{}` for format `{}` must use source extension `.{}`",
                entry.default_relative_path.as_str(),
                entry.format(),
                entry.source_extension.as_str()
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
    Ok(entries)
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
        let entries = workspace
            .into_iter()
            .map(PlatformDescriptor::from)
            .chain(external)
            .collect::<Vec<_>>();
        let (entries, default_index) = admit_platform_entries(entries)?;
        Ok(Self {
            entries,
            default_index,
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
        let entries = workspace
            .into_iter()
            .map(DefinitionFrontendDescriptor::from)
            .collect::<Vec<_>>();
        let entries = admit_frontend_entries(entries)?;
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

/// Admitted published platform inventory.
#[derive(Debug, Clone)]
pub struct PublishedPlatformCatalog {
    entries: Vec<PlatformDescriptor>,
    default_index: usize,
}

impl PublishedPlatformCatalog {
    /// Admit one authority-verified published inventory plus retained legacy entries.
    pub fn new(
        catalog: &PublishedProvisionerCatalog,
        external: Vec<PlatformDescriptor>,
    ) -> Result<Self, PlatformCatalogError> {
        let index =
            published_locator_index(catalog).map_err(PlatformCatalogError::InvalidCatalog)?;
        let entries = catalog
            .platforms
            .iter()
            .map(|entry| PlatformDescriptor {
                id: entry.id.clone(),
                is_default: entry.is_default,
                launch_class: entry.launch_class,
                binding_contract: entry.binding_contract,
                origin: PlatformDescriptorOrigin::Published(
                    index
                        .by_platform
                        .get(&entry.id)
                        .cloned()
                        .unwrap_or_default(),
                ),
            })
            .chain(external)
            .collect::<Vec<_>>();
        let (entries, default_index) = admit_platform_entries(entries)?;
        Ok(Self {
            entries,
            default_index,
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
impl PlatformCatalog for PublishedPlatformCatalog {
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

/// Admitted published definition-frontend inventory.
#[derive(Debug, Clone)]
pub struct PublishedDefinitionFrontendCatalog {
    entries: Vec<DefinitionFrontendDescriptor>,
}

impl PublishedDefinitionFrontendCatalog {
    /// Admit one authority-verified published format inventory.
    pub fn new(
        catalog: &PublishedProvisionerCatalog,
    ) -> Result<Self, DefinitionFrontendCatalogError> {
        let index = published_locator_index(catalog)
            .map_err(DefinitionFrontendCatalogError::InvalidCatalog)?;
        let entries = catalog
            .frontends
            .iter()
            .map(|entry| DefinitionFrontendDescriptor {
                format: entry.format.clone(),
                frontend_contract: entry.frontend_contract,
                source_extension: entry.source_extension.clone(),
                default_relative_path: entry.default_relative_path.clone(),
                origin: DefinitionFrontendDescriptorOrigin::Published(
                    index
                        .by_format
                        .get(&entry.format)
                        .cloned()
                        .unwrap_or_default(),
                ),
            })
            .collect::<Vec<_>>();
        Ok(Self {
            entries: admit_frontend_entries(entries)?,
        })
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
impl DefinitionFrontendCatalog for PublishedDefinitionFrontendCatalog {
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

#[derive(Debug)]
struct PublishedLocatorIndex {
    by_platform: BTreeMap<PlatformId, Vec<PublishedProvisionerLocator>>,
    by_format: BTreeMap<DefinitionFormatId, Vec<PublishedProvisionerLocator>>,
}

fn published_locator_index(
    catalog: &PublishedProvisionerCatalog,
) -> Result<PublishedLocatorIndex, String> {
    let platforms = catalog
        .platforms
        .iter()
        .map(|entry| entry.id.clone())
        .collect::<BTreeSet<_>>();
    let formats = catalog
        .frontends
        .iter()
        .map(|entry| entry.format.clone())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut by_platform = BTreeMap::<_, Vec<_>>::new();
    let mut by_format = BTreeMap::<_, Vec<_>>::new();
    for locator in &catalog.locators {
        if !platforms.contains(&locator.platform) {
            return Err(format!(
                "published locator references unknown platform `{}`",
                locator.platform
            ));
        }
        if !formats.contains(&locator.format) {
            return Err(format!(
                "published locator references unknown definition format `{}`",
                locator.format
            ));
        }
        for (field, value) in [
            ("definition seed", locator.definition_seed_ref.as_str()),
            ("bundle", locator.bundle_ref.as_str()),
        ] {
            if value.trim().is_empty() || value.chars().any(char::is_control) {
                return Err(format!(
                    "published {field} locator for `{}`/`{}` is empty or contains control characters",
                    locator.platform, locator.format
                ));
            }
        }
        let key = (
            locator.platform.clone(),
            locator.format.clone(),
            locator.engine_identity.digest(),
        );
        if !seen.insert(key) {
            return Err(format!(
                "duplicate published locator for `{}`/`{}` engine {}",
                locator.platform,
                locator.format,
                locator.engine_identity.digest().to_hex()
            ));
        }
        by_platform
            .entry(locator.platform.clone())
            .or_default()
            .push(locator.clone());
        by_format
            .entry(locator.format.clone())
            .or_default()
            .push(locator.clone());
    }
    for platform in platforms {
        if !by_platform.contains_key(&platform) {
            return Err(format!(
                "published platform `{platform}` has no admitted seed/bundle locator"
            ));
        }
    }
    for format in formats {
        if !by_format.contains_key(&format) {
            return Err(format!(
                "published definition format `{format}` has no admitted seed/bundle locator"
            ));
        }
    }
    for locators in by_platform.values_mut().chain(by_format.values_mut()) {
        locators.sort_by(|left, right| {
            left.platform
                .cmp(&right.platform)
                .then_with(|| left.format.cmp(&right.format))
                .then_with(|| {
                    left.engine_identity
                        .digest()
                        .cmp(&right.engine_identity.digest())
                })
        });
    }
    Ok(PublishedLocatorIndex {
        by_platform,
        by_format,
    })
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

/// Failure to construct both independent published catalogs from one admitted snapshot.
#[derive(Debug, Error)]
pub enum PublishedCatalogError {
    /// The platform inventory was invalid.
    #[error(transparent)]
    Platform(#[from] PlatformCatalogError),
    /// The frontend inventory was invalid.
    #[error(transparent)]
    Frontend(#[from] DefinitionFrontendCatalogError),
}

/// Both independent catalogs projected from one authority-admitted published inventory.
#[derive(Debug, Clone)]
pub struct PublishedCatalogs {
    /// Platform identities, launch classes, and admitted locators.
    pub platforms: PublishedPlatformCatalog,
    /// Definition formats, source conventions, and admitted locators.
    pub frontends: PublishedDefinitionFrontendCatalog,
}

impl PublishedCatalogs {
    /// Admit a published inventory without scanning installed crates or arbitrary paths.
    pub fn new(
        catalog: &PublishedProvisionerCatalog,
        external_platforms: Vec<PlatformDescriptor>,
    ) -> Result<Self, PublishedCatalogError> {
        Ok(Self {
            platforms: PublishedPlatformCatalog::new(catalog, external_platforms)?,
            frontends: PublishedDefinitionFrontendCatalog::new(catalog)?,
        })
    }
}

/// Trusted catalog source selected for one command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSourceFamily {
    /// Recognized source-workspace Cargo metadata.
    Workspace,
    /// Authority-admitted published descriptors and artifact locators.
    Published,
}

/// Exactly one active catalog family; source and published entries are never merged.
#[derive(Debug, Clone)]
pub enum TrustedCatalogs {
    /// Developer source-workspace catalogs.
    Workspace(WorkspaceCatalogs),
    /// Installed-distribution published catalogs.
    Published(PublishedCatalogs),
}

impl TrustedCatalogs {
    /// Select the complete source family for one command.
    ///
    /// A recognized workspace takes precedence over an available published
    /// inventory. Selecting the whole family before resolution prevents equal
    /// ids from two authorities from being silently merged.
    pub fn select(
        workspace: Option<WorkspaceCatalogs>,
        published: Option<PublishedCatalogs>,
    ) -> Result<Self, TrustedCatalogError> {
        workspace
            .map(Self::Workspace)
            .or_else(|| published.map(Self::Published))
            .ok_or(TrustedCatalogError::NoSource)
    }

    /// Report the selected source family.
    pub fn source_family(&self) -> CatalogSourceFamily {
        match self {
            Self::Workspace(_) => CatalogSourceFamily::Workspace,
            Self::Published(_) => CatalogSourceFamily::Published,
        }
    }

    /// Borrow the active provider-neutral platform catalog.
    pub fn platforms(&self) -> &dyn PlatformCatalog {
        match self {
            Self::Workspace(catalogs) => &catalogs.platforms,
            Self::Published(catalogs) => &catalogs.platforms,
        }
    }

    /// Borrow the active language-neutral frontend catalog.
    pub fn frontends(&self) -> &dyn DefinitionFrontendCatalog {
        match self {
            Self::Workspace(catalogs) => &catalogs.frontends,
            Self::Published(catalogs) => &catalogs.frontends,
        }
    }
}

/// Refusal to select a trusted catalog family.
#[derive(Debug, Error)]
pub enum TrustedCatalogError {
    /// Neither a recognized workspace nor an admitted published inventory was available.
    #[error("no trusted platform/definition-frontend catalog source is available")]
    NoSource,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use tokeira_provisioner::{
        BuildProfile, EngineIdentity, PublishedDefinitionFrontendDescriptor,
        PublishedPlatformDescriptor, Sha256Digest,
    };

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

    fn format(value: &str) -> DefinitionFormatId {
        DefinitionFormatId::new(value).expect("canonical test format")
    }

    fn engine(value: &str) -> EngineIdentity {
        EngineIdentity {
            source_closure: Sha256Digest::from_bytes(format!("source-{value}").as_bytes()),
            lock_closure: Sha256Digest::from_bytes(format!("lock-{value}").as_bytes()),
            toolchain: "rustc test".to_string(),
            build_container: None,
            features: BTreeSet::new(),
            profile: BuildProfile::Dist,
        }
    }

    fn published(platform: &str, format_id: &str) -> PublishedProvisionerCatalog {
        PublishedProvisionerCatalog {
            platforms: vec![PublishedPlatformDescriptor {
                id: id(platform),
                is_default: true,
                launch_class: PlatformLaunchClass::BoundProvisioner,
                binding_contract: PLATFORM_BINDING_CONTRACT,
            }],
            frontends: vec![PublishedDefinitionFrontendDescriptor {
                format: format(format_id),
                frontend_contract: DEFINITION_FRONTEND_CONTRACT,
                source_extension: DefinitionSourceExtension::new(format_id)
                    .expect("canonical extension"),
                default_relative_path: RelativeDefinitionPath::new(format!(
                    "definition.{format_id}"
                ))
                .expect("safe default path"),
            }],
            locators: vec![PublishedProvisionerLocator {
                platform: id(platform),
                format: format(format_id),
                engine_identity: engine("one"),
                definition_seed_ref: format!("seeds/{platform}/{format_id}"),
                bundle_ref: format!("bundles/{platform}/{format_id}"),
            }],
        }
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
        let DefinitionFrontendDescriptorOrigin::Workspace(package) = frontend.origin else {
            panic!("source discovery must return workspace coordinates");
        };
        assert_eq!(package.package_name, "tokeira-tkd");
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

    #[tokio::test]
    async fn published_catalog_resolves_open_platform_and_frontend_ids() {
        let published = published("eks-blue", "tkdp-next");
        let catalogs = PublishedCatalogs::new(&published, Vec::new()).expect("published catalogs");

        let platform = catalogs
            .platforms
            .resolve(&id("eks-blue"))
            .await
            .expect("resolve arbitrary platform id");
        assert_eq!(platform.id.as_str(), "eks-blue");
        assert!(matches!(
            platform.origin,
            PlatformDescriptorOrigin::Published(ref locators) if locators.len() == 1
        ));

        let frontend = catalogs
            .frontends
            .resolve(&format("tkdp-next"))
            .await
            .expect("resolve arbitrary frontend id");
        assert_eq!(
            frontend.default_relative_path.as_str(),
            "definition.tkdp-next"
        );
        assert!(matches!(
            frontend.origin,
            DefinitionFrontendDescriptorOrigin::Published(ref locators) if locators.len() == 1
        ));
    }

    #[test]
    fn published_catalog_rejects_unjoined_or_ambiguous_locators() {
        let mut missing = published("compose", "tkd");
        missing.locators.clear();
        let error = PublishedCatalogs::new(&missing, Vec::new())
            .expect_err("every descriptor needs an admitted locator");
        assert!(
            error
                .to_string()
                .contains("no admitted seed/bundle locator")
        );

        let mut duplicate = published("compose", "tkd");
        duplicate.locators.push(duplicate.locators[0].clone());
        let error = PublishedCatalogs::new(&duplicate, Vec::new())
            .expect_err("duplicate engine locator must be rejected");
        assert!(error.to_string().contains("duplicate published locator"));

        let mut unknown = published("compose", "tkd");
        unknown.locators[0].platform = id("eks");
        let error = PublishedCatalogs::new(&unknown, Vec::new())
            .expect_err("unknown join target must be rejected");
        assert!(error.to_string().contains("unknown platform `eks`"));
    }

    #[tokio::test]
    async fn recognized_workspace_takes_precedence_as_one_complete_family() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let workspace = WorkspaceCatalogs::discover(&root, vec![external("local", true)])
            .expect("workspace catalogs");
        let published = PublishedCatalogs::new(&published("compose", "tkdp"), Vec::new())
            .expect("published catalogs");

        let selected =
            TrustedCatalogs::select(Some(workspace), Some(published)).expect("one source family");
        assert_eq!(selected.source_family(), CatalogSourceFamily::Workspace);
        assert_eq!(
            selected
                .platforms()
                .resolve(&id("compose"))
                .await
                .expect("workspace Compose descriptor")
                .id,
            id("compose")
        );
        assert!(selected.frontends().resolve(&format("tkdp")).await.is_err());
    }

    proptest! {
        // Feature: platform-builder-abstraction, Property 22: catalog resolution and assembly select exactly one platform and frontend
        #[test]
        fn published_catalog_admission_matches_the_reference_model(
            platform_inputs in prop::collection::vec(("[a-z][a-z0-9]{0,5}", any::<bool>()), 1..7),
            format_inputs in prop::collection::vec("[a-z][a-z0-9]{0,5}", 1..7),
            bad_platform_contract in any::<bool>(),
            bad_frontend_contract in any::<bool>(),
            unsupported_launch_class in any::<bool>(),
            mismatched_path in any::<bool>(),
            duplicate_locator in any::<bool>(),
        ) {
            let platforms = platform_inputs
                .iter()
                .enumerate()
                .map(|(index, (value, is_default))| PublishedPlatformDescriptor {
                    id: id(value),
                    is_default: *is_default,
                    launch_class: if unsupported_launch_class && index == 0 {
                        PlatformLaunchClass::LegacyInProcess
                    } else {
                        PlatformLaunchClass::BoundProvisioner
                    },
                    binding_contract: if bad_platform_contract && index == 0 {
                        PLATFORM_BINDING_CONTRACT + 1
                    } else {
                        PLATFORM_BINDING_CONTRACT
                    },
                })
                .collect::<Vec<_>>();
            let frontends = format_inputs
                .iter()
                .enumerate()
                .map(|(index, value)| PublishedDefinitionFrontendDescriptor {
                    format: format(value),
                    frontend_contract: if bad_frontend_contract && index == 0 {
                        DEFINITION_FRONTEND_CONTRACT + 1
                    } else {
                        DEFINITION_FRONTEND_CONTRACT
                    },
                    source_extension: DefinitionSourceExtension::new(value)
                        .expect("generated extension is canonical"),
                    default_relative_path: RelativeDefinitionPath::new(if mismatched_path && index == 0 {
                        format!("definition.{value}-other")
                    } else {
                        format!("definition.{value}")
                    })
                    .expect("generated path is safe"),
                })
                .collect::<Vec<_>>();
            let distinct_platforms = platforms
                .iter()
                .map(|entry| entry.id.clone())
                .collect::<BTreeSet<_>>();
            let distinct_formats = frontends
                .iter()
                .map(|entry| entry.format.clone())
                .collect::<BTreeSet<_>>();
            let mut locators = distinct_platforms
                .iter()
                .flat_map(|platform| {
                    distinct_formats.iter().map(move |format_id| PublishedProvisionerLocator {
                        platform: platform.clone(),
                        format: format_id.clone(),
                        engine_identity: engine(&format!("{platform}-{format_id}")),
                        definition_seed_ref: format!("seed/{platform}/{format_id}"),
                        bundle_ref: format!("bundle/{platform}/{format_id}"),
                    })
                })
                .collect::<Vec<_>>();
            if duplicate_locator {
                locators.push(locators[0].clone());
            }
            let input = PublishedProvisionerCatalog {
                platforms,
                frontends,
                locators,
            };

            let platform_ids_are_unique = distinct_platforms.len() == platform_inputs.len();
            let format_ids_are_unique = distinct_formats.len() == format_inputs.len();
            let default_count = platform_inputs
                .iter()
                .filter(|(_, is_default)| *is_default)
                .count();
            let expected_platform_valid = platform_ids_are_unique
                && default_count == 1
                && !bad_platform_contract
                && !unsupported_launch_class;
            let expected_frontend_valid = format_ids_are_unique
                && !bad_frontend_contract
                && !mismatched_path;
            let expected_descriptor_valid = expected_platform_valid && expected_frontend_valid;
            let expected_valid = expected_descriptor_valid && !duplicate_locator;

            let workspace_platforms = input
                .platforms
                .iter()
                .enumerate()
                .map(|(index, entry)| PlatformPackageDescriptor {
                    id: entry.id.clone(),
                    is_default: entry.is_default,
                    launch_class: entry.launch_class,
                    binding_contract: entry.binding_contract,
                    package: PackageCoordinates {
                        package_id: format!("platform-package-{index}"),
                        package_name: format!("platform-package-{index}"),
                        library_target: format!("platform_package_{index}"),
                        manifest_path: Path::new("/workspace")
                            .join(format!("platform-{index}/Cargo.toml")),
                    },
                })
                .collect::<Vec<_>>();
            let workspace_frontends = input
                .frontends
                .iter()
                .enumerate()
                .map(|(index, entry)| DefinitionFrontendPackageDescriptor {
                    format: entry.format.clone(),
                    frontend_contract: entry.frontend_contract,
                    source_extension: entry.source_extension.clone(),
                    default_relative_path: entry.default_relative_path.clone(),
                    package: PackageCoordinates {
                        package_id: format!("frontend-package-{index}"),
                        package_name: format!("frontend-package-{index}"),
                        library_target: format!("frontend_package_{index}"),
                        manifest_path: Path::new("/workspace")
                            .join(format!("frontend-{index}/Cargo.toml")),
                    },
                })
                .collect::<Vec<_>>();
            let workspace_platforms = WorkspacePlatformCatalog::new(workspace_platforms, Vec::new());
            let workspace_frontends = WorkspaceDefinitionFrontendCatalog::new(workspace_frontends);
            prop_assert_eq!(workspace_platforms.is_ok(), expected_platform_valid);
            prop_assert_eq!(workspace_frontends.is_ok(), expected_frontend_valid);

            let actual = PublishedCatalogs::new(&input, Vec::new());
            prop_assert_eq!(actual.is_ok(), expected_valid);
            if let Ok(catalogs) = actual {
                let actual_platforms = catalogs
                    .platforms
                    .entries
                    .iter()
                    .map(|entry| entry.id.clone())
                    .collect::<Vec<_>>();
                let actual_formats = catalogs
                    .frontends
                    .entries
                    .iter()
                    .map(|entry| entry.format.clone())
                    .collect::<Vec<_>>();
                prop_assert_eq!(actual_platforms, distinct_platforms.into_iter().collect::<Vec<_>>());
                prop_assert_eq!(actual_formats, distinct_formats.into_iter().collect::<Vec<_>>());

                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("construct property runtime");
                let expected_default = input
                    .platforms
                    .iter()
                    .find(|entry| entry.is_default)
                    .expect("valid catalog has one default");
                let resolved_default = runtime
                    .block_on(catalogs.platforms.default())
                    .expect("resolve admitted default");
                prop_assert_eq!(resolved_default.id, expected_default.id.clone());
                for expected in &input.platforms {
                    let resolved = runtime
                        .block_on(catalogs.platforms.resolve(&expected.id))
                        .expect("resolve admitted platform");
                    prop_assert_eq!(resolved.id, expected.id.clone());
                    prop_assert_eq!(resolved.launch_class, expected.launch_class);
                }
                for expected in &input.frontends {
                    let resolved = runtime
                        .block_on(catalogs.frontends.resolve(&expected.format))
                        .expect("resolve admitted frontend");
                    prop_assert_eq!(resolved.format, expected.format.clone());
                    prop_assert_eq!(resolved.source_extension, expected.source_extension.clone());
                }
                let missing_platform = id("catalog-missing");
                let missing_format = format("catalog-missing");
                let platform_error = runtime
                    .block_on(catalogs.platforms.resolve(&missing_platform))
                    .expect_err("unknown platform must be refused");
                let frontend_error = runtime
                    .block_on(catalogs.frontends.resolve(&missing_format))
                    .expect_err("unknown frontend must be refused");
                prop_assert!(platform_error.to_string().contains("supported platforms"));
                prop_assert!(frontend_error.to_string().contains("supported formats"));
            }
        }
    }
}
