//! Trusted platform and definition-frontend catalog admission.
//!
//! One normalized catalog is loaded from either recognized workspace Cargo
//! metadata or one authority-admitted published inventory. Platform and
//! frontend identities remain independent; a source seed or published locator
//! is the concrete evidence that a selected pair exists.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use thiserror::Error;
use tokeira_build::{
    DefinitionFrontendPackageDescriptor, DiscoveryError, PlatformPackageDescriptor,
    discover_workspace_descriptors,
};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_provisioner::{PublishedProvisionerCatalog, PublishedProvisionerLocator};

/// Authority from which one complete catalog was loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSource {
    /// Recognized source-workspace Cargo metadata.
    Workspace,
    /// Authority-admitted published descriptors and artifact locators.
    Published,
}

/// Coordinates retained for a normalized platform descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlatformSource {
    /// Conventional library coordinates from Cargo metadata.
    Workspace(PlatformPackageDescriptor),
    /// Published provisioners containing this platform.
    Published(Vec<PublishedProvisionerLocator>),
}

/// Provider-neutral platform descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformDescriptor {
    /// Open platform identity.
    pub id: PlatformId,
    /// Whether the descriptor requests catalog-default status.
    pub is_default: bool,
    /// Exact Engine_Version the platform definition indicates.
    pub engine: String,
    /// Source-specific coordinates retained after admission.
    pub source: PlatformSource,
}

/// Coordinates retained for a normalized frontend descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendSource {
    /// Conventional library coordinates from Cargo metadata.
    Workspace(DefinitionFrontendPackageDescriptor),
    /// Published provisioners containing this frontend.
    Published(Vec<PublishedProvisionerLocator>),
}

/// Language-neutral definition-frontend descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendDescriptor {
    /// Open definition-format identity.
    pub format: DefinitionFormatId,
    /// Canonical source extension without a leading dot.
    pub source_extension: tokeira_orchestrator::DefinitionSourceExtension,
    /// Source-specific coordinates retained after admission.
    pub source: FrontendSource,
}

/// One normalized, deterministic platform/frontend inventory.
#[derive(Debug, Clone)]
pub struct PlatformCatalog {
    source: CatalogSource,
    platforms: Vec<PlatformDescriptor>,
    frontends: Vec<FrontendDescriptor>,
}

/// Catalog discovery, admission, or resolution failure.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Workspace metadata could not be decoded.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The selected source family is internally inconsistent.
    #[error("invalid platform catalog: {0}")]
    Invalid(String),
    /// An explicit platform id is absent.
    #[error("unknown platform `{requested}`; supported platforms: {supported}")]
    UnknownPlatform {
        /// Requested platform.
        requested: PlatformId,
        /// Stable supported inventory.
        supported: String,
    },
    /// An explicit definition format is absent.
    #[error("unknown definition format `{requested}`; supported formats: {supported}")]
    UnknownFormat {
        /// Requested format.
        requested: DefinitionFormatId,
        /// Stable supported inventory.
        supported: String,
    },
    /// No catalog source was available.
    #[error("no trusted platform catalog source is available")]
    NoSource,
}

/// Renders a declared-roots list for selection errors.
fn declared_list(
    candidates: &[(
        &FrontendDescriptor,
        &tokeira_orchestrator::RelativeDefinitionPath,
    )],
) -> String {
    candidates
        .iter()
        .map(|(frontend, entry)| format!("`{entry}` ({})", frontend.format))
        .collect::<Vec<_>>()
        .join(", ")
}

impl PlatformCatalog {
    /// Discover and normalize a recognized source workspace.
    pub fn from_workspace(workspace_root: &Path) -> Result<Self, CatalogError> {
        let discovered = discover_workspace_descriptors(workspace_root)?;
        let platforms = discovered
            .platforms
            .into_iter()
            .map(|descriptor| PlatformDescriptor {
                id: descriptor.id.clone(),
                is_default: descriptor.is_default,
                engine: descriptor.engine.clone(),
                source: PlatformSource::Workspace(descriptor),
            })
            .collect();
        let frontends = discovered
            .frontends
            .into_iter()
            .map(|descriptor| FrontendDescriptor {
                format: descriptor.format.clone(),
                source_extension: descriptor.source_extension.clone(),
                source: FrontendSource::Workspace(descriptor),
            })
            .collect();
        Self::admit(CatalogSource::Workspace, platforms, frontends)
    }

    /// Normalize one authority-admitted published inventory.
    pub fn from_published(published: &PublishedProvisionerCatalog) -> Result<Self, CatalogError> {
        let (by_platform, by_format) = admit_locators(published)?;
        let platforms = published
            .platforms
            .iter()
            .map(|descriptor| PlatformDescriptor {
                id: descriptor.id.clone(),
                is_default: descriptor.is_default,
                engine: descriptor.engine.clone(),
                source: PlatformSource::Published(
                    by_platform.get(&descriptor.id).cloned().unwrap_or_default(),
                ),
            })
            .collect();
        let frontends = published
            .frontends
            .iter()
            .map(|descriptor| FrontendDescriptor {
                format: descriptor.format.clone(),
                source_extension: descriptor.source_extension.clone(),
                source: FrontendSource::Published(
                    by_format
                        .get(&descriptor.format)
                        .cloned()
                        .unwrap_or_default(),
                ),
            })
            .collect();
        Self::admit(CatalogSource::Published, platforms, frontends)
    }

    /// Select one complete source family, preferring the recognized workspace.
    pub fn load(
        workspace: Option<&Path>,
        published: Option<&PublishedProvisionerCatalog>,
    ) -> Result<Self, CatalogError> {
        if let Some(workspace) = workspace {
            Self::from_workspace(workspace)
        } else if let Some(published) = published {
            Self::from_published(published)
        } else {
            Err(CatalogError::NoSource)
        }
    }

    fn admit(
        source: CatalogSource,
        mut platforms: Vec<PlatformDescriptor>,
        mut frontends: Vec<FrontendDescriptor>,
    ) -> Result<Self, CatalogError> {
        platforms.sort_by(|left, right| left.id.cmp(&right.id));
        frontends.sort_by(|left, right| left.format.cmp(&right.format));
        if platforms.is_empty() {
            return Err(CatalogError::Invalid(
                "expected at least one platform descriptor".to_string(),
            ));
        }
        if frontends.is_empty() {
            return Err(CatalogError::Invalid(
                "expected at least one definition frontend".to_string(),
            ));
        }
        for pair in platforms.windows(2) {
            if pair[0].id == pair[1].id {
                return Err(CatalogError::Invalid(format!(
                    "duplicate platform `{}`",
                    pair[0].id
                )));
            }
        }
        let default_count = platforms
            .iter()
            .filter(|platform| platform.is_default)
            .count();
        if default_count > 1 {
            return Err(CatalogError::Invalid(format!(
                "expected at most one default platform; found {default_count}"
            )));
        }
        for pair in frontends.windows(2) {
            if pair[0].format == pair[1].format {
                return Err(CatalogError::Invalid(format!(
                    "duplicate definition format `{}`",
                    pair[0].format
                )));
            }
        }
        Ok(Self {
            source,
            platforms,
            frontends,
        })
    }

    /// Report the selected authority family.
    pub fn source(&self) -> CatalogSource {
        self.source
    }

    /// Resolve a platform by open identity.
    pub fn platform(&self, id: &PlatformId) -> Result<&PlatformDescriptor, CatalogError> {
        self.platforms
            .binary_search_by(|entry| entry.id.cmp(id))
            .map(|index| &self.platforms[index])
            .map_err(|_| CatalogError::UnknownPlatform {
                requested: id.clone(),
                supported: self
                    .platforms
                    .iter()
                    .map(|entry| entry.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            })
    }

    /// Resolve a frontend by open format identity.
    pub fn frontend(
        &self,
        format: &DefinitionFormatId,
    ) -> Result<&FrontendDescriptor, CatalogError> {
        self.frontends
            .binary_search_by(|entry| entry.format.cmp(format))
            .map(|index| &self.frontends[index])
            .map_err(|_| CatalogError::UnknownFormat {
                requested: format.clone(),
                supported: self
                    .frontends
                    .iter()
                    .map(|entry| entry.format.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            })
    }

    /// Select a source-workspace frontend and its platform-owned seed.
    pub fn workspace_frontend(
        &self,
        platform: &PlatformDescriptor,
        requested: Option<&DefinitionFormatId>,
    ) -> Result<
        (
            &FrontendDescriptor,
            tokeira_orchestrator::RelativeDefinitionPath,
            PathBuf,
        ),
        CatalogError,
    > {
        let PlatformSource::Workspace(platform_package) = &platform.source else {
            return Err(CatalogError::Invalid(format!(
                "platform `{}` is not a source-workspace descriptor",
                platform.id
            )));
        };
        let package_dir = platform_package
            .package
            .manifest_path
            .parent()
            .ok_or_else(|| {
                CatalogError::Invalid(format!("platform `{}` manifest has no parent", platform.id))
            })?;
        // The platform names its own root documents; each entry's extension
        // selects the frontend. No engine-side name exists — convention is
        // the operator's business.
        let mut candidates = Vec::new();
        for entry in &platform_package.definitions {
            let extension = entry
                .as_path()
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            let Some(frontend) = self
                .frontends
                .iter()
                .find(|frontend| frontend.source_extension.as_str() == extension)
            else {
                return Err(CatalogError::Invalid(format!(
                    "platform `{}` declares definition `{entry}` but no frontend handles \
                     `.{extension}`",
                    platform.id
                )));
            };
            candidates.push((frontend, entry));
        }
        if candidates.is_empty() {
            return Err(CatalogError::Invalid(format!(
                "platform `{}` declares no definitions; name its root documents in the catalog \
                 descriptor (`definitions = [\"…\"]`)",
                platform.id
            )));
        }
        let (frontend, entry) = if let Some(format) = requested {
            // Resolve through the frontend inventory first so an unknown
            // format keeps its own error taxonomy.
            let _ = self.frontend(format)?;
            *candidates
                .iter()
                .find(|(frontend, _)| &frontend.format == format)
                .ok_or_else(|| {
                    CatalogError::Invalid(format!(
                        "platform `{}` declares no `{format}` definition; declared: {}",
                        platform.id,
                        declared_list(&candidates)
                    ))
                })?
        } else if let Some(format) = &platform_package.default_format {
            // No requested format: the platform's declared `default-format`
            // decides.
            *candidates
                .iter()
                .find(|(frontend, _)| &frontend.format == format)
                .ok_or_else(|| {
                    CatalogError::Invalid(format!(
                        "platform `{}` declares default definition format `{format}` but no \
                         matching definition; declared: {}",
                        platform.id,
                        declared_list(&candidates)
                    ))
                })?
        } else {
            // Peer formats are equals, so with several roots and no declared
            // default there is no principled winner — the operator selects.
            match candidates.as_slice() {
                [only] => *only,
                many => {
                    return Err(CatalogError::Invalid(format!(
                        "platform `{}` declares definitions for formats {} and no \
                         `default-format`; select one with `--format`",
                        platform.id,
                        many.iter()
                            .map(|(frontend, _)| format!("`{}`", frontend.format))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
        };
        let seed = package_dir.join(entry.as_path());
        if !seed.is_file() {
            return Err(CatalogError::Invalid(format!(
                "platform `{}` declares definition `{entry}` but the file is absent at {}",
                platform.id,
                seed.display()
            )));
        }
        Ok((frontend, entry.clone(), seed))
    }

    /// Borrow the deterministic admitted platform inventory.
    #[cfg(test)]
    fn platforms(&self) -> &[PlatformDescriptor] {
        &self.platforms
    }
}

type LocatorMap<K> = BTreeMap<K, Vec<PublishedProvisionerLocator>>;

fn admit_locators(
    catalog: &PublishedProvisionerCatalog,
) -> Result<(LocatorMap<PlatformId>, LocatorMap<DefinitionFormatId>), CatalogError> {
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
        if !platforms.contains(&locator.platform) || !formats.contains(&locator.format) {
            return Err(CatalogError::Invalid(format!(
                "published locator references unknown pair `{}/{}`",
                locator.platform, locator.format
            )));
        }
        if locator.definition_seed_ref.trim().is_empty()
            || locator.bundle_ref.trim().is_empty()
            || locator.definition_seed_ref.chars().any(char::is_control)
            || locator.bundle_ref.chars().any(char::is_control)
        {
            return Err(CatalogError::Invalid(format!(
                "published locator for `{}/{}` has an invalid seed or bundle reference",
                locator.platform, locator.format
            )));
        }
        let key = (
            locator.platform.clone(),
            locator.format.clone(),
            locator.engine_identity.digest(),
        );
        if !seen.insert(key) {
            return Err(CatalogError::Invalid(format!(
                "duplicate published locator for `{}/{}`",
                locator.platform, locator.format
            )));
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
            return Err(CatalogError::Invalid(format!(
                "published platform `{platform}` has no provisioner locator"
            )));
        }
    }
    for format in formats {
        if !by_format.contains_key(&format) {
            return Err(CatalogError::Invalid(format!(
                "published format `{format}` has no provisioner locator"
            )));
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
    Ok((by_platform, by_format))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tokeira_orchestrator::DefinitionSourceExtension;
    use tokeira_provisioner::{
        BuildProfile, EngineIdentity, PublishedDefinitionFrontendDescriptor,
        PublishedPlatformDescriptor, Sha256Digest,
    };

    use super::*;

    fn id(value: &str) -> PlatformId {
        PlatformId::new(value).expect("platform id")
    }

    fn format(value: &str) -> DefinitionFormatId {
        DefinitionFormatId::new(value).expect("format id")
    }

    fn published() -> PublishedProvisionerCatalog {
        let platform = id("compose");
        let format = format("tkd");
        PublishedProvisionerCatalog {
            platforms: vec![PublishedPlatformDescriptor {
                id: platform.clone(),
                is_default: false,
                engine: "0.1.0".to_string(),
            }],
            frontends: vec![PublishedDefinitionFrontendDescriptor {
                format: format.clone(),
                source_extension: DefinitionSourceExtension::new("tkd").expect("extension"),
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
    fn published_loader_normalizes_one_catalog() {
        let catalog = PlatformCatalog::from_published(&published()).expect("catalog");
        assert_eq!(catalog.source(), CatalogSource::Published);
        assert_eq!(
            catalog.platform(&id("compose")).expect("platform").id,
            id("compose")
        );
        assert_eq!(
            catalog.frontend(&format("tkd")).expect("frontend").format,
            format("tkd")
        );
    }

    #[test]
    fn workspace_loader_discovers_compose_and_its_seed() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let catalog = PlatformCatalog::from_workspace(&root).expect("workspace catalog");
        let platform = catalog.platform(&id("compose")).expect("compose");

        // Compose ships peer seeds; no requested format resolves through the
        // platform's declared `default-format`.
        let (frontend, _, seed) = catalog
            .workspace_frontend(platform, None)
            .expect("declared default seed");
        assert_eq!(frontend.format, format("tkd"));
        assert!(seed.ends_with("platforms/compose/deployment.tkd"));

        // An explicit format selects its peer seed.
        let (frontend, _, seed) = catalog
            .workspace_frontend(platform, Some(&format("tkdp")))
            .expect("requested tkdp seed");
        assert_eq!(frontend.format, format("tkdp"));
        assert!(seed.ends_with("platforms/compose/definition.tkdp"));
    }

    #[test]
    fn multiple_seeds_without_a_declared_default_demand_a_format() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let catalog = PlatformCatalog::from_workspace(&root).expect("workspace catalog");
        let compose = catalog.platform(&id("compose")).expect("compose");
        let PlatformSource::Workspace(package) = &compose.source else {
            panic!("compose is a workspace platform");
        };

        // Same package directory (both seed files present), declaration
        // withheld: selection must name the peer formats and the remedy.
        let undeclared = PlatformDescriptor {
            source: PlatformSource::Workspace(PlatformPackageDescriptor {
                default_format: None,
                ..package.clone()
            }),
            ..compose.clone()
        };
        let error = catalog
            .workspace_frontend(&undeclared, None)
            .expect_err("ambiguous seeds must not self-select");
        let rendered = error.to_string();
        for needle in ["`tkd`", "`tkdp`", "`--format`"] {
            assert!(rendered.contains(needle), "missing {needle} in: {rendered}");
        }
    }

    #[test]
    fn workspace_catalog_precedes_published_catalog() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let catalog = PlatformCatalog::load(Some(&root), Some(&published())).expect("catalog");
        assert_eq!(catalog.source(), CatalogSource::Workspace);
        assert!(
            catalog
                .platforms()
                .iter()
                .any(|entry| entry.id == id("compose"))
        );
    }
}
