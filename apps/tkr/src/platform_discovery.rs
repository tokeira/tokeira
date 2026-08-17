//! Platform discovery: trusted platform and definition-frontend admission.
//!
//! One normalized inventory is discovered from recognized workspace Cargo
//! metadata. Platform and frontend identities remain independent; a
//! platform-declared source seed is the concrete evidence that a selected
//! pair exists.

use std::path::{Path, PathBuf};

use thiserror::Error;
use tokeira_build::{
    DefinitionFrontendPackageDescriptor, DiscoveryError, PlatformPackageDescriptor,
    discover_workspace_descriptors,
};
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};

/// Provider-neutral platform descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformDescriptor {
    /// Open platform identity.
    pub id: PlatformId,
    /// Whether the descriptor requests discovery-default status.
    pub is_default: bool,
    /// Exact Engine_Version the platform definition indicates.
    pub engine: String,
    /// Workspace package coordinates retained after admission.
    pub package: PlatformPackageDescriptor,
}

/// Language-neutral definition-frontend descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendDescriptor {
    /// Open definition-format identity.
    pub format: DefinitionFormatId,
    /// Canonical source extension without a leading dot.
    pub source_extension: tokeira_orchestrator::DefinitionSourceExtension,
    /// Workspace package coordinates retained after admission.
    pub package: DefinitionFrontendPackageDescriptor,
}

/// One normalized, deterministic platform/frontend inventory.
#[derive(Debug, Clone)]
pub struct PlatformDiscovery {
    platforms: Vec<PlatformDescriptor>,
    frontends: Vec<FrontendDescriptor>,
}

/// Platform discovery, admission, or resolution failure.
#[derive(Debug, Error)]
pub enum PlatformDiscoveryError {
    /// Workspace metadata could not be decoded.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The discovered inventory is internally inconsistent.
    #[error("invalid platform discovery: {0}")]
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

impl PlatformDiscovery {
    /// Discover and normalize a recognized source workspace.
    pub fn from_workspace(workspace_root: &Path) -> Result<Self, PlatformDiscoveryError> {
        let discovered = discover_workspace_descriptors(workspace_root)?;
        let platforms = discovered
            .platforms
            .into_iter()
            .map(|descriptor| PlatformDescriptor {
                id: descriptor.id.clone(),
                is_default: descriptor.is_default,
                engine: descriptor.engine.clone(),
                package: descriptor,
            })
            .collect();
        let frontends = discovered
            .frontends
            .into_iter()
            .map(|descriptor| FrontendDescriptor {
                format: descriptor.format.clone(),
                source_extension: descriptor.source_extension.clone(),
                package: descriptor,
            })
            .collect();
        Self::admit(platforms, frontends)
    }

    fn admit(
        mut platforms: Vec<PlatformDescriptor>,
        mut frontends: Vec<FrontendDescriptor>,
    ) -> Result<Self, PlatformDiscoveryError> {
        platforms.sort_by(|left, right| left.id.cmp(&right.id));
        frontends.sort_by(|left, right| left.format.cmp(&right.format));
        if platforms.is_empty() {
            return Err(PlatformDiscoveryError::Invalid(
                "expected at least one platform descriptor".to_string(),
            ));
        }
        if frontends.is_empty() {
            return Err(PlatformDiscoveryError::Invalid(
                "expected at least one definition frontend".to_string(),
            ));
        }
        for pair in platforms.windows(2) {
            if pair[0].id == pair[1].id {
                return Err(PlatformDiscoveryError::Invalid(format!(
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
            return Err(PlatformDiscoveryError::Invalid(format!(
                "expected at most one default platform; found {default_count}"
            )));
        }
        for pair in frontends.windows(2) {
            if pair[0].format == pair[1].format {
                return Err(PlatformDiscoveryError::Invalid(format!(
                    "duplicate definition format `{}`",
                    pair[0].format
                )));
            }
        }
        Ok(Self {
            platforms,
            frontends,
        })
    }

    /// Resolve a platform by open identity.
    pub fn platform(&self, id: &PlatformId) -> Result<&PlatformDescriptor, PlatformDiscoveryError> {
        self.platforms
            .binary_search_by(|entry| entry.id.cmp(id))
            .map(|index| &self.platforms[index])
            .map_err(|_| PlatformDiscoveryError::UnknownPlatform {
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
    ) -> Result<&FrontendDescriptor, PlatformDiscoveryError> {
        self.frontends
            .binary_search_by(|entry| entry.format.cmp(format))
            .map(|index| &self.frontends[index])
            .map_err(|_| PlatformDiscoveryError::UnknownFormat {
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
        PlatformDiscoveryError,
    > {
        let package_dir = platform
            .package
            .package
            .manifest_path
            .parent()
            .ok_or_else(|| {
                PlatformDiscoveryError::Invalid(format!(
                    "platform `{}` manifest has no parent",
                    platform.id
                ))
            })?;
        // The platform names its own root documents; each entry's extension
        // selects the frontend. No engine-side name exists — convention is
        // the operator's business.
        let mut candidates = Vec::new();
        for entry in &platform.package.definitions {
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
                return Err(PlatformDiscoveryError::Invalid(format!(
                    "platform `{}` declares definition `{entry}` but no frontend handles \
                     `.{extension}`",
                    platform.id
                )));
            };
            candidates.push((frontend, entry));
        }
        if candidates.is_empty() {
            return Err(PlatformDiscoveryError::Invalid(format!(
                "platform `{}` declares no definitions; name its root documents in the \
                 platform descriptor (`definitions = [\"…\"]`)",
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
                    PlatformDiscoveryError::Invalid(format!(
                        "platform `{}` declares no `{format}` definition; declared: {}",
                        platform.id,
                        declared_list(&candidates)
                    ))
                })?
        } else if let Some(format) = &platform.package.default_format {
            // No requested format: the platform's declared `default-format`
            // decides.
            *candidates
                .iter()
                .find(|(frontend, _)| &frontend.format == format)
                .ok_or_else(|| {
                    PlatformDiscoveryError::Invalid(format!(
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
                    return Err(PlatformDiscoveryError::Invalid(format!(
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
            return Err(PlatformDiscoveryError::Invalid(format!(
                "platform `{}` declares definition `{entry}` but the file is absent at {}",
                platform.id,
                seed.display()
            )));
        }
        Ok((frontend, entry.clone(), seed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> PlatformId {
        PlatformId::new(value).expect("platform id")
    }

    fn format(value: &str) -> DefinitionFormatId {
        DefinitionFormatId::new(value).expect("format id")
    }

    #[test]
    fn workspace_discovery_finds_compose_and_its_seed() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let discovery = PlatformDiscovery::from_workspace(&root).expect("workspace discovery");
        let platform = discovery.platform(&id("compose")).expect("compose");

        // Compose ships peer seeds; no requested format resolves through the
        // platform's declared `default-format`.
        let (frontend, _, seed) = discovery
            .workspace_frontend(platform, None)
            .expect("declared default seed");
        assert_eq!(frontend.format, format("tkd"));
        assert!(seed.ends_with("platforms/compose/deployment.tkd"));

        // An explicit format selects its peer seed.
        let (frontend, _, seed) = discovery
            .workspace_frontend(platform, Some(&format("tkdp")))
            .expect("requested tkdp seed");
        assert_eq!(frontend.format, format("tkdp"));
        assert!(seed.ends_with("platforms/compose/definition.tkdp"));
    }

    #[test]
    fn multiple_seeds_without_a_declared_default_demand_a_format() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let discovery = PlatformDiscovery::from_workspace(&root).expect("workspace discovery");
        let compose = discovery.platform(&id("compose")).expect("compose");

        // Same package directory (both seed files present), declaration
        // withheld: selection must name the peer formats and the remedy.
        let undeclared = PlatformDescriptor {
            package: PlatformPackageDescriptor {
                default_format: None,
                ..compose.package.clone()
            },
            ..compose.clone()
        };
        let error = discovery
            .workspace_frontend(&undeclared, None)
            .expect_err("ambiguous seeds must not self-select");
        let rendered = error.to_string();
        for needle in ["`tkd`", "`tkdp`", "`--format`"] {
            assert!(rendered.contains(needle), "missing {needle} in: {rendered}");
        }
    }
}
