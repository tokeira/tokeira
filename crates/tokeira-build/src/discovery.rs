//! Trusted Cargo-metadata discovery for platform and definition-frontend packages.
//!
//! Discovery is deliberately confined to workspace members carrying recognized
//! private metadata. Descriptor values select conventional library exports;
//! they never contain an executable path or a free-form Rust symbol.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

use cargo_metadata::{Metadata, MetadataCommand, Package};
use serde::Deserialize;
use thiserror::Error;
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_platform::definition::RelativeDefinitionPath;

/// Private platform binding contract understood by this workspace.
pub const PLATFORM_BINDING_CONTRACT: u32 = 1;

/// Private definition-frontend contract understood by this workspace.
pub const DEFINITION_FRONTEND_CONTRACT: u32 = 1;

/// A platform's generic launch mechanism.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PlatformLaunchClass {
    /// Launch a provisioner statically assembled with one platform and frontend.
    BoundProvisioner,
    /// Retained adapter for the out-of-scope Local platform.
    LegacyInProcess,
}

/// Cargo coordinates for one trusted conventional library export.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PackageCoordinates {
    /// Cargo package id from the recognized workspace metadata.
    pub package_id: String,
    /// Cargo package name used in generated dependency specifications.
    pub package_name: String,
    /// The package's sole conventional library target.
    pub library_target: String,
    /// Absolute path to the package manifest within the recognized workspace.
    pub manifest_path: PathBuf,
}

/// Validated metadata for one source platform package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformPackageDescriptor {
    /// Open platform identity advertised by the package.
    pub id: PlatformId,
    /// Whether this source entry requests catalog-default status.
    pub is_default: bool,
    /// Generic launch mechanism, independent of platform identity.
    pub launch_class: PlatformLaunchClass,
    /// Private binding contract version.
    pub binding_contract: u32,
    /// Conventional source-library coordinates.
    pub package: PackageCoordinates,
}

/// Canonical source-file extension without a leading dot.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefinitionSourceExtension(String);

impl DefinitionSourceExtension {
    /// Validate a portable lower-kebab source extension.
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        DefinitionFormatId::new(value.clone())
            .map_err(|error| format!("invalid source extension `{value}`: {error}"))?;
        Ok(Self(value))
    }

    /// Borrow the extension without a leading dot.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Validated metadata for one source definition-frontend package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionFrontendPackageDescriptor {
    /// Open definition-format identity advertised by the package.
    pub format: DefinitionFormatId,
    /// Private frontend contract version.
    pub frontend_contract: u32,
    /// Seed-materialization source extension.
    pub source_extension: DefinitionSourceExtension,
    /// Safe default path relative to a deployment directory.
    pub default_relative_path: RelativeDefinitionPath,
    /// Conventional source-library coordinates.
    pub package: PackageCoordinates,
}

/// Independently discovered source platform and frontend descriptors.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WorkspaceDescriptors {
    /// Trusted platform package descriptors, ordered by platform id.
    pub platforms: Vec<PlatformPackageDescriptor>,
    /// Trusted frontend package descriptors, ordered by format id.
    pub frontends: Vec<DefinitionFrontendPackageDescriptor>,
}

/// Failure to decode or admit recognized workspace metadata.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// Cargo could not describe the requested workspace.
    #[error("cargo metadata failed: {0}")]
    Metadata(#[from] cargo_metadata::Error),
    /// A recognized package descriptor violated the private catalog contract.
    #[error("invalid {descriptor} descriptor in package `{package}`: {message}")]
    InvalidDescriptor {
        /// Cargo package carrying the descriptor.
        package: String,
        /// Descriptor family.
        descriptor: &'static str,
        /// Actionable contract violation.
        message: String,
    },
    /// Two workspace packages advertised the same platform identity.
    #[error("duplicate platform descriptor `{0}` in workspace Cargo metadata")]
    DuplicatePlatform(String),
    /// Two workspace packages advertised the same definition format.
    #[error("duplicate definition-frontend descriptor `{0}` in workspace Cargo metadata")]
    DuplicateFrontend(String),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct RawPlatformDescriptor {
    id: String,
    binding_contract: u32,
    launch_class: PlatformLaunchClass,
    default: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct RawFrontendDescriptor {
    format: String,
    frontend_contract: u32,
    source_extension: String,
    default_relative_path: String,
}

/// Decode recognized package descriptors from one trusted source workspace.
pub fn discover_workspace_descriptors(
    workspace_root: &Path,
) -> Result<WorkspaceDescriptors, DiscoveryError> {
    let metadata = MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .no_deps()
        .exec()?;
    descriptors_from_metadata(&metadata)
}

pub(crate) fn descriptors_from_metadata(
    metadata: &Metadata,
) -> Result<WorkspaceDescriptors, DiscoveryError> {
    let workspace_members = metadata.workspace_members.iter().collect::<BTreeSet<_>>();
    let mut packages = metadata
        .packages
        .iter()
        .filter(|package| workspace_members.contains(&package.id))
        .collect::<Vec<_>>();
    packages.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.manifest_path.cmp(&right.manifest_path))
    });

    let mut platforms = Vec::new();
    let mut frontends = Vec::new();
    for package in packages {
        if let Some(value) = package
            .metadata
            .get("tokeira")
            .and_then(|value| value.get("platform"))
        {
            platforms.push(decode_platform(package, value.clone())?);
        }
        if let Some(value) = package
            .metadata
            .get("tokeira")
            .and_then(|value| value.get("definition-frontend"))
        {
            frontends.push(decode_frontend(package, value.clone())?);
        }
    }

    platforms.sort_by(|left, right| left.id.cmp(&right.id));
    frontends.sort_by(|left, right| left.format.cmp(&right.format));
    reject_duplicate_platforms(&platforms)?;
    reject_duplicate_frontends(&frontends)?;

    Ok(WorkspaceDescriptors {
        platforms,
        frontends,
    })
}

fn decode_platform(
    package: &Package,
    value: serde_json::Value,
) -> Result<PlatformPackageDescriptor, DiscoveryError> {
    let raw: RawPlatformDescriptor = serde_json::from_value(value)
        .map_err(|error| invalid(package, "platform", error.to_string()))?;
    if raw.binding_contract != PLATFORM_BINDING_CONTRACT {
        return Err(invalid(
            package,
            "platform",
            format!(
                "unsupported binding contract {}; supported contract is {}",
                raw.binding_contract, PLATFORM_BINDING_CONTRACT
            ),
        ));
    }
    let id =
        PlatformId::new(raw.id).map_err(|error| invalid(package, "platform", error.to_string()))?;
    let coordinates = package_coordinates(package, "platform")?;
    Ok(PlatformPackageDescriptor {
        id,
        is_default: raw.default,
        launch_class: raw.launch_class,
        binding_contract: raw.binding_contract,
        package: coordinates,
    })
}

fn decode_frontend(
    package: &Package,
    value: serde_json::Value,
) -> Result<DefinitionFrontendPackageDescriptor, DiscoveryError> {
    let raw: RawFrontendDescriptor = serde_json::from_value(value)
        .map_err(|error| invalid(package, "definition-frontend", error.to_string()))?;
    if raw.frontend_contract != DEFINITION_FRONTEND_CONTRACT {
        return Err(invalid(
            package,
            "definition-frontend",
            format!(
                "unsupported frontend contract {}; supported contract is {}",
                raw.frontend_contract, DEFINITION_FRONTEND_CONTRACT
            ),
        ));
    }
    let format = DefinitionFormatId::new(raw.format)
        .map_err(|error| invalid(package, "definition-frontend", error.to_string()))?;
    let source_extension = DefinitionSourceExtension::new(raw.source_extension)
        .map_err(|error| invalid(package, "definition-frontend", error))?;
    let default_relative_path = RelativeDefinitionPath::new(raw.default_relative_path)
        .map_err(|error| invalid(package, "definition-frontend", error.to_string()))?;
    let path_extension = default_relative_path
        .as_path()
        .extension()
        .and_then(|extension| extension.to_str());
    if path_extension != Some(source_extension.as_str()) {
        return Err(invalid(
            package,
            "definition-frontend",
            format!(
                "default path `{}` must use source extension `.{}`",
                default_relative_path.as_str(),
                source_extension.as_str()
            ),
        ));
    }
    let coordinates = package_coordinates(package, "definition-frontend")?;
    Ok(DefinitionFrontendPackageDescriptor {
        format,
        frontend_contract: raw.frontend_contract,
        source_extension,
        default_relative_path,
        package: coordinates,
    })
}

pub(crate) fn package_coordinates(
    package: &Package,
    descriptor: &'static str,
) -> Result<PackageCoordinates, DiscoveryError> {
    if package.targets.iter().any(cargo_metadata::Target::is_bin) {
        return Err(invalid(
            package,
            descriptor,
            "descriptor packages must not define a binary target".to_string(),
        ));
    }
    let libraries = package
        .targets
        .iter()
        .filter(|target| target.is_lib())
        .collect::<Vec<_>>();
    let [library] = libraries.as_slice() else {
        return Err(invalid(
            package,
            descriptor,
            format!(
                "descriptor packages must define exactly one library target; found {}",
                libraries.len()
            ),
        ));
    };
    Ok(PackageCoordinates {
        package_id: package.id.to_string(),
        package_name: package.name.to_string(),
        library_target: library.name.clone(),
        manifest_path: package.manifest_path.as_std_path().to_path_buf(),
    })
}

fn reject_duplicate_platforms(
    descriptors: &[PlatformPackageDescriptor],
) -> Result<(), DiscoveryError> {
    for pair in descriptors.windows(2) {
        if pair[0].id == pair[1].id {
            return Err(DiscoveryError::DuplicatePlatform(pair[0].id.to_string()));
        }
    }
    Ok(())
}

fn reject_duplicate_frontends(
    descriptors: &[DefinitionFrontendPackageDescriptor],
) -> Result<(), DiscoveryError> {
    for pair in descriptors.windows(2) {
        if pair[0].format == pair[1].format {
            return Err(DiscoveryError::DuplicateFrontend(
                pair[0].format.to_string(),
            ));
        }
    }
    Ok(())
}

fn invalid(package: &Package, descriptor: &'static str, message: String) -> DiscoveryError {
    DiscoveryError::InvalidDescriptor {
        package: package.name.to_string(),
        descriptor,
        message,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn write_package(root: &Path, name: &str, metadata: &str, binary: bool) {
        let package = root.join(name);
        fs::create_dir_all(package.join("src")).expect("create package source directory");
        if binary {
            fs::write(package.join("src/main.rs"), "fn main() {}\n").expect("write binary");
        } else {
            fs::write(package.join("src/lib.rs"), "pub fn binding() {}\n").expect("write library");
        }
        fs::write(
            package.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n{metadata}\n"
            ),
        )
        .expect("write package manifest");
    }

    fn workspace(members: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().expect("create workspace");
        let members = members
            .iter()
            .map(|member| format!("\"{member}\""))
            .collect::<Vec<_>>()
            .join(", ");
        fs::write(
            root.path().join("Cargo.toml"),
            format!("[workspace]\nmembers = [{members}]\nresolver = \"3\"\n"),
        )
        .expect("write workspace manifest");
        root
    }

    #[test]
    fn discovers_independent_platform_and_frontend_descriptors() {
        let root = workspace(&["platform", "frontend"]);
        write_package(
            root.path(),
            "platform",
            r#"
[package.metadata.tokeira.platform]
id = "compose"
binding-contract = 1
launch-class = "bound-provisioner"
default = false
"#,
            false,
        );
        write_package(
            root.path(),
            "frontend",
            r#"
[package.metadata.tokeira.definition-frontend]
format = "tkd"
frontend-contract = 1
source-extension = "tkd"
default-relative-path = "definition.tkd"
"#,
            false,
        );

        let discovered = discover_workspace_descriptors(root.path()).expect("discover descriptors");
        assert_eq!(discovered.platforms[0].id.as_str(), "compose");
        assert_eq!(discovered.frontends[0].format.as_str(), "tkd");
        assert_eq!(
            discovered.frontends[0].default_relative_path.as_str(),
            "definition.tkd"
        );
    }

    #[test]
    fn rejects_binary_owning_descriptor_packages() {
        let root = workspace(&["frontend"]);
        write_package(
            root.path(),
            "frontend",
            r#"
[package.metadata.tokeira.definition-frontend]
format = "tkd"
frontend-contract = 1
source-extension = "tkd"
default-relative-path = "definition.tkd"
"#,
            true,
        );

        let error =
            discover_workspace_descriptors(root.path()).expect_err("binary must be rejected");
        assert!(error.to_string().contains("must not define a binary"));
    }

    #[test]
    fn rejects_duplicate_ids_and_mismatched_source_paths() {
        let root = workspace(&["one", "two"]);
        for package in ["one", "two"] {
            write_package(
                root.path(),
                package,
                r#"
[package.metadata.tokeira.platform]
id = "compose"
binding-contract = 1
launch-class = "bound-provisioner"
default = false
"#,
                false,
            );
        }
        assert!(matches!(
            discover_workspace_descriptors(root.path()),
            Err(DiscoveryError::DuplicatePlatform(id)) if id == "compose"
        ));

        let root = workspace(&["frontend"]);
        write_package(
            root.path(),
            "frontend",
            r#"
[package.metadata.tokeira.definition-frontend]
format = "tkd"
frontend-contract = 1
source-extension = "tkd"
default-relative-path = "definition.py"
"#,
            false,
        );
        let error =
            discover_workspace_descriptors(root.path()).expect_err("path must match format");
        assert!(error.to_string().contains("must use source extension"));
    }

    #[test]
    fn current_workspace_publishes_the_tkd_frontend() {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let descriptors =
            discover_workspace_descriptors(&workspace_root).expect("discover current workspace");
        let frontend = descriptors
            .frontends
            .iter()
            .find(|descriptor| descriptor.format.as_str() == "tkd")
            .expect("tkd descriptor");
        assert_eq!(frontend.package.package_name, "tokeira-tkd");
        assert_eq!(frontend.package.library_target, "tokeira_tkd");
    }
}
