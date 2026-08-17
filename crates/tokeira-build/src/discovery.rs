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
use tokeira_orchestrator::{
    DefinitionFormatId, DefinitionSourceExtension, PlatformId, RelativeDefinitionPath,
};

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
    /// Whether this source entry requests discovery-default status.
    pub is_default: bool,
    /// Exact Engine_Version this platform definition composes with.
    pub engine: String,
    /// Definition format seeded when creation names none. Optional for
    /// single-root platforms; a platform declaring roots for more than one
    /// format must declare it or force an explicit format selection.
    pub default_format: Option<DefinitionFormatId>,
    /// The platform's root documents, one per format — the format found
    /// through each entry's extension. The platform names its own files;
    /// no engine-side name exists.
    pub definitions: Vec<RelativeDefinitionPath>,
    /// Conventional source-library coordinates.
    pub package: PackageCoordinates,
}

/// Validated metadata for one source definition-frontend package.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefinitionFrontendPackageDescriptor {
    /// Open definition-format identity advertised by the package.
    ///
    /// Frontends carry no engine or contract field: they are engine
    /// components and version with the engine itself.
    pub format: DefinitionFormatId,
    /// Seed-materialization source extension.
    pub source_extension: DefinitionSourceExtension,
    /// Cargo feature enabling this frontend in its package; also the module
    /// path of the frontend's entry (`<crate>::<feature>::frontend`).
    pub feature: String,
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
    /// A recognized package descriptor violated the private discovery contract.
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
    engine: String,
    default: bool,
    #[serde(default)]
    default_format: Option<String>,
    #[serde(default)]
    definitions: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
struct RawFrontendDescriptor {
    format: String,
    source_extension: String,
    feature: String,
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
            // Multi-format frontend packages declare one entry per format
            // (`[[package.metadata.tokeira.definition-frontend]]`).
            let Some(entries) = value.as_array() else {
                return Err(invalid(
                    package,
                    "definition-frontend",
                    "expected an array of tables (one entry per format)".to_string(),
                ));
            };
            for entry in entries {
                frontends.push(decode_frontend(package, entry.clone())?);
            }
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
    // The indication is an exact version, never a requirement expression: the
    // in-tree assertion below is the whole compatibility handshake, and a
    // range would turn a reviewable adoption act into a silent float.
    if raw.engine.chars().any(|c| "^~><=*, ".contains(c)) {
        return Err(invalid(
            package,
            "platform",
            format!(
                "engine indication `{}` must be an exact version, not a range or constraint",
                raw.engine
            ),
        ));
    }
    // The package inherits the workspace version, so its own version IS the
    // Engine_Version of the tree being built. A stale indication is the
    // platform not yet adopting the engine's surface — refused here, before
    // any composition root exists.
    let workspace_engine = package.version.to_string();
    if raw.engine != workspace_engine {
        return Err(invalid(
            package,
            "platform",
            format!(
                "platform `{}` indicates engine {}; this workspace is engine {}. Adopt the {} \
                 surface (see its engine surface delta), then update `engine`",
                raw.id, raw.engine, workspace_engine, workspace_engine
            ),
        ));
    }
    let id =
        PlatformId::new(raw.id).map_err(|error| invalid(package, "platform", error.to_string()))?;
    let default_format = raw
        .default_format
        .map(DefinitionFormatId::new)
        .transpose()
        .map_err(|error| {
            invalid(
                package,
                "platform",
                format!("default-format is not a valid definition format: {error}"),
            )
        })?;
    // The platform names its own root documents — one per format, the
    // format found through the entry's extension. No engine-side name
    // exists; convention is the operator's business.
    let mut definitions = Vec::new();
    let mut seen_extensions: BTreeSet<String> = BTreeSet::new();
    for entry in raw.definitions {
        let path = RelativeDefinitionPath::new(entry)
            .map_err(|error| invalid(package, "platform", error.to_string()))?;
        let Some(extension) = path
            .as_path()
            .extension()
            .and_then(|extension| extension.to_str())
            .map(str::to_string)
        else {
            return Err(invalid(
                package,
                "platform",
                format!(
                    "definition `{}` has no extension; the extension selects the frontend",
                    path.as_str()
                ),
            ));
        };
        if !seen_extensions.insert(extension.clone()) {
            return Err(invalid(
                package,
                "platform",
                format!(
                    "two definitions carry the `.{extension}` extension; a platform declares \
                     one root per format"
                ),
            ));
        }
        definitions.push(path);
    }
    let coordinates = package_coordinates(package, "platform")?;
    Ok(PlatformPackageDescriptor {
        id,
        is_default: raw.default,
        engine: raw.engine,
        default_format,
        definitions,
        package: coordinates,
    })
}

fn decode_frontend(
    package: &Package,
    value: serde_json::Value,
) -> Result<DefinitionFrontendPackageDescriptor, DiscoveryError> {
    let raw: RawFrontendDescriptor = serde_json::from_value(value)
        .map_err(|error| invalid(package, "definition-frontend", error.to_string()))?;
    let format = DefinitionFormatId::new(raw.format)
        .map_err(|error| invalid(package, "definition-frontend", error.to_string()))?;
    let source_extension = DefinitionSourceExtension::new(raw.source_extension)
        .map_err(|error| invalid(package, "definition-frontend", error.to_string()))?;
    let coordinates = package_coordinates(package, "definition-frontend")?;
    if raw.feature.is_empty()
        || !raw
            .feature
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(invalid(
            package,
            "definition-frontend",
            format!(
                "feature `{}` is not a valid cargo feature name",
                raw.feature
            ),
        ));
    }
    Ok(DefinitionFrontendPackageDescriptor {
        format,
        source_extension,
        feature: raw.feature,
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

    use proptest::prelude::*;

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

    fn write_shaped_package(root: &Path, has_library: bool, has_binary: bool) {
        let package = root.join("descriptor");
        fs::create_dir_all(package.join("src")).expect("create package source directory");
        if has_library {
            fs::write(package.join("src/lib.rs"), "pub fn binding() {}\n").expect("write library");
        }
        if has_binary {
            fs::write(package.join("src/main.rs"), "fn main() {}\n").expect("write binary");
        }
        fs::write(
            package.join("Cargo.toml"),
            r#"[package]
name = "descriptor"
version = "0.1.0"
edition = "2024"

[package.metadata.tokeira.platform]
id = "synthetic"
engine = "0.1.0"
default = true
"#,
        )
        .expect("write package manifest");
    }

    #[test]
    fn discovers_independent_platform_and_frontend_descriptors() {
        let root = workspace(&["platform", "bare", "frontend"]);
        write_package(
            root.path(),
            "platform",
            r#"
[package.metadata.tokeira.platform]
id = "compose"
engine = "0.1.0"
default = false
default-format = "tkd"
definitions = ["deployment.tkd"]
"#,
            false,
        );
        write_package(
            root.path(),
            "bare",
            r#"
[package.metadata.tokeira.platform]
id = "bare"
engine = "0.1.0"
default = false
"#,
            false,
        );
        write_package(
            root.path(),
            "frontend",
            r#"
[[package.metadata.tokeira.definition-frontend]]
format = "tkd"
source-extension = "tkd"
feature = "tkd"
"#,
            false,
        );

        let discovered = discover_workspace_descriptors(root.path()).expect("discover descriptors");
        assert_eq!(discovered.platforms[0].id.as_str(), "bare");
        assert_eq!(discovered.platforms[0].default_format, None);
        assert_eq!(discovered.platforms[1].id.as_str(), "compose");
        assert_eq!(
            discovered.platforms[1]
                .default_format
                .as_ref()
                .map(|format| format.as_str()),
            Some("tkd")
        );
        assert_eq!(discovered.frontends[0].format.as_str(), "tkd");
        assert_eq!(
            discovered.platforms[1].definitions[0].as_str(),
            "deployment.tkd"
        );
    }

    #[test]
    fn rejects_invalid_default_format() {
        let root = workspace(&["platform"]);
        write_package(
            root.path(),
            "platform",
            r#"
[package.metadata.tokeira.platform]
id = "compose"
engine = "0.1.0"
default = false
default-format = "Not A Format!"
"#,
            false,
        );
        let error =
            discover_workspace_descriptors(root.path()).expect_err("format id must be admitted");
        assert!(
            error
                .to_string()
                .contains("default-format is not a valid definition format"),
            "{error}"
        );
    }

    #[test]
    fn rejects_binary_owning_descriptor_packages() {
        let root = workspace(&["frontend"]);
        write_package(
            root.path(),
            "frontend",
            r#"
[[package.metadata.tokeira.definition-frontend]]
format = "tkd"
source-extension = "tkd"
feature = "tkd"
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
engine = "0.1.0"
default = false
"#,
                false,
            );
        }
        assert!(matches!(
            discover_workspace_descriptors(root.path()),
            Err(DiscoveryError::DuplicatePlatform(id)) if id == "compose"
        ));

        // The platform names its roots one per format: two entries sharing
        // an extension refuse at discovery.
        let root = workspace(&["dupes"]);
        write_package(
            root.path(),
            "dupes",
            r#"
[package.metadata.tokeira.platform]
id = "dupes"
engine = "0.1.0"
default = false
definitions = ["a.tkd", "b.tkd"]
"#,
            false,
        );
        let error = discover_workspace_descriptors(root.path()).expect_err("one root per format");
        assert!(error.to_string().contains("one root per format"));
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
        assert_eq!(frontend.package.package_name, "tokeira-platform-definition");
        assert_eq!(
            frontend.package.library_target,
            "tokeira_platform_definition"
        );
        assert_eq!(frontend.feature, "tkd");
        let tkdp = descriptors
            .frontends
            .iter()
            .find(|descriptor| descriptor.format.as_str() == "tkdp")
            .expect("tkdp descriptor");
        assert_eq!(tkdp.package.package_name, "tokeira-platform-definition");
        assert_eq!(tkdp.feature, "tkdp");
    }

    proptest! {
        // Feature: platform-builder-abstraction, Property 22: catalog selection determines one static root.
        #[test]
        fn descriptor_target_admission_matches_the_reference_shape(
            has_library in any::<bool>(),
            has_binary in any::<bool>(),
        ) {
            let root = workspace(&["descriptor"]);
            write_shaped_package(root.path(), has_library, has_binary);

            let admitted = discover_workspace_descriptors(root.path()).is_ok();
            prop_assert_eq!(admitted, has_library && !has_binary);
        }
    }
}
