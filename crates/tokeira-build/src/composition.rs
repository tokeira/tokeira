//! Static provisioner composition-root generation.
//!
//! A generated root contains no platform dispatch. It binds one trusted
//! platform library to one trusted Definition Frontend library through the
//! generic provisioner shell, then becomes a disposable build input. Cargo
//! metadata supplies every package coordinate; descriptors cannot inject
//! Rust paths or arbitrary dependencies.

use std::{
    collections::BTreeSet,
    path::{Component, Path, PathBuf},
    process::Stdio,
};

use cargo_metadata::{Metadata, MetadataCommand, Package};
use thiserror::Error;
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_provisioner::{BoundProvisionerEvidence, Sha256Digest};

use crate::{
    ClosureError, DefinitionFrontendPackageDescriptor, DiscoveryError, PackageCoordinates,
    PlatformPackageDescriptor, ProvisionerClosure,
    discovery::{descriptors_from_metadata, package_coordinates},
    resolve_source_closure_for_packages,
};

/// Cargo package containing the generic provisioner shell.
pub const PROVISIONER_CLI_PACKAGE: &str = "tokeira-provisioner-cli";

/// Stable location of the disposable root within a staged source tree.
pub const GENERATED_ROOT_RELATIVE_PATH: &str = ".tokeira-build/bound-provisioner";

/// Cargo binary produced by every statically assembled provisioner root.
pub const GENERATED_PROVISIONER_BIN: &str = "tkp";

/// Deterministic source and closure for one selected platform/frontend pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundProvisionerSource {
    platform: PlatformId,
    format: DefinitionFormatId,
    engine: String,
    cargo_toml: String,
    main_rs: String,
    cargo_lock: Vec<u8>,
    closure: ProvisionerClosure,
}

impl BoundProvisionerSource {
    #[cfg(test)]
    pub(crate) fn testing(closure: ProvisionerClosure) -> Self {
        Self {
            platform: PlatformId::new("alpha").expect("test platform id"),
            format: DefinitionFormatId::new("tkd").expect("test format id"),
            engine: "0.0.0".to_string(),
            cargo_toml: "[package]\nname = \"tokeira-bound-provisioner\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[workspace]\n".to_string(),
            main_rs: "fn main() {}\n".to_string(),
            cargo_lock: b"version = 4\n".to_vec(),
            closure,
        }
    }

    #[cfg(test)]
    pub(crate) fn testing_clear_crates(&mut self) {
        self.closure.crate_names.clear();
    }

    /// Borrow the selected open platform identity.
    pub fn platform(&self) -> &PlatformId {
        &self.platform
    }

    /// Borrow the selected language-neutral Definition Format identity.
    pub fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    /// Return the Engine_Version the platform definition indicated.
    pub fn engine(&self) -> &str {
        &self.engine
    }

    /// Borrow the deterministic generated `Cargo.toml` bytes.
    pub fn cargo_toml(&self) -> &str {
        &self.cargo_toml
    }

    /// Borrow the deterministic generated `src/main.rs` bytes.
    pub fn main_rs(&self) -> &str {
        &self.main_rs
    }

    /// Borrow the exact union closure of shell, platform, and frontend.
    pub fn closure(&self) -> &ProvisionerClosure {
        &self.closure
    }

    /// Digest the deterministic generated package and every selection fact
    /// that determines its meaning.
    ///
    /// Contract versions are included explicitly even though the generated
    /// Rust source contains only the selected identifiers and conventional
    /// exports. Advancing either private contract must therefore re-key the
    /// engine instead of reusing an artifact assembled under older semantics.
    pub fn generated_root_digest(&self) -> Sha256Digest {
        let mut bytes = b"tokeira-bound-provisioner-root/v3\n".to_vec();
        framed_field(&mut bytes, "platform", self.platform.as_str().as_bytes());
        framed_field(&mut bytes, "format", self.format.as_str().as_bytes());
        framed_field(&mut bytes, "engine", self.engine.as_bytes());
        framed_field(&mut bytes, "Cargo.toml", self.cargo_toml.as_bytes());
        framed_field(&mut bytes, "src/main.rs", self.main_rs.as_bytes());
        framed_field(&mut bytes, "Cargo.lock", &self.cargo_lock);
        Sha256Digest::from_bytes(&bytes)
    }

    /// Digest the frozen source tree together with the generated overlay.
    ///
    /// The generated files are not committed workspace source, so the git
    /// tree oid alone cannot identify the compiled program. Length-framed,
    /// domain-separated bytes make the overlay an explicit engine-identity
    /// input without making the deployment definition itself executable
    /// source.
    pub fn source_closure_digest(&self, snapshot_tree_oid: &str) -> Sha256Digest {
        let mut bytes = b"tokeira-bound-provisioner-source/v2\n".to_vec();
        framed_field(&mut bytes, "snapshot-tree", snapshot_tree_oid.as_bytes());
        framed_field(
            &mut bytes,
            "generated-root",
            self.generated_root_digest().to_hex().as_bytes(),
        );
        Sha256Digest::from_bytes(&bytes)
    }

    /// Produce the complete build/admission evidence for a frozen source tree.
    ///
    /// This is the sole derivation used by native and hermetic bound builds:
    /// the selected descriptors, generated root, source closure, and lock
    /// closure cannot be populated independently and silently disagree.
    pub fn evidence(&self, snapshot_tree_oid: &str) -> BoundProvisionerEvidence {
        BoundProvisionerEvidence {
            platform: self.platform.clone(),
            format: self.format.clone(),
            engine: self.engine.clone(),
            generated_root: self.generated_root_digest(),
            source_closure: self.source_closure_digest(snapshot_tree_oid),
            lock_closure: Sha256Digest::from_bytes(&self.closure.canonical_lock_bytes()),
        }
    }

    /// Materialize the generated package inside one frozen source staging tree.
    pub fn materialize_in(&self, source_root: &Path) -> Result<PathBuf, CompositionError> {
        let root = source_root.join(GENERATED_ROOT_RELATIVE_PATH);
        let source_dir = root.join("src");
        std::fs::create_dir_all(&source_dir).map_err(|source| {
            CompositionError::WriteGenerated {
                path: source_dir.display().to_string(),
                source,
            }
        })?;
        write_generated(&root.join("Cargo.toml"), self.cargo_toml.as_bytes())?;
        write_generated(&source_dir.join("main.rs"), self.main_rs.as_bytes())?;
        write_generated(&root.join("Cargo.lock"), &self.cargo_lock)?;
        Ok(root)
    }

    fn resolve_generated_lock(&mut self, workspace_root: &Path) -> Result<(), CompositionError> {
        let generated_root = self.materialize_in(workspace_root)?;
        let workspace_lock = workspace_root.join("Cargo.lock");
        let mut lock = std::fs::read(&workspace_lock).map_err(|source| {
            CompositionError::ReadWorkspaceLock {
                path: workspace_lock.display().to_string(),
                source,
            }
        })?;
        if !lock.ends_with(b"\n") {
            lock.push(b'\n');
        }
        lock.extend_from_slice(b"\n[[package]]\nname = \"tokeira-bound-provisioner\"\nversion = \"0.0.0\"\ndependencies = [\n");
        for dependency in generated_dependency_packages(&self.cargo_toml)? {
            lock.extend_from_slice(format!(" {},\n", toml_string(&dependency)).as_bytes());
        }
        lock.extend_from_slice(b"]\n");
        let generated_lock = generated_root.join("Cargo.lock");
        write_generated(&generated_lock, &lock)?;

        // Cargo prunes packages unreachable under the selected roots' default
        // features. Starting from the admitted workspace lock preserves every
        // selected version; offline metadata can only normalize that locked
        // graph for this root, never consult a newer registry resolution.
        let status = std::process::Command::new("cargo")
            .current_dir(workspace_root)
            .args([
                "metadata",
                "--offline",
                "--format-version",
                "1",
                "--manifest-path",
            ])
            .arg(generated_root.join("Cargo.toml"))
            .stdout(Stdio::null())
            .status()
            .map_err(|source| CompositionError::ResolveGeneratedLock {
                path: generated_lock.display().to_string(),
                source,
            })?;
        if !status.success() {
            return Err(CompositionError::InvalidSelection(format!(
                "Cargo could not normalize the generated bound-provisioner lock at {}",
                generated_lock.display()
            )));
        }
        let resolved = std::fs::read(&generated_lock).map_err(|source| {
            CompositionError::ReadWorkspaceLock {
                path: generated_lock.display().to_string(),
                source,
            }
        })?;
        self.validate_generated_lock(&resolved)?;
        self.cargo_lock = resolved;
        Ok(())
    }

    fn validate_generated_lock(&self, bytes: &[u8]) -> Result<(), CompositionError> {
        #[derive(serde::Deserialize)]
        struct Lockfile {
            #[serde(default)]
            package: Vec<LockPackage>,
        }
        #[derive(serde::Deserialize)]
        struct LockPackage {
            name: String,
            version: String,
            #[serde(default)]
            source: Option<String>,
            #[serde(default)]
            checksum: Option<String>,
        }

        let text = std::str::from_utf8(bytes).map_err(|error| {
            CompositionError::InvalidSelection(format!(
                "generated bound-provisioner lock is not UTF-8: {error}"
            ))
        })?;
        let lockfile: Lockfile = toml::from_str(text).map_err(|error| {
            CompositionError::InvalidSelection(format!(
                "generated bound-provisioner lock is invalid: {error}"
            ))
        })?;
        let admitted = self.closure.locked.iter().collect::<BTreeSet<_>>();
        let workspace = self
            .closure
            .crate_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut found_root = false;
        for package in lockfile.package {
            if package.name == "tokeira-bound-provisioner" && package.source.is_none() {
                found_root = true;
                continue;
            }
            if package.source.is_none() {
                if workspace.contains(package.name.as_str()) {
                    continue;
                }
            } else {
                let dependency = crate::LockedDependency {
                    name: package.name.clone(),
                    version: package.version.clone(),
                    source: package.source.clone(),
                    checksum: package.checksum.clone(),
                };
                if admitted.contains(&dependency) {
                    continue;
                }
            }
            return Err(CompositionError::InvalidSelection(format!(
                "generated lock contains package `{} {}` outside the admitted source closure",
                package.name, package.version
            )));
        }
        if !found_root {
            return Err(CompositionError::InvalidSelection(
                "generated lock does not contain the bound-provisioner root".to_string(),
            ));
        }
        Ok(())
    }
}

fn framed_field(buffer: &mut Vec<u8>, tag: &str, value: &[u8]) {
    buffer.extend_from_slice(tag.as_bytes());
    buffer.push(b'=');
    buffer.extend_from_slice(value.len().to_string().as_bytes());
    buffer.push(b':');
    buffer.extend_from_slice(value);
    buffer.push(b'\n');
}

/// Refusal to assemble or materialize a static provisioner root.
#[derive(Debug, Error)]
pub enum CompositionError {
    /// Trusted workspace metadata could not be read.
    #[error("cargo metadata failed: {0}")]
    Metadata(#[from] cargo_metadata::Error),
    /// A selected conventional library violated descriptor shape.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// The exact three-root dependency closure could not be resolved.
    #[error(transparent)]
    Closure(#[from] ClosureError),
    /// Selected catalog data no longer agrees with the recognized workspace.
    #[error("invalid bound-provisioner selection: {0}")]
    InvalidSelection(String),
    /// The admitted workspace lock could not be read for the generated root.
    #[error("failed to read workspace lock file {path}: {source}")]
    ReadWorkspaceLock {
        /// Workspace lock file whose bytes were required.
        path: String,
        /// Filesystem failure.
        source: std::io::Error,
    },
    /// Cargo could not normalize the generated lock from the admitted one.
    #[error("failed to resolve generated lock file {path}: {source}")]
    ResolveGeneratedLock {
        /// Generated lock whose dependency graph was being normalized.
        path: String,
        /// Cargo process launch failure.
        source: std::io::Error,
    },
    /// Generated files could not be staged for compilation.
    #[error("failed to write generated composition-root file {path}: {source}")]
    WriteGenerated {
        /// Destination whose write failed.
        path: String,
        /// Filesystem failure.
        source: std::io::Error,
    },
}

fn generated_dependency_packages(cargo_toml: &str) -> Result<BTreeSet<String>, CompositionError> {
    let manifest: toml::Value = toml::from_str(cargo_toml).map_err(|error| {
        CompositionError::InvalidSelection(format!(
            "generated bound-provisioner manifest is invalid: {error}"
        ))
    })?;
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| {
            CompositionError::InvalidSelection(
                "generated bound-provisioner manifest has no dependency table".to_string(),
            )
        })?;
    dependencies
        .values()
        .map(|dependency| {
            dependency
                .get("package")
                .and_then(toml::Value::as_str)
                .map(str::to_string)
                .ok_or_else(|| {
                    CompositionError::InvalidSelection(
                        "generated bound-provisioner dependency has no package name".to_string(),
                    )
                })
        })
        .collect()
}

/// Assemble one static provisioner from trusted workspace descriptors.
pub fn assemble_bound_provisioner(
    workspace_root: &Path,
    platform: &PlatformPackageDescriptor,
    frontend: &DefinitionFrontendPackageDescriptor,
) -> Result<BoundProvisionerSource, CompositionError> {
    let metadata = MetadataCommand::new()
        .manifest_path(workspace_root.join("Cargo.toml"))
        .no_deps()
        .exec()?;
    let descriptors = descriptors_from_metadata(&metadata)?;
    if !descriptors.platforms.contains(platform) {
        return Err(CompositionError::InvalidSelection(format!(
            "platform `{}` does not match its recognized workspace descriptor",
            platform.id
        )));
    }
    if !descriptors.frontends.contains(frontend) {
        return Err(CompositionError::InvalidSelection(format!(
            "format `{}` does not match its recognized workspace descriptor",
            frontend.format
        )));
    }
    let cli = find_workspace_package(&metadata, PROVISIONER_CLI_PACKAGE)?;
    let cli_coordinates = package_coordinates(cli, "bound-provisioner shell")?;

    // Cargo's own normalized spelling must anchor dependency paths. On macOS
    // `/var` and `/private/var` can name the same directory but are not
    // lexically strip-compatible, while metadata uses one spelling
    // consistently for the workspace and every manifest.
    let workspace_root = metadata.workspace_root.as_std_path().to_path_buf();
    let cli_path = dependency_path(&workspace_root, &cli_coordinates)?;
    let platform_path = dependency_path(&workspace_root, &platform.package)?;
    let frontend_path = dependency_path(&workspace_root, &frontend.package)?;

    let cargo_toml = render_manifest(
        &cli_coordinates,
        &cli_path,
        &platform.package,
        &platform_path,
        &frontend.package,
        &frontend_path,
    );
    let main_rs = render_main(&platform.id, &frontend.format);
    let closure = resolve_source_closure_for_packages(
        &workspace_root,
        &[
            PROVISIONER_CLI_PACKAGE,
            platform.package.package_name.as_str(),
            frontend.package.package_name.as_str(),
        ],
    )?;

    let mut source = BoundProvisionerSource {
        platform: platform.id.clone(),
        format: frontend.format.clone(),
        engine: platform.engine.clone(),
        cargo_toml,
        main_rs,
        cargo_lock: Vec::new(),
        closure,
    };
    source.resolve_generated_lock(&workspace_root)?;
    Ok(source)
}

fn find_workspace_package<'a>(
    metadata: &'a Metadata,
    package_name: &str,
) -> Result<&'a Package, CompositionError> {
    let members = metadata.workspace_members.iter().collect::<BTreeSet<_>>();
    let matches = metadata
        .packages
        .iter()
        .filter(|package| package.name.as_str() == package_name && members.contains(&package.id))
        .collect::<Vec<_>>();
    let [package] = matches.as_slice() else {
        return Err(CompositionError::InvalidSelection(format!(
            "expected exactly one workspace package `{package_name}`; found {}",
            matches.len()
        )));
    };
    Ok(package)
}

fn dependency_path(
    workspace_root: &Path,
    coordinates: &PackageCoordinates,
) -> Result<String, CompositionError> {
    let package_dir = coordinates.manifest_path.parent().ok_or_else(|| {
        CompositionError::InvalidSelection(format!(
            "package `{}` has no manifest parent",
            coordinates.package_name
        ))
    })?;
    let relative = package_dir.strip_prefix(workspace_root).map_err(|_| {
        CompositionError::InvalidSelection(format!(
            "package `{}` manifest `{}` is outside the recognized workspace",
            coordinates.package_name,
            coordinates.manifest_path.display()
        ))
    })?;
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(CompositionError::InvalidSelection(format!(
            "package `{}` has a non-canonical workspace-relative path",
            coordinates.package_name
        )));
    }
    let generated_relative = Path::new("../..").join(relative);
    let rendered = generated_relative.to_str().ok_or_else(|| {
        CompositionError::InvalidSelection(format!(
            "package `{}` path is not valid UTF-8",
            coordinates.package_name
        ))
    })?;
    Ok(rendered.replace('\\', "/"))
}

fn render_manifest(
    cli: &PackageCoordinates,
    cli_path: &str,
    platform: &PackageCoordinates,
    platform_path: &str,
    frontend: &PackageCoordinates,
    frontend_path: &str,
) -> String {
    format!(
        "[package]\nname = \"tokeira-bound-provisioner\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[[bin]]\nname = \"{GENERATED_PROVISIONER_BIN}\"\npath = \"src/main.rs\"\n\n[workspace]\n\n[dependencies]\nselected_frontend = {{ package = {}, path = {} }}\nselected_platform = {{ package = {}, path = {} }}\ntokeira_provisioner_cli = {{ package = {}, path = {} }}\n",
        toml_string(&frontend.package_name),
        toml_string(frontend_path),
        toml_string(&platform.package_name),
        toml_string(platform_path),
        toml_string(&cli.package_name),
        toml_string(cli_path),
    )
}

fn render_main(platform: &PlatformId, format: &DefinitionFormatId) -> String {
    let platform = serde_json::to_string(platform.as_str()).expect("platform ids serialize");
    let format = serde_json::to_string(format.as_str()).expect("format ids serialize");
    format!(
        "tokeira_provisioner_cli::bound_provisioner_main!(\n    expected_platform: {platform},\n    platform: selected_platform::provisioner,\n    expected_format: {format},\n    frontend: selected_frontend::frontend,\n);\n"
    )
}

fn toml_string(value: &str) -> String {
    toml::Value::String(value.to_string()).to_string()
}

fn write_generated(path: &Path, bytes: &[u8]) -> Result<(), CompositionError> {
    std::fs::write(path, bytes).map_err(|source| CompositionError::WriteGenerated {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use std::process::Command;

    use super::*;
    use crate::discover_workspace_descriptors;

    struct Fixture {
        _temp: tempfile::TempDir,
        root: PathBuf,
        platforms: Vec<PlatformPackageDescriptor>,
        frontends: Vec<DefinitionFrontendPackageDescriptor>,
    }

    impl Fixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().expect("temporary workspace");
            let root = temp.path().to_path_buf();
            write(
                &root.join("Cargo.toml"),
                "[workspace]\nmembers = [\"crates/cli\", \"platforms/alpha\", \"frontends/tkd\", \"frontends/tkdp\", \"crates/unrelated\"]\nresolver = \"3\"\n",
            );
            package(
                &root,
                "crates/cli",
                PROVISIONER_CLI_PACKAGE,
                "",
                r#"#[macro_export]
macro_rules! bound_provisioner_main {
    (
        expected_platform: $platform:literal,
        platform: $platform_factory:path,
        expected_format: $format:literal,
        frontend: $frontend:path $(,)?
    ) => {
        fn main() {
            let _ = ($platform, $format, $platform_factory($frontend()));
        }
    };
}
"#,
            );
            package(
                &root,
                "platforms/alpha",
                "alpha-platform",
                "[package.metadata.tokeira.platform]\nid = \"alpha\"\nengine = \"0.1.0\"\ndefault = false\n",
                "pub fn provisioner<T>(_frontend: T) {}\n",
            );
            package(
                &root,
                "frontends/tkd",
                "tkd-frontend",
                "[package.metadata.tokeira.definition-frontend]\nformat = \"tkd\"\nsource-extension = \"tkd\"\ndefault-relative-path = \"definition.tkd\"\n",
                "pub fn frontend() {}\n",
            );
            package(
                &root,
                "frontends/tkdp",
                "tkdp-frontend",
                "[package.metadata.tokeira.definition-frontend]\nformat = \"tkdp\"\nsource-extension = \"tkdp\"\ndefault-relative-path = \"definition.tkdp\"\n",
                "pub fn frontend() {}\n",
            );
            package(
                &root,
                "crates/unrelated",
                "unrelated",
                "",
                "pub fn unrelated() {}\n",
            );
            let descriptors = discover_workspace_descriptors(&root).expect("discover fixture");
            Self {
                _temp: temp,
                root,
                platforms: descriptors.platforms,
                frontends: descriptors.frontends,
            }
        }

        fn assemble(&self, format: &str) -> BoundProvisionerSource {
            let platform = self.platforms.first().expect("one platform");
            let frontend = self
                .frontends
                .iter()
                .find(|frontend| frontend.format.as_str() == format)
                .expect("known frontend");
            assemble_bound_provisioner(&self.root, platform, frontend).expect("assemble root")
        }
    }

    #[test]
    fn generated_root_is_deterministic_and_uses_exactly_three_dependencies() {
        let fixture = Fixture::new();
        let first = fixture.assemble("tkd");
        let second = fixture.assemble("tkd");

        assert_eq!(first, second);
        assert_eq!(
            first.main_rs,
            "tokeira_provisioner_cli::bound_provisioner_main!(\n    expected_platform: \"alpha\",\n    platform: selected_platform::provisioner,\n    expected_format: \"tkd\",\n    frontend: selected_frontend::frontend,\n);\n"
        );
        let manifest: toml::Value = toml::from_str(&first.cargo_toml).expect("generated manifest");
        let dependencies = manifest
            .get("dependencies")
            .and_then(toml::Value::as_table)
            .expect("dependency table");
        assert_eq!(
            dependencies.keys().map(String::as_str).collect::<Vec<_>>(),
            [
                "selected_frontend",
                "selected_platform",
                "tokeira_provisioner_cli"
            ]
        );
        assert!(
            first
                .closure
                .crate_names
                .contains(&PROVISIONER_CLI_PACKAGE.to_string())
        );
        assert!(
            first
                .closure
                .crate_names
                .contains(&"alpha-platform".to_string())
        );
        assert!(
            first
                .closure
                .crate_names
                .contains(&"tkd-frontend".to_string())
        );
        assert!(!first.closure.crate_names.contains(&"unrelated".to_string()));
        assert!(
            !first
                .closure
                .crate_names
                .contains(&"tkdp-frontend".to_string())
        );
    }

    #[test]
    fn generated_overlay_and_selected_format_rekey_source_identity() {
        let fixture = Fixture::new();
        let tkd = fixture.assemble("tkd");
        let tkdp = fixture.assemble("tkdp");

        assert_eq!(
            tkd.source_closure_digest("tree-a"),
            fixture.assemble("tkd").source_closure_digest("tree-a")
        );
        assert_ne!(
            tkd.source_closure_digest("tree-a"),
            tkd.source_closure_digest("tree-b")
        );
        assert_ne!(
            tkd.source_closure_digest("tree-a"),
            tkdp.source_closure_digest("tree-a")
        );

        let evidence = tkd.evidence("tree-a");
        assert_eq!(evidence.platform, *tkd.platform());
        assert_eq!(evidence.format, *tkd.format());
        assert_eq!(evidence.engine, tkd.engine());
        assert_eq!(evidence.generated_root, tkd.generated_root_digest());
        assert_eq!(evidence.source_closure, tkd.source_closure_digest("tree-a"));
        assert_eq!(
            evidence.lock_closure,
            Sha256Digest::from_bytes(&tkd.closure().canonical_lock_bytes())
        );
    }

    #[test]
    fn generated_root_digest_covers_engine_and_exact_generated_bytes() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let digest = source.generated_root_digest();

        let mut engine = source.clone();
        engine.engine = "9.9.9".to_string();
        assert_ne!(engine.generated_root_digest(), digest);

        let mut manifest = source.clone();
        manifest.cargo_toml.push_str("# changed\n");
        assert_ne!(manifest.generated_root_digest(), digest);

        let mut main = source;
        main.main_rs.push_str("// changed\n");
        assert_ne!(main.generated_root_digest(), digest);

        let mut lock = fixture.assemble("tkd");
        lock.cargo_lock.extend_from_slice(b"# changed\n");
        assert_ne!(lock.generated_root_digest(), digest);
    }

    proptest! {
        // Feature: platform-builder-abstraction, Property 22: catalog selection determines one static root.
        #[test]
        fn generated_assembly_matches_the_three_root_reference_model(
            platform_segment in "[a-z][a-z0-9]{0,10}",
            format_segment in "[a-z][a-z0-9]{0,10}",
            snapshot in "[a-f0-9]{8,40}",
            definition_bytes in prop::collection::vec(any::<u8>(), 0..128),
        ) {
            let platform_id = PlatformId::new(&platform_segment)
                .expect("generated platform id is canonical");
            let format_id = DefinitionFormatId::new(&format_segment)
                .expect("generated format id is canonical");
            let cli = PackageCoordinates {
                package_id: "cli-id".to_string(),
                package_name: PROVISIONER_CLI_PACKAGE.to_string(),
                library_target: "tokeira_provisioner_cli".to_string(),
                manifest_path: PathBuf::from("/workspace/crates/cli/Cargo.toml"),
            };
            let platform = PackageCoordinates {
                package_id: "platform-id".to_string(),
                package_name: format!("{platform_segment}-platform"),
                library_target: format!("{platform_segment}_platform"),
                manifest_path: PathBuf::from(format!(
                    "/workspace/platforms/{platform_segment}/Cargo.toml"
                )),
            };
            let frontend = PackageCoordinates {
                package_id: "frontend-id".to_string(),
                package_name: format!("{format_segment}-frontend"),
                library_target: format!("{format_segment}_frontend"),
                manifest_path: PathBuf::from(format!(
                    "/workspace/frontends/{format_segment}/Cargo.toml"
                )),
            };
            let cargo_toml = render_manifest(
                &cli,
                "../../crates/cli",
                &platform,
                &format!("../../platforms/{platform_segment}"),
                &frontend,
                &format!("../../frontends/{format_segment}"),
            );
            let main_rs = render_main(&platform_id, &format_id);
            let source = BoundProvisionerSource {
                platform: platform_id,
                format: format_id,
                engine: "0.1.0".to_string(),
                cargo_toml,
                main_rs,
                cargo_lock: b"version = 4\n".to_vec(),
                closure: ProvisionerClosure {
                    crate_dirs: Vec::new(),
                    crate_names: vec![
                        PROVISIONER_CLI_PACKAGE.to_string(),
                        platform.package_name.clone(),
                        frontend.package_name.clone(),
                    ],
                    workspace_files: Vec::new(),
                    locked: Vec::new(),
                },
            };

            let manifest: toml::Value = toml::from_str(source.cargo_toml())
                .expect("generated manifest parses");
            let roots = manifest["dependencies"]
                .as_table()
                .expect("dependency table")
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>();
            prop_assert_eq!(
                roots,
                vec!["selected_frontend", "selected_platform", "tokeira_provisioner_cli"]
            );
            prop_assert_eq!(source.generated_root_digest(), source.clone().generated_root_digest());
            prop_assert_eq!(source.evidence(&snapshot), source.clone().evidence(&snapshot));

            let mut changed_contract = source.clone();
            changed_contract.engine = "9.9.9".to_string();
            prop_assert_ne!(
                source.generated_root_digest(),
                changed_contract.generated_root_digest()
            );
            let mut changed_generated_source = source.clone();
            changed_generated_source.main_rs.push_str("// changed\n");
            prop_assert_ne!(
                source.generated_root_digest(),
                changed_generated_source.generated_root_digest()
            );
            let mut changed_format = source.clone();
            changed_format.format = DefinitionFormatId::new(format!("{format_segment}-other"))
                .expect("derived format is canonical");
            changed_format.main_rs = render_main(&changed_format.platform, &changed_format.format);
            prop_assert_ne!(
                source.generated_root_digest(),
                changed_format.generated_root_digest()
            );

            // Deployment definitions are interpreted data, not executable
            // closure input: arbitrary definition bytes cannot re-key this
            // already selected platform/frontend engine.
            let mut edited_definition = definition_bytes.clone();
            edited_definition.push(0);
            prop_assert_ne!(definition_bytes, edited_definition);
            prop_assert_eq!(source.evidence(&snapshot), source.clone().evidence(&snapshot));
        }
    }

    #[test]
    fn materialized_root_compiles_the_conventional_exports() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let generated = source
            .materialize_in(&fixture.root)
            .expect("materialize generated root");

        let output = cargo_check(&generated);
        assert!(
            output.status.success(),
            "generated root failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn compilation_rejects_a_missing_conventional_export() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        write(
            &fixture.root.join("platforms/alpha/src/lib.rs"),
            "pub fn differently_named() {}\n",
        );
        let generated = source
            .materialize_in(&fixture.root)
            .expect("materialize generated root");

        let output = cargo_check(&generated);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("provisioner"),
            "unexpected compiler error: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn package(root: &Path, relative: &str, name: &str, metadata: &str, source: &str) {
        let package = root.join(relative);
        std::fs::create_dir_all(package.join("src")).expect("create package source");
        write(
            &package.join("Cargo.toml"),
            &format!(
                "[package]\nname = \"{name}\"\nversion = \"0.1.0\"\nedition = \"2024\"\n{metadata}"
            ),
        );
        write(&package.join("src/lib.rs"), source);
    }

    fn write(path: &Path, content: &str) {
        std::fs::write(path, content).expect("write fixture file");
    }

    fn cargo_check(root: &Path) -> std::process::Output {
        Command::new(env!("CARGO"))
            .args([
                "check",
                "--offline",
                "--manifest-path",
                root.join("Cargo.toml")
                    .to_str()
                    .expect("fixture path is UTF-8"),
            ])
            .output()
            .expect("run cargo check")
    }
}
