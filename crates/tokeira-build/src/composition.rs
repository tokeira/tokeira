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
};

use cargo_metadata::{Metadata, MetadataCommand, Package};
use thiserror::Error;
use tokeira_orchestrator::{DefinitionFormatId, PlatformId};
use tokeira_provisioner::{BoundProvisionerEvidence, Sha256Digest};

use crate::{
    ClosureError, DEFINITION_FRONTEND_CONTRACT, DefinitionFrontendPackageDescriptor,
    DiscoveryError, PLATFORM_BINDING_CONTRACT, PackageCoordinates, PlatformLaunchClass,
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
    binding_contract: u32,
    frontend_contract: u32,
    cargo_toml: String,
    main_rs: String,
    closure: ProvisionerClosure,
}

impl BoundProvisionerSource {
    /// Borrow the selected open platform identity.
    pub fn platform(&self) -> &PlatformId {
        &self.platform
    }

    /// Borrow the selected language-neutral Definition Format identity.
    pub fn format(&self) -> &DefinitionFormatId {
        &self.format
    }

    /// Return the admitted private Platform Binding contract version.
    pub fn binding_contract(&self) -> u32 {
        self.binding_contract
    }

    /// Return the admitted private Definition Frontend contract version.
    pub fn frontend_contract(&self) -> u32 {
        self.frontend_contract
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
        let mut bytes = b"tokeira-bound-provisioner-root/v1\n".to_vec();
        framed_field(&mut bytes, "platform", self.platform.as_str().as_bytes());
        framed_field(&mut bytes, "format", self.format.as_str().as_bytes());
        framed_field(
            &mut bytes,
            "binding-contract",
            self.binding_contract.to_string().as_bytes(),
        );
        framed_field(
            &mut bytes,
            "frontend-contract",
            self.frontend_contract.to_string().as_bytes(),
        );
        framed_field(&mut bytes, "Cargo.toml", self.cargo_toml.as_bytes());
        framed_field(&mut bytes, "src/main.rs", self.main_rs.as_bytes());
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
            binding_contract: self.binding_contract,
            frontend_contract: self.frontend_contract,
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
        Ok(root)
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
    /// Generated files could not be staged for compilation.
    #[error("failed to write generated composition-root file {path}: {source}")]
    WriteGenerated {
        /// Destination whose write failed.
        path: String,
        /// Filesystem failure.
        source: std::io::Error,
    },
}

/// Assemble one static provisioner from trusted workspace descriptors.
pub fn assemble_bound_provisioner(
    workspace_root: &Path,
    platform: &PlatformPackageDescriptor,
    frontend: &DefinitionFrontendPackageDescriptor,
) -> Result<BoundProvisionerSource, CompositionError> {
    validate_contracts(platform, frontend)?;

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

    Ok(BoundProvisionerSource {
        platform: platform.id.clone(),
        format: frontend.format.clone(),
        binding_contract: platform.binding_contract,
        frontend_contract: frontend.frontend_contract,
        cargo_toml,
        main_rs,
        closure,
    })
}

fn validate_contracts(
    platform: &PlatformPackageDescriptor,
    frontend: &DefinitionFrontendPackageDescriptor,
) -> Result<(), CompositionError> {
    if platform.launch_class != PlatformLaunchClass::BoundProvisioner {
        return Err(CompositionError::InvalidSelection(format!(
            "platform `{}` uses launch class {:?}, not bound-provisioner",
            platform.id, platform.launch_class
        )));
    }
    if platform.binding_contract != PLATFORM_BINDING_CONTRACT {
        return Err(CompositionError::InvalidSelection(format!(
            "platform `{}` uses binding contract {}; supported contract is {}",
            platform.id, platform.binding_contract, PLATFORM_BINDING_CONTRACT
        )));
    }
    if frontend.frontend_contract != DEFINITION_FRONTEND_CONTRACT {
        return Err(CompositionError::InvalidSelection(format!(
            "format `{}` uses frontend contract {}; supported contract is {}",
            frontend.format, frontend.frontend_contract, DEFINITION_FRONTEND_CONTRACT
        )));
    }
    Ok(())
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
        "tokeira_provisioner_cli::bound_provisioner_main!(\n    expected_platform: {platform},\n    binding: selected_platform::binding,\n    expected_format: {format},\n    frontend: selected_frontend::frontend,\n);\n"
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
        binding: $binding:path,
        expected_format: $format:literal,
        frontend: $frontend:path $(,)?
    ) => {
        fn main() {
            let _ = ($platform, $format, $binding(), $frontend());
        }
    };
}
"#,
            );
            package(
                &root,
                "platforms/alpha",
                "alpha-platform",
                "[package.metadata.tokeira.platform]\nid = \"alpha\"\nbinding-contract = 1\nlaunch-class = \"bound-provisioner\"\ndefault = false\n",
                "pub fn binding() {}\n",
            );
            package(
                &root,
                "frontends/tkd",
                "tkd-frontend",
                "[package.metadata.tokeira.definition-frontend]\nformat = \"tkd\"\nfrontend-contract = 1\nsource-extension = \"tkd\"\ndefault-relative-path = \"definition.tkd\"\n",
                "pub fn frontend() {}\n",
            );
            package(
                &root,
                "frontends/tkdp",
                "tkdp-frontend",
                "[package.metadata.tokeira.definition-frontend]\nformat = \"tkdp\"\nfrontend-contract = 1\nsource-extension = \"tkdp\"\ndefault-relative-path = \"definition.tkdp\"\n",
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
            "tokeira_provisioner_cli::bound_provisioner_main!(\n    expected_platform: \"alpha\",\n    binding: selected_platform::binding,\n    expected_format: \"tkd\",\n    frontend: selected_frontend::frontend,\n);\n"
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
        assert_eq!(evidence.binding_contract, tkd.binding_contract());
        assert_eq!(evidence.frontend_contract, tkd.frontend_contract());
        assert_eq!(evidence.generated_root, tkd.generated_root_digest());
        assert_eq!(evidence.source_closure, tkd.source_closure_digest("tree-a"));
        assert_eq!(
            evidence.lock_closure,
            Sha256Digest::from_bytes(&tkd.closure().canonical_lock_bytes())
        );
    }

    #[test]
    fn generated_root_digest_covers_contracts_and_exact_generated_bytes() {
        let fixture = Fixture::new();
        let source = fixture.assemble("tkd");
        let digest = source.generated_root_digest();

        let mut binding_contract = source.clone();
        binding_contract.binding_contract += 1;
        assert_ne!(binding_contract.generated_root_digest(), digest);

        let mut frontend_contract = source.clone();
        frontend_contract.frontend_contract += 1;
        assert_ne!(frontend_contract.generated_root_digest(), digest);

        let mut manifest = source.clone();
        manifest.cargo_toml.push_str("# changed\n");
        assert_ne!(manifest.generated_root_digest(), digest);

        let mut main = source;
        main.main_rs.push_str("// changed\n");
        assert_ne!(main.generated_root_digest(), digest);
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
            String::from_utf8_lossy(&output.stderr).contains("binding"),
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
