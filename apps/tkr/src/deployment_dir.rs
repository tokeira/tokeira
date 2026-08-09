//! Deployment resolution, on-disk layout, and platform-config loading.
//!
//! Every `tkr` command that targets a specific deployment ends up here to
//! turn a `--deployment <name>` flag (or the `.latest` sentinel) into a
//! fully-loaded [`DeploymentContext`] for legacy in-process platforms. A
//! definition-bound platform is resolved from admitted metadata and operated by
//! the provisioner married into its directory.
//!
//! # On-disk layout
//!
//! A deployment directory looks like:
//!
//! ```text
//! ~/Library/Application Support/tokeira/tkr/<name>/
//!   definition.<fmt>  # definition-bound platform source, when recorded
//!   deployment.toml   # legacy Local/ECS platform config, otherwise
//!   tokeirad.toml     # TokeiraConfig consumed by the tokeirad server binary
//!   metadata.json     # identity + status tracked by the CLI
//!   tkp               # generated platform/frontend provisioner, when bound
//!   state/            # infra + deploy engine state (single-doc CAS files)
//!   tokeirad.pid      # written while `tkr deploy apply` runs against local platform
//! ```
//!
//! The parent directory also carries a `.latest` sentinel (name of the
//! deployment most recently targeted or selected via `tkr deployment use`).
//!
//! # Naming invariant
//!
//! All deployment names are round-tripped through [`normalize_name`] — the
//! filesystem entry and the in-memory name always match, and user-supplied
//! names with spaces or uppercase letters are accepted but normalised on
//! write and on lookup. The property tests in `main.rs` cover this.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use directories::ProjectDirs;
use tokeira_ecs_deployment::EcsConfig;
use tokeira_local_deployment::LocalConfig;
use tokeira_orchestrator::{PlatformId, StorageKind};
use tokeira_provisioner::RecordedDefinition;
use uuid::Uuid;

use crate::metadata::{self, DeploymentMetadata, DeploymentStatus};

pub(crate) const DEPLOYMENT_TOML: &str = "deployment.toml";
pub(crate) const TOKEIRAD_TOML: &str = "tokeirad.toml";
pub(crate) const METADATA_JSON: &str = "metadata.json";
pub(crate) const LATEST_FILE: &str = ".latest";
/// The deployment-local provisioner binary — the `tkp` married to this
/// deployment, placed at create and required by the launcher.
/// The provisioner's name **inside a deployment dir** — always `tkp`,
/// regardless of which platform's binary it is (Req 14.4).
pub(crate) const PROVISIONER_BIN: &str = "tkp";

/// External definition source selected before deployment staging begins.
#[derive(Debug, Clone)]
pub(crate) struct DefinitionSeed {
    pub(crate) definition: RecordedDefinition,
    pub(crate) bytes: Vec<u8>,
    /// The definition's part files, staged beside the root: every sibling
    /// carrying the root's extension in the platform package. A part is a
    /// document of the definition set — without them a split root cannot
    /// resolve at check or evaluation. Mirrors the retention rule in
    /// `tokeira-provisioner-cli::config_history`.
    pub(crate) parts: Vec<(String, Vec<u8>)>,
}

/// Collect the definition's part files beside the seed: every sibling file
/// with the root's extension, excluding the root itself, ascending by name.
pub(crate) fn sibling_parts(seed_path: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    let Some(dir) = seed_path.parent() else {
        return Ok(Vec::new());
    };
    let Some(extension) = seed_path.extension() else {
        return Ok(Vec::new());
    };
    let root_name = seed_path.file_name();
    let mut parts = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension() == Some(extension) && path.file_name() != root_name && path.is_file() {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("part file {} has a non-UTF-8 name", path.display()))?
                .to_string();
            let bytes = fs::read(&path)
                .with_context(|| format!("failed to read definition part {}", path.display()))?;
            parts.push((name, bytes));
        }
    }
    parts.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(parts)
}

/// Incomplete deployment held away from its final name until every staged
/// artifact and definition check succeeds.
#[derive(Debug)]
pub(crate) struct PendingDeployment {
    final_path: PathBuf,
    staging_path: PathBuf,
    latest_path: PathBuf,
    metadata: DeploymentMetadata,
    published: bool,
}

impl PendingDeployment {
    pub(crate) fn path(&self) -> &Path {
        &self.staging_path
    }

    /// Publish the complete directory, then atomically replace `.latest`.
    /// If targeting publication fails, the just-published directory is
    /// removed so callers never observe a half-created deployment.
    pub(crate) fn publish(mut self) -> Result<DeploymentMetadata> {
        fs::rename(&self.staging_path, &self.final_path).with_context(|| {
            format!(
                "failed to publish staged deployment {} as {}",
                self.staging_path.display(),
                self.final_path.display()
            )
        })?;

        let latest_tmp = self
            .latest_path
            .with_extension(format!("latest-{}", Uuid::new_v4().simple()));
        let latest_result = fs::write(&latest_tmp, &self.metadata.name)
            .and_then(|()| fs::rename(&latest_tmp, &self.latest_path));
        if let Err(error) = latest_result {
            let _ = fs::remove_file(&latest_tmp);
            let rollback = fs::remove_dir_all(&self.final_path);
            if let Err(rollback) = rollback {
                return Err(anyhow!(
                    "failed to publish {}: {error}; rollback of {} also failed: {rollback}",
                    self.latest_path.display(),
                    self.final_path.display()
                ));
            }
            return Err(error)
                .with_context(|| format!("failed to publish {}", self.latest_path.display()));
        }
        self.published = true;
        Ok(self.metadata.clone())
    }
}

impl Drop for PendingDeployment {
    fn drop(&mut self) {
        if !self.published && self.staging_path.is_dir() {
            let _ = fs::remove_dir_all(&self.staging_path);
        }
    }
}

/// Resolves deployment names to on-disk paths and mediates the `.latest`
/// selection sentinel.
///
/// In production use [`DeploymentResolver::default`] (which locates the
/// platform-appropriate state directory); tests use
/// `DeploymentResolver::with_root` (`#[cfg(test)]`) to sandbox under a
/// `tempfile::TempDir`.
pub(crate) struct DeploymentResolver {
    root: PathBuf,
}

impl DeploymentResolver {
    pub(crate) fn default() -> Result<Self> {
        // Use "tokeira" as the application to get ~/Library/Application Support/tokeira/
        // on macOS, then append "tkr" for the deployment subdirectory.
        let project_dirs = ProjectDirs::from("", "", "tokeira")
            .ok_or_else(|| anyhow!("could not determine application state directory"))?;
        let base = project_dirs
            .state_dir()
            .unwrap_or_else(|| project_dirs.data_local_dir());
        Ok(Self {
            root: base.join("tkr"),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_root(root: PathBuf) -> Self {
        Self { root }
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn path(&self, name: &str) -> PathBuf {
        self.root.join(normalize_name(name))
    }

    pub(crate) fn latest_path(&self) -> PathBuf {
        self.root.join(LATEST_FILE)
    }

    pub(crate) fn latest_name(&self) -> Option<String> {
        fs::read_to_string(self.latest_path())
            .ok()
            .map(|name| normalize_name(name.trim()))
            .filter(|name| !name.is_empty())
    }

    /// Resolves a user-supplied name (or falls back to the `.latest`
    /// sentinel) and returns the normalised form. The returned name is the
    /// filesystem entry under [`DeploymentResolver::root`].
    pub(crate) fn resolve_name(&self, requested: Option<&str>) -> Result<String> {
        if let Some(name) = requested {
            return Ok(normalize_name(name));
        }
        let latest_path = self.latest_path();
        let latest = fs::read_to_string(&latest_path).with_context(|| {
            format!(
                "no deployment selected; run `tkr deployment use <name>` or pass --deployment (missing {})",
                latest_path.display()
            )
        })?;
        Ok(normalize_name(latest.trim()))
    }

    /// Resolve a deployment to its on-disk directory, verifying it exists.
    ///
    /// Unlike [`load_context`], this parses no legacy in-process platform config.
    /// A bound `tkp` drives a definition-bound deployment from this directory.
    pub(crate) fn resolve_dir(&self, requested: Option<&str>) -> Result<PathBuf> {
        let name = self.resolve_name(requested)?;
        let path = self.path(&name);
        if !path.join(METADATA_JSON).exists() {
            bail!("{}", self.not_found_message(&name)?);
        }
        Ok(path)
    }

    /// Whether the resolved deployment records a bound definition.
    ///
    /// Routing uses admitted metadata, never source-file presence or extension.
    pub(crate) fn uses_bound_provisioner(&self, requested: Option<&str>) -> Result<bool> {
        let path = self.resolve_dir(requested)?;
        let metadata = metadata::read(&path)?;
        Ok(metadata.definition.is_some())
    }

    /// Build the catalog-selected composition root and marry its exact `tkp`
    /// bytes to the staged deployment.
    pub(crate) fn place_provisioner_at(
        &self,
        deployment_dir: &Path,
        workspace: &Path,
        platform: &tokeira_build::PlatformPackageDescriptor,
        frontend: &tokeira_build::DefinitionFrontendPackageDescriptor,
    ) -> Result<()> {
        let source = Self::build_provisioner_from_workspace(workspace, platform, frontend)?;
        let bytes =
            fs::read(&source).with_context(|| format!("failed to read {}", source.display()))?;
        let sha256 = tokeira_provisioner::sha256_hex(&bytes);
        let dest = deployment_dir.join(PROVISIONER_BIN);
        fs::write(&dest, &bytes).with_context(|| format!("failed to place {}", dest.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&dest)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&dest, perms)?;
        }
        println!(
            "provisioner: placed generated `{}/{}` tkp (sha256 {}…)",
            platform.id,
            frontend.format,
            &sha256[..12]
        );
        Ok(())
    }

    pub(crate) fn build_provisioner_from_workspace(
        workspace: &Path,
        platform: &tokeira_build::PlatformPackageDescriptor,
        frontend: &tokeira_build::DefinitionFrontendPackageDescriptor,
    ) -> Result<PathBuf> {
        let source = tokeira_build::assemble_bound_provisioner(workspace, platform, frontend)
            .context("failed to assemble the bound provisioner source")?;
        let generated = source
            .materialize_in(workspace)
            .context("failed to materialize the bound provisioner source")?;
        eprintln!(
            "provisioner: building generated `{}/{}` root {}…",
            source.platform(),
            source.format(),
            &source.generated_root_digest().to_hex()[..12]
        );
        let status = std::process::Command::new("cargo")
            .current_dir(workspace)
            .args(["build", "--locked", "--manifest-path"])
            .arg(generated.join("Cargo.toml"))
            .status()
            .context("failed to run Cargo for the generated provisioner")?;
        if !status.success() {
            bail!("the generated bound-provisioner build failed; see Cargo output above");
        }
        let artifact = generated
            .join("target/debug")
            .join(tokeira_build::GENERATED_PROVISIONER_BIN);
        if !artifact.is_file() {
            bail!(
                "the provisioner build succeeded but {} is missing",
                artifact.display()
            );
        }
        Ok(artifact)
    }

    /// Stage a complete deployment away from its operator-visible final path.
    pub(crate) fn begin_create(
        &self,
        name: &str,
        platform: PlatformId,
        storage: StorageKind,
        region: Option<String>,
        definition_seed: Option<DefinitionSeed>,
    ) -> Result<PendingDeployment> {
        let name = normalize_name(name);
        let final_path = self.path(&name);
        if final_path.exists() {
            bail!(
                "deployment '{name}' already exists at {}",
                final_path.display()
            );
        }
        fs::create_dir_all(&self.root)?;
        let path = self
            .root
            .join(format!(".{name}.create-{}", Uuid::new_v4().simple()));
        let mut pending = PendingDeployment {
            final_path,
            staging_path: path.clone(),
            latest_path: self.latest_path(),
            metadata: DeploymentMetadata {
                name: String::new(),
                id: Uuid::nil(),
                platform: platform.clone(),
                definition: None,
                storage,
                status: DeploymentStatus::Created,
                created_at: String::new(),
                updated_at: String::new(),
            },
            published: false,
        };
        fs::create_dir_all(path.join("state"))?;

        let recorded_definition = definition_seed.as_ref().map(|seed| seed.definition.clone());
        if let Some(seed) = definition_seed {
            let definition_path = path.join(seed.definition.path.as_path());
            let definition_parent = definition_path
                .parent()
                .expect("a deployment-relative definition has a parent")
                .to_path_buf();
            fs::create_dir_all(&definition_parent)?;
            fs::write(definition_path, seed.bytes)?;
            // The definition set stages whole: parts land beside the root,
            // where check, evaluation, retention, and retarget resolve them.
            for (name, bytes) in &seed.parts {
                fs::write(definition_parent.join(name), bytes)?;
            }
            fs::write(
                path.join(TOKEIRAD_TOML),
                tokeira_config::TokeiraConfig::default().to_toml()?,
            )?;
        } else {
            let legacy = crate::legacy::LegacyPlatform::from_id(&platform).ok_or_else(|| {
                anyhow!("platform `{platform}` has neither a catalog seed nor a legacy adapter")
            })?;
            fs::write(
                path.join(DEPLOYMENT_TOML),
                legacy.deployment_config(storage),
            )?;
            fs::write(
                path.join(TOKEIRAD_TOML),
                legacy.server_config(storage, region.as_deref())?,
            )?;
        }
        let now = timestamp();
        let metadata = DeploymentMetadata {
            name: name.clone(),
            id: Uuid::new_v4(),
            platform,
            definition: recorded_definition,
            storage,
            status: DeploymentStatus::Created,
            created_at: now.clone(),
            updated_at: now,
        };
        metadata::write(&path, &metadata)?;
        pending.metadata = metadata;
        Ok(pending)
    }

    /// Transitional convenience used by legacy call sites and tests.
    /// Bound creation commands use [`begin_create`](Self::begin_create) so
    /// the provisioner and its validation join the same transaction.
    #[cfg(test)]
    pub(crate) fn create(
        &self,
        name: &str,
        platform: PlatformId,
        storage: StorageKind,
        region: Option<String>,
    ) -> Result<DeploymentMetadata> {
        let seed = if crate::legacy::LegacyPlatform::from_id(&platform).is_some() {
            None
        } else {
            let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let catalog = crate::catalog::PlatformCatalog::from_workspace(&workspace)?;
            let platform_descriptor = catalog.platform(&platform)?;
            let (frontend, definition_path, path) =
                catalog.workspace_frontend(platform_descriptor, None)?;
            Some(DefinitionSeed {
                definition: RecordedDefinition {
                    format: frontend.format.clone(),
                    path: definition_path,
                },
                parts: sibling_parts(&path)?,
                bytes: fs::read(path)?,
            })
        };
        self.begin_create(name, platform, storage, region, seed)?
            .publish()
    }

    pub(crate) fn list(&self) -> Result<Vec<DeploymentMetadata>> {
        if !self.root.exists() {
            return Ok(Vec::new());
        }
        let mut deployments = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if path.join(METADATA_JSON).exists() {
                deployments.push(metadata::read(&path)?);
            }
        }
        deployments.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(deployments)
    }

    pub(crate) fn deployment_names(&self) -> Result<Vec<String>> {
        Ok(self
            .list()?
            .into_iter()
            .map(|metadata| metadata.name)
            .collect())
    }

    pub(crate) fn mark_latest(&self, name: &str) -> Result<()> {
        let name = normalize_name(name);
        let path = self.path(&name);
        if !path.join(METADATA_JSON).exists() {
            bail!("{}", self.not_found_message(&name)?);
        }
        fs::create_dir_all(&self.root)?;
        fs::write(self.latest_path(), name)?;
        Ok(())
    }

    pub(crate) fn remove(&self, name: &str) -> Result<()> {
        let name = normalize_name(name);
        let path = self.path(&name);
        if !path.exists() {
            bail!("deployment '{name}' does not exist");
        }
        fs::remove_dir_all(&path)?;
        if fs::read_to_string(self.latest_path())
            .map(|latest| normalize_name(latest.trim()) == name)
            .unwrap_or(false)
        {
            let _ = fs::remove_file(self.latest_path());
        }
        Ok(())
    }

    pub(crate) fn update_status(&self, name: &str, status: DeploymentStatus) -> Result<()> {
        let path = self.path(name);
        let mut metadata = metadata::read(&path)?;
        metadata.status = status;
        metadata.updated_at = timestamp();
        metadata::write(&path, &metadata)
    }

    /// Build a helpful "deployment not found" error that lists what is
    /// actually available. Extracted so every command surfaces the same
    /// guidance when a caller passes an unknown `--deployment`.
    pub(crate) fn not_found_message(&self, name: &str) -> Result<String> {
        let available = self.deployment_names()?;
        if available.is_empty() {
            Ok(format!(
                "deployment '{name}' does not exist; create one with `tkr deployment create <name>`"
            ))
        } else {
            Ok(format!(
                "deployment '{name}' does not exist; available deployments: {}",
                available.join(", ")
            ))
        }
    }
}

/// Platform-specific config loaded from deployment.toml.
///
/// Each variant carries the fully parsed config for a legacy in-process
/// platform. Definition-bound platform config is owned by its provisioner.
pub(crate) enum PlatformDeploymentConfig {
    Local(LocalConfig),
    Ecs(Box<EcsConfig>),
}

/// Fully-loaded view of a deployment, as consumed by command handlers.
///
/// A handler that receives a `DeploymentContext` never needs to touch the
/// filesystem again for config — everything in [`DeploymentContext::path`]
/// has already been parsed into [`DeploymentContext::metadata`] and
/// [`DeploymentContext::platform_config`].
pub(crate) struct DeploymentContext {
    pub name: String,
    pub path: PathBuf,
    pub metadata: DeploymentMetadata,
    pub platform_config: PlatformDeploymentConfig,
}

/// Resolve a deployment by name (or fall back to `.latest`) and return a
/// fully-parsed [`DeploymentContext`].
///
/// This is the single entry point every non-`dev` subcommand uses to turn
/// an optional `--deployment` flag into a runnable context.
pub(crate) fn load_context(
    deployments: &DeploymentResolver,
    requested_name: Option<&str>,
) -> Result<DeploymentContext> {
    let name = deployments.resolve_name(requested_name)?;
    let path = deployments.path(&name);
    if !path.join(METADATA_JSON).exists() {
        bail!("{}", deployments.not_found_message(&name)?);
    }
    let metadata = metadata::read(&path)?;
    let deployment_config_path = path.join(DEPLOYMENT_TOML);
    // A definition-bound deployment has no in-process platform config. Every
    // caller of this function speaks the legacy dialect, so refuse in domain
    // terms instead of surfacing a missing `deployment.toml`.
    if metadata.definition.is_some() {
        let definition = metadata
            .definition
            .as_ref()
            .map(|definition| definition.path.as_str())
            .unwrap_or("recorded definition");
        bail!(
            "deployment '{name}' is defined by `{definition}` and operated through its bound \
             `{PROVISIONER_BIN}`; this command drives in-process (`{DEPLOYMENT_TOML}`) platforms"
        );
    }
    let platform_config = match metadata.platform.as_str() {
        "local" => {
            let config: LocalConfig = tokeira_config::load_config(&deployment_config_path, None)
                .with_context(|| format!("failed to load {}", deployment_config_path.display()))?;
            PlatformDeploymentConfig::Local(config)
        }
        "ecs" => {
            let config: EcsConfig = tokeira_config::load_config(&deployment_config_path, None)
                .with_context(|| format!("failed to load {}", deployment_config_path.display()))?;
            PlatformDeploymentConfig::Ecs(Box::new(config))
        }
        platform => {
            bail!("deployment '{name}' records unsupported in-process platform `{platform}`")
        }
    };
    Ok(DeploymentContext {
        name,
        path,
        metadata,
        platform_config,
    })
}

/// Normalise a user-supplied deployment name to a safe filesystem entry.
///
/// Lowercased; trimmed; non-alphanumeric characters (other than `-` / `_`)
/// collapse to `-`; leading/trailing dashes are stripped. This means two
/// operators typing the same deployment "My Dev" vs "my-dev" will always
/// resolve to the same directory.
pub(crate) fn normalize_name(name: &str) -> String {
    name.trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_ascii_lowercase()
}

fn timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}
