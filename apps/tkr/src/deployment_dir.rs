//! Deployment resolution, on-disk layout, and platform-config loading.
//!
//! Every `tkr` command that targets a specific deployment ends up here to
//! turn a `--deployment <name>` flag (or the `.latest` sentinel) into a
//! fully-loaded [`DeploymentContext`]. The context bundles the deployment's
//! on-disk path, its metadata, and its parsed platform config so downstream
//! handlers can dispatch off `metadata.platform` without re-reading TOML.
//!
//! # On-disk layout
//!
//! A deployment directory looks like:
//!
//! ```text
//! ~/Library/Application Support/tokeira/tkr/<name>/
//!   deployment.toml   # platform-specific config (LocalConfig | ComposeConfig | EcsConfig)
//!   tokeirad.toml     # TokeiraConfig consumed by the tokeirad server binary
//!   metadata.json     # identity + status tracked by the CLI
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
use tokeira_compose_deployment::ComposeConfig;
use tokeira_ecs_deployment::EcsConfig;
use tokeira_local_deployment::LocalConfig;
use tokeira_orchestrator::{PlatformKind, StorageKind};
use uuid::Uuid;

use crate::metadata::{self, DeploymentMetadata, DeploymentStatus};

pub(crate) const DEPLOYMENT_TOML: &str = "deployment.toml";
pub(crate) const TOKEIRAD_TOML: &str = "tokeirad.toml";
pub(crate) const DEFINITION_TKD: &str = "definition.tkd";
pub(crate) const METADATA_JSON: &str = "metadata.json";
pub(crate) const LATEST_FILE: &str = ".latest";
/// The deployment-local provisioner binary — the `tkp` married to this
/// deployment, placed at create and preferred by the launcher over any on `PATH`.
/// The provisioner's name **inside a deployment dir** — always `tkp`,
/// regardless of which platform's binary it is (Req 14.4).
pub(crate) const PROVISIONER_BIN: &str = "tkp";
/// The per-platform **source** binary `create` resolves and copies in: the
/// `tkp-compose` bin target of `platforms/compose-syn` (all forwarded
/// deployments are compose today).
pub(crate) const PROVISIONER_SOURCE_BIN: &str = "tkp-compose";

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
    /// Unlike [`load_context`], this parses **no** in-process platform config —
    /// it is the entry point for **forwarded** (`.tkd`) deployments, which the
    /// bound `tkp` drives from the directory alone (the launcher needs only the
    /// path).
    pub(crate) fn resolve_dir(&self, requested: Option<&str>) -> Result<PathBuf> {
        let name = self.resolve_name(requested)?;
        let path = self.path(&name);
        if !path.join(METADATA_JSON).exists() {
            bail!("{}", self.not_found_message(&name)?);
        }
        Ok(path)
    }

    /// Whether the resolved deployment is a **`.tkd`/forwarded** deployment —
    /// provisioned by the bound `tkp`, not the legacy in-process engine. Detected
    /// by the presence of `definition.tkd`, mirroring `tkp`'s own `platform::detect`.
    pub(crate) fn is_forwarded(&self, requested: Option<&str>) -> Result<bool> {
        Ok(self.resolve_dir(requested)?.join(DEFINITION_TKD).exists())
    }

    /// Introduce the deployment's bound provisioner — copy `tkp` into `<name>/` so
    /// the deployment carries its own binary (the deployment-married provisioner,
    /// Proposal 005). The launcher prefers this deployment-local copy over any
    /// `tkp` on `PATH`, so the binary that mutates the deployment is exactly the
    /// one married to it at create.
    ///
    /// Phase 0 (native-cargo dev binding, Proposal 005): resolves the
    /// **per-platform source binary** (`tkp-compose`, a bin target of
    /// `platforms/compose-syn` — the platform ships its own provisioner) and
    /// copies its bytes in as `tkp`. Resolution order: installed on PATH, then
    /// the running `tkr`'s own directory (a dev `tkr` in `target/debug` finds
    /// its sibling `tkp-compose` from the same build). The hermetic
    /// build/obtain + bundle verification supersede this (tasks 16-18).
    pub(crate) fn place_provisioner(&self, name: &str) -> Result<()> {
        let source = which::which(PROVISIONER_SOURCE_BIN)
            .ok()
            .or_else(|| {
                std::env::current_exe()
                    .ok()
                    .and_then(|exe| Some(exe.parent()?.join(PROVISIONER_SOURCE_BIN)))
                    .filter(|sibling| sibling.is_file())
            })
            .ok_or_else(|| {
                anyhow!(
                    "cannot introduce the compose provisioner: no `{PROVISIONER_SOURCE_BIN}` on \
                     PATH or beside this `tkr`. Build it (`cargo build -p tokeira-compose-syn \
                     --bin {PROVISIONER_SOURCE_BIN}`) and re-run `tkr deployment create`."
                )
            })?;
        let dest = self.path(name).join(PROVISIONER_BIN);
        fs::copy(&source, &dest).with_context(|| {
            format!("failed to copy {} -> {}", source.display(), dest.display())
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&dest)?.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&dest, perms)?;
        }
        Ok(())
    }

    /// Create a fresh deployment: writes the two TOML files, the metadata
    /// JSON, an empty `state/` subdir, and flips `.latest` to the new name.
    ///
    /// Fails fast if a directory with the same normalised name already
    /// exists so we never silently clobber operator state.
    pub(crate) fn create(
        &self,
        name: &str,
        platform: PlatformKind,
        storage: StorageKind,
        region: Option<String>,
    ) -> Result<DeploymentMetadata> {
        let name = normalize_name(name);
        let path = self.path(&name);
        if path.exists() {
            bail!("deployment '{name}' already exists at {}", path.display());
        }
        fs::create_dir_all(path.join("state"))?;
        match platform {
            // The compose platform is `.tkd`-defined and provisioned by a
            // forwarded compose `tkp`: seed its definition (storage/region baked
            // into `config()`) plus a prototypical `tokeirad.toml` the operator can
            // edit before the first apply (writeback-updated at apply). No legacy
            // in-process `deployment.toml`.
            PlatformKind::Compose => {
                fs::write(
                    path.join(DEFINITION_TKD),
                    crate::prototypical::compose_definition(storage, region.as_deref())?,
                )?;
                fs::write(
                    path.join(TOKEIRAD_TOML),
                    crate::prototypical::server_config(platform, storage, region.as_deref())?,
                )?;
            }
            // Legacy in-process platforms (`local`; `ecs` still in-process for now).
            PlatformKind::Local | PlatformKind::Ecs => {
                fs::write(
                    path.join(DEPLOYMENT_TOML),
                    crate::prototypical::deployment_config(platform, storage, region.as_deref())?,
                )?;
                fs::write(
                    path.join(TOKEIRAD_TOML),
                    crate::prototypical::server_config(platform, storage, region.as_deref())?,
                )?;
            }
        }
        let now = timestamp();
        let metadata = DeploymentMetadata {
            name: name.clone(),
            id: Uuid::new_v4(),
            platform,
            storage,
            status: DeploymentStatus::Created,
            created_at: now.clone(),
            updated_at: now,
        };
        metadata::write(&path, &metadata)?;
        fs::create_dir_all(&self.root)?;
        fs::write(self.latest_path(), &name)?;
        Ok(metadata)
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
/// Each variant carries the fully parsed config for that platform so
/// handlers can branch on the deployment's platform kind without
/// re-reading files from disk. `Compose` and `Ecs` configs are boxed
/// because they're significantly larger than `LocalConfig` (observability
/// stacks, ECR mirror lists, etc.).
pub(crate) enum PlatformDeploymentConfig {
    Local(LocalConfig),
    Compose(Box<ComposeConfig>),
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
    let platform_config = match metadata.platform {
        PlatformKind::Local => {
            let config: LocalConfig = tokeira_config::load_config(&deployment_config_path, None)
                .with_context(|| format!("failed to load {}", deployment_config_path.display()))?;
            PlatformDeploymentConfig::Local(config)
        }
        PlatformKind::Compose => {
            let mut config: ComposeConfig =
                tokeira_config::load_config(&deployment_config_path, None).with_context(|| {
                    format!("failed to load {}", deployment_config_path.display())
                })?;
            config.deployment_dir = path.clone();
            PlatformDeploymentConfig::Compose(Box::new(config))
        }
        PlatformKind::Ecs => {
            let config: EcsConfig = tokeira_config::load_config(&deployment_config_path, None)
                .with_context(|| format!("failed to load {}", deployment_config_path.display()))?;
            PlatformDeploymentConfig::Ecs(Box::new(config))
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
