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
use tokeira_orchestrator::{
    DefinitionFormatId, PlatformKind, PlatformLaunchClass, RelativeDefinitionPath, StorageKind,
};
use tokeira_provisioner::RecordedDefinition;
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
/// The **source** binary `create` resolves and copies in: the `tkp` bin
/// target of `platforms/compose` (all forwarded deployments are compose
/// today; the constructed binary is `tkp`, never `tkp-<platform>`).
pub(crate) const PROVISIONER_SOURCE_BIN: &str = "tkp";

/// External definition source selected before deployment staging begins.
#[derive(Debug, Clone)]
pub(crate) struct DefinitionSeed {
    pub(crate) definition: RecordedDefinition,
    pub(crate) bytes: Vec<u8>,
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

    pub(crate) fn metadata(&self) -> &DeploymentMetadata {
        &self.metadata
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

    /// Whether the resolved deployment uses its bound provisioner.
    ///
    /// Routing is admitted metadata, never inferred from source-file presence.
    pub(crate) fn uses_bound_provisioner(&self, requested: Option<&str>) -> Result<bool> {
        let path = self.resolve_dir(requested)?;
        let metadata = metadata::read(&path)?;
        match metadata.launch_class {
            Some(PlatformLaunchClass::BoundProvisioner) => Ok(true),
            Some(PlatformLaunchClass::LegacyInProcess) => Ok(false),
            None => bail!(
                "deployment '{}' predates recorded launch-class metadata; recreate or migrate it before running lifecycle commands",
                metadata.name
            ),
        }
    }

    /// Introduce the deployment's bound provisioner — copy `tkp` into `<name>/` so
    /// the deployment carries its own binary (the deployment-married provisioner,
    /// Proposal 005). The launcher prefers this deployment-local copy over any
    /// `tkp` on `PATH`, so the binary that mutates the deployment is exactly the
    /// one married to it at create.
    ///
    /// Phase 0 (native-cargo dev binding, Proposal 005): resolves the
    /// **platform source binary** (`tkp`, a bin target of
    /// `platforms/compose` — the platform ships its own provisioner) and
    /// copies its bytes in as `tkp`. Resolution order: installed on PATH,
    /// then the running `tkr`'s own directory (a dev `tkr` in `target/debug`
    /// finds its sibling from the same build), then — inside the workspace —
    /// **`tkr` builds it from the platform crate** (15.5's "tkr compiles tkp
    /// from `platforms/<platform>`", the create-time leg). The hermetic
    /// build/obtain + bundle verification supersede this (tasks 16-18).
    /// Resolve the per-platform provisioner **source** binary from the
    /// Phase-0 pool, labeled for provenance reporting: installed on PATH →
    /// beside the running `tkr` → built from the workspace. The pool is
    /// where fresh bytes come from — placement at create and the upgrade
    /// re-marry both draw from it (the deployment's married copy is
    /// definitionally the *old* engine and never a candidate).
    pub(crate) fn resolve_provisioner_source() -> Result<(PathBuf, &'static str)> {
        if let Ok(path) = which::which(PROVISIONER_SOURCE_BIN) {
            return Ok((path, "installed on PATH"));
        }
        if let Some(sibling) = std::env::current_exe()
            .ok()
            .and_then(|exe| Some(exe.parent()?.join(PROVISIONER_SOURCE_BIN)))
            .filter(|sibling| sibling.is_file())
        {
            return Ok((sibling, "beside this tkr"));
        }
        Ok((
            Self::build_provisioner_from_workspace()?,
            "built from the workspace",
        ))
    }

    pub(crate) fn place_provisioner_at(&self, deployment_dir: &Path) -> Result<()> {
        // Resolution is labeled: placement is a provenance event, and the
        // operator report says which leg supplied the bytes and from where.
        let (source, how) = Self::resolve_provisioner_source()?;
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
            "provisioner: placed `tkp` ({how}: {}, sha256 {}…)",
            source.display(),
            &sha256[..12]
        );
        Ok(())
    }

    /// The create-time build leg (task 15.5): inside the workspace, `tkr`
    /// compiles the platform's provisioner bin and returns the built
    /// artifact — "tkr compiles tkp from `platforms/<platform>`", literally.
    /// Outside a workspace (an installed `tkr` with no source tree),
    /// resolution has honestly run out and the error names everything tried.
    ///
    /// (An associated function, not a method: the resolver's root plays no
    /// part in where the provisioner is built from.)
    fn build_provisioner_from_workspace() -> Result<PathBuf> {
        let cwd = std::env::current_dir().context("cannot determine the current directory")?;
        let Ok(workspace) = crate::bundle_create::workspace_root_from(&cwd) else {
            bail!(
                "cannot introduce the compose provisioner: no `{PROVISIONER_SOURCE_BIN}` on \
                 PATH, none beside this `tkr`, and no tokeira workspace above {} to build one \
                 from — install `{PROVISIONER_SOURCE_BIN}` or run from inside the workspace",
                cwd.display()
            );
        };
        eprintln!(
            "provisioner: building `{PROVISIONER_SOURCE_BIN}` from the workspace (Phase 0 dev \
             binding)…"
        );
        let status = std::process::Command::new("cargo")
            .current_dir(&workspace)
            .args([
                "build",
                "-p",
                "tokeira-compose-deployment",
                "--bin",
                PROVISIONER_SOURCE_BIN,
            ])
            .status()
            .context("failed to run `cargo build` for the provisioner")?;
        if !status.success() {
            bail!(
                "`cargo build -p tokeira-compose-deployment --bin {PROVISIONER_SOURCE_BIN}` failed — \
                 see the build output above"
            );
        }
        let artifact = workspace.join("target/debug").join(PROVISIONER_SOURCE_BIN);
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
        platform: PlatformKind,
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
                platform,
                launch_class: None,
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
        match platform {
            // The compose platform is `.tkd`-defined and provisioned by a
            // forwarded compose `tkp`: seed its definition (storage/region baked
            // into `config()`) plus a prototypical `tokeirad.toml` the operator can
            // edit before the first apply (writeback-updated at apply). No legacy
            // in-process `deployment.toml`.
            PlatformKind::Compose => {
                let seed = definition_seed.ok_or_else(|| {
                    anyhow!("the Compose deployment requires an external definition seed")
                })?;
                let definition_path = path.join(seed.definition.path.as_path());
                let definition_parent = definition_path
                    .parent()
                    .expect("a deployment-relative definition has a parent");
                fs::create_dir_all(definition_parent)?;
                fs::write(definition_path, seed.bytes)?;
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
            launch_class: Some(match platform {
                PlatformKind::Compose => PlatformLaunchClass::BoundProvisioner,
                PlatformKind::Local | PlatformKind::Ecs => PlatformLaunchClass::LegacyInProcess,
            }),
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
        platform: PlatformKind,
        storage: StorageKind,
        region: Option<String>,
    ) -> Result<DeploymentMetadata> {
        let seed = if platform == PlatformKind::Compose {
            Some(compose_definition_seed(storage, region.as_deref())?)
        } else {
            None
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

/// Resolve the current workspace's Compose seed as an ordinary artifact.
/// Installed operation retains the existing embedded fallback until the
/// admitted published-seed transport completes task 10.3.
pub(crate) fn compose_definition_seed(
    storage: StorageKind,
    region: Option<&str>,
) -> Result<DefinitionSeed> {
    let cwd = std::env::current_dir().context("cannot determine the current directory")?;
    let source = match crate::bundle_create::workspace_root_from(&cwd) {
        Ok(workspace) => {
            let seed_path = workspace.join("platforms/compose/definition.tkd");
            fs::read_to_string(&seed_path).with_context(|| {
                format!("failed to read definition seed {}", seed_path.display())
            })?
        }
        Err(_) => tokeira_compose_deployment::DEFAULT_TKD.to_string(),
    };
    let source = crate::prototypical::compose_definition(&source, storage, region)?;
    Ok(DefinitionSeed {
        definition: RecordedDefinition {
            format: DefinitionFormatId::new("tkd")
                .expect("the built-in tkd format id is canonical"),
            path: RelativeDefinitionPath::new(DEFINITION_TKD)
                .expect("the built-in definition path is safe"),
        },
        bytes: source.into_bytes(),
    })
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
    // A forwarded (`.tkd`) deployment has no in-process platform config — its
    // definition IS `definition.tkd`, interpreted by the married `tkp`. Every
    // caller of this function speaks the in-process dialect, so refuse in
    // domain terms: without this guard each caller surfaces a bare "no such
    // file" for a file the deployment was never meant to have. (Which
    // operational verbs the forwarded surface grows is a design decision, not
    // this error's business — the message states the contract, not a roadmap.)
    if metadata.launch_class == Some(PlatformLaunchClass::BoundProvisioner) {
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
