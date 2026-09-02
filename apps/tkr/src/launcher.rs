//! The launch seam (Requirement 7, task 9.1).
//!
//! `tkr` owns the staged Day-0 creation record, then never performs a
//! definition-bound lifecycle mutation itself. It executes the `tkp` married
//! to that deployment, applying the appropriate binding and integrity gate
//! before each post-creation mutation.
//!
//! | Class | When | Binary | Verified against |
//! |-------|------|--------|------------------|
//! | **Bound** | normal versioned mutation | the recorded binary | the recorded integrity manifest |
//! | **Candidate-upgrade** | `upgrade` | operator/release-resolved B (manifest still records A) | external release metadata (follow-on) |
//! | **Dev-candidate** | apply to a `dev` deployment | the current local dev build | gate permits `DevIterate` |
//! | **Rollback** | `rollback` | bound B (undo), then retained A (reconcile) | B from the manifest; A from the checkpoint |
//!
//! The launcher requires the deployment-local `tkp` and enforces the **Bound**
//! and **Rollback** classes' checksum against the recorded manifest (abort on mismatch, Req 7.2 —
//! rollback launches `B`, which the envelope's manifest still records at launch
//! time), and execs `tkp <verb> --deployment-dir <dir>`. The candidate-upgrade
//! external-metadata verification is a follow-on; the two-binary rollback
//! re-exec is [`launch_rollback`] (task 19.3).

use std::{
    path::{Path, PathBuf},
    process::Stdio,
};

use anyhow::{Context, Result, bail};
use tokeira_deployment::{
    BUNDLE_MANIFEST_BASENAME, BinaryStore, BuildMode, ConfigSource, DeploymentStateEnvelope,
    DeploymentStateLocation, DeploymentStateStores, ORCHESTRATED_LOCK_HOLDER_ENV,
    ORCHESTRATED_LOCK_TOKEN_ENV, ProvenanceStamp, ProvisionerBundle, Target, config_history,
};
use tokeira_state::LocalBackend;

use crate::deployment_dir::PROVISIONER_BIN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LaunchClass {
    Bound,
    CandidateUpgrade,
    DevCandidate,
    Rollback,
    /// A read-only verb (`describe`, checks, plans, or operational reads).
    /// Never verified and never refused: these verbs deliberately remain
    /// available when a binding mismatch blocks mutation.
    ReadOnly,
}

/// Resolve the launch class for a (possibly namespaced) `verb` — the token
/// sequence forwarded to `tkp` (`["describe"]`, `["infra", "plan"]`, …) — from
/// the deployment's recorded binding.
///
/// Read-only verbs are never gated (Req 6.1/8.3 — they must work precisely when
/// the mutating classes would refuse); any namespace's `plan` is read-only.
/// `upgrade` cannot run the recorded binary (A cannot know how to advance to B)
/// → candidate-upgrade; `rollback` runs B-then-A. Otherwise a versioned binding
/// is **bound** (run exactly the recorded binary) and a dev/unstamped binding is
/// a **dev-candidate**. An unstamped directory is incomplete and the
/// provisioner's mutation gate will refuse it.
pub(crate) fn resolve_class(verb: &[&str], envelope: &DeploymentStateEnvelope) -> LaunchClass {
    match verb {
        ["describe"]
        | ["definition", "check"]
        | ["observability", "check"]
        | ["logs"]
        | ["port-mappings"]
        | [_, "plan"] => LaunchClass::ReadOnly,
        ["upgrade"] => LaunchClass::CandidateUpgrade,
        ["rollback"] => LaunchClass::Rollback,
        _ => match envelope.binding.as_ref().map(|b| b.build_mode) {
            Some(BuildMode::Versioned) => LaunchClass::Bound,
            _ => LaunchClass::DevCandidate,
        },
    }
}

/// The deployment-married provisioner binary.
struct TkpBinary(PathBuf);

impl TkpBinary {
    /// Resolve the exact binary married at deployment creation.
    fn resolve(deployment_dir: &Path) -> Result<Self> {
        let bound = deployment_dir.join(crate::deployment_dir::PROVISIONER_BIN);
        if bound.is_file() {
            return Ok(Self(bound));
        }
        bail!(
            "deployment has no married provisioner at {}; recreate it through discovered deployment creation",
            bound.display()
        )
    }

    fn command(&self) -> (String, Vec<String>) {
        (self.0.display().to_string(), Vec::new())
    }
}

/// Prepare every authoritative store from the one placement admitted in
/// `metadata.json`. A remote client is loaded once for this preparation and
/// then cloned cheaply by the individual store constructors.
async fn prepare_state_stores(deployment_dir: &Path) -> Result<DeploymentStateStores> {
    let metadata = crate::metadata::read(deployment_dir)?;
    metadata
        .state
        .validate()
        .context("deployment metadata records an invalid state location")?;
    match metadata.state {
        DeploymentStateLocation::Local => Ok(DeploymentStateStores::local(deployment_dir)),
        DeploymentStateLocation::S3 {
            bucket,
            region,
            prefix,
        } => {
            let clients = tokeira_aws::AwsClients::load(None).await;
            Ok(DeploymentStateStores::s3(
                clients.s3_for(&region),
                bucket,
                prefix,
            ))
        }
    }
}

async fn load_envelope_from(state: &DeploymentStateStores) -> Result<DeploymentStateEnvelope> {
    let (envelope, _version) = state
        .envelope_store()
        .load()
        .await
        .context("failed to read the deployment's provisioner state")?;
    Ok(envelope)
}

/// Load the deployment's provisioner envelope from its recorded placement.
/// A definition-bound deployment without a binding is incomplete: creation
/// realizes local/remote Day 0 before publishing its staging directory, while
/// repository fetch reconnects to the signed locator.
pub(crate) async fn load_envelope(deployment_dir: &Path) -> Result<DeploymentStateEnvelope> {
    let state = prepare_state_stores(deployment_dir).await?;
    load_envelope_from(&state).await
}

/// Whether this launch must run a concrete installed binary that
/// checksum-matches the recorded integrity manifest.
///
/// **Bound** launches the recorded binary by definition. **Rollback** launches
/// `B` for its undo phase — and at launch time the envelope still records `B`'s
/// manifest (`upgrade` re-recorded it at the ownership transfer; `A`'s is only
/// restored later, inside the re-pin) — so the resolved binary is verifiable
/// against the manifest *today*, and an unverified byte-substituted `tkp` must
/// not perform the destructive undo + re-pin. tkp's own binding gate compares
/// provenance *strings* (compile-time build-info), never bytes; this is the only
/// byte check in the chain. Candidate-upgrade genuinely cannot verify against
/// the manifest (B is not recorded yet — external release metadata is the
/// follow-on); read-only and dev launches stay permissive. A dev/unstamped
/// binding skips verification even for rollback — dev iteration rebuilds change
/// the hash on every build, and tkp's gate handles dev semantics downstream.
fn requires_manifest_verification(class: LaunchClass, envelope: &DeploymentStateEnvelope) -> bool {
    let versioned = matches!(
        envelope.binding.as_ref().map(|b| b.build_mode),
        Some(BuildMode::Versioned)
    );
    versioned && matches!(class, LaunchClass::Bound | LaunchClass::Rollback)
}

/// Verify a concrete `tkp` binary against the recorded integrity manifest's
/// descriptor for **this host's target** (task 4.2's verify path): parsed-digest
/// comparison, size fast-fail, and a duplicate-target manifest refused as
/// ambiguous. Abort on any failure (Req 7.2).
fn verify_against_manifest(path: &Path, envelope: &DeploymentStateEnvelope) -> Result<()> {
    let manifest = envelope.integrity.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "deployment is bound but records no integrity manifest; the deployment creation \
             record is incomplete"
        )
    })?;
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    // tkr and the tkp it launches run on the same host, so the launcher's own
    // compile target names the descriptor the installed tkp must match.
    let target = Target(env!("TKR_TARGET").to_string());
    manifest.verify_artifact(&bytes, &target).with_context(|| {
        format!(
            "integrity check failed for the `tkp` at {} — refusing to launch the bound binary \
             (Req 7.2)",
            path.display()
        )
    })
}

/// Forward a lifecycle `verb` (a token sequence — `["infra", "plan"]` mirrors
/// `tkr infra plan`, Req 7.3) to the deployment's bound `tkp`: resolve the
/// launch class, checksum-verify (bound), then exec
/// `tkp <verb…> --deployment-dir <dir> [extra_args]`, inheriting stdio and
/// propagating the exit status.
pub(crate) async fn launch(
    deployment_dir: &Path,
    verb: &[&str],
    extra_args: &[String],
) -> Result<()> {
    launch_with_envs(deployment_dir, verb, extra_args, &[]).await
}

/// [`launch`], additionally exporting `envs` to the spawned `tkp` — the
/// orchestrated-rollback lease handoff (task 19.3) rides these.
async fn launch_with_envs(
    deployment_dir: &Path,
    verb: &[&str],
    extra_args: &[String],
    envs: &[(&str, String)],
) -> Result<()> {
    let envelope = load_envelope(deployment_dir).await?;
    let class = resolve_class(verb, &envelope);
    let binary = TkpBinary::resolve(deployment_dir)?;
    if requires_manifest_verification(class, &envelope) {
        verify_against_manifest(&binary.0, &envelope)?;
    }

    let (program, mut args) = binary.command();
    args.extend(verb.iter().map(|token| token.to_string()));
    args.push("--deployment-dir".to_string());
    args.push(deployment_dir.display().to_string());
    args.extend(extra_args.iter().cloned());

    // Operator narration: none. The forwarded verb's own report is the
    // answer; *which binary executed it* is evidence, not answer — it joins
    // the `--detail`/`--json` surface when mutating verbs migrate onto the
    // output contract (docs/platforms/operator-output-contract.md). The
    // Launch provenance remains internal either way.
    let status = tokio::process::Command::new(&program)
        .args(&args)
        .envs(envs.iter().map(|(k, v)| (*k, v.as_str())))
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("failed to launch `{program}` (the deployment's provisioner)"))?;
    if !status.success() {
        // Propagate the provisioner's actual exit status (Req 7) — tkp already
        // reported its error on the inherited stderr, so exit with its code
        // rather than wrapping it into a fresh (always-1) error of our own.
        std::process::exit(status.code().unwrap_or(1));
    }
    Ok(())
}

/// The two-binary rollback orchestration (task 19.3, Proposal 002):
///
/// 1. acquire the deployment's operation lease — held by THIS process across
///    the whole sequence (12.2 extended to two binaries: no window at the
///    process boundary where a third writer could take the lock);
/// 2. launch **B** (the bound binary) with `--handoff`: it verifies both
///    binaries, runs the delete-only pass, commits the re-pin to A, and
///    stops with the marker open;
/// 3. retrieve **A**'s retained bytes (identity-keyed, verified against the
///    re-pinned envelope's manifest — A's own) and place them as the
///    deployment's `tkp` — the physical half of re-marrying A;
/// 4. relaunch `rollback`, now A: the 19.4 resume path reconciles and
///    completes;
/// 5. release the lease.
///
/// A **pre-identity** checkpoint has no retained A to relaunch: the flow
/// falls back to the single-process rollback (the dev loop, where A and B
/// are the same build) — same verb, no handoff.
pub(crate) async fn launch_rollback(deployment_dir: &Path) -> Result<()> {
    let state = prepare_state_stores(deployment_dir).await?;
    let envelope = load_envelope_from(&state).await?;
    let retained_identity = envelope
        .checkpoint
        .as_ref()
        .and_then(|c| c.from_integrity.engine_identity.clone());
    // No checkpoint (tkp refuses with its own message) or a pre-identity A
    // (nothing retained to relaunch): the single-process flow.
    let Some(identity) = retained_identity else {
        return launch(deployment_dir, &["rollback"], &[]).await;
    };

    let lock = state.operation_lock();
    let holder = format!("tkr-rollback-pid{}", std::process::id());
    let guard = lock
        .acquire(&holder, ORCHESTRATION_LOCK_TTL)
        .await
        .context(
            "failed to acquire the operation lease for the two-binary rollback — another \
             provisioner may be operating this deployment",
        )?;
    let lease_envs = [
        (ORCHESTRATED_LOCK_HOLDER_ENV, guard.holder.clone()),
        (ORCHESTRATED_LOCK_TOKEN_ENV, guard.token.clone()),
    ];

    let result = orchestrate_rollback(deployment_dir, &state, &identity, &lease_envs).await;
    // Release regardless of outcome; an interrupted sequence is recovered by
    // re-running `rollback` (19.4), which re-acquires.
    if let Err(release_err) = lock.release(guard).await {
        eprintln!("warning: failed to release the rollback lease: {release_err}");
    }
    result
}

/// Lease TTL for the orchestrated rollback — matches tkp's own lock lease.
const ORCHESTRATION_LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(120);

async fn orchestrate_rollback(
    deployment_dir: &Path,
    state: &DeploymentStateStores,
    identity: &tokeira_deployment::EngineIdentity,
    lease_envs: &[(&str, String)],
) -> Result<()> {
    // Phase 1 — B: verify both, delete what B created, re-pin, stop.
    launch_with_envs(
        deployment_dir,
        &["rollback"],
        &["--handoff".to_string()],
        lease_envs,
    )
    .await
    .context("rollback phase 1 (B undo + re-pin) failed")?;

    // Phase 2 — place the retained A as the deployment's `tkp`. The re-pinned
    // envelope now records A's manifest, so retrieval re-verifies A's exact
    // bytes against it (trust flows from the CAS-guarded manifest, never the
    // stored blob).
    let repinned = load_envelope_from(state).await?;
    let manifest = repinned.integrity.as_ref().ok_or_else(|| {
        anyhow::anyhow!("the re-pinned envelope records no integrity manifest — cannot place A")
    })?;
    let retention = BinaryStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state"))),
        "binaries",
    );
    let bytes = retention
        .retrieve_verified(identity, &Target(env!("TKR_TARGET").to_string()), manifest)
        .await
        .context("A's retained binary failed retrieval/verification at placement")?;
    let dest = deployment_dir.join(PROVISIONER_BIN);
    std::fs::write(&dest, &bytes)
        .with_context(|| format!("failed to place A at {}", dest.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o755))?;
    }
    eprintln!("launcher: retained A placed as the deployment's tkp — relaunching to reconcile");

    // Phase 3 — A resumes `rollback` (19.4) and completes.
    launch_with_envs(deployment_dir, &["rollback"], &[], lease_envs)
        .await
        .context("rollback phase 2 (A reconcile) failed — re-run `rollback` to resume")
}

/// The upgrade orchestration — the sanctioned re-marry (an upgrade without a
/// definition change is idempotent; the *binary* advances whenever the
/// implementation changed):
///
/// 1. resolve the **candidate** from the recorded platform selection — never the
///    married copy, which is definitionally the engine being upgraded FROM
///    (resolving it as its own candidate is how `upgrade` was wedged: A
///    evaluated `upgrade(A, A)` and refused forever);
/// 2. byte-compare candidate vs bound — identical bytes mean nothing to
///    upgrade: report and stop (idempotency lives here: the dev sentinel
///    hash cannot see implementation changes, but the bytes can);
/// 3. drive `tkp upgrade` **via the candidate binary** (it evaluates the
///    advance, runs migrations, transfers ownership, applies, records the
///    audit log);
/// 4. on success, re-place `<deployment>/tkp` with the candidate's bytes —
///    the physical half of the re-marry (the envelope re-bound inside the
///    verb; the file must follow it).
pub(crate) async fn launch_upgrade(deployment_dir: &Path) -> Result<()> {
    let metadata = crate::metadata::read(deployment_dir)?;
    let definition = metadata
        .definition
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("legacy in-process deployments have no bound upgrade"))?;
    let cwd = std::env::current_dir().context("cannot determine current directory")?;
    let workspace = crate::bundle_create::workspace_root_from(&cwd)?;
    let discovery = crate::platform_discovery::PlatformDiscovery::from_workspace(&workspace)?;
    let platform = &discovery.platform(&metadata.platform)?.package;
    let frontend = &discovery.frontend(&definition.format)?.package;
    let candidate = crate::deployment_dir::DeploymentResolver::build_provisioner_from_workspace(
        &workspace, platform, frontend,
    )?;
    let candidate_bytes = std::fs::read(&candidate)
        .with_context(|| format!("failed to read the candidate at {}", candidate.display()))?;
    let bound_path = deployment_dir.join(PROVISIONER_BIN);
    let sha256 = tokeira_deployment::sha256_hex(&candidate_bytes);
    if let Ok(bound_bytes) = std::fs::read(&bound_path)
        && bound_bytes == candidate_bytes
    {
        println!(
            "upgrade: no changes to apply — the deployment's provisioner is already current (sha256 {}…)",
            &sha256[..12]
        );
        return Ok(());
    }

    let status = tokio::process::Command::new(&candidate)
        .args([
            "upgrade",
            "--deployment-dir",
            &deployment_dir.display().to_string(),
        ])
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("failed to launch the candidate `{}`", candidate.display()))?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    // The physical re-marry: the file follows the binding.
    std::fs::write(&bound_path, &candidate_bytes)
        .with_context(|| format!("failed to re-place {}", bound_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bound_path, std::fs::Permissions::from_mode(0o755))?;
    }
    // No narration: the placement is evidence, not answer — the upgrade
    // document tkp just rendered is the report, and `describe --detail`
    // carries the recorded sha for verification.
    Ok(())
}

/// Validate a staged deployment through the exact provisioner bytes that will
/// be published with it. Staging remains invisible if the check fails.
/// The staged check's identity facts (the extended check report,
/// Requirement 1.4): what create's birth publication claims. Deserialized
/// from the engine's own `--json` check report — the engine computes the
/// identity; `tkr` never recomputes it.
#[derive(Debug, serde::Deserialize)]
pub(crate) struct StagedCheckFacts {
    pub(crate) verifies: bool,
    #[serde(default)]
    pub(crate) identity: Option<tokeira_platform::definition::ConfigurationIdentity>,
    #[serde(default)]
    pub(crate) companions: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) provenance: Option<ProvenanceStamp>,
    #[serde(default)]
    pub(crate) source: Option<ConfigSource>,
}

pub(crate) async fn validate_staged_definition(deployment_dir: &Path) -> Result<StagedCheckFacts> {
    let provisioner = deployment_dir.join(crate::deployment_dir::PROVISIONER_BIN);
    if !provisioner.is_file() {
        bail!(
            "staged deployment has no provisioner at {}",
            provisioner.display()
        );
    }
    let output = tokio::process::Command::new(&provisioner)
        .args([
            "--json",
            "definition",
            "check",
            "--deployment-dir",
            &deployment_dir.display().to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .output()
        .await
        .with_context(|| format!("failed to launch staged `{}`", provisioner.display()))?;
    if !output.status.success() {
        // The captured report is the refusal's whole story — surface it
        // before failing the create.
        print!("{}", String::from_utf8_lossy(&output.stdout));
        bail!("staged provisioner refused the deployment definition");
    }
    let facts: StagedCheckFacts = serde_json::from_slice(&output.stdout).with_context(|| {
        format!(
            "staged provisioner check succeeded but its report does not decode: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })?;
    if !facts.verifies {
        bail!("staged provisioner check exited successfully but reported `verifies: false`");
    }
    Ok(facts)
}

/// Seed the staged deployment's server configuration through the exact
/// provisioner bytes that will be published with it. A platform that has not
/// adopted the render answers not-applicable inside the verb and exits
/// success, keeping the generic seeded document; a failure here is a
/// defective definition and keeps staging invisible.
pub(crate) async fn seed_staged_config(deployment_dir: &Path) -> Result<()> {
    let provisioner = deployment_dir.join(crate::deployment_dir::PROVISIONER_BIN);
    let status = tokio::process::Command::new(&provisioner)
        .args([
            "config",
            "seed",
            "--deployment-dir",
            &deployment_dir.display().to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await
        .with_context(|| format!("failed to launch staged `{}`", provisioner.display()))?;
    if !status.success() {
        bail!("staged provisioner could not seed the server configuration");
    }
    Ok(())
}

/// Commit the complete Day-0 creation record while the deployment is still
/// staged and invisible.
///
/// The bound provisioner's read-only definition check supplied `provenance`
/// and the exact admitted `source`; `tkr` independently verifies the placed
/// bytes against the bundle before recording either claim. No platform or
/// provider method runs here: creation owns the initial envelope and source
/// retention, while subsequent lifecycle mutation remains wholly in `tkp`.
pub(crate) async fn realize_staged_deployment(
    deployment_dir: &Path,
    facts: &StagedCheckFacts,
    revision: u64,
) -> Result<()> {
    realize_staged_state(deployment_dir, facts, revision, StagedStateMode::Create).await
}

/// Reconnect a fetched seat to the state placement authenticated by its
/// publication. Local publications have no portable state and therefore get a
/// fresh envelope; remote publications must already expose an exact matching
/// envelope, otherwise fetch would create a divergent writer or attach to the
/// wrong deployment.
pub(crate) async fn realize_fetched_deployment(
    deployment_dir: &Path,
    facts: &StagedCheckFacts,
    revision: u64,
    claimed_deployment_name: &str,
) -> Result<()> {
    realize_staged_state(
        deployment_dir,
        facts,
        revision,
        StagedStateMode::Fetch {
            deployment_id: claimed_deployment_name,
        },
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum StagedStateMode<'a> {
    Create,
    Fetch { deployment_id: &'a str },
}

async fn realize_staged_state(
    deployment_dir: &Path,
    facts: &StagedCheckFacts,
    revision: u64,
    mode: StagedStateMode<'_>,
) -> Result<()> {
    let provenance = facts.provenance.clone().ok_or_else(|| {
        anyhow::anyhow!("staged provisioner check reported no running-engine provenance")
    })?;
    let source = facts.source.as_ref().ok_or_else(|| {
        anyhow::anyhow!("staged provisioner check reported no admitted configuration source")
    })?;
    let bundle_path = deployment_dir.join(BUNDLE_MANIFEST_BASENAME);
    let bundle: ProvisionerBundle = serde_json::from_slice(
        &std::fs::read(&bundle_path)
            .with_context(|| format!("failed to read {}", bundle_path.display()))?,
    )
    .with_context(|| format!("failed to decode {}", bundle_path.display()))?;
    bundle
        .validate_bound_evidence()
        .context("placed provisioner bundle has invalid bound evidence")?;
    let manifest = bundle.integrity_manifest();
    manifest
        .validate()
        .context("placed provisioner bundle has an invalid integrity manifest")?;
    let provisioner = deployment_dir.join(crate::deployment_dir::PROVISIONER_BIN);
    let bytes = std::fs::read(&provisioner)
        .with_context(|| format!("failed to read {}", provisioner.display()))?;
    manifest
        .verify_artifact(&bytes, &Target(env!("TKR_TARGET").to_string()))
        .context("the staged provisioner disagrees with its placed bundle")?;

    let metadata = crate::metadata::read(deployment_dir)?;
    let state = prepare_state_stores(deployment_dir).await?;
    let (deployment_id, initialize) = match mode {
        StagedStateMode::Create => (metadata.name.as_str(), true),
        StagedStateMode::Fetch { deployment_id } => (
            deployment_id,
            matches!(metadata.state, DeploymentStateLocation::Local),
        ),
    };
    let remote_create = matches!(mode, StagedStateMode::Create)
        && matches!(metadata.state, DeploymentStateLocation::S3 { .. });
    // The operation lease closes the race between checking an unused prefix
    // and committing Day 0. Its released record may remain, but a competing
    // create cannot enter this critical section and initialize sibling stores
    // under the same prefix concurrently.
    let creation_lock = if remote_create {
        let lock = state.operation_lock();
        let holder = format!("tkr-create-pid{}", std::process::id());
        let guard = lock
            .acquire(&holder, ORCHESTRATION_LOCK_TTL)
            .await
            .context(
                "failed to reserve the remote-state prefix for Day-0 creation — another \
                 provisioner may be using it",
            )?;
        Some((lock, guard))
    } else {
        None
    };

    let result = async {
        let store = state.envelope_store();
        let (existing, version) = store
            .load()
            .await
            .context("failed to load the staged deployment envelope")?;

        if initialize {
            if remote_create {
                let (_, infra_version) = state
                    .infra_store()
                    .load()
                    .await
                    .context("failed to inspect the remote infrastructure-state namespace")?;
                let (_, deploy_version) = state
                    .deploy_store()
                    .load()
                    .await
                    .context("failed to inspect the remote deploy-state namespace")?;
                if [&version, &infra_version, &deploy_version]
                    .into_iter()
                    .any(|candidate| !candidate.is_empty())
                {
                    let location = metadata
                        .state
                        .remote_uri()
                        .expect("remote creation has an S3 location");
                    bail!(
                        "remote-state prefix {location} already contains deployment state; \
                         choose an unused prefix or fetch its signed deployment publication"
                    );
                }
            } else if existing.binding.is_some() {
                bail!("staged local deployment already carries a Day-0 binding");
            }
        } else {
            if existing.deployment_id != deployment_id {
                bail!(
                    "remote state belongs to deployment `{}` but the signed publication claims \
                     `{deployment_id}`",
                    existing.deployment_id
                );
            }
            if !existing
                .binding
                .as_ref()
                .is_some_and(|recorded| same_engine_binding(recorded, &provenance))
            {
                bail!(
                    "remote state binding disagrees with the provisioner authenticated by the \
                     signed publication"
                );
            }
            if existing.integrity.as_ref() != Some(&manifest) {
                bail!(
                    "remote state integrity manifest disagrees with the engine authenticated by \
                     the signed publication"
                );
            }
            if existing.config_revision != revision {
                bail!(
                    "remote state is at configuration revision {} but the signed publication is \
                     at revision {revision}; publish the committed transition before fetching \
                     another seat",
                    existing.config_revision
                );
            }
        }

        config_history::snapshot(deployment_dir, source, revision)
            .context("failed to retain the staged deployment's initial source")?;
        if initialize {
            let envelope = DeploymentStateEnvelope {
                deployment_id: deployment_id.to_string(),
                binding: Some(provenance),
                integrity: Some(manifest),
                config_revision: revision,
                ..Default::default()
            };
            store
                .save(&envelope, &version)
                .await
                .context("failed to commit the staged deployment's Day-0 envelope")?;
        }
        Ok(())
    }
    .await;

    let Some((lock, guard)) = creation_lock else {
        return result;
    };
    match (result, lock.release(guard).await) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(anyhow::anyhow!(
            "Day-0 state committed but its remote-state prefix lease could not be released: {error}"
        )),
        (Err(error), Err(release_error)) => Err(error.context(format!(
            "the remote-state prefix lease also could not be released: {release_error}"
        ))),
    }
}

/// Compare the engine identity carried by two provenance observations.
/// `recorded_at` says when a seat observed the engine and necessarily differs
/// on fetch; treating it as identity would reject the exact same verified
/// binary on every second machine.
fn same_engine_binding(left: &ProvenanceStamp, right: &ProvenanceStamp) -> bool {
    left.version == right.version
        && left.git_sha == right.git_sha
        && left.source_tree_hash == right.source_tree_hash
        && left.build_mode == right.build_mode
}

/// Forward `infra apply`. Creation has already committed the deployment's
/// binding and initial source revision; apply never doubles as inception.
pub(crate) async fn launch_apply(
    deployment_dir: &Path,
    yes: bool,
    module: Option<&str>,
    explanation: Option<&Path>,
) -> Result<()> {
    let mut extra = Vec::new();
    if yes {
        extra.push("--yes".to_string());
    }
    if let Some(name) = module {
        extra.push("--module".to_string());
        extra.push(name.to_string());
    }
    if let Some(path) = explanation {
        extra.push("--explanation".to_string());
        extra.push(path.display().to_string());
    }
    launch(deployment_dir, &["infra", "apply"], &extra).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tokeira_deployment::{
        BinaryArtifactDescriptor, IntegrityManifest, ProvenanceStamp, Target,
    };

    fn envelope_with(build_mode: Option<BuildMode>) -> DeploymentStateEnvelope {
        DeploymentStateEnvelope {
            binding: build_mode.map(|mode| ProvenanceStamp {
                build_mode: mode,
                ..ProvenanceStamp::current(Utc::now())
            }),
            ..Default::default()
        }
    }

    #[test]
    fn fetch_binding_identity_ignores_only_the_observation_time() {
        let first = ProvenanceStamp::current(Utc::now());
        let mut later = first.clone();
        later.recorded_at += chrono::Duration::seconds(1);
        assert!(same_engine_binding(&first, &later));

        later.source_tree_hash.push_str("-different");
        assert!(!same_engine_binding(&first, &later));
    }

    #[test]
    fn resolve_class_maps_verb_and_binding() {
        let versioned = envelope_with(Some(BuildMode::Versioned));
        let dev = envelope_with(Some(BuildMode::Dev));
        let unstamped = envelope_with(None);

        assert_eq!(
            resolve_class(&["upgrade"], &versioned),
            LaunchClass::CandidateUpgrade
        );
        assert_eq!(
            resolve_class(&["rollback"], &versioned),
            LaunchClass::Rollback
        );
        assert_eq!(
            resolve_class(&["infra", "apply"], &versioned),
            LaunchClass::Bound
        );
        assert_eq!(
            resolve_class(&["infra", "apply"], &dev),
            LaunchClass::DevCandidate
        );
        assert_eq!(
            resolve_class(&["infra", "apply"], &unstamped),
            LaunchClass::DevCandidate
        );

        // Read-only verbs are never gated — regardless of the binding, so a
        // versioned-bound deployment with a missing/mismatched tkp can still be
        // described (that is exactly when the operator needs diagnostics). Any
        // namespace's `plan` is read-only.
        for envelope in [&versioned, &dev, &unstamped] {
            assert_eq!(
                resolve_class(&["describe"], envelope),
                LaunchClass::ReadOnly
            );
            assert_eq!(
                resolve_class(&["infra", "plan"], envelope),
                LaunchClass::ReadOnly
            );
            assert_eq!(
                resolve_class(&["deploy", "plan"], envelope),
                LaunchClass::ReadOnly
            );
            assert_eq!(
                resolve_class(&["observability", "check"], envelope),
                LaunchClass::ReadOnly
            );
        }
    }

    #[test]
    fn manifest_verification_covers_bound_and_rollback_on_versioned_bindings() {
        let versioned = envelope_with(Some(BuildMode::Versioned));
        let dev = envelope_with(Some(BuildMode::Dev));
        let unstamped = envelope_with(None);

        // Bound + Rollback both launch the manifest-recorded binary → verified.
        assert!(requires_manifest_verification(
            LaunchClass::Bound,
            &versioned
        ));
        assert!(requires_manifest_verification(
            LaunchClass::Rollback,
            &versioned
        ));
        // Candidate-upgrade cannot verify against the manifest (B unrecorded);
        // read-only never gates; dev launches are permissive.
        assert!(!requires_manifest_verification(
            LaunchClass::CandidateUpgrade,
            &versioned
        ));
        assert!(!requires_manifest_verification(
            LaunchClass::ReadOnly,
            &versioned
        ));
        assert!(!requires_manifest_verification(
            LaunchClass::DevCandidate,
            &dev
        ));
        // Dev/unstamped bindings skip verification even for rollback — dev
        // rebuilds change the hash every build; tkp's gate governs downstream.
        assert!(!requires_manifest_verification(LaunchClass::Rollback, &dev));
        assert!(!requires_manifest_verification(
            LaunchClass::Rollback,
            &unstamped
        ));
    }

    fn manifest_for(sha: &str, target: &str) -> IntegrityManifest {
        IntegrityManifest {
            provisioner_version: "1.0.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target(target.to_string()),
                sha256: sha.to_string(),
                retrieval_ref: None,
                size_bytes: 0,
            }],
            ..Default::default()
        }
    }

    #[test]
    fn verify_matches_recorded_checksum_and_rejects_mismatch() {
        use tokeira_deployment::sha256_hex;

        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("tkp");
        std::fs::write(&bin, b"the-binary-bytes").unwrap();
        let host_target = env!("TKR_TARGET");

        // The descriptor for this host's target, matching digest → passes.
        let mut env = envelope_with(Some(BuildMode::Versioned));
        env.integrity = Some(manifest_for(&sha256_hex(b"the-binary-bytes"), host_target));
        verify_against_manifest(&bin, &env).expect("matching checksum passes");

        // Same target, different digest → aborts.
        env.integrity = Some(manifest_for(&sha256_hex(b"other-bytes"), host_target));
        let err = verify_against_manifest(&bin, &env).expect_err("mismatch aborts");
        assert!(
            err.to_string().contains("integrity check failed"),
            "unexpected: {err}"
        );

        // A matching digest recorded under a DIFFERENT target does not vouch for
        // this host's binary — target-scoped, not any-artifact (Req 7.2).
        env.integrity = Some(manifest_for(
            &sha256_hex(b"the-binary-bytes"),
            "some-other-triple",
        ));
        let err = verify_against_manifest(&bin, &env).expect_err("wrong target aborts");
        assert!(
            err.to_string().contains("integrity check failed"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn verify_requires_a_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("tkp");
        std::fs::write(&bin, b"x").unwrap();
        let env = envelope_with(Some(BuildMode::Versioned)); // no integrity
        let err = verify_against_manifest(&bin, &env).expect_err("no manifest aborts");
        assert!(
            err.to_string().contains("no integrity manifest"),
            "unexpected: {err}"
        );
    }

    #[tokio::test]
    async fn staged_realization_commits_binding_integrity_and_initial_source() {
        use std::collections::BTreeSet;

        use tokeira_deployment::{
            BoundProvisionerEvidence, BuildAuthority, BuildManifest, BuildProfile, EngineIdentity,
            ProvisionerBundle, Sha256Digest, TestEvidence,
        };

        let tmp = tempfile::tempdir().expect("deployment");
        std::fs::create_dir_all(tmp.path().join("state")).expect("state");
        std::fs::write(
            tmp.path().join("metadata.json"),
            serde_json::to_vec(&serde_json::json!({
                "name": "created",
                "id": "00000000-0000-0000-0000-000000000000",
                "platform": "compose",
                "definition": {"format": "tkd", "path": "deployment.tkd"},
                "storage": "in-memory",
                "status": "created",
                "created_at": "2026-08-25T00:00:00Z",
                "updated_at": "2026-08-25T00:00:00Z"
            }))
            .expect("metadata json"),
        )
        .expect("metadata");
        std::fs::write(tmp.path().join("deployment.tkd"), b"definition").expect("definition");
        std::fs::write(tmp.path().join("tokeirad.toml"), b"").expect("server config");
        let provisioner_bytes = b"bound provisioner";
        std::fs::write(tmp.path().join(PROVISIONER_BIN), provisioner_bytes).expect("provisioner");
        let identity = EngineIdentity {
            source_closure: Sha256Digest::from_bytes(b"source"),
            lock_closure: Sha256Digest::from_bytes(b"lock"),
            toolchain: "rustc test".to_string(),
            build_container: None,
            features: BTreeSet::new(),
            profile: BuildProfile::Dev,
        };
        let bundle = ProvisionerBundle {
            bound: Some(BoundProvisionerEvidence {
                platform: tokeira_orchestrator::PlatformId::new("compose").expect("platform"),
                format: tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format"),
                engine: "0.1.0".to_string(),
                generated_root: Sha256Digest::from_bytes(b"root"),
                source_closure: identity.source_closure,
                lock_closure: identity.lock_closure,
            }),
            identity,
            authority: BuildAuthority::LocalDeveloper,
            provisioner_version: "0.1.0".to_string(),
            artifacts: vec![BinaryArtifactDescriptor {
                target: Target(env!("TKR_TARGET").to_string()),
                sha256: tokeira_deployment::sha256_hex(provisioner_bytes),
                retrieval_ref: None,
                size_bytes: provisioner_bytes.len() as u64,
            }],
            tests: TestEvidence {
                command: "test".to_string(),
                passed: true,
            },
            build: BuildManifest {
                request_id: "request".to_string(),
                source_tree_oid: "tree".to_string(),
                snapshot_commit_oid: "commit".to_string(),
                toolchain: "rustc test".to_string(),
                builder: "test".to_string(),
            },
        };
        std::fs::write(
            tmp.path().join(BUNDLE_MANIFEST_BASENAME),
            serde_json::to_vec(&bundle).expect("bundle json"),
        )
        .expect("bundle");
        let provenance = ProvenanceStamp::current(Utc::now());
        let source = ConfigSource::recorded(
            tokeira_orchestrator::DefinitionFormatId::new("tkd").expect("format"),
            tokeira_orchestrator::RelativeDefinitionPath::new("deployment.tkd").expect("path"),
        );
        let facts = StagedCheckFacts {
            verifies: true,
            identity: None,
            companions: Some(Vec::new()),
            provenance: Some(provenance.clone()),
            source: Some(source),
        };

        realize_staged_deployment(tmp.path(), &facts, 0)
            .await
            .expect("realize creation");

        let envelope = load_envelope(tmp.path()).await.expect("load envelope");
        assert_eq!(envelope.deployment_id, "created");
        assert_eq!(envelope.binding, Some(provenance));
        assert_eq!(envelope.integrity, Some(bundle.integrity_manifest()));
        assert_eq!(envelope.config_revision, 0);
        assert_eq!(
            std::fs::read(tmp.path().join("state/config-revisions/0/deployment.tkd"))
                .expect("revision zero"),
            b"definition"
        );
    }
}
