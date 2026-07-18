//! `tkp` — the Tokeira platform provisioner.
//!
//! A specialized binary scoped to a single deployment's lifecycle (no operator /
//! global verbs — those live in `tkr`). Its identity is stamped into the state it
//! writes, and every mutating verb gates on the binding between the running
//! binary and the deployment's recorded provenance.
//!
//! This is the scaffold. `describe` (read-only, never gates) is implemented and
//! exercises the provisioner foundation — provenance stamping, the binding gate,
//! the integrity verifier, and the deployment state envelope. The applying verbs
//! (`apply`/`upgrade`/`rollback`) land with the engine wiring. An interrupted
//! `upgrade`/`rollback` recovers by re-running that verb (idempotent, marker-driven);
//! there is no separate `resume` verb.

// CLI: stdout/stderr are the user interface.
#![allow(clippy::print_stdout, clippy::print_stderr)]
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use serde::Serialize;
use tokeira_provisioner::{
    BindingVerdict, BuildMode, DeploymentStateEnvelope, ProvenanceStamp, check_binding,
};
use tokeira_state::{CasStore, DeploymentStore, LocalBackend};

mod apply;
mod config_history;
mod destroy;
mod gate;
mod init;
mod lock;
mod plan;
mod platform;
mod revert;
mod rollback;
mod upgrade;

#[derive(Parser)]
#[command(
    name = "tkp",
    version,
    about = "Tokeira platform provisioner — deployment lifecycle"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Day-0 mandatory versioning: write the first provenance stamp + integrity
    /// manifest before any resource create.
    Init(LifecycleArgs),
    /// Read-only report of identity, recorded provenance, binding verdict, and
    /// state facts. Never gates.
    Describe(DescribeArgs),
    /// Show the binding verdict + the infrastructure plan. Read-only; never gates.
    Plan(LifecycleArgs),
    /// Plan and apply the deployment, gated on the binding.
    Apply(LifecycleArgs),
    /// Tear down the deployment's infrastructure, gated on the binding. Irreversible.
    Destroy(DestroyArgs),
    /// Revert to a prior config revision — a same-engine apply, gated on the binding.
    Revert(RevertArgs),
    /// Upgrade to a new engine identity. (Not yet implemented.)
    Upgrade(LifecycleArgs),
    /// Roll back to the retained prior configuration revision. (Not yet implemented.)
    Rollback(LifecycleArgs),
}

#[derive(Args)]
struct DescribeArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Emit JSON instead of human-readable text.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
struct LifecycleArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
}

#[derive(Args)]
struct DestroyArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// Confirm the irreversible teardown (required).
    #[arg(long)]
    yes: bool,
}

#[derive(Args)]
struct RevertArgs {
    /// Deployment directory holding the state envelope.
    #[arg(long)]
    deployment_dir: PathBuf,
    /// The prior config revision to revert to (a same-engine re-apply of its config).
    #[arg(long)]
    to: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        // Read-only: never gates, never locks.
        Command::Describe(args) => describe(args).await,
        Command::Plan(args) => plan::plan(&args.deployment_dir).await,
        // Mutating verbs run under the deployment's operation lock (Req 11).
        // `rollback` holds one continuous lock across its whole sequence (12.2).
        Command::Init(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "init", || init::init(&dir)).await
        }
        Command::Apply(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "apply", || apply::apply(&dir)).await
        }
        Command::Destroy(args) => {
            let dir = args.deployment_dir;
            let yes = args.yes;
            lock::with_operation_lock(&dir, "destroy", || destroy::destroy(&dir, yes)).await
        }
        Command::Revert(args) => {
            let dir = args.deployment_dir;
            let to = args.to;
            lock::with_operation_lock(&dir, "revert", || revert::revert(&dir, to)).await
        }
        Command::Upgrade(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "upgrade", || upgrade::upgrade(&dir)).await
        }
        Command::Rollback(args) => {
            let dir = args.deployment_dir;
            lock::with_operation_lock(&dir, "rollback", || rollback::rollback(&dir)).await
        }
    }
}

/// The deployment-level envelope store.
///
/// For now a local CAS store under `{deployment_dir}/state/envelope`; cloud
/// deployments will select an `S3StateStore` through the platform store seam
/// (task 13.2), just like the infra/runtime state.
fn envelope_store(deployment_dir: &Path) -> Box<dyn DeploymentStore<DeploymentStateEnvelope>> {
    Box::new(CasStore::new(
        Box::new(LocalBackend::new(deployment_dir.join("state/envelope"))),
        "envelope".to_string(),
    ))
}

async fn describe(args: DescribeArgs) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _version) = envelope_store(&args.deployment_dir).load().await?;
    let report = DescribeReport::build(&running, &envelope);

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        report.print_human(&args.deployment_dir);
    }
    Ok(())
}

// ── Report model (also the `--json` shape) ────────────────────────────────

#[derive(Serialize)]
struct StampView {
    version: String,
    git_sha: String,
    source_tree_hash: String,
    build_mode: BuildMode,
}

impl StampView {
    fn of(stamp: &ProvenanceStamp) -> Self {
        Self {
            version: stamp.version.clone(),
            git_sha: stamp.git_sha.clone(),
            source_tree_hash: stamp.source_tree_hash.clone(),
            build_mode: stamp.build_mode,
        }
    }
}

#[derive(Serialize)]
struct BindingView {
    /// The deployment's recorded stamp, or `None` when unstamped (Unknown).
    recorded: Option<StampView>,
    verdict: &'static str,
    proceeds: bool,
    authoritative: bool,
}

#[derive(Serialize)]
struct IntegrityView {
    provisioner_version: String,
    artifact_count: usize,
}

#[derive(Serialize)]
struct DescribeReport {
    running: StampView,
    deployment_id: String,
    schema_version: u32,
    config_revision: u64,
    effective_config_ref: Option<String>,
    binding: BindingView,
    integrity: Option<IntegrityView>,
    infra_head_present: bool,
    runtime_head_present: bool,
    operation: Option<String>,
    lock_holder: Option<String>,
}

impl DescribeReport {
    fn build(running: &ProvenanceStamp, envelope: &DeploymentStateEnvelope) -> Self {
        let verdict = check_binding(envelope.binding.as_ref(), running);
        Self {
            running: StampView::of(running),
            deployment_id: envelope.deployment_id.clone(),
            schema_version: envelope.schema_version,
            config_revision: envelope.config_revision,
            effective_config_ref: envelope.effective_config_ref.clone(),
            binding: BindingView {
                recorded: envelope.binding.as_ref().map(StampView::of),
                verdict: verdict_label(verdict),
                proceeds: verdict.proceeds(),
                authoritative: verdict.is_authoritative(),
            },
            integrity: envelope.integrity.as_ref().map(|m| IntegrityView {
                provisioner_version: m.provisioner_version.clone(),
                artifact_count: m.artifacts.len(),
            }),
            infra_head_present: envelope.infra_head.is_some(),
            runtime_head_present: envelope.runtime_head.is_some(),
            operation: envelope
                .operation
                .as_ref()
                .map(|op| format!("{:?} (phase: {})", op.kind, op.phase)),
            lock_holder: envelope.lock.as_ref().map(|l| l.holder.clone()),
        }
    }

    fn print_human(&self, deployment_dir: &Path) {
        println!(
            "tkp describe — deployment at {}\n",
            deployment_dir.display()
        );

        println!("Running provisioner");
        println!("  version           {}", self.running.version);
        println!("  git_sha           {}", self.running.git_sha);
        println!(
            "  source_tree_hash  {}  (authoritative drift key)",
            self.running.source_tree_hash
        );
        println!("  build_mode        {:?}\n", self.running.build_mode);

        println!("Deployment envelope");
        let id = if self.deployment_id.is_empty() {
            "(uninitialized)"
        } else {
            &self.deployment_id
        };
        println!("  deployment_id     {id}");
        println!("  schema_version    {}", self.schema_version);
        println!("  config_revision   {}", self.config_revision);
        println!(
            "  effective_config  {}\n",
            self.effective_config_ref.as_deref().unwrap_or("(none)")
        );

        println!("Binding");
        match &self.binding.recorded {
            Some(s) => println!(
                "  recorded          {} / {} ({:?})",
                s.version, s.source_tree_hash, s.build_mode
            ),
            None => println!("  recorded          unstamped (Unknown)"),
        }
        let mark = if self.binding.proceeds {
            "proceeds"
        } else {
            "REFUSES"
        };
        let auth = if self.binding.authoritative {
            " (authoritative)"
        } else {
            ""
        };
        println!(
            "  verdict           {} — {mark}{auth}\n",
            self.binding.verdict
        );

        match &self.integrity {
            Some(i) => println!(
                "Integrity           provisioner {}, {} artifact(s)\n",
                i.provisioner_version, i.artifact_count
            ),
            None => println!("Integrity           none recorded\n"),
        }

        println!("State heads");
        println!(
            "  infra             {}",
            if self.infra_head_present {
                "present"
            } else {
                "none"
            }
        );
        println!(
            "  runtime           {}\n",
            if self.runtime_head_present {
                "present"
            } else {
                "none"
            }
        );

        println!(
            "Operation           {}",
            self.operation.as_deref().unwrap_or("none")
        );
        println!(
            "Lock                {}",
            match &self.lock_holder {
                Some(h) => format!("held by {h}"),
                None => "free".to_string(),
            }
        );
    }
}

fn verdict_label(verdict: BindingVerdict) -> &'static str {
    match verdict {
        BindingVerdict::Match => "Match",
        BindingVerdict::DevIterate => "DevIterate",
        BindingVerdict::Mismatch => "Mismatch",
        BindingVerdict::Downgrade => "Downgrade",
        BindingVerdict::ModeRegression => "ModeRegression",
        BindingVerdict::Unknown => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn describe_uninitialized_deployment_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let (envelope, _) = envelope_store(tmp.path()).load().await.unwrap();
        // No envelope written yet → default, unbound.
        assert!(envelope.binding.is_none());

        let running = ProvenanceStamp::current(Utc::now());
        let report = DescribeReport::build(&running, &envelope);
        assert_eq!(report.binding.verdict, "Unknown");
        assert!(!report.binding.proceeds, "an unstamped deployment refuses");
        assert!(report.integrity.is_none());
    }

    fn versioned_stamp(hash: &str) -> ProvenanceStamp {
        ProvenanceStamp {
            version: "1.0.0".to_string(),
            git_sha: "sha".to_string(),
            source_tree_hash: hash.to_string(),
            build_mode: BuildMode::Versioned,
            recorded_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn describe_reports_a_recorded_matching_binding() {
        let tmp = tempfile::tempdir().unwrap();
        let store = envelope_store(tmp.path());

        // Explicit Versioned stamps so the Match path is deterministic regardless
        // of how this test binary itself was built (a cargo build is Dev mode).
        let running = versioned_stamp("hashA");
        let envelope = DeploymentStateEnvelope {
            deployment_id: "dep-1".to_string(),
            binding: Some(running.clone()),
            config_revision: 3,
            ..Default::default()
        };
        let (_, version) = store.load().await.unwrap();
        store.save(&envelope, &version).await.unwrap();

        // Re-load and describe — round-trips through the envelope store.
        let (loaded, _) = store.load().await.unwrap();
        let report = DescribeReport::build(&running, &loaded);
        assert_eq!(report.deployment_id, "dep-1");
        assert_eq!(report.config_revision, 3);
        assert_eq!(report.binding.verdict, "Match");
        assert!(report.binding.proceeds && report.binding.authoritative);
    }

    #[tokio::test]
    async fn describe_reports_mode_regression_for_dev_binary_on_versioned_deployment() {
        // A versioned deployment described by a dev binary refuses (ModeRegression).
        let recorded = versioned_stamp("hashA");
        let envelope = DeploymentStateEnvelope {
            binding: Some(recorded),
            ..Default::default()
        };
        let dev_running = ProvenanceStamp {
            build_mode: BuildMode::Dev,
            ..versioned_stamp("hashA")
        };
        let report = DescribeReport::build(&dev_running, &envelope);
        assert_eq!(report.binding.verdict, "ModeRegression");
        assert!(!report.binding.proceeds);
    }
}
