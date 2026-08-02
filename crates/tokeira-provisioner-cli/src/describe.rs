//! `tkp describe` — read-only report of identity, recorded provenance, binding
//! verdict, and state facts, in **two views** (design §Command behaviour and
//! outputs). Never gates: it must work precisely when the applying verbs would
//! refuse, so diagnosis works on a drifted or mismatched deployment.
//!
//! - **Operator view** (default): a tight summary answering "is this healthy and
//!   safe to act on" — no checksums, no digests.
//! - **Verification / debug view** (`--detail` human; `--json` always emits the
//!   full record): the complete auditable record — the per-artifact integrity
//!   manifest (SHA-256), the retained-revision set, the operation marker and
//!   lock holder. The `EngineIdentity` fields and source-snapshot ref join this
//!   view when they exist (tasks 16/17; tracked by 19.1).

use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use tokeira_provisioner::{
    BindingVerdict, BuildAuthority, BuildMode, DeploymentStateEnvelope, ProvenanceStamp,
    check_binding,
};

use crate::{ProvisionerPlatform, config_history, envelope_store};

pub(crate) async fn describe<P: ProvisionerPlatform>(
    platform: &P,
    deployment_dir: &Path,
    json: bool,
    detail: bool,
) -> Result<()> {
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _version) = envelope_store(deployment_dir).load().await?;
    let retained = config_history::retained_revisions(
        deployment_dir,
        platform.config_basename(deployment_dir),
    );
    let report = DescribeReport::build(
        &running,
        &envelope,
        platform.label(deployment_dir),
        retained,
        load_bundle_view(deployment_dir),
    );

    if json {
        // The JSON view is always the full verification record (stable,
        // machine-parseable).
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if detail {
        report.print_verification(deployment_dir);
    } else {
        report.print_operator();
    }
    Ok(())
}

/// Read the deployment's bundle sidecar for the verification view. `describe`
/// never gates and never fails on diagnostics inputs: an absent sidecar is
/// simply a pre-bundle deployment, and a corrupt one is reported to stderr
/// and omitted.
fn load_bundle_view(deployment_dir: &Path) -> Option<BundleView> {
    let raw =
        std::fs::read(deployment_dir.join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME)).ok()?;
    match serde_json::from_slice::<tokeira_provisioner::ProvisionerBundle>(&raw) {
        Ok(bundle) => Some(BundleView {
            source_tree_oid: bundle.build.source_tree_oid,
            snapshot_commit_oid: bundle.build.snapshot_commit_oid,
            build_toolchain: bundle.build.toolchain,
            builder: bundle.build.builder,
            request_id: bundle.build.request_id,
            tests_passed: bundle.tests.passed,
            test_command: bundle.tests.command,
        }),
        Err(e) => {
            eprintln!("warning: bundle sidecar unreadable ({e}) — omitting build provenance");
            None
        }
    }
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
struct ArtifactView {
    target: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Serialize)]
struct IntegrityView {
    provisioner_version: String,
    /// The full closure-scoped identity the artifacts are keyed by
    /// (tasks 16.2/19.1); `None` is a pre-identity (native dev) manifest.
    engine_identity: Option<IdentityView>,
    /// Who built the bytes (the admission gate's input).
    authority: BuildAuthority,
    /// The complete per-artifact record — this is what verifies a binding by
    /// hand (verification view; the operator view shows only the count).
    artifacts: Vec<ArtifactView>,
}

/// The complete `EngineIdentity` field set (task 19.1) — the verification
/// view's answer to "what exactly is this engine keyed on?".
#[derive(Serialize)]
struct IdentityView {
    digest: String,
    source_closure: String,
    lock_closure: String,
    toolchain: String,
    /// `None` = a native (non-hermetic) build.
    build_container: Option<String>,
    features: Vec<String>,
    profile: tokeira_provisioner::BuildProfile,
}

impl IdentityView {
    fn of(identity: &tokeira_provisioner::EngineIdentity) -> Self {
        Self {
            digest: identity.digest().to_hex(),
            source_closure: identity.source_closure.to_hex(),
            lock_closure: identity.lock_closure.to_hex(),
            toolchain: identity.toolchain.clone(),
            build_container: identity.build_container.map(|d| d.to_hex()),
            features: identity.features.iter().cloned().collect(),
            profile: identity.profile,
        }
    }
}

/// Build provenance from the deployment's bundle sidecar (task 19.1): the
/// frozen source refs (task 17), the builder, and the test evidence — where
/// the bound bytes came from. Absent for pre-bundle deployments.
#[derive(Serialize)]
struct BundleView {
    source_tree_oid: String,
    snapshot_commit_oid: String,
    build_toolchain: String,
    builder: String,
    request_id: String,
    tests_passed: bool,
    test_command: String,
}

#[derive(Serialize)]
struct DescribeReport {
    platform: &'static str,
    running: StampView,
    deployment_id: String,
    schema_version: u32,
    config_revision: u64,
    effective_config_ref: Option<String>,
    /// Revisions retained for this platform — the revertable set (task 14.3).
    retained_revisions: Vec<u64>,
    binding: BindingView,
    integrity: Option<IntegrityView>,
    /// Build provenance from the bundle sidecar, when the deployment carries
    /// one (never gates; a corrupt sidecar is reported and omitted).
    bundle: Option<BundleView>,
    infra_head_present: bool,
    runtime_head_present: bool,
    operation: Option<String>,
    lock_holder: Option<String>,
}

impl DescribeReport {
    fn build(
        running: &ProvenanceStamp,
        envelope: &DeploymentStateEnvelope,
        platform: &'static str,
        retained_revisions: Vec<u64>,
        bundle: Option<BundleView>,
    ) -> Self {
        let verdict = check_binding(envelope.binding.as_ref(), running);
        Self {
            platform,
            running: StampView::of(running),
            deployment_id: envelope.deployment_id.clone(),
            schema_version: envelope.schema_version,
            config_revision: envelope.config_revision,
            effective_config_ref: envelope.effective_config_ref.clone(),
            retained_revisions,
            binding: BindingView {
                recorded: envelope.binding.as_ref().map(StampView::of),
                verdict: verdict_label(verdict),
                proceeds: verdict.proceeds(),
                authoritative: verdict.is_authoritative(),
            },
            integrity: envelope.integrity.as_ref().map(|m| IntegrityView {
                provisioner_version: m.provisioner_version.clone(),
                engine_identity: m.engine_identity.as_ref().map(IdentityView::of),
                authority: m.authority.clone(),
                artifacts: m
                    .artifacts
                    .iter()
                    .map(|a| ArtifactView {
                        target: a.target.0.clone(),
                        sha256: a.sha256.clone(),
                        size_bytes: a.size_bytes,
                    })
                    .collect(),
            }),
            bundle,
            infra_head_present: envelope.infra_head.is_some(),
            runtime_head_present: envelope.runtime_head.is_some(),
            operation: envelope.operation.as_ref().map(|op| {
                match op.audit_log.as_ref().map(|log| log.entries.len()) {
                    // The in-flight audit log (task 19.2): what B committed,
                    // visible exactly when recovery needs it.
                    Some(recorded) => format!(
                        "{:?} (phase: {}, {recorded} change(s) recorded)",
                        op.kind, op.phase
                    ),
                    None => format!("{:?} (phase: {})", op.kind, op.phase),
                }
            }),
            lock_holder: envelope.lock.as_ref().map(|l| l.holder.clone()),
        }
    }

    /// The default human view: "is this deployment healthy and safe to act on"
    /// at a glance — short identity, binding status, revision, operation, lock.
    /// No checksums, no digests.
    fn print_operator(&self) {
        let id = if self.deployment_id.is_empty() {
            "(uninitialized)"
        } else {
            &self.deployment_id
        };
        println!("deployment  {id}  [{}]", self.platform);
        println!(
            "engine      {} ({:?})  {}",
            self.running.version,
            self.running.build_mode,
            short_hash(&self.running.source_tree_hash)
        );

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
        println!("binding     {} — {mark}{auth}", self.binding.verdict);

        println!(
            "config      revision {} ({})",
            self.config_revision,
            self.effective_config_ref
                .as_deref()
                .map(short_ref)
                .unwrap_or_else(|| "none applied".to_string())
        );
        println!(
            "operation   {}",
            self.operation.as_deref().unwrap_or("none")
        );
        println!(
            "lock        {}",
            match &self.lock_holder {
                Some(h) => format!("held by {h}"),
                None => "free".to_string(),
            }
        );
        println!("\n(`--detail` for the verification view: full manifest, revisions, heads)");
    }

    /// The verification / debug view: the full auditable record, for verifying
    /// a binding by hand and debugging a refusal.
    fn print_verification(&self, deployment_dir: &Path) {
        println!(
            "tkp describe — deployment at {}\n",
            deployment_dir.display()
        );

        println!("Running provisioner   [{}]", self.platform);
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
            "  effective_config  {}",
            self.effective_config_ref.as_deref().unwrap_or("(none)")
        );
        println!(
            "  retained          {}\n",
            if self.retained_revisions.is_empty() {
                "(no revisions retained)".to_string()
            } else {
                self.retained_revisions
                    .iter()
                    .map(|r| r.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
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
            Some(i) => {
                println!("Integrity           provisioner {}", i.provisioner_version);
                let authority = match &i.authority {
                    BuildAuthority::LocalDeveloper => "local developer".to_string(),
                    BuildAuthority::TrustedCi {
                        provider, build_id, ..
                    } => format!("trusted CI ({provider} build {build_id})"),
                };
                println!("  authority         {authority}");
                match &i.engine_identity {
                    Some(id) => {
                        println!("  engine identity   {}", id.digest);
                        println!("    source closure  {}", id.source_closure);
                        println!("    lock closure    {}", id.lock_closure);
                        println!("    toolchain       {}", id.toolchain);
                        println!(
                            "    container       {}",
                            id.build_container
                                .as_deref()
                                .unwrap_or("native (non-hermetic)")
                        );
                        println!(
                            "    features        {} · profile {:?}",
                            if id.features.is_empty() {
                                "(none)".to_string()
                            } else {
                                id.features.join(", ")
                            },
                            id.profile
                        );
                    }
                    None => println!("  engine identity   none (pre-identity build)"),
                }
                for artifact in &i.artifacts {
                    println!(
                        "  {}  {} bytes\n    sha256:{}",
                        artifact.target, artifact.size_bytes, artifact.sha256
                    );
                }
                println!();
            }
            None => println!("Integrity           none recorded\n"),
        }

        // Build provenance (task 19.1) — where the bound bytes came from:
        // the frozen source refs, the builder, the test evidence.
        if let Some(b) = &self.bundle {
            println!("Bundle provenance   request {}", b.request_id);
            println!("  source tree       {}", b.source_tree_oid);
            println!("  snapshot commit   {}", b.snapshot_commit_oid);
            println!("  toolchain         {}", b.build_toolchain);
            println!("  builder           {}", b.builder);
            println!(
                "  tests             {} ({})\n",
                if b.tests_passed { "passed" } else { "FAILED" },
                b.test_command
            );
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

/// A short, glanceable prefix of a digest for the operator view.
fn short_hash(hash: &str) -> String {
    if hash.len() > 12 {
        format!("{}…", &hash[..12])
    } else {
        hash.to_string()
    }
}

/// A short form of an effective-config ref (`sha256:abcd…`).
fn short_ref(config_ref: &str) -> String {
    match config_ref.split_once(':') {
        Some((scheme, digest)) => format!("{scheme}:{}", short_hash(digest)),
        None => config_ref.to_string(),
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
        let report = DescribeReport::build(&running, &envelope, "test", Vec::new(), None);
        assert_eq!(report.binding.verdict, "Unknown");
        assert!(!report.binding.proceeds, "an unstamped deployment refuses");
        assert!(report.integrity.is_none());
        assert_eq!(report.platform, "test");
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
        let report = DescribeReport::build(&running, &loaded, "test", vec![1, 3], None);
        assert_eq!(report.deployment_id, "dep-1");
        assert_eq!(report.config_revision, 3);
        assert_eq!(report.binding.verdict, "Match");
        assert!(report.binding.proceeds && report.binding.authoritative);
        assert_eq!(report.retained_revisions, vec![1, 3]);
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
        let report = DescribeReport::build(&dev_running, &envelope, "test", Vec::new(), None);
        assert_eq!(report.binding.verdict, "ModeRegression");
        assert!(!report.binding.proceeds);
    }

    // The verification record carries the complete per-artifact manifest —
    // the by-hand binding-verification surface (19.1's two-view split).
    #[test]
    fn verification_record_carries_the_full_artifact_manifest() {
        use tokeira_provisioner::{BinaryArtifactDescriptor, IntegrityManifest, Target};

        let envelope = DeploymentStateEnvelope {
            binding: Some(versioned_stamp("hashA")),
            integrity: Some(IntegrityManifest {
                provisioner_version: "1.0.0".to_string(),
                artifacts: vec![BinaryArtifactDescriptor {
                    target: Target("aarch64-apple-darwin".to_string()),
                    sha256: "abc123".to_string(),
                    retrieval_ref: None,
                    size_bytes: 42,
                }],
                ..Default::default()
            }),
            ..Default::default()
        };
        let report = DescribeReport::build(
            &versioned_stamp("hashA"),
            &envelope,
            "test",
            Vec::new(),
            None,
        );
        let integrity = report.integrity.expect("manifest present");
        assert_eq!(integrity.artifacts.len(), 1);
        assert_eq!(integrity.artifacts[0].sha256, "abc123");
        assert_eq!(integrity.artifacts[0].target, "aarch64-apple-darwin");
        assert_eq!(integrity.artifacts[0].size_bytes, 42);
    }

    // Task 19.1: the verification record carries the FULL identity field set
    // and the bundle sidecar's build provenance.
    #[test]
    fn verification_record_carries_identity_fields_and_bundle_provenance() {
        use tokeira_provisioner::{
            BuildProfile, EngineIdentity, IntegrityManifest, ProvisionerBundle, Sha256Digest,
            bundle::{BuildManifest, TestEvidence},
        };

        let identity = EngineIdentity {
            source_closure: Sha256Digest::from_bytes(b"src"),
            lock_closure: Sha256Digest::from_bytes(b"lock"),
            toolchain: "1.95".into(),
            build_container: Some(Sha256Digest::from_bytes(b"img")),
            features: ["provisioner".to_string()].into(),
            profile: BuildProfile::Dist,
        };
        let envelope = DeploymentStateEnvelope {
            binding: Some(versioned_stamp("hashA")),
            integrity: Some(IntegrityManifest {
                engine_identity: Some(identity.clone()),
                provisioner_version: "1.0.0".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        // A sidecar on disk feeds the bundle-provenance view.
        let tmp = tempfile::tempdir().unwrap();
        let bundle = ProvisionerBundle {
            identity: identity.clone(),
            bound: None,
            authority: tokeira_provisioner::BuildAuthority::LocalDeveloper,
            provisioner_version: "1.0.0".into(),
            artifacts: vec![],
            tests: TestEvidence {
                command: "cargo test --locked".into(),
                passed: true,
            },
            build: BuildManifest {
                request_id: "req-9".into(),
                source_tree_oid: "tree-abc".into(),
                snapshot_commit_oid: "commit-def".into(),
                toolchain: "1.95".into(),
                builder: "dagger-local".into(),
            },
        };
        std::fs::write(
            tmp.path()
                .join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME),
            serde_json::to_vec(&bundle).unwrap(),
        )
        .unwrap();

        let report = DescribeReport::build(
            &versioned_stamp("hashA"),
            &envelope,
            "test",
            Vec::new(),
            load_bundle_view(tmp.path()),
        );

        let id_view = report
            .integrity
            .expect("manifest present")
            .engine_identity
            .expect("identity present");
        assert_eq!(id_view.digest, identity.digest().to_hex());
        assert_eq!(id_view.source_closure, identity.source_closure.to_hex());
        assert_eq!(id_view.toolchain, "1.95");
        assert_eq!(id_view.features, vec!["provisioner"]);

        let bundle_view = report.bundle.expect("sidecar surfaced");
        assert_eq!(bundle_view.source_tree_oid, "tree-abc");
        assert_eq!(bundle_view.snapshot_commit_oid, "commit-def");
        assert!(bundle_view.tests_passed);

        // A corrupt sidecar is omitted, never fatal — describe must work
        // precisely when things are broken.
        std::fs::write(
            tmp.path()
                .join(tokeira_provisioner::BUNDLE_MANIFEST_BASENAME),
            b"not json",
        )
        .unwrap();
        assert!(load_bundle_view(tmp.path()).is_none());
    }

    #[test]
    fn short_forms_truncate_for_the_operator_view() {
        assert_eq!(short_hash("abcdef0123456789"), "abcdef012345…");
        assert_eq!(short_hash("short"), "short");
        assert_eq!(short_ref("sha256:abcdef0123456789"), "sha256:abcdef012345…");
        assert_eq!(short_ref("default"), "default");
    }
}
