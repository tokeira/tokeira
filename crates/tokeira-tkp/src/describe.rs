//! `tkp describe` — read-only report of identity, recorded provenance, binding
//! verdict, and state facts, in **two views**. Never gates: it must work precisely when the applying verbs would
//! refuse, so diagnosis works on a drifted or mismatched deployment.
//!
//! - **Operator view** (default): a tight summary answering "is this healthy and
//!   safe to act on" — no checksums, no digests.
//! - **Verification / debug view** (`--detail` human; `--json` always emits the
//!   full record): the complete auditable record — the per-artifact integrity
//!   manifest (SHA-256), the retained-revision set, the operation marker and
//!   lock holder. The `EngineIdentity` fields and source-snapshot ref join this
//!   view when they exist.

use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use tokeira_deployment::{
    BindingVerdict, BuildAuthority, BuildMode, DeploymentStateEnvelope, ProvenanceStamp,
    check_binding,
};

use tokeira_platform::definition::DefinitionFrontend;

use crate::{config_history, engine::Engine, platform::Admitted};

pub(crate) async fn describe<F: DefinitionFrontend>(
    engine: &Engine<F>,
    admitted: &Admitted,
    json: bool,
    detail: bool,
) -> Result<()> {
    let deployment_dir = admitted.deployment_ref.dir.as_path();
    let running = ProvenanceStamp::current(Utc::now());
    let (envelope, _version) = admitted.state.envelope_store().load().await?;
    let source = admitted.config_source();
    let retained = config_history::retained_revisions(deployment_dir, &source);
    let report = DescribeReport::build(
        &running,
        &envelope,
        engine.platform().id().as_str(),
        retained,
        load_bundle_view(deployment_dir),
        read_storage(deployment_dir),
        deployment_dir,
    );

    if json {
        // The JSON view is always the full verification record (stable,
        // machine-parseable).
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else if detail {
        report.print_verification(deployment_dir);
    } else {
        // The operator summary is Markdown narrative under the output
        // contract: termimad-skinned on a terminal, deterministic raw
        // Markdown everywhere else (D8).
        crate::emit_report(
            &report.summary_markdown(),
            tokeira_report::Mode::resolve(false, false),
        );
    }
    Ok(())
}

/// The deployment's storage kind as `tokeirad.toml` records it
/// (`infrastructure.storage`) — the definitive statement of what the
/// deployment runs on. Parsed tolerantly: `describe` must keep working
/// against a config written by a newer engine, so every other field is
/// ignored, and an absent or unreadable file simply omits the fact.
fn read_storage(deployment_dir: &Path) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ServerConfig {
        infrastructure: Option<Infrastructure>,
    }
    #[derive(serde::Deserialize)]
    struct Infrastructure {
        storage: Option<String>,
    }
    let raw = std::fs::read_to_string(deployment_dir.join(config_history::SERVER_CONFIG)).ok()?;
    toml::from_str::<ServerConfig>(&raw)
        .ok()?
        .infrastructure?
        .storage
}

/// Read the deployment's bundle sidecar for the verification view. `describe`
/// never gates and never fails on diagnostics inputs: an absent sidecar is
/// simply a pre-bundle deployment, and a corrupt one is reported to stderr
/// and omitted.
fn load_bundle_view(deployment_dir: &Path) -> Option<BundleView> {
    let raw =
        std::fs::read(deployment_dir.join(tokeira_deployment::BUNDLE_MANIFEST_BASENAME)).ok()?;
    match serde_json::from_slice::<tokeira_deployment::ProvisionerBundle>(&raw) {
        Ok(bundle) => Some(BundleView {
            source_tree_oid: bundle.build.source_tree_oid,
            snapshot_commit_oid: bundle.build.snapshot_commit_oid,
            build_toolchain: bundle.build.toolchain,
            builder: bundle.build.builder,
            request_id: bundle.build.request_id,
            tests_passed: bundle.tests.passed,
            test_command: bundle.tests.command,
            hermetic: bundle.identity.build_container.is_some(),
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
    ///; `None` is a pre-identity (native dev) manifest.
    engine_identity: Option<IdentityView>,
    /// Who built the bytes (the admission gate's input).
    authority: BuildAuthority,
    /// The complete per-artifact record — this is what verifies a binding by
    /// hand (verification view; the operator view shows only the count).
    artifacts: Vec<ArtifactView>,
}

/// The complete `EngineIdentity` field set — the verification
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
    profile: tokeira_deployment::BuildProfile,
}

impl IdentityView {
    fn of(identity: &tokeira_deployment::EngineIdentity) -> Self {
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

/// Build provenance from the deployment's bundle sidecar: the
/// frozen source refs, the builder, and the test evidence — where
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
    /// Whether the engine was built in a pinned container. A non-hermetic
    /// engine is the dev loop: its build runs no tests, and the tests line
    /// states the tier instead of pass/fail.
    hermetic: bool,
}

#[derive(Serialize)]
struct DescribeReport {
    platform: String,
    running: StampView,
    /// Storage kind from `tokeirad.toml` (`infrastructure.storage`) — the
    /// definitive record; `None` when the config is absent or unreadable.
    storage: Option<String>,
    /// The deployment directory — where the envelope, definition, and
    /// married provisioner live.
    state_dir: std::path::PathBuf,
    deployment_id: String,
    schema_version: u32,
    config_revision: u64,
    effective_config_ref: Option<String>,
    /// Revisions retained for this platform — the revertable set.
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
    /// Attention-only binding prose (shared with the plan reports). A
    /// rendering concern, not model content — excluded from `--json`, which
    /// already carries the verdict itself.
    #[serde(skip)]
    binding_attention: Option<&'static str>,
}

impl DescribeReport {
    fn build(
        running: &ProvenanceStamp,
        envelope: &DeploymentStateEnvelope,
        platform: &str,
        retained_revisions: Vec<u64>,
        bundle: Option<BundleView>,
        storage: Option<String>,
        deployment_dir: &Path,
    ) -> Self {
        let verdict = check_binding(envelope.binding.as_ref(), running);
        Self {
            platform: platform.to_string(),
            running: StampView::of(running),
            storage,
            state_dir: deployment_dir.to_path_buf(),
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
                    // The in-flight audit log: what B committed,
                    // visible exactly when recovery needs it.
                    Some(recorded) => format!(
                        "{:?} (phase: {}, {} recorded)",
                        op.kind,
                        op.phase,
                        tokeira_report::counted(recorded, "change")
                    ),
                    None => format!("{:?} (phase: {})", op.kind, op.phase),
                }
            }),
            lock_holder: envelope.lock.as_ref().map(|l| l.holder.clone()),
            binding_attention: crate::render::binding_attention(
                envelope.binding.is_some(),
                verdict,
            ),
        }
    }

    /// The default human view: "is this deployment healthy and safe to act
    /// on" at a glance — the facts an operator acts on, as Markdown
    /// narrative. No checksums, no digests, no revision bookkeeping (those
    /// live in `--detail`/`--json`); an operation in flight and a binding
    /// that would refuse are the only lines that join uninvited.
    fn summary_markdown(&self) -> String {
        let mut out = String::new();
        let id = if self.deployment_id.is_empty() {
            "(uninitialized)"
        } else {
            &self.deployment_id
        };
        out.push_str(&format!("# {id}\n"));
        push_fact(&mut out, "platform", &format!("`{}`", self.platform));
        let engine = match self.running.build_mode {
            // The qualifier is exceptional-only: a released build is the
            // norm and renders bare; a dev build says so, because it is why
            // the binding gate treats the deployment as a dev iterate.
            BuildMode::Dev => format!("{} (dev build)", self.running.version),
            BuildMode::Versioned => self.running.version.clone(),
        };
        push_fact(&mut out, "engine", &engine);
        if let Some(storage) = &self.storage {
            push_fact(&mut out, "storage", &storage_prose(storage));
        }
        push_fact(&mut out, "state", &home_abbreviated(&self.state_dir));
        push_fact(
            &mut out,
            "infra",
            if self.infra_head_present {
                "Provisioned"
            } else {
                "Not provisioned"
            },
        );
        let services = if self.runtime_head_present {
            format!("Applied at revision {}", self.config_revision)
        } else {
            "None — nothing applied yet".to_string()
        };
        push_fact(&mut out, "services", &services);
        let lock = match &self.lock_holder {
            Some(holder) => format!("Held by {holder}"),
            None => "Not locked".to_string(),
        };
        push_fact(&mut out, "lock", &lock);
        if let Some(operation) = &self.operation {
            push_fact(&mut out, "operation", operation);
        }
        if let Some(attention) = self.binding_attention {
            push_fact(&mut out, "binding", attention);
        }
        out
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

        // Build provenance — where the bound bytes came from:
        // the frozen source refs, the builder, the test evidence.
        if let Some(b) = &self.bundle {
            println!("Bundle provenance   request {}", b.request_id);
            println!("  source tree       {}", b.source_tree_oid);
            println!("  snapshot commit   {}", b.snapshot_commit_oid);
            println!("  toolchain         {}", b.build_toolchain);
            println!("  builder           {}", b.builder);
            // A dev engine's build runs no tests — the line states the
            // tier, never a pass/fail that did not happen.
            if b.hermetic {
                println!(
                    "  tests             {} ({})\n",
                    if b.tests_passed { "passed" } else { "FAILED" },
                    b.test_command
                );
            } else {
                println!("  tests             {}\n", b.test_command);
            }
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

/// Value column for the summary's fact lines: the longest label
/// (`services`) plus two spaces, so values align down the page.
const FACT_LABEL_WIDTH: usize = 10;

/// One `- **label**  value` fact line, value-aligned across the summary.
fn push_fact(out: &mut String, label: &str, value: &str) {
    let pad = " ".repeat(FACT_LABEL_WIDTH.saturating_sub(label.len()));
    out.push_str(&format!("- **{label}**{pad}{value}\n"));
}

/// Human prose for the recorded storage kind (`--json` keeps the raw value).
fn storage_prose(raw: &str) -> String {
    match raw {
        "in-memory" => "In-memory".to_string(),
        "dsql" => "DSQL".to_string(),
        other => other.to_string(),
    }
}

/// Abbreviate `$HOME` to `~` in the summary's state line.
fn home_abbreviated(path: &Path) -> String {
    let rendered = path.display().to_string();
    if let Ok(home) = std::env::var("HOME")
        && !home.is_empty()
        && let Some(rest) = rendered.strip_prefix(&home)
        && rest.starts_with('/')
    {
        return format!("~{rest}");
    }
    rendered
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
    use crate::envelope_store;

    #[tokio::test]
    async fn describe_uninitialized_deployment_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let (envelope, _) = envelope_store(tmp.path()).load().await.unwrap();
        // No envelope written yet → default, unbound.
        assert!(envelope.binding.is_none());

        let running = ProvenanceStamp::current(Utc::now());
        let report = DescribeReport::build(
            &running,
            &envelope,
            "test",
            Vec::new(),
            None,
            None,
            tmp.path(),
        );
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
        let report = DescribeReport::build(
            &running,
            &loaded,
            "test",
            vec![1, 3],
            None,
            None,
            tmp.path(),
        );
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
        let report = DescribeReport::build(
            &dev_running,
            &envelope,
            "test",
            Vec::new(),
            None,
            None,
            Path::new("dep"),
        );
        assert_eq!(report.binding.verdict, "ModeRegression");
        assert!(!report.binding.proceeds);
    }

    // The verification record carries the complete per-artifact manifest —
    // the by-hand binding-verification surface of the two-view split.
    #[test]
    fn verification_record_carries_the_full_artifact_manifest() {
        use tokeira_deployment::{BinaryArtifactDescriptor, IntegrityManifest, Target};

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
            None,
            Path::new("dep"),
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
        use tokeira_deployment::{
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
            authority: tokeira_deployment::BuildAuthority::LocalDeveloper,
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
                .join(tokeira_deployment::BUNDLE_MANIFEST_BASENAME),
            serde_json::to_vec(&bundle).unwrap(),
        )
        .unwrap();

        let report = DescribeReport::build(
            &versioned_stamp("hashA"),
            &envelope,
            "test",
            Vec::new(),
            load_bundle_view(tmp.path()),
            None,
            tmp.path(),
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
                .join(tokeira_deployment::BUNDLE_MANIFEST_BASENAME),
            b"not json",
        )
        .unwrap();
        assert!(load_bundle_view(tmp.path()).is_none());
    }

    // The operator summary: the agreed fact lines, value-aligned, in prose —
    // and silent on binding/operation when neither carries attention.
    #[test]
    fn operator_summary_states_the_facts() {
        let envelope = DeploymentStateEnvelope {
            deployment_id: "dev-compose-tkd".to_string(),
            binding: Some(ProvenanceStamp {
                build_mode: BuildMode::Dev,
                ..versioned_stamp("hashA")
            }),
            ..Default::default()
        };
        let dev_running = ProvenanceStamp {
            version: "0.1.0".to_string(),
            build_mode: BuildMode::Dev,
            ..versioned_stamp("hashA")
        };
        let report = DescribeReport::build(
            &dev_running,
            &envelope,
            "compose",
            Vec::new(),
            None,
            Some("in-memory".to_string()),
            Path::new("/deployments/dev-compose-tkd"),
        );
        let summary = report.summary_markdown();
        assert!(summary.starts_with("# dev-compose-tkd\n"), "{summary}");
        assert!(summary.contains("- **platform**  `compose`\n"), "{summary}");
        assert!(
            summary.contains("- **engine**    0.1.0 (dev build)\n"),
            "{summary}"
        );
        assert!(summary.contains("- **storage**   In-memory\n"), "{summary}");
        assert!(
            summary.contains("- **state**     /deployments/dev-compose-tkd\n"),
            "{summary}"
        );
        assert!(
            summary.contains("- **infra**     Not provisioned\n"),
            "{summary}"
        );
        assert!(
            summary.contains("- **services**  None — nothing applied yet\n"),
            "{summary}"
        );
        assert!(
            summary.contains("- **lock**      Not locked\n"),
            "{summary}"
        );
        // A dev binding on a dev deployment proceeds — no uninvited lines.
        assert!(!summary.contains("**binding**"), "{summary}");
        assert!(!summary.contains("**operation**"), "{summary}");
    }

    // Attention lines join uninvited exactly when they should: an incomplete
    // (unstamped) deployment speaks; a versioned engine renders bare; an
    // unrecorded storage line is absent, never guessed.
    #[test]
    fn operator_summary_attention_lines_speak_when_needed() {
        let report = DescribeReport::build(
            &versioned_stamp("hashA"),
            &DeploymentStateEnvelope::default(),
            "compose",
            Vec::new(),
            None,
            None,
            Path::new("/d"),
        );
        let summary = report.summary_markdown();
        assert!(summary.starts_with("# (uninitialized)\n"), "{summary}");
        assert!(summary.contains("- **engine**    1.0.0\n"), "{summary}");
        assert!(!summary.contains("(dev build)"), "{summary}");
        assert!(
            summary.contains("- **binding**   creation record missing"),
            "{summary}"
        );
        assert!(!summary.contains("**storage**"), "{summary}");
    }
}
