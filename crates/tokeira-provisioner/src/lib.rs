//! Provisioner (`tkp`) data models.
//!
//! The deployment-level authority for a provisioned deployment is the
//! [`DeploymentStateEnvelope`]: it binds provenance, integrity, the config
//! revision, the rollback checkpoint, the in-flight operation marker, the
//! operation lock, and the infra/runtime state heads under one revision. These
//! are pure serde data models — the logic that *populates* them (provenance
//! stamping, integrity verification, upgrade/rollback orchestration) lives in
//! the provisioner binary and later tasks; this crate is the shared vocabulary
//! those tasks read and write.
//!
//! Rollback is **definition-driven** (Proposal 002): the envelope carries no
//! before-images. `checkpoint.from_config_ref` (the retained prior configuration
//! revision) is the rollback baseline, and `operation.audit_log` — if present —
//! is an ids-only change log for observability, never a rollback mechanism.
//!
//! The envelope references [`tokeira_state::SnapshotRef`] for its state heads and
//! checkpoint, and implements [`tokeira_state::Validate`] + [`Default`] so it can
//! ride on a [`tokeira_state::S3StateStore`] as the deployment's authoritative
//! document.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokeira_state::{SnapshotRef, StateError, Validate};

pub mod admission;
pub mod binary_store;
pub mod binding;
pub mod identity;
pub mod integrity;
pub mod migration;
pub mod upgrade;
mod version;
pub use admission::{AdmissionError, RevocationList, admit_artifact};
pub use binary_store::{BinaryError, BinaryStore};
pub use binding::{BindingVerdict, check_binding};
pub use identity::{AuthorityTier, BuildAuthority, BuildProfile, EngineIdentity};
pub use integrity::{ChecksumFormatError, IntegrityError, Sha256Digest, sha256_hex};
pub use migration::{MigrationError, MigrationRegistry, envelope_migrations};
pub use upgrade::{UpgradeDecision, evaluate_upgrade};

/// Current `DeploymentStateEnvelope` schema version.
///
/// v2 (task 16.2): the integrity manifest is keyed by [`EngineIdentity`] —
/// it gains `engine_identity` + `authority`, and per-artifact `version` is
/// gone. v1 documents deserialize compatibly (the new fields default; the
/// stale key is ignored); the canonical registry
/// ([`migration::envelope_migrations`]) bridges 1 → 2 at the upgrade boundary.
pub const ENVELOPE_SCHEMA_VERSION: u32 = 2;

/// How a provisioner binary was built. Only [`Versioned`](Self::Versioned) is
/// authoritative; a `Dev` build is an advisory local iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BuildMode {
    /// A released, authoritative build.
    Versioned,
    /// A local development build. Never authoritative.
    #[default]
    Dev,
}

impl BuildMode {
    /// Parse the `tokeira-build-info` `BUILD_MODE` string (`"versioned"` /
    /// `"dev"`). Anything unrecognized is the advisory [`Dev`](Self::Dev) — the
    /// safe, non-authoritative default.
    pub fn from_build_info(mode: &str) -> Self {
        match mode {
            "versioned" => BuildMode::Versioned,
            _ => BuildMode::Dev,
        }
    }
}

/// A Rust target triple (`aarch64-unknown-linux-musl`, `aarch64-apple-darwin`, …).
///
/// `os`/`arch` alone is not precise enough for an executable artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Target(pub String);

/// Recorded identity of the binary bound to a deployment.
///
/// A **missing** stamp is an explicit `Unknown` (represented as `None` wherever a
/// `ProvenanceStamp` is optional) — never coerced to a concrete version. The
/// authoritative drift key is [`source_tree_hash`](Self::source_tree_hash), a
/// whole-workspace digest, not the semver (which a developer can forget to bump).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceStamp {
    pub version: String,
    pub git_sha: String,
    pub source_tree_hash: String,
    pub build_mode: BuildMode,
    pub recorded_at: DateTime<Utc>,
}

impl ProvenanceStamp {
    /// Stamp the running provisioner from `tokeira-build-info` (task 2.1).
    ///
    /// `source_tree_hash` is the **authoritative drift key** — a whole-workspace
    /// digest a developer cannot forget to bump (unlike the semver). `recorded_at`
    /// is supplied by the caller so the stamp is deterministic in tests.
    pub fn current(recorded_at: DateTime<Utc>) -> Self {
        Self {
            version: tokeira_build_info::TOKEIRA_VERSION.to_string(),
            git_sha: tokeira_build_info::TOKEIRA_GIT_SHA.to_string(),
            source_tree_hash: tokeira_build_info::SOURCE_TREE_HASH.to_string(),
            build_mode: BuildMode::from_build_info(tokeira_build_info::BUILD_MODE),
            recorded_at,
        }
    }
}

/// One binary artifact for one target, addressed by content hash. Its key half
/// is the enclosing manifest's [`engine_identity`](IntegrityManifest::engine_identity)
/// — the artifact carries no version of its own (task 16.2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryArtifactDescriptor {
    pub target: Target,
    pub sha256: String,
    /// Optional retrieval pointer (e.g. an S3 key) when the blob is stored.
    pub retrieval_ref: Option<String>,
    pub size_bytes: u64,
}

/// The set of binary artifacts one engine publishes, across targets — keyed by
/// `EngineIdentity × target` (task 16.2), with the semver kept as a
/// human-facing label only.
///
/// CAS-guarded in the envelope; cannot be silently rewritten. Carries **all**
/// targets because a rollback may run from a different operator platform than the
/// one that performed the upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IntegrityManifest {
    /// The closure-scoped engine identity of these artifacts (task 16.1).
    /// `None` is a pre-identity manifest — a native dev build, whose closure
    /// inputs exist only once the source snapshot (task 17) supplies them.
    /// There are no partially-known identities.
    #[serde(default)]
    pub engine_identity: Option<EngineIdentity>,
    /// Who built the bytes. Defaults to the lowest tier — trust is recorded
    /// explicitly by the build pipeline, never assumed (Proposal 005).
    #[serde(default)]
    pub authority: BuildAuthority,
    pub provisioner_version: String,
    pub artifacts: Vec<BinaryArtifactDescriptor>,
}

/// The kind of change an apply committed. The audit log records the id and op
/// only — **no before-images** (Proposal 002).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOp {
    Created,
    Updated,
    Deleted,
}

/// One entry in the ids-only audit change log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeLogEntry {
    pub id: String,
    pub op: ChangeOp,
}

/// Optional ids-only record of what an apply committed, for observability and
/// richer `plan`/`describe` output. It is **never** the rollback mechanism —
/// rollback is definition-driven (Proposal 002).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChangeLog {
    pub entries: Vec<ChangeLogEntry>,
}

/// The retained clone of **[A final]**, captured atomically at the start of
/// `upgrade`. Its [`from_config_ref`](Self::from_config_ref) — the prior
/// configuration revision — is the **load-bearing** rollback baseline (A
/// re-applies its own retained revision; Proposal 002). The `from_*_head`
/// snapshots pin [A final]'s infra and runtime state, spanning both engines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackCheckpoint {
    pub from_provenance: ProvenanceStamp,
    pub from_integrity: IntegrityManifest,
    /// [A final] infrastructure state head.
    pub from_infra_head: Option<SnapshotRef>,
    /// [A final] runtime/service state head (rollback spans both engines).
    pub from_runtime_head: Option<SnapshotRef>,
    /// A's prior configuration-revision ref — the rollback baseline A re-applies.
    pub from_config_ref: Option<String>,
    pub recorded_at: DateTime<Utc>,
}

/// Which operation is in flight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    UpgradeInFlight,
    RollbackInFlight,
}

/// The in-flight operation marker. While present it gates the deployment to
/// `resume`/`rollback`/`describe`; it records a resumable [`phase`](Self::phase)
/// so an interrupted upgrade or rollback re-enters rather than leaving a
/// half-applied deployment. `None` in steady state. Carries only an optional
/// ids-only [`audit_log`](Self::audit_log) — never before-images (Proposal 002).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Operation {
    pub operation_id: String,
    pub kind: OperationKind,
    /// Resumable step marker; every step is idempotent.
    pub phase: String,
    pub audit_log: Option<ChangeLog>,
}

/// The remote mutual-exclusion lease guarding mutating operations (distinct from
/// the operator `lock.toml`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationLock {
    pub holder: String,
    pub acquired_at: DateTime<Utc>,
    pub renewed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    /// The operation this lock is held for, if any.
    pub operation_id: Option<String>,
}

/// The single deployment-level authority, bound under one revision.
///
/// - `binding` is the engine identity (changes only on `upgrade`); `None` is an
///   explicit `Unknown` (unstamped), never coerced.
/// - `config_revision` advances on every ordinary `apply`.
/// - `infra_head` / `runtime_head` are the current infra and runtime state
///   snapshot pointers.
/// - `checkpoint` holds [A final] while an upgrade may still be rolled back.
/// - `operation` marks an in-flight upgrade/rollback; `lock` is the remote lease.
///
/// Implements [`Default`] and [`Validate`] so it can be stored via
/// [`tokeira_state::S3StateStore`] as the deployment's authoritative document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentStateEnvelope {
    pub schema_version: u32,
    pub deployment_id: String,
    /// The bound engine identity, or `None` for an explicit `Unknown`.
    pub binding: Option<ProvenanceStamp>,
    pub integrity: Option<IntegrityManifest>,
    pub config_revision: u64,
    pub checkpoint: Option<RollbackCheckpoint>,
    pub operation: Option<Operation>,
    pub lock: Option<OperationLock>,
    pub infra_head: Option<SnapshotRef>,
    pub runtime_head: Option<SnapshotRef>,
    pub effective_config_ref: Option<String>,
}

impl Default for DeploymentStateEnvelope {
    fn default() -> Self {
        Self {
            schema_version: ENVELOPE_SCHEMA_VERSION,
            deployment_id: String::new(),
            binding: None,
            integrity: None,
            config_revision: 0,
            checkpoint: None,
            operation: None,
            lock: None,
            infra_head: None,
            runtime_head: None,
            effective_config_ref: None,
        }
    }
}

impl Validate for DeploymentStateEnvelope {
    fn validate(&self) -> Result<(), StateError> {
        if self.schema_version == 0 {
            return Err(StateError::Corrupted(
                "deployment state envelope has schema_version 0".into(),
            ));
        }
        if self.schema_version > ENVELOPE_SCHEMA_VERSION {
            return Err(StateError::Corrupted(format!(
                "deployment state envelope schema_version {} is newer than supported {}",
                self.schema_version, ENVELOPE_SCHEMA_VERSION
            )));
        }
        // A held operation lock must name a holder.
        if let Some(lock) = &self.lock
            && lock.holder.is_empty()
        {
            return Err(StateError::Corrupted(
                "operation lock has an empty holder".into(),
            ));
        }
        Ok(())
    }
}

impl DeploymentStateEnvelope {
    /// The atomic ownership transfer that opens an `upgrade` (task 5.3): capture
    /// the current state as the **[A final]** [`RollbackCheckpoint`], flip the
    /// binding to `to` (`B`), and open the `UpgradeInFlight` operation marker. The
    /// caller persists the mutated envelope in **one CAS commit before any
    /// provider mutation**, so a crash always recovers as `B` with an open marker
    /// (never an ambiguous "pending" state).
    pub fn begin_upgrade(
        &mut self,
        to: ProvenanceStamp,
        operation_id: impl Into<String>,
        recorded_at: DateTime<Utc>,
    ) {
        // Capture [A final] before flipping the binding. `from` is A's recorded
        // stamp (the caller guarantees the deployment is stamped before upgrade).
        let from = self.binding.clone().unwrap_or_else(|| to.clone());
        self.checkpoint = Some(RollbackCheckpoint {
            from_provenance: from,
            from_integrity: self.integrity.clone().unwrap_or_default(),
            from_infra_head: self.infra_head.clone(),
            from_runtime_head: self.runtime_head.clone(),
            from_config_ref: self.effective_config_ref.clone(),
            recorded_at,
        });
        self.binding = Some(to);
        self.operation = Some(Operation {
            operation_id: operation_id.into(),
            kind: OperationKind::UpgradeInFlight,
            phase: "ownership-transferred".to_string(),
            audit_log: None,
        });
    }

    /// Begin a **definition-driven rollback** (Proposal 002, task 8.5): re-pin the
    /// binding to the checkpoint's recorded engine `A`, restore `A`'s integrity
    /// manifest, state heads, and configuration-revision ref from the checkpoint,
    /// and open the `RollbackInFlight` marker. `A` then forward-reconciles toward
    /// its retained prior configuration revision (`effective_config_ref`). Returns
    /// `Err` when there is no checkpoint to roll back to.
    ///
    /// The integrity manifest travels with the binding: it must always describe
    /// the engine the binding names (the launcher's bound-class verification
    /// checks the installed binary against it), and `upgrade` re-records it for
    /// `B` at the ownership transfer — so the re-pin to `A` restores `A`'s.
    ///
    /// The superseded binary `B` deletes what it created *before* this re-pin (a
    /// delete-only pass over `keys(S_B) − keys(S_A)`); this method performs the
    /// re-pin the caller persists in one CAS commit.
    pub fn begin_rollback(
        &mut self,
        operation_id: impl Into<String>,
        _recorded_at: DateTime<Utc>,
    ) -> Result<(), String> {
        let checkpoint = self
            .checkpoint
            .clone()
            .ok_or_else(|| "no rollback checkpoint — nothing to roll back to".to_string())?;
        self.binding = Some(checkpoint.from_provenance);
        self.integrity = Some(checkpoint.from_integrity);
        self.infra_head = checkpoint.from_infra_head;
        self.runtime_head = checkpoint.from_runtime_head;
        self.effective_config_ref = checkpoint.from_config_ref;
        self.operation = Some(Operation {
            operation_id: operation_id.into(),
            kind: OperationKind::RollbackInFlight,
            phase: "re-pinned-to-A".to_string(),
            audit_log: None,
        });
        Ok(())
    }

    /// Complete a rollback: clear the in-flight marker **and** consume the
    /// checkpoint (the [A final] state it pinned is now the live state).
    pub fn complete_rollback(&mut self) {
        self.operation = None;
        self.checkpoint = None;
    }

    /// Close the in-flight operation marker (the upgrade or rollback completed).
    pub fn close_operation(&mut self) {
        self.operation = None;
    }

    /// Stamp the current schema version before a mutating save.
    ///
    /// Serde reads older envelope shapes compatibly, but re-serializing always
    /// emits the **current** shape — so the claimed `schema_version` must
    /// follow the bytes or the document lies to older readers. Every mutating
    /// verb calls this before its save: a dev deployment thereby advances
    /// shape freely inside the `DevIterate` loop, while a versioned deployment
    /// only reaches a mutating save through `upgrade`, where the migration
    /// registry gates the schema transition first.
    pub fn stamp_current_schema(&mut self) {
        self.schema_version = ENVELOPE_SCHEMA_VERSION;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_mode_parses_build_info_strings() {
        assert_eq!(
            BuildMode::from_build_info("versioned"),
            BuildMode::Versioned
        );
        assert_eq!(BuildMode::from_build_info("dev"), BuildMode::Dev);
        // Anything unrecognized is the safe, non-authoritative Dev default.
        assert_eq!(BuildMode::from_build_info("garbage"), BuildMode::Dev);
    }

    #[test]
    fn current_stamp_reads_build_info() {
        let now = "2026-07-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let stamp = ProvenanceStamp::current(now);
        assert_eq!(stamp.version, tokeira_build_info::TOKEIRA_VERSION);
        assert_eq!(stamp.git_sha, tokeira_build_info::TOKEIRA_GIT_SHA);
        assert_eq!(stamp.source_tree_hash, tokeira_build_info::SOURCE_TREE_HASH);
        assert_eq!(
            stamp.build_mode,
            BuildMode::from_build_info(tokeira_build_info::BUILD_MODE)
        );
        assert_eq!(stamp.recorded_at, now);
    }

    #[test]
    fn default_envelope_is_valid_and_unbound() {
        let env = DeploymentStateEnvelope::default();
        assert_eq!(env.schema_version, ENVELOPE_SCHEMA_VERSION);
        assert!(
            env.binding.is_none(),
            "an unstamped envelope is Unknown, not coerced"
        );
        assert!(env.operation.is_none());
        assert_eq!(env.config_revision, 0);
        env.validate().expect("default envelope validates");
    }

    #[test]
    fn envelope_round_trips_through_serde() {
        let env = DeploymentStateEnvelope {
            deployment_id: "dep-1".into(),
            binding: Some(ProvenanceStamp {
                version: "1.2.3".into(),
                git_sha: "abc123".into(),
                source_tree_hash: "deadbeef".into(),
                build_mode: BuildMode::Versioned,
                recorded_at: DateTime::parse_from_rfc3339("2026-07-01T00:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
            }),
            integrity: Some(IntegrityManifest {
                engine_identity: Some(EngineIdentity {
                    source_closure: Sha256Digest::from_bytes(b"src"),
                    lock_closure: Sha256Digest::from_bytes(b"lock"),
                    toolchain: "rustc 1.88.0".into(),
                    build_container: None,
                    features: ["provisioner".to_string()].into(),
                    profile: identity::BuildProfile::Dist,
                }),
                authority: BuildAuthority::TrustedCi {
                    provider: "buildkite".into(),
                    build_id: "b-42".into(),
                    source_commit: "abc123".into(),
                },
                provisioner_version: "1.2.3".into(),
                artifacts: vec![BinaryArtifactDescriptor {
                    target: Target("aarch64-unknown-linux-musl".into()),
                    sha256: "cafe".into(),
                    retrieval_ref: Some("s3://bin/1.2.3".into()),
                    size_bytes: 4096,
                }],
            }),
            config_revision: 7,
            operation: Some(Operation {
                operation_id: "op-9".into(),
                kind: OperationKind::UpgradeInFlight,
                phase: "migrate".into(),
                audit_log: Some(ChangeLog {
                    entries: vec![ChangeLogEntry {
                        id: "vpc/main".into(),
                        op: ChangeOp::Created,
                    }],
                }),
            }),
            ..Default::default()
        };

        let json = serde_json::to_string(&env).expect("serialize");
        let back: DeploymentStateEnvelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, back);
        back.validate().expect("round-tripped envelope validates");
    }

    #[test]
    fn begin_upgrade_captures_checkpoint_and_flips_binding() {
        let a = ProvenanceStamp {
            version: "1.0.0".into(),
            git_sha: "shaA".into(),
            source_tree_hash: "hA".into(),
            build_mode: BuildMode::Versioned,
            recorded_at: Utc::now(),
        };
        let b = ProvenanceStamp {
            source_tree_hash: "hB".into(),
            version: "2.0.0".into(),
            ..a.clone()
        };
        let mut env = DeploymentStateEnvelope {
            binding: Some(a.clone()),
            config_revision: 5,
            effective_config_ref: Some("cfg-A".into()),
            ..Default::default()
        };

        env.begin_upgrade(b.clone(), "op-1", Utc::now());

        // Binding flipped to B, [A final] captured in the checkpoint.
        assert_eq!(env.binding.as_ref(), Some(&b));
        let cp = env.checkpoint.as_ref().expect("checkpoint captured");
        assert_eq!(cp.from_provenance, a);
        assert_eq!(cp.from_config_ref.as_deref(), Some("cfg-A"));
        // Operation marker open.
        let op = env.operation.as_ref().expect("marker open");
        assert_eq!(op.kind, OperationKind::UpgradeInFlight);

        // Closing the marker leaves the flipped binding + checkpoint in place.
        env.close_operation();
        assert!(env.operation.is_none());
        assert_eq!(env.binding.as_ref(), Some(&b));
        assert!(env.checkpoint.is_some());
    }

    #[test]
    fn begin_rollback_repins_to_checkpoint_and_completes() {
        let a = ProvenanceStamp {
            version: "1.0.0".into(),
            git_sha: "shaA".into(),
            source_tree_hash: "hA".into(),
            build_mode: BuildMode::Versioned,
            recorded_at: Utc::now(),
        };
        let b = ProvenanceStamp {
            source_tree_hash: "hB".into(),
            version: "2.0.0".into(),
            ..a.clone()
        };
        let manifest_a = IntegrityManifest {
            provisioner_version: "1.0.0".into(),
            ..Default::default()
        };
        let manifest_b = IntegrityManifest {
            provisioner_version: "2.0.0".into(),
            ..Default::default()
        };
        // Post-upgrade envelope: bound to B (with B's re-recorded integrity
        // manifest), checkpoint holds [A final].
        let mut env = DeploymentStateEnvelope {
            binding: Some(a.clone()),
            integrity: Some(manifest_a.clone()),
            effective_config_ref: Some("cfg-A".into()),
            ..Default::default()
        };
        env.begin_upgrade(b.clone(), "op-up", Utc::now());
        env.integrity = Some(manifest_b); // what `upgrade` re-records for B
        env.close_operation();
        assert_eq!(env.binding.as_ref(), Some(&b));

        // Rollback: re-pin to A from the checkpoint.
        env.begin_rollback("op-rb", Utc::now())
            .expect("checkpoint present");
        assert_eq!(env.binding.as_ref(), Some(&a), "re-pinned to A");
        assert_eq!(
            env.integrity
                .as_ref()
                .map(|m| m.provisioner_version.as_str()),
            Some("1.0.0"),
            "A's integrity manifest restored with A's binding"
        );
        assert_eq!(
            env.effective_config_ref.as_deref(),
            Some("cfg-A"),
            "A's retained config ref restored"
        );
        assert_eq!(
            env.operation.as_ref().unwrap().kind,
            OperationKind::RollbackInFlight
        );

        // Complete: marker cleared, checkpoint consumed, binding stays A.
        env.complete_rollback();
        assert!(env.operation.is_none());
        assert!(env.checkpoint.is_none());
        assert_eq!(env.binding.as_ref(), Some(&a));
    }

    #[test]
    fn begin_rollback_without_checkpoint_errors() {
        let mut env = DeploymentStateEnvelope::default();
        assert!(env.begin_rollback("op", Utc::now()).is_err());
    }

    // A v1 envelope document (per-artifact `version`, no `engine_identity` /
    // `authority`) must keep loading under the v2 shape: the stale key is
    // ignored and the new fields take their safe defaults (no identity;
    // lowest-tier authority).
    #[test]
    fn v1_envelope_documents_deserialize_compatibly() {
        let v1 = serde_json::json!({
            "schema_version": 1,
            "deployment_id": "dep-legacy",
            "binding": null,
            "integrity": {
                "provisioner_version": "0.1.0",
                "artifacts": [{
                    "version": "0.1.0",
                    "target": "aarch64-apple-darwin",
                    "sha256": "cafe",
                    "retrieval_ref": null,
                    "size_bytes": 7
                }]
            },
            "config_revision": 3,
            "checkpoint": null,
            "operation": null,
            "lock": null,
            "infra_head": null,
            "runtime_head": null,
            "effective_config_ref": null
        });
        let env: DeploymentStateEnvelope =
            serde_json::from_value(v1).expect("v1 document loads under the v2 shape");
        env.validate().expect("older schema versions stay readable");
        let manifest = env.integrity.expect("manifest kept");
        assert!(manifest.engine_identity.is_none(), "no identity is Unknown");
        assert_eq!(manifest.authority, BuildAuthority::LocalDeveloper);
        assert_eq!(manifest.artifacts[0].sha256, "cafe");
    }

    #[test]
    fn stamp_current_schema_advances_a_loaded_v1_envelope() {
        let mut env = DeploymentStateEnvelope {
            schema_version: 1,
            ..Default::default()
        };
        env.stamp_current_schema();
        assert_eq!(env.schema_version, ENVELOPE_SCHEMA_VERSION);
    }

    #[test]
    fn schema_version_zero_is_rejected() {
        let env = DeploymentStateEnvelope {
            schema_version: 0,
            ..Default::default()
        };
        assert!(matches!(env.validate(), Err(StateError::Corrupted(_))));
    }

    #[test]
    fn newer_schema_version_is_rejected() {
        let env = DeploymentStateEnvelope {
            schema_version: ENVELOPE_SCHEMA_VERSION + 1,
            ..Default::default()
        };
        assert!(matches!(env.validate(), Err(StateError::Corrupted(_))));
    }

    // Compile-time proof the envelope satisfies the S3StateStore document bounds
    // (Serialize + DeserializeOwned + Default + Validate) — i.e. it can ride on
    // the store as the deployment's authoritative document.
    #[test]
    fn envelope_is_storable() {
        fn assert_storable<
            T: serde::Serialize + serde::de::DeserializeOwned + Default + Validate,
        >() {
        }
        assert_storable::<DeploymentStateEnvelope>();
    }
}
