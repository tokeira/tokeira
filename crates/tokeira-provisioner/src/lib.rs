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

pub mod binding;
pub mod integrity;
pub use binding::{BindingVerdict, check_binding};
pub use integrity::{IntegrityError, sha256_hex};

/// Current `DeploymentStateEnvelope` schema version.
pub const ENVELOPE_SCHEMA_VERSION: u32 = 1;

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

/// One binary artifact for one target, addressed by content hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryArtifactDescriptor {
    pub version: String,
    pub target: Target,
    pub sha256: String,
    /// Optional retrieval pointer (e.g. an S3 key) when the blob is stored.
    pub retrieval_ref: Option<String>,
    pub size_bytes: u64,
}

/// The set of binary artifacts a provisioner version publishes, across targets.
///
/// CAS-guarded in the envelope; cannot be silently rewritten. Carries **all**
/// targets because a rollback may run from a different operator platform than the
/// one that performed the upgrade.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct IntegrityManifest {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_mode_parses_build_info_strings() {
        assert_eq!(BuildMode::from_build_info("versioned"), BuildMode::Versioned);
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
        assert!(env.binding.is_none(), "an unstamped envelope is Unknown, not coerced");
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
                provisioner_version: "1.2.3".into(),
                artifacts: vec![BinaryArtifactDescriptor {
                    version: "1.2.3".into(),
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
