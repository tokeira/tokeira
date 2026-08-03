//! Write and read the explanation artifact (operator-explanation Req 3).
//!
//! The artifact is the complete [`DeploymentExplanation`] as JSON in a file —
//! the form a CI system gates on without scraping terminal output. It is
//! produced and consumed **only** through the filesystem: this module uses
//! `std::fs` alone, which is the structural form of the no-socket guarantee
//! (Req 7.5, umbrella decision D1 — the provisioner is an artifact, never a
//! service).
//!
//! Invariants owned here:
//! - the artifact is the model, whole: the same serialization `--json` emits,
//!   schema version included (Req 7.1, 7.2);
//! - reading needs nothing but the file: [`read`] takes no deployment
//!   directory, so a parsed artifact is self-contained by construction
//!   (Req 7.3 — evidence closure is the model's own property, asserted by
//!   Property 3 at build time and Property 10 over the round-trip);
//! - failures carry the path and the underlying reason, so the verb that
//!   requested the artifact can fail without inventing its own copy
//!   (Req 7.6).

use std::path::{Path, PathBuf};

use crate::model::DeploymentExplanation;

/// Explanation-layer failures. Artifact variants carry the path so the
/// operator's error names the file they asked for, not an internal step.
#[derive(Debug, thiserror::Error)]
pub enum ExplainError {
    /// The artifact could not be written to the requested path. The verb that
    /// requested it must fail and must not report success (Req 7.6).
    #[error("could not write the explanation artifact to {path}: {source}")]
    ArtifactWrite {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The artifact file could not be read.
    #[error("could not read the explanation artifact at {path}: {source}")]
    ArtifactRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The file exists but is not an explanation artifact this schema version
    /// understands.
    #[error("the explanation artifact at {path} does not parse: {source}")]
    ArtifactParse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// The model itself failed to serialize — a programming error surfaced
    /// explicitly rather than swallowed (the model is designed to always
    /// serialize; string-keyed evidence, no non-JSON values).
    #[error("the explanation model failed to serialize: {0}")]
    Serialize(#[source] serde_json::Error),
}

/// Write the complete explanation model as pretty JSON to `path` (Req 7.1).
///
/// The bytes are exactly the model's serialization — no wrapper, no envelope —
/// so the artifact and a captured `--json` stdout are one schema. A trailing
/// newline keeps the file friendly to POSIX tooling and CI diffs.
pub fn write(path: &Path, explanation: &DeploymentExplanation) -> Result<(), ExplainError> {
    let mut bytes = serde_json::to_vec_pretty(explanation).map_err(ExplainError::Serialize)?;
    bytes.push(b'\n');
    std::fs::write(path, bytes).map_err(|source| ExplainError::ArtifactWrite {
        path: path.to_path_buf(),
        source,
    })
}

/// Read an explanation artifact back into the model.
///
/// Takes only the file: no deployment directory, no platform, no live state —
/// the artifact must stand alone (Req 3.3), and this signature is what holds
/// that line.
pub fn read(path: &Path) -> Result<DeploymentExplanation, ExplainError> {
    let bytes = std::fs::read(path).map_err(|source| ExplainError::ArtifactRead {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| ExplainError::ArtifactParse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> DeploymentExplanation {
        crate::explain_plan(
            crate::DeploymentContext {
                deployment: "t".to_string(),
                platform: "test".to_string(),
                operation: "infra plan".to_string(),
                current_revision: 1,
                proposed_revision: None,
                definition_ref: None,
            },
            &tokeira_iac::PlanOutcome::default(),
        )
    }

    // Req 7.6: a failed write names the path and the underlying reason — the
    // verb's refusal copy is this error, so it must carry both.
    #[test]
    fn a_failed_write_names_the_path_and_reason() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing-parent").join("explanation.json");
        let err = write(&path, &minimal()).expect_err("unwritable path fails");
        let message = err.to_string();
        assert!(message.contains("missing-parent"), "path named: {message}");
        assert!(
            matches!(err, ExplainError::ArtifactWrite { .. }),
            "write failure is the write variant"
        );
    }

    #[test]
    fn a_missing_artifact_reads_as_read_error_and_garbage_as_parse() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("explanation.json");
        assert!(matches!(
            read(&path),
            Err(ExplainError::ArtifactRead { .. })
        ));
        std::fs::write(&path, b"not json").unwrap();
        assert!(matches!(
            read(&path),
            Err(ExplainError::ArtifactParse { .. })
        ));
    }
}
