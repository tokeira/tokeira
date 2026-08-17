//! The binding gate for the applying verbs.
//!
//! Every mutating `tkp` verb evaluates the gate *before* any provider mutation.
//! A versioned deployment proceeds only on the authoritative `Match`; a dev
//! deployment + dev binary proceeds permissively as a `DevIterate` (advisory,
//! re-stamped, warned); every other verdict refuses with no override.

use tokeira_deployment::{BindingVerdict, ProvenanceStamp, check_binding};

/// What the gate decides for an applying verb.
#[derive(Debug)]
pub(crate) enum GateOutcome {
    /// Proceed. `authoritative` is true for a versioned `Match`, false for a dev
    /// iteration (permissive bring-up; the re-stamp records the advisory dev
    /// stamp and the caller warns).
    Proceed {
        verdict: BindingVerdict,
        authoritative: bool,
    },
    /// Refuse before any mutation, with the verdict and operator guidance.
    Refuse {
        verdict: BindingVerdict,
        reason: String,
    },
}

/// Evaluate the binding gate for a deployment's `recorded` provenance (`None`
/// when unstamped) against the `running` provisioner.
pub(crate) fn evaluate_gate(
    recorded: Option<&ProvenanceStamp>,
    running: &ProvenanceStamp,
) -> GateOutcome {
    let verdict = check_binding(recorded, running);
    match verdict {
        BindingVerdict::Match => GateOutcome::Proceed {
            verdict,
            authoritative: true,
        },
        BindingVerdict::DevIterate => GateOutcome::Proceed {
            verdict,
            authoritative: false,
        },
        BindingVerdict::Mismatch => GateOutcome::Refuse {
            verdict,
            reason: "the running engine does not match the deployment's recorded engine \
                     (source_tree_hash differs) — resolve with the matching binary or `tkp upgrade`"
                .to_string(),
        },
        BindingVerdict::Downgrade => GateOutcome::Refuse {
            verdict,
            reason: "the running engine is older than the deployment's recorded engine; \
                     downgrade is refused"
                .to_string(),
        },
        BindingVerdict::ModeRegression => GateOutcome::Refuse {
            verdict,
            reason: "a dev binary cannot operate a versioned deployment".to_string(),
        },
        BindingVerdict::Unknown => GateOutcome::Refuse {
            verdict,
            reason: "the deployment was never initialized (no Day-0 stamp) — `tkr deployment \
                     apply` initializes it on first run"
                .to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tokeira_deployment::BuildMode;

    fn stamp(mode: BuildMode, version: &str, hash: &str) -> ProvenanceStamp {
        ProvenanceStamp {
            version: version.to_string(),
            git_sha: "sha".to_string(),
            source_tree_hash: hash.to_string(),
            build_mode: mode,
            recorded_at: Utc::now(),
        }
    }

    #[test]
    fn versioned_match_proceeds_authoritative() {
        let s = stamp(BuildMode::Versioned, "1.0.0", "h");
        assert!(matches!(
            evaluate_gate(Some(&s), &s),
            GateOutcome::Proceed {
                authoritative: true,
                ..
            }
        ));
    }

    #[test]
    fn dev_iterate_proceeds_non_authoritative() {
        let recorded = stamp(BuildMode::Dev, "0", "hA");
        let running = stamp(BuildMode::Dev, "0", "hB");
        assert!(matches!(
            evaluate_gate(Some(&recorded), &running),
            GateOutcome::Proceed {
                authoritative: false,
                ..
            }
        ));
    }

    #[test]
    fn mismatch_refuses() {
        let recorded = stamp(BuildMode::Versioned, "1.0.0", "hA");
        let running = stamp(BuildMode::Versioned, "1.0.0", "hB");
        assert!(matches!(
            evaluate_gate(Some(&recorded), &running),
            GateOutcome::Refuse {
                verdict: BindingVerdict::Mismatch,
                ..
            }
        ));
    }

    #[test]
    fn unknown_refuses() {
        let running = stamp(BuildMode::Versioned, "1.0.0", "hA");
        assert!(matches!(
            evaluate_gate(None, &running),
            GateOutcome::Refuse {
                verdict: BindingVerdict::Unknown,
                ..
            }
        ));
    }

    #[test]
    fn mode_regression_refuses() {
        let recorded = stamp(BuildMode::Versioned, "1.0.0", "hA");
        let running = stamp(BuildMode::Dev, "1.0.0", "hA");
        assert!(matches!(
            evaluate_gate(Some(&recorded), &running),
            GateOutcome::Refuse {
                verdict: BindingVerdict::ModeRegression,
                ..
            }
        ));
    }
}
