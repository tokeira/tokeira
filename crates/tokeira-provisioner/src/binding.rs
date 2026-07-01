//! The binding / mismatch gate (Requirement 6, task 3).
//!
//! Every binding-aware verb compares the deployment's **recorded** provenance
//! against the **running** provisioner's provenance and gets a [`BindingVerdict`].
//! `plan` surfaces the verdict; applying verbs gate on it. The authoritative
//! drift key is [`source_tree_hash`](crate::ProvenanceStamp::source_tree_hash) —
//! a whole-workspace digest, never the semver (which a developer can forget to
//! bump). A `dev` stamp is advisory and never yields the authoritative
//! [`Match`](BindingVerdict::Match).

use crate::{BuildMode, ProvenanceStamp, version::is_older};

/// The outcome of comparing recorded vs running provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingVerdict {
    /// Recorded and running are the same versioned engine (`source_tree_hash`
    /// matches). The only authoritative proceed.
    Match,
    /// Dev deployment + dev binary: the permissive bring-up loop — apply, re-stamp
    /// the advisory dev stamp, warn. Never authoritative.
    DevIterate,
    /// Different `source_tree_hash` on a versioned deployment, not a downgrade — a
    /// forgotten version bump, or a newer engine that must arrive via `upgrade`.
    /// Refuse (no override).
    Mismatch,
    /// The running engine's version is older than the recorded one. Refuse
    /// (ordering is by monotonic version, never by hash).
    Downgrade,
    /// A dev binary against a versioned deployment. Refuse.
    ModeRegression,
    /// No recorded provenance (unstamped / an explicit `Unknown`). Identity
    /// cannot be verified. Refuse on the versioned regime.
    Unknown,
}

impl BindingVerdict {
    /// Whether a binding-aware **mutating** verb may proceed under this verdict.
    ///
    /// [`Match`](Self::Match) (authoritative) and [`DevIterate`](Self::DevIterate)
    /// (advisory bring-up) proceed; every other verdict refuses — the operator
    /// resolves via a matching binary or `upgrade`.
    pub fn proceeds(&self) -> bool {
        matches!(self, BindingVerdict::Match | BindingVerdict::DevIterate)
    }

    /// Whether this verdict is the authoritative match. A `dev` iteration is
    /// never authoritative.
    pub fn is_authoritative(&self) -> bool {
        matches!(self, BindingVerdict::Match)
    }
}

/// Compute the binding verdict for a deployment's `recorded` provenance (`None`
/// when unstamped) against the `running` provisioner's provenance.
pub fn check_binding(
    recorded: Option<&ProvenanceStamp>,
    running: &ProvenanceStamp,
) -> BindingVerdict {
    let Some(recorded) = recorded else {
        return BindingVerdict::Unknown;
    };

    match recorded.build_mode {
        BuildMode::Dev => match running.build_mode {
            // Dev deployment + dev binary — the permissive bring-up loop.
            BuildMode::Dev => BindingVerdict::DevIterate,
            // A versioned binary against a dev deployment is the dev → versioned
            // promotion, which goes through `upgrade`, not a plain apply.
            BuildMode::Versioned => BindingVerdict::Mismatch,
        },
        BuildMode::Versioned => match running.build_mode {
            // A dev binary must never operate a versioned deployment.
            BuildMode::Dev => BindingVerdict::ModeRegression,
            BuildMode::Versioned => {
                if running.source_tree_hash == recorded.source_tree_hash {
                    BindingVerdict::Match
                } else if is_older(&running.version, &recorded.version) {
                    BindingVerdict::Downgrade
                } else {
                    // Different hash, same-or-newer version: forgotten bump or a
                    // newer engine — resolve via `upgrade`, not a plain apply.
                    BindingVerdict::Mismatch
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

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
    fn matching_versioned_hash_is_authoritative_match() {
        let recorded = stamp(BuildMode::Versioned, "1.2.0", "hashA");
        let running = stamp(BuildMode::Versioned, "1.2.0", "hashA");
        let verdict = check_binding(Some(&recorded), &running);
        assert_eq!(verdict, BindingVerdict::Match);
        assert!(verdict.proceeds());
        assert!(verdict.is_authoritative());
    }

    #[test]
    fn different_hash_same_version_is_mismatch() {
        // Forgotten version bump: same semver, different source tree.
        let recorded = stamp(BuildMode::Versioned, "1.2.0", "hashA");
        let running = stamp(BuildMode::Versioned, "1.2.0", "hashB");
        let verdict = check_binding(Some(&recorded), &running);
        assert_eq!(verdict, BindingVerdict::Mismatch);
        assert!(!verdict.proceeds());
    }

    #[test]
    fn older_version_is_downgrade() {
        let recorded = stamp(BuildMode::Versioned, "1.3.0", "hashNew");
        let running = stamp(BuildMode::Versioned, "1.2.5", "hashOld");
        assert_eq!(
            check_binding(Some(&recorded), &running),
            BindingVerdict::Downgrade
        );
    }

    #[test]
    fn newer_version_different_hash_is_mismatch_not_match() {
        // A newer engine must arrive via `upgrade`, not a plain apply.
        let recorded = stamp(BuildMode::Versioned, "1.2.0", "hashA");
        let running = stamp(BuildMode::Versioned, "1.3.0", "hashB");
        assert_eq!(
            check_binding(Some(&recorded), &running),
            BindingVerdict::Mismatch
        );
    }

    #[test]
    fn dev_binary_on_versioned_deployment_is_mode_regression() {
        let recorded = stamp(BuildMode::Versioned, "1.2.0", "hashA");
        let running = stamp(BuildMode::Dev, "1.2.0", "hashA");
        let verdict = check_binding(Some(&recorded), &running);
        assert_eq!(verdict, BindingVerdict::ModeRegression);
        assert!(!verdict.proceeds());
    }

    #[test]
    fn dev_deployment_and_dev_binary_is_dev_iterate() {
        let recorded = stamp(BuildMode::Dev, "0.0.0", "hashA");
        let running = stamp(BuildMode::Dev, "0.0.0", "hashB");
        let verdict = check_binding(Some(&recorded), &running);
        assert_eq!(verdict, BindingVerdict::DevIterate);
        assert!(verdict.proceeds(), "dev iteration is permissive");
        assert!(
            !verdict.is_authoritative(),
            "a dev stamp is never authoritative"
        );
    }

    #[test]
    fn versioned_binary_on_dev_deployment_is_mismatch() {
        // dev -> versioned promotion goes through `upgrade`.
        let recorded = stamp(BuildMode::Dev, "0.0.0", "hashA");
        let running = stamp(BuildMode::Versioned, "1.0.0", "hashB");
        assert_eq!(
            check_binding(Some(&recorded), &running),
            BindingVerdict::Mismatch
        );
    }

    #[test]
    fn no_recorded_stamp_is_unknown() {
        let running = stamp(BuildMode::Versioned, "1.2.0", "hashA");
        let verdict = check_binding(None, &running);
        assert_eq!(verdict, BindingVerdict::Unknown);
        assert!(!verdict.proceeds());
    }

    #[test]
    fn unparseable_version_difference_is_mismatch_not_downgrade() {
        let recorded = stamp(BuildMode::Versioned, "nightly", "hashA");
        let running = stamp(BuildMode::Versioned, "1.2.0", "hashB");
        assert_eq!(
            check_binding(Some(&recorded), &running),
            BindingVerdict::Mismatch
        );
    }
}
