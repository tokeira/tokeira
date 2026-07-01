//! The upgrade decision (Requirement 4, tasks 5.2 / 8.4).
//!
//! `upgrade` is how an operator *resolves* a binding `Mismatch` — it
//! authoritatively advances the deployment's recorded engine identity. It is the
//! only verb that does so. This module decides whether a proposed upgrade from
//! the recorded engine `A` to the running engine `B` is allowed, enforcing the
//! monotonic-advance and mode rules; the atomic ownership transfer that commits
//! it lives on [`DeploymentStateEnvelope::begin_upgrade`](crate::DeploymentStateEnvelope::begin_upgrade).

use std::cmp::Ordering;

use crate::{BuildMode, ProvenanceStamp, version::compare_versions};

/// The outcome of evaluating a proposed upgrade from recorded `A` to running `B`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpgradeDecision {
    /// `B` is a newer versioned engine than the recorded versioned `A`.
    VersionedAdvance,
    /// The recorded `A` is a `dev` deployment being promoted to a versioned `B`.
    Promotion,
    /// The upgrade is refused, with operator-facing guidance.
    Refuse(String),
}

/// Decide whether upgrading from `recorded` (`A`) to `running` (`B`) is allowed.
pub fn evaluate_upgrade(recorded: &ProvenanceStamp, running: &ProvenanceStamp) -> UpgradeDecision {
    match running.build_mode {
        // Upgrading *to* a dev binary is never an authoritative advance.
        BuildMode::Dev => {
            if recorded.build_mode == BuildMode::Versioned {
                UpgradeDecision::Refuse(
                    "cannot re-stamp a versioned deployment back to `dev`".to_string(),
                )
            } else {
                UpgradeDecision::Refuse(
                    "dev → dev is not an upgrade; use `apply` (the DevIterate loop)".to_string(),
                )
            }
        }
        BuildMode::Versioned => match recorded.build_mode {
            // dev → versioned promotion.
            BuildMode::Dev => UpgradeDecision::Promotion,
            BuildMode::Versioned => {
                if running.source_tree_hash == recorded.source_tree_hash {
                    UpgradeDecision::Refuse(
                        "already bound to this engine (same source_tree_hash); nothing to upgrade"
                            .to_string(),
                    )
                } else {
                    match compare_versions(&running.version, &recorded.version) {
                        Some(Ordering::Greater) => UpgradeDecision::VersionedAdvance,
                        Some(Ordering::Less) => UpgradeDecision::Refuse(
                            "the target engine's version is older than the recorded engine; \
                             downgrade is refused"
                                .to_string(),
                        ),
                        Some(Ordering::Equal) => UpgradeDecision::Refuse(
                            "same version, different source tree (a forgotten version bump); \
                             the ambiguous upgrade is refused"
                                .to_string(),
                        ),
                        None => UpgradeDecision::Refuse(
                            "cannot establish a monotonic version advance (unparseable version)"
                                .to_string(),
                        ),
                    }
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
    fn newer_versioned_engine_is_a_versioned_advance() {
        let a = stamp(BuildMode::Versioned, "1.2.0", "hA");
        let b = stamp(BuildMode::Versioned, "1.3.0", "hB");
        assert_eq!(evaluate_upgrade(&a, &b), UpgradeDecision::VersionedAdvance);
    }

    #[test]
    fn dev_to_versioned_is_promotion() {
        let a = stamp(BuildMode::Dev, "0.0.0", "hA");
        let b = stamp(BuildMode::Versioned, "1.0.0", "hB");
        assert_eq!(evaluate_upgrade(&a, &b), UpgradeDecision::Promotion);
    }

    #[test]
    fn older_version_is_refused_downgrade() {
        let a = stamp(BuildMode::Versioned, "1.3.0", "hA");
        let b = stamp(BuildMode::Versioned, "1.2.0", "hB");
        assert!(matches!(
            evaluate_upgrade(&a, &b),
            UpgradeDecision::Refuse(_)
        ));
    }

    #[test]
    fn same_version_different_hash_is_refused() {
        let a = stamp(BuildMode::Versioned, "1.2.0", "hA");
        let b = stamp(BuildMode::Versioned, "1.2.0", "hB");
        assert!(matches!(
            evaluate_upgrade(&a, &b),
            UpgradeDecision::Refuse(_)
        ));
    }

    #[test]
    fn same_engine_is_refused_nothing_to_upgrade() {
        let a = stamp(BuildMode::Versioned, "1.2.0", "hA");
        assert!(matches!(
            evaluate_upgrade(&a, &a),
            UpgradeDecision::Refuse(_)
        ));
    }

    #[test]
    fn versioned_to_dev_restamp_is_refused() {
        let a = stamp(BuildMode::Versioned, "1.2.0", "hA");
        let b = stamp(BuildMode::Dev, "1.2.0", "hA");
        assert!(matches!(
            evaluate_upgrade(&a, &b),
            UpgradeDecision::Refuse(_)
        ));
    }
}
