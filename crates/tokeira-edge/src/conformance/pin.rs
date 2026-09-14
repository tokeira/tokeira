//! Pin-consistency gate for Tier-2 functional conformance.
//!
//! The corpus must match `tokeira_build_info::TEMPORAL_SERVER_TARGET`, the release
//! under compatibility campaign. The target can lead the advertised claim while
//! regressions are measured; accepting the claim instead would prevent that baseline
//! run (`temporal-v1.32-compatibility`, Requirement 7.6). The fork's `main` remains
//! forbidden because upstream HEAD can silently change the measured contract.
//!
//! The harness observes the fork ref and supplies it as [`ForkPin`]. This module
//! performs no VCS or process I/O. It strips only one conventional leading `v` from
//! the tag; all remaining bytes must match the target exactly.

use thiserror::Error;
use tokeira_build_info::TEMPORAL_SERVER_TARGET;

/// The fork's self-reported conformance ref, as observed by the harness/operator.
///
/// This is the *input* to the pin gate: the harness inspects the `../temporal` checkout
/// (the tag the conformance branch is pinned at and the branch currently checked out) and
/// hands the observation here as plain data. Keeping it a borrowed view (`&str`) reflects
/// that the strings are owned by the caller's observation and this check neither stores
/// nor mutates them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForkPin<'a> {
    /// The Temporal tag the conformance branch is pinned at, conventionally `v`-prefixed
    /// (e.g. `v1.32.0`). `None` when the harness could not resolve a tag for the checked-out
    /// ref — which is itself a rejectable condition (an un-pinned corpus has no provenance).
    pub tag: Option<&'a str>,

    /// The name of the branch currently checked out in the fork (e.g.
    /// `tokeira/conformance-v1.32.0`, or `main`). Used to reject a run from the fork's
    /// `main`, which tracks upstream `HEAD` ahead of the target tag (Requirement 1.4).
    pub branch: &'a str,
}

/// The branch name the conformance corpus must never be run from.
///
/// The fork's `main` tracks upstream Temporal `HEAD`, which is ahead of the pinned
/// target tag; running the corpus from it would measure a moving contract.
const FORK_MAIN_BRANCH: &str = "main";

/// Why a [`ForkPin`] failed the consistency gate.
///
/// Each variant is a distinct, actionable rejection so the harness can surface *which*
/// drift occurred rather than a generic "pin mismatch". The `Display` messages name both
/// the observed value and the expected target so the operator sees the divergence without
/// re-deriving it. `thiserror` is used per AGENTS.md §1 (library crates use `thiserror`),
/// matching the crate's existing [`crate::errors::EdgeError`] style.
/// The `expected_compat` field name is retained for source compatibility; its value
/// now names the campaign target, not the advertised claim.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PinMismatch {
    /// The corpus was run from the fork's `main` branch. Rejected unconditionally because
    /// `main` tracks upstream `HEAD` ahead of the target tag (Requirement 1.4); this is
    /// checked before the tag so the most dangerous drift gets the most specific message.
    #[error(
        "conformance corpus must not be run from the fork's `{branch}` branch; \
         it tracks upstream HEAD independently of TEMPORAL_SERVER_TARGET={expected_compat} \
         (use the conformance branch pinned at v{expected_compat})"
    )]
    RanFromMain {
        /// The offending branch name (`main`).
        branch: String,
        /// The `TEMPORAL_SERVER_TARGET` the corpus should be pinned to.
        expected_compat: String,
    },

    /// The harness could not resolve a tag for the checked-out conformance ref. An
    /// un-pinned corpus has no verifiable provenance, so it is rejected rather than
    /// assumed-good (Requirement 1.1).
    #[error(
        "conformance branch reported no pinned tag; expected the corpus pinned at \
         v{expected_compat} to match TEMPORAL_SERVER_TARGET={expected_compat}"
    )]
    MissingTag {
        /// The `TEMPORAL_SERVER_TARGET` the corpus should be pinned to.
        expected_compat: String,
    },

    /// The reported tag, after stripping a leading `v`, does not equal
    /// `TEMPORAL_SERVER_TARGET`. Both values are carried so the operator sees exactly what
    /// was found versus targeted (Requirement 1.1, 1.3).
    #[error(
        "conformance branch tag `{tag}` does not match TEMPORAL_SERVER_TARGET={expected_compat} \
         (expected tag v{expected_compat} or {expected_compat})"
    )]
    TagMismatch {
        /// The tag as reported by the harness (with its original `v` prefix, if any).
        tag: String,
        /// The `TEMPORAL_SERVER_TARGET` the tag was compared against.
        expected_compat: String,
    },
}

/// Assert the fork's observed conformance ref matches [`TEMPORAL_SERVER_TARGET`].
///
/// Returns `Ok(())` only when all of the following hold; otherwise it returns the most
/// specific [`PinMismatch`] for the first failing condition, in this order:
///
/// 1. The checked-out branch is not the fork's `main` ([`PinMismatch::RanFromMain`],
///    Requirement 1.4). Checked first because a `main` run is the highest-risk drift and a
///    tag check on `main` would be misleadingly "fine".
/// 2. A pinned tag is present ([`PinMismatch::MissingTag`], Requirement 1.1).
/// 3. The tag, after stripping a single leading `v`, equals
///    [`TEMPORAL_SERVER_TARGET`] ([`PinMismatch::TagMismatch`], Requirements 1.1, 1.3).
///
/// The comparison normalizes only the conventional `v` prefix (see the module docs); it is
/// otherwise an exact string equality against the bare-semver target. This is a pure
/// decision over its input — it performs no I/O — so the harness owns observing the fork
/// ref and this crate stays off the VCS/process surface (see the module docs).
pub fn check_pin_consistency(fork: ForkPin<'_>) -> Result<(), PinMismatch> {
    if fork.branch == FORK_MAIN_BRANCH {
        return Err(PinMismatch::RanFromMain {
            branch: fork.branch.to_owned(),
            expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
        });
    }

    let tag = fork.tag.ok_or_else(|| PinMismatch::MissingTag {
        expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
    })?;

    // Bare-semver target vs. conventionally `v`-prefixed tag name the same release; close
    // only that cosmetic gap, leave the rest of the comparison exact.
    let normalized = tag.strip_prefix('v').unwrap_or(tag);
    if normalized != TEMPORAL_SERVER_TARGET {
        return Err(PinMismatch::TagMismatch {
            tag: tag.to_owned(),
            expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_v_prefixed_tag_matching_target_on_conformance_branch() {
        let tag = format!("v{TEMPORAL_SERVER_TARGET}");
        let fork = ForkPin {
            tag: Some(&tag),
            branch: "tokeira/conformance-v1.32.0",
        };

        assert_eq!(check_pin_consistency(fork), Ok(()));
    }

    #[test]
    fn accepts_bare_semver_tag_matching_target() {
        // The normalization strips only a leading `v`; a tag already equal to the bare
        // target must also be accepted.
        let fork = ForkPin {
            tag: Some(TEMPORAL_SERVER_TARGET),
            branch: "tokeira/conformance-v1.32.0",
        };

        assert_eq!(check_pin_consistency(fork), Ok(()));
    }

    #[test]
    fn rejects_run_from_fork_main_even_with_matching_tag() {
        // `main` is rejected ahead of the tag check, so a matching tag does not rescue it.
        let tag = format!("v{TEMPORAL_SERVER_TARGET}");
        let fork = ForkPin {
            tag: Some(&tag),
            branch: "main",
        };

        assert_eq!(
            check_pin_consistency(fork),
            Err(PinMismatch::RanFromMain {
                branch: "main".to_owned(),
                expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_missing_tag() {
        let fork = ForkPin {
            tag: None,
            branch: "tokeira/conformance-v1.32.0",
        };

        assert_eq!(
            check_pin_consistency(fork),
            Err(PinMismatch::MissingTag {
                expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_claim_tag_while_campaign_target_is_ahead() {
        let claim = tokeira_build_info::TEMPORAL_SERVER_COMPAT;
        let tag = format!("v{claim}");
        let result = check_pin_consistency(ForkPin {
            tag: Some(&tag),
            branch: "tokeira/conformance",
        });
        if claim == TEMPORAL_SERVER_TARGET {
            assert_eq!(result, Ok(()));
        } else {
            assert_eq!(
                result,
                Err(PinMismatch::TagMismatch {
                    tag,
                    expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
                })
            );
        }
    }

    #[test]
    fn rejection_messages_name_target_and_remedy() {
        for fork in [
            ForkPin {
                tag: None,
                branch: "main",
            },
            ForkPin {
                tag: None,
                branch: "tokeira/conformance",
            },
            ForkPin {
                tag: Some("v0.0.0"),
                branch: "tokeira/conformance",
            },
        ] {
            let message = check_pin_consistency(fork)
                .expect_err("invalid fork")
                .to_string();
            assert!(message.contains("TEMPORAL_SERVER_TARGET"), "{message}");
            assert!(
                message.contains(&format!("v{TEMPORAL_SERVER_TARGET}")),
                "{message}"
            );
        }
    }

    #[test]
    fn normalization_only_strips_one_lowercase_v() {
        for tag in [
            format!("vv{TEMPORAL_SERVER_TARGET}"),
            format!("V{TEMPORAL_SERVER_TARGET}"),
            format!("v{TEMPORAL_SERVER_TARGET} "),
            format!("v{TEMPORAL_SERVER_TARGET}-rc.1"),
        ] {
            let result = check_pin_consistency(ForkPin {
                tag: Some(&tag),
                branch: "tokeira/conformance",
            });
            assert!(matches!(result, Err(PinMismatch::TagMismatch { .. })));
        }
    }

    #[test]
    fn rejects_tag_that_does_not_match_target() {
        // A different patch release is a mismatch, not a near-match.
        let fork = ForkPin {
            tag: Some("v1.32.1"),
            branch: "tokeira/conformance-v1.32.1",
        };

        assert_eq!(
            check_pin_consistency(fork),
            Err(PinMismatch::TagMismatch {
                tag: "v1.32.1".to_owned(),
                expected_compat: TEMPORAL_SERVER_TARGET.to_owned(),
            })
        );
    }
}
