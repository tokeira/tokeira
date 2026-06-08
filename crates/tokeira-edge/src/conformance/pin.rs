//! Pin-consistency gate for Tier-2 functional conformance.
//!
//! Tier 2 runs Temporal's own functional Go suite, unmodified, over the real gRPC
//! wire against a running `tokeirad` (see `.kiro/specs/temporal-functional-conformance`).
//! The signal that suite produces is only meaningful if the corpus it runs is the
//! corpus for the *exact* Temporal release whose behaviour Tokeira claims to match.
//! That claim is the single pin `tokeira_build_info::TEMPORAL_SERVER_COMPAT`
//! (`crates/tokeira-build-info/src/pinned.rs`, currently `1.31.0`); the corpus lives on
//! the fork's conformance branch pinned at tag `v1.31.0`.
//!
//! ## Why this gate exists, and why it must fail loud and early
//!
//! If the two drift, the failure is silent and *inverted*: running a corpus newer than
//! the behavioural claim asserts semantics Tokeira does not yet implement, so the suite
//! reports failures that are not real gaps, and — worse — a run from the fork's `main`
//! (which tracks upstream `HEAD`, ahead of the claimed tag) would quietly redefine the
//! conformance target without anyone bumping `TEMPORAL_SERVER_COMPAT`. The conformance
//! report would then be measuring against the wrong baseline while looking healthy. This
//! check is the loud, early fail-fast gate that makes that drift impossible to ignore:
//! it refuses to let a run proceed unless the observed fork ref matches the claim
//! (Design Property 2; Requirements 1.1, 1.3, 1.4).
//!
//! ## Why this is a pure function (no git here)
//!
//! `tokeira-edge` is a library on the request-serving path; it has no business shelling
//! out to `git` or depending on a process/VCS crate. The fork ref is something only the
//! harness/operator can observe (it lives in the *other* checkout, `../temporal`), so
//! this module takes that observation as data — [`ForkPin`] — and decides over it. The
//! crate stays I/O-free and the decision stays unit-testable without a working tree.
//!
//! ## The `v`-prefix normalization
//!
//! `TEMPORAL_SERVER_COMPAT` is a bare semver (`1.31.0`); the fork tag is conventionally
//! `v`-prefixed (`v1.31.0`). The two name the same release, so [`check_pin_consistency`]
//! strips a single leading `v` from the reported tag before comparing. The comparison is
//! otherwise exact — a tag of `v1.31.1` or `1.32.0` is a mismatch, not a near-match — so
//! the normalization closes only the cosmetic prefix gap and nothing else.

use thiserror::Error;
use tokeira_build_info::TEMPORAL_SERVER_COMPAT;

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
    /// (e.g. `v1.31.0`). `None` when the harness could not resolve a tag for the checked-out
    /// ref — which is itself a rejectable condition (an un-pinned corpus has no provenance).
    pub tag: Option<&'a str>,

    /// The name of the branch currently checked out in the fork (e.g.
    /// `tokeira/conformance-v1.31.0`, or `main`). Used to reject a run from the fork's
    /// `main`, which tracks upstream `HEAD` ahead of the claimed tag (Requirement 1.4).
    pub branch: &'a str,
}

/// The branch name the conformance corpus must never be run from.
///
/// The fork's `main` tracks upstream Temporal `HEAD`, which is ahead of the pinned
/// `v1.31.0` tag; running the corpus from it would assert post-`1.31.0` behaviour the
/// claim does not cover (Requirement 1.4, Design "Ground-Truth Pins").
const FORK_MAIN_BRANCH: &str = "main";

/// Why a [`ForkPin`] failed the consistency gate.
///
/// Each variant is a distinct, actionable rejection so the harness can surface *which*
/// drift occurred rather than a generic "pin mismatch". The `Display` messages name both
/// the observed value and the expected claim so the operator sees the divergence without
/// re-deriving it. `thiserror` is used per AGENTS.md §1 (library crates use `thiserror`),
/// matching the crate's existing [`crate::errors::EdgeError`] style.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PinMismatch {
    /// The corpus was run from the fork's `main` branch. Rejected unconditionally because
    /// `main` tracks upstream `HEAD` ahead of the claimed tag (Requirement 1.4); this is
    /// checked before the tag so the most dangerous drift gets the most specific message.
    #[error(
        "conformance corpus must not be run from the fork's `{branch}` branch; \
         it tracks upstream HEAD ahead of the claimed Temporal {expected_compat} \
         (use the conformance branch pinned at v{expected_compat})"
    )]
    RanFromMain {
        /// The offending branch name (`main`).
        branch: String,
        /// The claimed `TEMPORAL_SERVER_COMPAT` the corpus should be pinned to.
        expected_compat: String,
    },

    /// The harness could not resolve a tag for the checked-out conformance ref. An
    /// un-pinned corpus has no verifiable provenance, so it is rejected rather than
    /// assumed-good (Requirement 1.1).
    #[error(
        "conformance branch reported no pinned tag; expected the corpus pinned at \
         v{expected_compat} to match the Temporal server-compat claim {expected_compat}"
    )]
    MissingTag {
        /// The claimed `TEMPORAL_SERVER_COMPAT` the corpus should be pinned to.
        expected_compat: String,
    },

    /// The reported tag, after stripping a leading `v`, does not equal
    /// `TEMPORAL_SERVER_COMPAT`. Both values are carried so the operator sees exactly what
    /// was found versus claimed (Requirement 1.1, 1.3).
    #[error(
        "conformance branch tag `{tag}` does not match the Temporal server-compat claim \
         {expected_compat} (expected tag v{expected_compat} or {expected_compat})"
    )]
    TagMismatch {
        /// The tag as reported by the harness (with its original `v` prefix, if any).
        tag: String,
        /// The claimed `TEMPORAL_SERVER_COMPAT` the tag was compared against.
        expected_compat: String,
    },
}

/// Assert the fork's observed conformance ref matches the Temporal server-compat claim.
///
/// Returns `Ok(())` only when all of the following hold; otherwise it returns the most
/// specific [`PinMismatch`] for the first failing condition, in this order:
///
/// 1. The checked-out branch is not the fork's `main` ([`PinMismatch::RanFromMain`],
///    Requirement 1.4). Checked first because a `main` run is the highest-risk drift and a
///    tag check on `main` would be misleadingly "fine".
/// 2. A pinned tag is present ([`PinMismatch::MissingTag`], Requirement 1.1).
/// 3. The tag, after stripping a single leading `v`, equals
///    [`TEMPORAL_SERVER_COMPAT`] ([`PinMismatch::TagMismatch`], Requirements 1.1, 1.3).
///
/// The comparison normalizes only the conventional `v` prefix (see the module docs); it is
/// otherwise an exact string equality against the bare-semver claim. This is a pure
/// decision over its input — it performs no I/O — so the harness owns observing the fork
/// ref and this crate stays off the VCS/process surface (see the module docs).
pub fn check_pin_consistency(fork: ForkPin<'_>) -> Result<(), PinMismatch> {
    if fork.branch == FORK_MAIN_BRANCH {
        return Err(PinMismatch::RanFromMain {
            branch: fork.branch.to_owned(),
            expected_compat: TEMPORAL_SERVER_COMPAT.to_owned(),
        });
    }

    let tag = fork.tag.ok_or_else(|| PinMismatch::MissingTag {
        expected_compat: TEMPORAL_SERVER_COMPAT.to_owned(),
    })?;

    // Bare-semver claim vs. conventionally `v`-prefixed tag name the same release; close
    // only that cosmetic gap, leave the rest of the comparison exact.
    let normalized = tag.strip_prefix('v').unwrap_or(tag);
    if normalized != TEMPORAL_SERVER_COMPAT {
        return Err(PinMismatch::TagMismatch {
            tag: tag.to_owned(),
            expected_compat: TEMPORAL_SERVER_COMPAT.to_owned(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_v_prefixed_tag_matching_compat_on_conformance_branch() {
        let tag = format!("v{TEMPORAL_SERVER_COMPAT}");
        let fork = ForkPin {
            tag: Some(&tag),
            branch: "tokeira/conformance-v1.31.0",
        };

        assert_eq!(check_pin_consistency(fork), Ok(()));
    }

    #[test]
    fn accepts_bare_semver_tag_matching_compat() {
        // The normalization strips only a leading `v`; a tag already equal to the bare
        // claim must also be accepted.
        let fork = ForkPin {
            tag: Some(TEMPORAL_SERVER_COMPAT),
            branch: "tokeira/conformance-v1.31.0",
        };

        assert_eq!(check_pin_consistency(fork), Ok(()));
    }

    #[test]
    fn rejects_run_from_fork_main_even_with_matching_tag() {
        // `main` is rejected ahead of the tag check, so a matching tag does not rescue it.
        let tag = format!("v{TEMPORAL_SERVER_COMPAT}");
        let fork = ForkPin {
            tag: Some(&tag),
            branch: "main",
        };

        assert_eq!(
            check_pin_consistency(fork),
            Err(PinMismatch::RanFromMain {
                branch: "main".to_owned(),
                expected_compat: TEMPORAL_SERVER_COMPAT.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_missing_tag() {
        let fork = ForkPin {
            tag: None,
            branch: "tokeira/conformance-v1.31.0",
        };

        assert_eq!(
            check_pin_consistency(fork),
            Err(PinMismatch::MissingTag {
                expected_compat: TEMPORAL_SERVER_COMPAT.to_owned(),
            })
        );
    }

    #[test]
    fn rejects_tag_that_does_not_match_compat() {
        // A different patch release is a mismatch, not a near-match.
        let fork = ForkPin {
            tag: Some("v1.31.1"),
            branch: "tokeira/conformance-v1.31.1",
        };

        assert_eq!(
            check_pin_consistency(fork),
            Err(PinMismatch::TagMismatch {
                tag: "v1.31.1".to_owned(),
                expected_compat: TEMPORAL_SERVER_COMPAT.to_owned(),
            })
        );
    }
}
