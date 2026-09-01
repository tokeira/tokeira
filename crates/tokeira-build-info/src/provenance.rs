//! Pure build-profile provenance selection shared by the build script and its tests.

/// Git provenance selected for one Cargo build-script invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedGitSha {
    /// Value embedded in the binary.
    pub(crate) value: String,
    /// Whether Cargo should warn that a local release build is deliberately degraded.
    pub(crate) warn_local_release: bool,
}

/// Resolve the source revision without coupling the policy to process-global test state.
pub(crate) fn resolve_git_sha(
    profile: &str,
    ci_is_set: bool,
    supplied: Option<&str>,
    debug_fallback: impl FnOnce() -> Option<String>,
) -> Result<ResolvedGitSha, String> {
    let supplied = supplied.map(str::trim).filter(|value| !value.is_empty());

    if profile == "release" {
        if !ci_is_set {
            return Ok(ResolvedGitSha {
                value: "dev".to_owned(),
                warn_local_release: true,
            });
        }

        let value = supplied.ok_or_else(|| {
            "release build in CI requires non-empty TOKEIRA_GIT_SHA provenance".to_owned()
        })?;
        if value == "dev" {
            return Err(
                "release build in CI cannot use the degraded TOKEIRA_GIT_SHA value `dev`"
                    .to_owned(),
            );
        }
        return Ok(ResolvedGitSha {
            value: value.to_owned(),
            warn_local_release: false,
        });
    }

    Ok(ResolvedGitSha {
        value: supplied
            .map(str::to_owned)
            .or_else(debug_fallback)
            .unwrap_or_else(|| "dev".to_owned()),
        warn_local_release: false,
    })
}

/// Choose the full source revision that accompanies the short provenance.
///
/// Supplied values outrank live Git for the same reason `resolve_git_sha` prefers
/// them: a release build must carry the revision its manifest names, not whatever
/// the builder's checkout happens to be. Live Git only fills a gap for development
/// builds, and the short provenance is the last resort so the two values never
/// disagree about a degraded build.
pub(crate) fn resolve_source_revision(
    manifest: Option<&str>,
    injected: Option<&str>,
    supplied_git_sha: Option<&str>,
    live_git: impl FnOnce() -> Option<String>,
    short_provenance: &str,
) -> String {
    let full = |value: Option<&str>| {
        value
            .map(str::trim)
            .filter(|value| is_full_revision(value))
            .map(str::to_owned)
    };
    full(manifest)
        .or_else(|| full(injected))
        .or_else(|| full(supplied_git_sha))
        .or_else(live_git)
        .unwrap_or_else(|| short_provenance.to_owned())
}

fn is_full_revision(value: &str) -> bool {
    value.len() == 40 && value.chars().all(|character| character.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_ci_fails_closed_without_non_degraded_provenance() {
        for supplied in [None, Some(""), Some("   "), Some("dev")] {
            assert!(
                resolve_git_sha("release", true, supplied, || Some("ignored".to_owned())).is_err()
            );
        }
    }

    #[test]
    fn local_release_warns_and_uses_dev_even_when_git_is_available() {
        let resolved = resolve_git_sha("release", false, None, || Some("12345678".to_owned()))
            .expect("local release provenance");

        assert_eq!(resolved.value, "dev");
        assert!(resolved.warn_local_release);
    }

    #[test]
    fn debug_prefers_injected_then_git_then_dev() {
        let injected = resolve_git_sha("debug", false, Some("abcdef12"), || None)
            .expect("injected provenance");
        let git = resolve_git_sha("debug", false, None, || Some("12345678".to_owned()))
            .expect("git provenance");
        let degraded = resolve_git_sha("debug", false, None, || None).expect("degraded provenance");

        assert_eq!(injected.value, "abcdef12");
        assert_eq!(git.value, "12345678");
        assert_eq!(degraded.value, "dev");
    }

    #[test]
    fn source_revision_prefers_manifest_then_injected_then_supplied_then_live_git() {
        let manifest = "a".repeat(40);
        let injected = "b".repeat(40);
        let supplied = "c".repeat(40);
        let live = "d".repeat(40);
        assert_eq!(
            resolve_source_revision(
                Some(&manifest),
                Some(&injected),
                Some(&supplied),
                || Some(live.clone()),
                "short"
            ),
            manifest
        );
        assert_eq!(
            resolve_source_revision(
                Some("abcdef12"),
                Some(&injected),
                Some(&supplied),
                || None,
                "short"
            ),
            injected,
            "a short manifest value is not a full revision"
        );
        assert_eq!(
            resolve_source_revision(None, None, Some(&supplied), || Some(live.clone()), "short"),
            supplied
        );
        assert_eq!(
            resolve_source_revision(None, None, Some("abcdef12"), || Some(live.clone()), "short"),
            live
        );
        assert_eq!(
            resolve_source_revision(None, None, None, || None, "dev"),
            "dev",
            "without any revision the short provenance stands in"
        );
    }
}
