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
}
