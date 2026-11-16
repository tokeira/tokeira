//! The GitHub CLI release that the release train's GitHub step runs.
//!
//! The GitHub CLI publishes no container image, so the release-notes step
//! installs this release from its upstream Linux archive, checked against the
//! SHA-256 that the release's checksums file publishes.

/// One upstream GitHub CLI archive for a Linux executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GithubCliAsset {
    /// `uname -m` values of the executors that run this archive.
    pub(crate) machines: &'static [&'static str],
    /// The archive's top-level directory, which holds `bin/gh`.
    pub(crate) directory: &'static str,
    /// HTTPS download URL of the immutable release asset.
    pub(crate) url: &'static str,
    /// Lowercase SHA-256 published for the archive.
    pub(crate) sha256: &'static str,
}

/// One immutable GitHub CLI release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GithubCliRelease {
    /// Version reported by `gh --version`.
    pub(crate) version: &'static str,
    /// The Linux archives, one per executor architecture.
    pub(crate) assets: &'static [GithubCliAsset],
}

/// The sole GitHub CLI release accepted by the release train.
pub(crate) const GITHUB_CLI_RELEASE: GithubCliRelease = GithubCliRelease {
    version: "2.65.0",
    assets: &[
        GithubCliAsset {
            machines: &["x86_64"],
            directory: "gh_2.65.0_linux_amd64",
            url: "https://github.com/cli/cli/releases/download/v2.65.0/gh_2.65.0_linux_amd64.tar.gz",
            sha256: "762569efe785082b7d1feb06995efece1a9cecce16da8503ac6fdbcbea04085b",
        },
        GithubCliAsset {
            machines: &["aarch64", "arm64"],
            directory: "gh_2.65.0_linux_arm64",
            url: "https://github.com/cli/cli/releases/download/v2.65.0/gh_2.65.0_linux_arm64.tar.gz",
            sha256: "8bcec9f3ee5c7c3700359a75da774c71221064a0ba017537795aa32ac8bbb481",
        },
    ],
};

/// Shell that installs [`GITHUB_CLI_RELEASE`] as `/usr/local/bin/gh` on a Linux
/// executor that has `curl`, `sha256sum` and `tar`.
pub(crate) fn github_cli_acquisition_script() -> String {
    let cases = GITHUB_CLI_RELEASE
        .assets
        .iter()
        .map(|asset| {
            format!(
                "  {}) url='{}'; sha='{}'; dir='{}' ;;\n",
                asset.machines.join("|"),
                asset.url,
                asset.sha256,
                asset.directory
            )
        })
        .collect::<String>();
    format!(
        r#"set -eu
case "$(uname -m)" in
{cases}  *) echo 'unsupported gh executor architecture' >&2; exit 3 ;;
esac
curl --fail --location --silent --show-error --proto '=https' --proto-redir '=https' --output /tmp/gh.tar.gz "$url"
printf '%s  %s\n' "$sha" /tmp/gh.tar.gz | sha256sum --check --strict
tar -xzf /tmp/gh.tar.gz -C /tmp "$dir/bin/gh"
install -m 0755 "/tmp/$dir/bin/gh" /usr/local/bin/gh
gh --version | grep -F 'gh version {version} ' >/dev/null
"#,
        version = GITHUB_CLI_RELEASE.version,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_release_pin_is_complete() {
        assert_eq!(GITHUB_CLI_RELEASE.version, "2.65.0");
        assert_eq!(GITHUB_CLI_RELEASE.assets.len(), 2);
        for asset in GITHUB_CLI_RELEASE.assets {
            assert_eq!(asset.sha256.len(), 64);
            assert!(
                asset.url.starts_with(&format!(
                    "https://github.com/cli/cli/releases/download/v{}/{}",
                    GITHUB_CLI_RELEASE.version, asset.directory
                )),
                "{}",
                asset.url
            );
        }
    }

    #[test]
    fn acquisition_verifies_each_archive_and_the_installed_version() {
        let script = github_cli_acquisition_script();
        for asset in GITHUB_CLI_RELEASE.assets {
            assert!(script.contains(asset.url));
            assert!(script.contains(asset.sha256));
        }
        assert!(script.contains("  x86_64) "));
        assert!(script.contains("  aarch64|arm64) "));
        assert!(script.contains("sha256sum --check --strict"));
        assert!(script.contains("grep -F 'gh version 2.65.0 '"));
    }
}
