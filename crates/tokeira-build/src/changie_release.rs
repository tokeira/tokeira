//! Immutable changie release metadata shared by host and Dagger resolvers.

/// One supported upstream changie archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangieAsset {
    /// Stable platform selector used by the resolver.
    pub platform: &'static str,
    /// Exact upstream archive filename.
    pub name: &'static str,
    /// HTTPS download URL for the immutable release asset.
    pub url: &'static str,
    /// Lowercase SHA-256 published for the archive.
    pub sha256: &'static str,
}

/// One immutable changie source and binary release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangieRelease {
    /// Version reported by `changie --version`.
    pub version: &'static str,
    /// Full upstream source revision corresponding to this release.
    pub source_revision: &'static str,
    /// Complete set of host and executor assets admitted by the release contract.
    pub assets: &'static [ChangieAsset],
}

const ASSETS: [ChangieAsset; 4] = [
    ChangieAsset {
        platform: "macos-x86_64",
        name: "changie_1.25.2_darwin_amd64.tar.gz",
        url: "https://github.com/miniscruff/changie/releases/download/v1.25.2/changie_1.25.2_darwin_amd64.tar.gz",
        sha256: "729561d13d45c2cdf0daef2c6eb494bf185135747bbdf600e4e0e586683f372b",
    },
    ChangieAsset {
        platform: "macos-aarch64",
        name: "changie_1.25.2_darwin_arm64.tar.gz",
        url: "https://github.com/miniscruff/changie/releases/download/v1.25.2/changie_1.25.2_darwin_arm64.tar.gz",
        sha256: "03205b2ddc042458693e4e8e1d663d0bcc1cec9c519e15e92c8b81a286e0977e",
    },
    ChangieAsset {
        platform: "linux-x86_64",
        name: "changie_1.25.2_linux_amd64.tar.gz",
        url: "https://github.com/miniscruff/changie/releases/download/v1.25.2/changie_1.25.2_linux_amd64.tar.gz",
        sha256: "7489b5a6a595e5a9f8b0d392114b10c130634639ef1190fafb2f15a5cd9058cd",
    },
    ChangieAsset {
        platform: "linux-aarch64",
        name: "changie_1.25.2_linux_arm64.tar.gz",
        url: "https://github.com/miniscruff/changie/releases/download/v1.25.2/changie_1.25.2_linux_arm64.tar.gz",
        sha256: "84c3f158906da24f9a4941518dcf55a2badf9524bfb9579c78b5e7876ae675fa",
    },
];

/// The sole changie release accepted by Tokeira release operations.
pub const CHANGIE_RELEASE: ChangieRelease = ChangieRelease {
    version: "1.25.2",
    source_revision: "8406ffac34697bd95d153550d0423e403fac9a90",
    assets: &ASSETS,
};

impl ChangieRelease {
    /// Select an asset by the resolver's stable platform key.
    pub fn asset(self, platform: &str) -> Option<ChangieAsset> {
        self.assets
            .iter()
            .copied()
            .find(|asset| asset.platform == platform)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immutable_release_pin_is_complete() {
        assert_eq!(CHANGIE_RELEASE.version, "1.25.2");
        assert_eq!(
            CHANGIE_RELEASE.source_revision,
            "8406ffac34697bd95d153550d0423e403fac9a90"
        );
        assert_eq!(CHANGIE_RELEASE.assets.len(), 4);
        assert!(CHANGIE_RELEASE.asset("windows-x86_64").is_none());
    }
}
