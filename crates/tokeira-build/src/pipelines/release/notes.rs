//! Artifact parity and deterministic changelog-authored release notes.

use super::{PackageOutcome, PackageResult, ReleaseError, digest::sha256_hex};

/// Prove equality of hermetic bytes, downloaded bytes, and registry metadata.
pub fn verify_artifact_parity(
    package: &str,
    version: &str,
    hermetic_bytes: &[u8],
    downloaded_bytes: &[u8],
    registry_sha256: &str,
) -> Result<String, ReleaseError> {
    let hermetic = sha256_hex(hermetic_bytes);
    let downloaded = sha256_hex(downloaded_bytes);
    if hermetic == downloaded && downloaded == registry_sha256 {
        return Ok(hermetic);
    }
    Err(ReleaseError::ArtifactMismatch {
        package: package.to_owned(),
        version: version.to_owned(),
        hermetic,
        downloaded,
        registry: registry_sha256.to_owned(),
    })
}

/// Append consumer facts to the exact changie version body.
pub fn generate_release_notes(
    version_body: &str,
    packages: &[PackageResult],
) -> Result<Vec<u8>, ReleaseError> {
    if let Some(package) = packages.iter().find(|package| {
        !matches!(
            package.outcome,
            PackageOutcome::Published | PackageOutcome::ExistingVerified
        ) || package.hermetic_sha256.is_none()
            || package.downloaded_sha256.is_none()
            || package.registry_sha256.is_none()
            || package.readme_url.is_none()
    }) {
        return Err(ReleaseError::Plan {
            reason: format!(
                "release notes require complete parity evidence for {} {}",
                package.name, package.version
            ),
        });
    }

    let mut packages = packages.iter().collect::<Vec<_>>();
    packages.sort_by(|left, right| left.name.cmp(&right.name));
    let mut notes = version_body.trim_end().to_owned();
    if !notes.is_empty() {
        notes.push_str("\n\n");
    }
    notes.push_str("Requires Rust 1.97 or newer.\n\n");
    notes.push_str("| Package | Version | SHA-256 | crates.io | README |\n");
    notes.push_str("|---|---:|---|---|---|\n");
    for package in packages {
        let checksum = package
            .registry_sha256
            .as_deref()
            .expect("complete package evidence was checked above");
        let readme = package
            .readme_url
            .as_deref()
            .expect("complete package evidence was checked above");
        notes.push_str(&format!(
            "| `{}` | `{}` | `{}` | [package](https://crates.io/crates/{}/{}) | [README]({}) |\n",
            package.name, package.version, checksum, package.name, package.version, readme
        ));
    }
    Ok(notes.into_bytes())
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn package(name: String, bytes: &[u8]) -> PackageResult {
        let digest = sha256_hex(bytes);
        PackageResult {
            name: name.clone(),
            version: "1.2.3".to_owned(),
            outcome: PackageOutcome::ExistingVerified,
            hermetic_sha256: Some(digest.clone()),
            downloaded_sha256: Some(digest.clone()),
            registry_sha256: Some(digest),
            readme_url: Some(format!(
                "https://crates.io/api/v1/crates/{name}/1.2.3/readme"
            )),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        // Feature: release-engineering, Property 11: Artifact Parity is three-way equality
        #[test]
        fn parity_is_exactly_three_way_equality(
            local in proptest::collection::vec(any::<u8>(), 0..128),
            downloaded in proptest::collection::vec(any::<u8>(), 0..128),
            registry_matches_download in any::<bool>(),
        ) {
            let registry = if registry_matches_download {
                sha256_hex(&downloaded)
            } else {
                "0".repeat(64)
            };
            let admitted = verify_artifact_parity(
                "example",
                "1.0.0",
                &local,
                &downloaded,
                &registry,
            )
            .is_ok();
            prop_assert_eq!(
                admitted,
                local == downloaded && sha256_hex(&downloaded) == registry
            );
        }

        // Feature: release-engineering, Property 12: release notes are deterministic and changelog-authored
        #[test]
        fn notes_preserve_body_and_sort_packages(
            body in "[A-Za-z0-9 .*-]{0,120}",
            names in proptest::collection::btree_set("[a-z][a-z0-9-]{0,12}", 1..12),
        ) {
            let packages = names
                .iter()
                .rev()
                .map(|name| package(name.clone(), name.as_bytes()))
                .collect::<Vec<_>>();
            let first = generate_release_notes(&body, &packages).expect("complete evidence");
            let second = generate_release_notes(&body, &packages).expect("complete evidence");
            let text = String::from_utf8(first.clone()).expect("notes are UTF-8");

            prop_assert_eq!(first, second);
            prop_assert!(text.starts_with(body.trim_end()));
            prop_assert!(text.contains("Rust 1.97 or newer"));
            let mut previous = 0;
            for name in names {
                let position = text.find(&format!("`{name}`")).expect("package row");
                prop_assert!(position >= previous);
                previous = position;
            }
        }
    }
}
