use serde::{Deserialize, Serialize};

use crate::CompatibilityEvidence;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SdkVerificationState {
    Untested,
    SmokeTested,
    ConformancePartial,
    ConformancePassing,
}

impl SdkVerificationState {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Untested => "untested",
            Self::SmokeTested => "smoke-tested",
            Self::ConformancePartial => "conformance-partial",
            Self::ConformancePassing => "conformance-passing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncompatibleVersion {
    pub version: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SdkCompatEntry {
    pub language: &'static str,
    pub minimum_supported_version: &'static str,
    pub maximum_tested_version: &'static str,
    pub known_incompatible: &'static [IncompatibleVersion],
    pub verification_state: SdkVerificationState,
    pub evidence: &'static [CompatibilityEvidence],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedSdkCompatEntry {
    pub language: String,
    pub minimum_supported_version: String,
    pub maximum_tested_version: String,
    pub known_incompatible: Vec<OwnedIncompatibleVersion>,
    pub verification_state: SdkVerificationState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnedIncompatibleVersion {
    pub version: String,
    pub reason: String,
}

pub const SDK_MATRIX: &[SdkCompatEntry] = &[
    SdkCompatEntry {
        language: ".NET",
        minimum_supported_version: "1.0.0",
        maximum_tested_version: "unknown",
        known_incompatible: &[],
        verification_state: SdkVerificationState::Untested,
        evidence: &[],
    },
    SdkCompatEntry {
        language: "Go",
        minimum_supported_version: "1.0.0",
        maximum_tested_version: "unknown",
        known_incompatible: &[],
        verification_state: SdkVerificationState::Untested,
        evidence: &[],
    },
    SdkCompatEntry {
        language: "Java",
        minimum_supported_version: "1.0.0",
        maximum_tested_version: "unknown",
        known_incompatible: &[],
        verification_state: SdkVerificationState::Untested,
        evidence: &[],
    },
    SdkCompatEntry {
        language: "Python",
        minimum_supported_version: "1.0.0",
        maximum_tested_version: "unknown",
        known_incompatible: &[],
        verification_state: SdkVerificationState::Untested,
        evidence: &[],
    },
    SdkCompatEntry {
        language: "TypeScript",
        minimum_supported_version: "1.0.0",
        maximum_tested_version: "unknown",
        known_incompatible: &[],
        verification_state: SdkVerificationState::Untested,
        evidence: &[],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_matrix_includes_required_languages() {
        let languages = SDK_MATRIX
            .iter()
            .map(|entry| entry.language)
            .collect::<Vec<_>>();
        assert!(languages.contains(&"Go"));
        assert!(languages.contains(&"TypeScript"));
        assert!(languages.contains(&"Python"));
        assert!(languages.contains(&"Java"));
        assert!(languages.contains(&".NET"));
    }

    #[test]
    fn sdk_matrix_json_round_trips_to_owned_entries() {
        let json = serde_json::to_string(SDK_MATRIX).expect("SDK matrix serializes");
        let decoded: Vec<OwnedSdkCompatEntry> =
            serde_json::from_str(&json).expect("SDK matrix decodes");

        assert_eq!(decoded.len(), SDK_MATRIX.len());
        for (decoded, source) in decoded.iter().zip(SDK_MATRIX) {
            assert_eq!(decoded.language, source.language);
            assert_eq!(
                decoded.minimum_supported_version,
                source.minimum_supported_version
            );
            assert_eq!(
                decoded.maximum_tested_version,
                source.maximum_tested_version
            );
            assert_eq!(decoded.verification_state, source.verification_state);
            assert_eq!(
                decoded.known_incompatible.len(),
                source.known_incompatible.len()
            );
        }
    }

    #[test]
    fn sdk_version_bounds_are_ordered_when_known() {
        for entry in SDK_MATRIX {
            let Some(minimum) = parse_semver_like(entry.minimum_supported_version) else {
                panic!("minimum version must be concrete: {}", entry.language);
            };

            if let Some(maximum) = parse_semver_like(entry.maximum_tested_version) {
                assert!(
                    minimum <= maximum,
                    "{} has minimum version greater than maximum tested version",
                    entry.language
                );
                assert!(
                    !entry
                        .known_incompatible
                        .iter()
                        .any(|version| version.version == entry.maximum_tested_version),
                    "{} marks maximum tested version as incompatible",
                    entry.language
                );
            } else {
                assert_eq!(
                    entry.maximum_tested_version, "unknown",
                    "{} maximum version must be semver or unknown",
                    entry.language
                );
            }

            for incompatible in entry.known_incompatible {
                assert!(
                    !incompatible.reason.trim().is_empty(),
                    "{} incompatible version lacks a reason",
                    entry.language
                );
                assert!(
                    parse_semver_like(incompatible.version).is_some(),
                    "{} incompatible version is not semver-like",
                    entry.language
                );
            }
        }
    }

    fn parse_semver_like(value: &str) -> Option<(u64, u64, u64)> {
        let mut parts = value.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next()?.parse().ok()?;
        let patch = parts.next()?.parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some((major, minor, patch))
    }
}
