use crate::{FEATURE_MATRIX, SDK_MATRIX, feature::FeatureEntry, sdk::SdkCompatEntry};

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

pub fn feature_matrix_digest() -> String {
    feature_matrix_digest_for(FEATURE_MATRIX)
}

fn feature_matrix_digest_for(entries: &[FeatureEntry]) -> String {
    let mut hash = Fnv1a::new();
    for entry in entries {
        hash_feature_entry(&mut hash, entry);
    }
    hash.finish_hex()
}

pub fn sdk_matrix_digest() -> String {
    let mut hash = Fnv1a::new();
    for entry in SDK_MATRIX {
        hash_sdk_entry(&mut hash, entry);
    }
    hash.finish_hex()
}

fn hash_feature_entry(hash: &mut Fnv1a, entry: &FeatureEntry) {
    hash.write_str(entry.id);
    hash.write_str(entry.state.label());
    for surface in entry.surfaces {
        hash.write_str(surface.kind.label());
        hash.write_str(surface.identifier);
    }
    for evidence in entry.evidence {
        hash.write_str(evidence.kind.label());
        hash.write_str(evidence.reference);
    }
}

fn hash_sdk_entry(hash: &mut Fnv1a, entry: &SdkCompatEntry) {
    hash.write_str(entry.language);
    hash.write_str(entry.minimum_supported_version);
    hash.write_str(entry.maximum_tested_version);
    hash.write_str(entry.verification_state.label());
    for incompatible in entry.known_incompatible {
        hash.write_str(incompatible.version);
        hash.write_str(incompatible.reason);
    }
    for evidence in entry.evidence {
        hash.write_str(evidence.kind.label());
        hash.write_str(evidence.reference);
    }
}

#[derive(Debug, Clone, Copy)]
struct Fnv1a {
    value: u64,
}

impl Fnv1a {
    const fn new() -> Self {
        Self { value: FNV_OFFSET }
    }

    fn write_str(&mut self, value: &str) {
        for byte in value.as_bytes() {
            self.value ^= u64::from(*byte);
            self.value = self.value.wrapping_mul(FNV_PRIME);
        }
        self.write_separator();
    }

    fn write_separator(&mut self) {
        self.value ^= 0xff;
        self.value = self.value.wrapping_mul(FNV_PRIME);
    }

    fn finish_hex(self) -> String {
        format!("{:016x}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_are_stable_across_calls() {
        assert_eq!(feature_matrix_digest(), feature_matrix_digest());
        assert_eq!(sdk_matrix_digest(), sdk_matrix_digest());
    }

    #[test]
    fn feature_digest_changes_when_state_changes() {
        let mut entries = FEATURE_MATRIX.to_vec();
        entries[0].state = crate::FeatureState::Implemented;

        assert_ne!(feature_matrix_digest(), feature_matrix_digest_for(&entries));
    }

    #[test]
    fn feature_digest_changes_when_surface_changes() {
        let mut entries = FEATURE_MATRIX.to_vec();
        entries[0].surfaces = &[crate::CompatibilitySurface {
            kind: crate::CompatibilitySurfaceKind::Rpc,
            identifier: "WorkflowService.ChangedSurface",
        }];

        assert_ne!(feature_matrix_digest(), feature_matrix_digest_for(&entries));
    }

    #[test]
    fn feature_digest_changes_when_evidence_changes() {
        let mut entries = FEATURE_MATRIX.to_vec();
        entries[0].evidence = &[crate::CompatibilityEvidence {
            kind: crate::CompatibilityEvidenceKind::ManualReview,
            reference: "docs/changed-evidence.md",
        }];

        assert_ne!(feature_matrix_digest(), feature_matrix_digest_for(&entries));
    }
}
