//! Engine identity and build authority (task 16.1; Proposal 005).
//!
//! [`EngineIdentity`] answers *"are two `tkp` binaries interchangeable?"* — a
//! content key over the provisioner's **dependency closure**, deliberately not
//! the whole workspace (Req 15.2). This completes task 14.1's narrowing: a
//! `tkr`-only dep bump must not re-key every deployment's engine identity
//! (over-invalidation is the make-or-break risk of Proposal 005).
//!
//! [`BuildAuthority`] answers the orthogonal question *"who built the bytes,
//! under what controls?"* Identity says bytes are interchangeable; authority
//! says they are **trusted**. Because Rust builds are not bit-reproducible, a
//! `LocalDeveloper` build and a `TrustedCi` build of the *same* identity are
//! different bytes — so admission (task 16.2) gates on authority separately,
//! and the bundle cache is partitioned by it (Proposal 005, guardrail 1).
//!
//! The identity's inputs are supplied by the build pipeline: the source-closure
//! digest is computed over the immutable source snapshot (task 17), and the
//! hermetic container build (task 18) pins the rest. Native dev builds carry no
//! `EngineIdentity` at all (an [`IntegrityManifest`](crate::IntegrityManifest)
//! with `engine_identity: None`) — there are no partially-known identities.
//!
//! `RUSTFLAGS`/link configuration is not a separate field: for the hermetic
//! canonical build it is determined by the build container (pinned by digest)
//! plus `.cargo` config inside the source closure. If an out-of-band flag
//! channel is ever added to the canonical build, it must join the identity
//! (open decision 1 records this).

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::Sha256Digest;

/// Version tag of the canonical serialization [`EngineIdentity::digest`] is
/// computed over. Changing the identity field set (or its encoding) must change
/// this tag, so digests from different canonical forms can never collide.
const IDENTITY_CANONICAL_VERSION: &str = "tokeira-engine-identity/v1";

/// The cargo build profile a provisioner artifact was produced under.
///
/// The workspace defines three: `dev` (iteration), `release` (diagnosable
/// optimized), and `dist` (shipped artifacts).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildProfile {
    Dev,
    Release,
    Dist,
}

impl BuildProfile {
    fn canonical(self) -> &'static str {
        match self {
            BuildProfile::Dev => "dev",
            BuildProfile::Release => "release",
            BuildProfile::Dist => "dist",
        }
    }
}

/// What makes two `tkp` binaries interchangeable (task 16.1, Req 15.2).
///
/// Closure-scoped: every field is an input that changes the produced
/// provisioner, and nothing outside the provisioner's closure is included —
/// the deployment definition digest is deliberately absent (config is data;
/// the same engine interprets many definitions), and so is anything reachable
/// only from `tkr`.
///
/// Two identities are the same key iff [`digest`](Self::digest) matches; the
/// digest is over a versioned canonical serialization, not the serde JSON form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineIdentity {
    /// Digest of the provisioner's source closure, computed over the immutable
    /// source snapshot (task 17) — never over a live working tree.
    pub source_closure: Sha256Digest,
    /// Digest of the **locked** dependency set reachable from the provisioner
    /// bin target — not the whole workspace `Cargo.lock`.
    pub lock_closure: Sha256Digest,
    /// The exact toolchain that compiled the artifact (e.g.
    /// `rustc 1.88.0 (6b00bc388 2025-06-23)`).
    pub toolchain: String,
    /// The hermetic build container, pinned by image digest — never a floating
    /// tag. `None` means a native host build (the dev loop), which is
    /// non-hermetic by construction and never cache-canonical.
    pub build_container: Option<Sha256Digest>,
    /// Enabled cargo features of the provisioner bin target (e.g.
    /// `provisioner`). Ordered canonically by the set.
    pub features: BTreeSet<String>,
    /// The cargo profile the artifact was built under.
    pub profile: BuildProfile,
}

impl EngineIdentity {
    /// The canonical byte serialization the identity digest is computed over.
    ///
    /// Fields are encoded as `tag=<len>:<value>\n` with an explicit length
    /// prefix, so a crafted value containing delimiters (a `\n` in a toolchain
    /// string, a `,` ambiguity in features) cannot make two distinct identities
    /// serialize identically. The form opens with
    /// [`IDENTITY_CANONICAL_VERSION`], so evolving the field set re-keys
    /// explicitly rather than colliding silently.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        fn field(buf: &mut Vec<u8>, tag: &str, value: &str) {
            buf.extend_from_slice(tag.as_bytes());
            buf.push(b'=');
            buf.extend_from_slice(value.len().to_string().as_bytes());
            buf.push(b':');
            buf.extend_from_slice(value.as_bytes());
            buf.push(b'\n');
        }

        let mut buf = Vec::new();
        field(&mut buf, "canonical", IDENTITY_CANONICAL_VERSION);
        field(&mut buf, "source_closure", &self.source_closure.to_hex());
        field(&mut buf, "lock_closure", &self.lock_closure.to_hex());
        field(&mut buf, "toolchain", &self.toolchain);
        field(
            &mut buf,
            "build_container",
            &self
                .build_container
                .map(Sha256Digest::to_hex)
                .unwrap_or_else(|| "native".to_string()),
        );
        // Each feature is its own length-prefixed field: a set is not encoded
        // as a joined string, so no join character can alias two sets.
        for feature in &self.features {
            field(&mut buf, "feature", feature);
        }
        field(&mut buf, "profile", self.profile.canonical());
        buf
    }

    /// The identity's content digest — the store/cache key
    /// (`EngineIdentity × target` addresses an artifact; task 16.2).
    pub fn digest(&self) -> Sha256Digest {
        Sha256Digest::from_bytes(&self.canonical_bytes())
    }
}

/// Who built a provisioner artifact, and under what controls (Proposal 005).
///
/// Orthogonal to [`BuildMode`](crate::BuildMode): `BuildMode` governs the
/// *binding* gate (running vs recorded engine), `BuildAuthority` the
/// *admission* gate (are these bytes trusted for this deployment?). The
/// default is the lowest tier — trust is opted into, never assumed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BuildAuthority {
    /// Built on a developer workstation (the dev loop). Lowest trust.
    #[default]
    LocalDeveloper,
    /// Built by trusted CI from a pinned, reachable source commit.
    TrustedCi {
        /// The CI provider (e.g. `buildkite`).
        provider: String,
        /// The provider's build identifier, for audit.
        build_id: String,
        /// The immutable source commit the build was pinned to.
        source_commit: String,
    },
}

/// The ordered trust rank of a [`BuildAuthority`] — the admission gate compares
/// offered rank against the deployment's required rank (task 16.2). Audit
/// metadata (build id, commit) does not participate in ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityTier {
    LocalDeveloper,
    TrustedCi,
}

impl BuildAuthority {
    /// This authority's trust rank.
    pub fn tier(&self) -> AuthorityTier {
        match self {
            BuildAuthority::LocalDeveloper => AuthorityTier::LocalDeveloper,
            BuildAuthority::TrustedCi { .. } => AuthorityTier::TrustedCi,
        }
    }

    /// Whether this authority satisfies a deployment's `required` tier: a hit
    /// from a lower-trust tier is inadmissible for a higher-trust deployment
    /// (a laptop build must never satisfy a production create — Proposal 005,
    /// guardrail 1).
    pub fn satisfies(&self, required: AuthorityTier) -> bool {
        self.tier() >= required
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_of(byte: u8) -> Sha256Digest {
        Sha256Digest::from_bytes(&[byte])
    }

    fn identity() -> EngineIdentity {
        EngineIdentity {
            source_closure: digest_of(1),
            lock_closure: digest_of(2),
            toolchain: "rustc 1.88.0 (6b00bc388 2025-06-23)".to_string(),
            build_container: Some(digest_of(3)),
            features: ["provisioner".to_string()].into(),
            profile: BuildProfile::Dist,
        }
    }

    #[test]
    fn equal_identities_digest_equally() {
        assert_eq!(identity().digest(), identity().digest());
    }

    #[test]
    fn every_field_discriminates_the_digest() {
        let base = identity().digest();
        let variants = [
            EngineIdentity {
                source_closure: digest_of(9),
                ..identity()
            },
            EngineIdentity {
                lock_closure: digest_of(9),
                ..identity()
            },
            EngineIdentity {
                toolchain: "rustc 1.89.0".to_string(),
                ..identity()
            },
            EngineIdentity {
                build_container: Some(digest_of(9)),
                ..identity()
            },
            EngineIdentity {
                build_container: None,
                ..identity()
            },
            EngineIdentity {
                features: BTreeSet::new(),
                ..identity()
            },
            EngineIdentity {
                profile: BuildProfile::Release,
                ..identity()
            },
        ];
        for (i, variant) in variants.iter().enumerate() {
            assert_ne!(
                variant.digest(),
                base,
                "variant {i} must change the identity digest"
            );
        }
    }

    #[test]
    fn canonical_form_resists_delimiter_injection() {
        // A toolchain string that textually contains what looks like the next
        // field must not collide with an identity where that text IS the next
        // field's value: the length prefix keeps field boundaries unambiguous.
        let smuggled = EngineIdentity {
            toolchain: "rustc\nbuild_container=6:native".to_string(),
            build_container: Some(digest_of(3)),
            ..identity()
        };
        let honest = EngineIdentity {
            toolchain: "rustc".to_string(),
            build_container: None,
            ..identity()
        };
        assert_ne!(smuggled.digest(), honest.digest());
    }

    #[test]
    fn feature_sets_cannot_alias_across_boundaries() {
        // {"a,b"} and {"a","b"} must be distinct keys — features are encoded
        // as separate length-prefixed fields, never joined.
        let joined = EngineIdentity {
            features: ["a,b".to_string()].into(),
            ..identity()
        };
        let split = EngineIdentity {
            features: ["a".to_string(), "b".to_string()].into(),
            ..identity()
        };
        assert_ne!(joined.digest(), split.digest());
    }

    #[test]
    fn identity_round_trips_through_serde() {
        let id = identity();
        let json = serde_json::to_string(&id).expect("serialize");
        let back: EngineIdentity = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(id, back);
        assert_eq!(id.digest(), back.digest());
    }

    #[test]
    fn authority_tiers_order_local_below_ci() {
        assert!(AuthorityTier::LocalDeveloper < AuthorityTier::TrustedCi);
        let local = BuildAuthority::LocalDeveloper;
        let ci = BuildAuthority::TrustedCi {
            provider: "buildkite".to_string(),
            build_id: "b-1".to_string(),
            source_commit: "abc".to_string(),
        };
        assert!(local.satisfies(AuthorityTier::LocalDeveloper));
        assert!(!local.satisfies(AuthorityTier::TrustedCi));
        assert!(ci.satisfies(AuthorityTier::LocalDeveloper));
        assert!(ci.satisfies(AuthorityTier::TrustedCi));
    }

    #[test]
    fn default_authority_is_the_lowest_tier() {
        assert_eq!(
            BuildAuthority::default().tier(),
            AuthorityTier::LocalDeveloper
        );
    }
}
