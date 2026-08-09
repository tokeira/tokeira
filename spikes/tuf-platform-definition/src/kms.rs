//! AWS KMS as a TUF signing backend, through `tough-kms`.
//!
//! The claim under test is substitutability: a KMS-held key is just another
//! `KeySource`, so `author_root` and `publish_set` take it without change —
//! the private key never leaves KMS, and the publisher's AWS identity needs
//! only `kms:GetPublicKey` + `kms:Sign` on the key.
//!
//! Constraints found (recorded in the README):
//!
//! - `tough-kms` 0.16 supports **RSA keys only**, signing with
//!   `RSASSA_PSS_SHA_256` (`KmsSigningAlgorithm` has a single variant).
//!   Asymmetric ECC KMS keys are not usable; Ed25519 is not offered by KMS
//!   at all. A KMS-backed role therefore appears in root.json as an `rsa`
//!   key while file-backed roles can stay `ed25519` — TUF is fine with the
//!   mix.
//! - The sensible split: online roles (targets/snapshot/timestamp) on KMS
//!   where the publishing pipeline runs, root on an offline key (or its own
//!   KMS key in a separately-guarded account).
//!
//! Nothing here talks to AWS in tests; construction is pure configuration.
//! The CLI wires it live when `--kms-key-id` is passed.

use tough_kms::{KmsKeySource, KmsSigningAlgorithm};

use crate::publish::SharedKeySource;

/// A role key held in KMS. `key_id` may be a key id, key ARN, or alias.
///
/// With `client: None`, `tough-kms` builds a client from the ambient AWS
/// configuration (profile/region resolution) at signing time.
pub fn kms_role_key(profile: Option<String>, key_id: String) -> SharedKeySource {
    SharedKeySource::new(KmsKeySource {
        profile,
        key_id,
        client: None,
        signing_algorithm: KmsSigningAlgorithm::RsassaPssSha256,
    })
}
