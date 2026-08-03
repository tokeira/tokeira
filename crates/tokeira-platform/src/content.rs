//! Small content-identity utility shared by desired resource manifests.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Versioned SHA-256 identity of one domain-separated non-secret value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentIdentity {
    /// Domain preventing equal bytes in different roles from sharing identity.
    pub domain: String,
    /// Lowercase SHA-256 digest.
    pub sha256: String,
}

impl ContentIdentity {
    /// Hash explicitly supplied non-secret content.
    pub fn new(domain: &str, content: &[u8]) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"tokeira.content.v1\0");
        digest.update((domain.len() as u64).to_be_bytes());
        digest.update(domain.as_bytes());
        digest.update((content.len() as u64).to_be_bytes());
        digest.update(content);
        Self {
            domain: domain.to_string(),
            sha256: hex::encode(digest.finalize()),
        }
    }

    /// Render the digest in the conventional manifest form.
    pub fn prefixed_sha256(&self) -> String {
        format!("sha256:{}", self.sha256)
    }
}
