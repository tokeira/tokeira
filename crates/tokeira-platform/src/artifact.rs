//! Platform-owned artifact declarations and provider-neutral content identity.

use std::{
    collections::BTreeSet,
    marker::PhantomData,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable key selecting provider-owned delivery mechanics.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeliveryKey(String);

impl DeliveryKey {
    /// Construct a non-empty delivery key.
    pub fn new(value: impl Into<String>) -> Result<Self, crate::error::BindingError> {
        let value = value.into();
        if value.is_empty() {
            return Err(crate::error::BindingError::new(
                "provider delivery key cannot be empty",
            ));
        }
        Ok(Self(value))
    }

    /// Borrow the delivery key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Provider-defined structured desired content selected by a platform.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DesiredDocument {
    /// Provider-owned schema identity.
    pub schema: String,
    /// Platform-populated provider document.
    pub value: serde_json::Value,
}

/// Provider-validated canonical bytes for one platform-owned desired document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalDocument {
    /// Deterministic provider representation.
    pub bytes: Vec<u8>,
}

/// Platform-owned non-secret artifact content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DesiredContent {
    /// UTF-8 source content.
    Text(String),
    /// Opaque non-secret bytes.
    Bytes(Vec<u8>),
    /// Authoritative non-secret input already present in the deployment directory.
    DeploymentFile(RelativeArtifactPath),
}

/// Validated deployment-relative path for authoritative artifact input.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct RelativeArtifactPath(PathBuf);

impl RelativeArtifactPath {
    /// Construct a canonical relative path that cannot escape a deployment root lexically.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, crate::error::BindingError> {
        let path = path.into();
        if path.as_os_str().is_empty()
            || path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(crate::error::BindingError::new(format!(
                "artifact source path `{}` is not a safe canonical relative path",
                path.display()
            )));
        }
        Ok(Self(path))
    }

    /// Borrow the validated relative path.
    pub fn as_path(&self) -> &Path {
        &self.0
    }
}

impl<'de> Deserialize<'de> for RelativeArtifactPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = PathBuf::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Whether an artifact is consumed operationally or published only for inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ArtifactClass {
    /// Provider materialization may be consumed by declared workloads during apply.
    Operational,
    /// Reproducible output that no lifecycle command reads as desired state.
    Inspection,
}

/// Engine universe in which an operational artifact's declared consumers converge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationalArtifactStage {
    /// Consumers realized as ordinary infrastructure resources.
    Infrastructure,
    /// Consumers realized through the separate deploy-engine workload universe.
    Workload,
}

/// Reference from a platform service to one platform-owned artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactUse {
    /// Artifact logical identity.
    pub artifact: String,
    /// Provider-specific mount, object, or reference role.
    pub role: String,
}

/// One platform-owned desired artifact declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlatformArtifact {
    /// Stable logical identity within the platform binding.
    pub logical_id: String,
    /// Operational or inspection ownership boundary.
    pub class: ArtifactClass,
    /// Exact platform-owned desired bytes.
    pub content: DesiredContent,
    /// Logical services permitted to consume operational materialization.
    pub consumers: Vec<String>,
    /// Selected provider delivery mechanics.
    pub delivery: DeliveryKey,
}

/// Borrowed apply-time request to publish one declared operational artifact.
#[derive(Debug, Clone, Copy)]
pub struct OperationalArtifactRequest<'a> {
    /// Platform-owned declaration.
    pub artifact: &'a PlatformArtifact,
    /// Exact resolved bytes selected for this apply invocation.
    pub content: &'a [u8],
    /// Deterministic non-secret content identity carried by consumers.
    pub identity: &'a ContentIdentity,
}

/// Provider-owned evidence recording where operational content was published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationalArtifactReceipt {
    /// Logical platform artifact identity.
    pub artifact: String,
    /// Provider reference or deployment-relative path used by declared consumers.
    pub provider_reference: String,
    /// Exact content identity published.
    pub identity: ContentIdentity,
    /// Consumers authorized by the platform declaration.
    pub consumers: Vec<String>,
}

/// Ordered provider receipts produced before declared consumers converge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperationalArtifactReceipts {
    receipts: Vec<OperationalArtifactReceipt>,
}

impl OperationalArtifactReceipts {
    /// Construct receipts after provider responses have passed framework validation.
    pub(crate) fn new(receipts: Vec<OperationalArtifactReceipt>) -> Self {
        Self { receipts }
    }

    /// Borrow receipts in platform artifact declaration order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &OperationalArtifactReceipt> {
        self.receipts.iter()
    }
}

/// Versioned SHA-256 identity of one domain-separated non-secret content value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentIdentity {
    /// Domain that prevents identical bytes in different roles sharing an identity.
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
}

/// Deterministically ordered identities consumed by one workload.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContentIdentitySet {
    identities: Vec<(ArtifactUse, ContentIdentity)>,
}

impl ContentIdentitySet {
    /// Construct a set after rejecting duplicate logical artifact identities.
    pub fn new(
        identities: Vec<(ArtifactUse, ContentIdentity)>,
    ) -> Result<Self, crate::error::BindingError> {
        let mut seen = BTreeSet::new();
        for (use_, _) in &identities {
            if !seen.insert((use_.artifact.clone(), use_.role.clone())) {
                return Err(crate::error::BindingError::new(format!(
                    "duplicate content identity `{}` for role `{}`",
                    use_.artifact, use_.role
                )));
            }
        }
        Ok(Self { identities })
    }

    /// Iterate over artifact uses and identities in service declaration order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&ArtifactUse, &ContentIdentity)> {
        self.identities
            .iter()
            .map(|(use_, identity)| (use_, identity))
    }
}

/// Platform-supplied renderer for one reproducible inspection artifact.
#[derive(Clone)]
pub struct InspectionSpec<P> {
    /// Safe deployment-relative publication path.
    pub path: PathBuf,
    /// Stable renderer identity.
    pub renderer: String,
    marker: PhantomData<fn() -> P>,
}

impl<P> std::fmt::Debug for InspectionSpec<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectionSpec")
            .field("path", &self.path)
            .field("renderer", &self.renderer)
            .finish()
    }
}

impl<P> InspectionSpec<P> {
    /// Construct an inspection declaration; path safety is validated by the binding.
    pub fn new(path: PathBuf, renderer: impl Into<String>) -> Self {
        Self {
            path,
            renderer: renderer.into(),
            marker: PhantomData,
        }
    }
}

/// Immutable artifact inventory supplied by one platform package.
#[derive(Debug, Clone)]
pub struct ArtifactCatalog<P> {
    entries: Vec<PlatformArtifact>,
    marker: PhantomData<fn() -> P>,
}

impl<P> ArtifactCatalog<P> {
    /// Construct a platform-owned artifact catalog.
    pub fn new(entries: Vec<PlatformArtifact>) -> Self {
        Self {
            entries,
            marker: PhantomData,
        }
    }

    /// Borrow declarations in platform order.
    pub fn entries(&self) -> &[PlatformArtifact] {
        &self.entries
    }

    /// Resolve one logical artifact without constructing a second inventory.
    pub fn get(&self, logical_id: &str) -> Option<&PlatformArtifact> {
        self.entries
            .iter()
            .find(|artifact| artifact.logical_id == logical_id)
    }
}

impl<P> Default for ArtifactCatalog<P> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
