//! Platform-owned artifact declarations and provider-neutral content identity.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    io::Write,
    marker::PhantomData,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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

impl CanonicalDocument {
    /// Validate one provider schema through its typed document and serialize canonical JSON bytes.
    ///
    /// Provider crates may wrap this baseline when their natural representation
    /// is YAML or another format, but the typed decode remains the point where
    /// platform-populated semantic content is admitted without additions.
    pub fn typed<T>(
        document: &DesiredDocument,
        expected_schema: &str,
    ) -> Result<Self, crate::error::DeliveryError>
    where
        T: serde::de::DeserializeOwned + Serialize,
    {
        if document.schema != expected_schema {
            return Err(crate::error::DeliveryError::new(format!(
                "expected provider document schema `{expected_schema}`, found `{}`",
                document.schema
            )));
        }
        let typed: T = serde_json::from_value(document.value.clone())
            .map_err(|source| crate::error::DeliveryError::new(source.to_string()))?;
        let bytes = serde_json::to_vec(&typed)
            .map_err(|source| crate::error::DeliveryError::new(source.to_string()))?;
        Ok(Self { bytes })
    }
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

/// Desired-only input supplied to a platform-owned inspection renderer.
#[derive(Debug, Clone, Copy)]
pub struct InspectionRenderRequest<'a, P: crate::binding::Platform> {
    /// Typed platform choices admitted from the definition.
    pub config: &'a P::Config,
    /// Immutable invocation facts admitted before definition evaluation.
    pub invocation: &'a crate::context::InvocationContext,
    /// Verified logical graph for the represented definition revision.
    pub graph: &'a crate::graph::VerifiedGraph,
    /// Complete platform-owned service catalog.
    pub services: &'a crate::catalog::ServiceCatalog<P>,
    content_by_service: &'a BTreeMap<String, ContentIdentitySet>,
}

impl<'a, P: crate::binding::Platform> InspectionRenderRequest<'a, P> {
    /// Borrow content identities carried by one active service's desired representation.
    pub fn content_for(&self, logical_service: &str) -> Option<&ContentIdentitySet> {
        self.content_by_service.get(logical_service)
    }

    pub(crate) fn new(
        config: &'a P::Config,
        invocation: &'a crate::context::InvocationContext,
        graph: &'a crate::graph::VerifiedGraph,
        services: &'a crate::catalog::ServiceCatalog<P>,
        content_by_service: &'a BTreeMap<String, ContentIdentitySet>,
    ) -> Self {
        Self {
            config,
            invocation,
            graph,
            services,
            content_by_service,
        }
    }
}

/// Platform-owned pure projection of verified desired state into inspection bytes.
pub trait InspectionRenderer<P: crate::binding::Platform>: std::fmt::Debug + Send + Sync {
    /// Stable renderer identity used in binding validation and errors.
    fn key(&self) -> &str;

    /// Render without provider calls, state reads, filesystem writes, or prior output bytes.
    fn render(
        &self,
        request: InspectionRenderRequest<'_, P>,
    ) -> Result<Vec<u8>, crate::error::InspectionRenderError>;
}

/// Platform-selected renderer and deployment-relative publication target.
#[derive(Clone)]
pub struct InspectionSpec<P: crate::binding::Platform> {
    path: RelativeArtifactPath,
    renderer: Arc<dyn InspectionRenderer<P>>,
}

impl<P: crate::binding::Platform> std::fmt::Debug for InspectionSpec<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InspectionSpec")
            .field("path", &self.path)
            .field("renderer", &self.renderer.key())
            .finish()
    }
}

impl<P: crate::binding::Platform> InspectionSpec<P> {
    /// Select one pure renderer for a validated deployment-relative path.
    pub fn new(path: RelativeArtifactPath, renderer: Arc<dyn InspectionRenderer<P>>) -> Self {
        Self { path, renderer }
    }

    /// Validated deployment-relative publication target.
    pub fn path(&self) -> &RelativeArtifactPath {
        &self.path
    }

    /// Stable selected renderer identity.
    pub fn renderer_key(&self) -> &str {
        self.renderer.key()
    }

    pub(crate) fn render(
        &self,
        request: InspectionRenderRequest<'_, P>,
    ) -> Result<RenderedInspection, crate::error::InspectionError> {
        let bytes = self.renderer.render(request).map_err(|source| {
            crate::error::InspectionError::Render {
                renderer: self.renderer.key().to_string(),
                path: self.path.as_path().to_path_buf(),
                source,
            }
        })?;
        Ok(RenderedInspection {
            path: self.path.clone(),
            bytes,
        })
    }
}

/// Evidence for one atomically replaced non-authoritative inspection artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InspectionPublication {
    /// Validated deployment-relative target.
    pub path: RelativeArtifactPath,
    /// Identity of the exact published bytes.
    pub identity: ContentIdentity,
}

/// Pure platform-rendered inspection bytes awaiting lifecycle-owned publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedInspection {
    path: RelativeArtifactPath,
    bytes: Vec<u8>,
}

impl RenderedInspection {
    /// Validated deployment-relative target selected by the platform binding.
    pub fn path(&self) -> &RelativeArtifactPath {
        &self.path
    }

    /// Borrow exact desired-only bytes without reading any prior publication.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

struct StagedInspection {
    relative: RelativeArtifactPath,
    target: PathBuf,
    temporary: Option<PathBuf>,
    identity: ContentIdentity,
}

impl StagedInspection {
    fn commit(mut self) -> Result<InspectionPublication, crate::error::InspectionError> {
        let temporary = self
            .temporary
            .take()
            .expect("staged inspection owns one temporary path");
        if let Err(source) = std::fs::rename(&temporary, &self.target) {
            self.temporary = Some(temporary);
            return Err(crate::error::InspectionError::Publish {
                path: self.relative.as_path().to_path_buf(),
                source,
            });
        }
        Ok(InspectionPublication {
            path: self.relative.clone(),
            identity: self.identity.clone(),
        })
    }
}

impl Drop for StagedInspection {
    fn drop(&mut self) {
        if let Some(path) = &self.temporary {
            let _ = std::fs::remove_file(path);
        }
    }
}

static NEXT_INSPECTION_TEMP: AtomicU64 = AtomicU64::new(0);

/// Atomically replace every rendered artifact from same-directory staged bytes.
///
/// All artifacts are rendered before this function is called and all temporary
/// files are staged before the first replacement. Each replacement is atomic;
/// callers that require a single publication transaction should declare one
/// inspection artifact, as the current Compose platform does.
pub fn publish_rendered_inspection(
    deployment_dir: &Path,
    rendered: Vec<RenderedInspection>,
) -> Result<Vec<InspectionPublication>, crate::error::InspectionError> {
    let root = std::fs::canonicalize(deployment_dir).map_err(|source| {
        crate::error::InspectionError::Prepare {
            path: PathBuf::from("."),
            source,
        }
    })?;
    let mut staged = Vec::with_capacity(rendered.len());
    for artifact in rendered {
        staged.push(stage_inspection(&root, artifact)?);
    }
    staged.into_iter().map(StagedInspection::commit).collect()
}

fn stage_inspection(
    root: &Path,
    artifact: RenderedInspection,
) -> Result<StagedInspection, crate::error::InspectionError> {
    let relative = artifact.path;
    let canonical_parent = prepare_inspection_parent(root, &relative)?;
    let target = canonical_parent.join(
        relative
            .as_path()
            .file_name()
            .expect("validated relative inspection target has a file name"),
    );

    let temporary = create_inspection_temp(&canonical_parent, &target, &relative)?;
    let identity = ContentIdentity::new(
        &format!("inspection-artifact/{}", relative.as_path().display()),
        &artifact.bytes,
    );
    let mut file = temporary.1;
    let temporary_path = temporary.0;
    if let Err(source) = file
        .write_all(&artifact.bytes)
        .and_then(|()| file.flush())
        .and_then(|()| file.sync_all())
    {
        let _ = std::fs::remove_file(&temporary_path);
        return Err(crate::error::InspectionError::Stage {
            path: relative.as_path().to_path_buf(),
            source,
        });
    }
    drop(file);
    Ok(StagedInspection {
        relative,
        target,
        temporary: Some(temporary_path),
        identity,
    })
}

fn prepare_inspection_parent(
    root: &Path,
    relative: &RelativeArtifactPath,
) -> Result<PathBuf, crate::error::InspectionError> {
    let mut current = root.to_path_buf();
    let parent = relative
        .as_path()
        .parent()
        .expect("a non-empty relative inspection target has a parent");
    for component in parent.components() {
        let Component::Normal(segment) = component else {
            unreachable!("RelativeArtifactPath admits only normal components");
        };
        current.push(segment);
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(crate::error::InspectionError::EscapingTarget {
                    path: relative.as_path().to_path_buf(),
                });
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(crate::error::InspectionError::Prepare {
                    path: relative.as_path().to_path_buf(),
                    source: std::io::Error::new(
                        std::io::ErrorKind::NotADirectory,
                        format!("{} is not a directory", current.display()),
                    ),
                });
            }
            Ok(_) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current).map_err(|source| {
                    crate::error::InspectionError::Prepare {
                        path: relative.as_path().to_path_buf(),
                        source,
                    }
                })?;
            }
            Err(source) => {
                return Err(crate::error::InspectionError::Prepare {
                    path: relative.as_path().to_path_buf(),
                    source,
                });
            }
        }
        current = std::fs::canonicalize(&current).map_err(|source| {
            crate::error::InspectionError::Prepare {
                path: relative.as_path().to_path_buf(),
                source,
            }
        })?;
        if !current.starts_with(root) {
            return Err(crate::error::InspectionError::EscapingTarget {
                path: relative.as_path().to_path_buf(),
            });
        }
    }
    Ok(current)
}

fn create_inspection_temp(
    parent: &Path,
    target: &Path,
    relative: &RelativeArtifactPath,
) -> Result<(PathBuf, File), crate::error::InspectionError> {
    let file_name = target
        .file_name()
        .expect("validated relative inspection target has a file name")
        .to_string_lossy();
    for _ in 0..32 {
        let sequence = NEXT_INSPECTION_TEMP.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".{file_name}.tokeira-{}-{sequence}.tmp",
            std::process::id()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(source) => {
                return Err(crate::error::InspectionError::Stage {
                    path: relative.as_path().to_path_buf(),
                    source,
                });
            }
        }
    }
    Err(crate::error::InspectionError::Stage {
        path: relative.as_path().to_path_buf(),
        source: std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not reserve a unique same-directory temporary file",
        ),
    })
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
