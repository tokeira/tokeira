//! S3 as a TUF repository home: a `tough::Transport` for `s3://` URLs and a
//! publisher that uploads a repository under the write policy the layout
//! earns.
//!
//! ## Transport
//!
//! `S3Transport` maps `Transport::fetch` onto `GetObject`. Two mappings
//! matter for correctness:
//!
//! - `NoSuchKey` must surface as `TransportErrorKind::FileNotFound`. TUF's
//!   root-rotation walk probes `N+1.root.json` until it is absent; a
//!   transport that reports absence as a generic failure breaks every load
//!   (TUF v1.0.16 §5.2.2, quoted on the `FileNotFound` variant in `tough`).
//! - The object body streams through unchanged; `tough` owns hash/length
//!   verification against signed metadata, so the transport adds no
//!   integrity duties of its own.
//!
//! ## Write policy
//!
//! With consistent snapshots on, every object except `timestamp.json` is
//! version- or digest-named and therefore immutable: uploaded create-only
//! (`If-None-Match: *`), with a byte-compare on collision — the same
//! discipline the platform-source-set spec assigns its blob and descriptor
//! classes. `timestamp.json` is the single mutable head, uploaded
//! last-writer-wins (production would gate it on `If-Match` and serialize
//! publishers with an operation lease).

use std::path::Path;

use anyhow::Context as _;
use aws_sdk_s3::{Client, error::SdkError, primitives::ByteStream};
use tough::{Transport, TransportError, TransportErrorKind, TransportStream};
use url::Url;

/// A `tough` transport that serves `s3://<bucket>/<key>` URLs via
/// `GetObject`.
#[derive(Debug, Clone)]
pub struct S3Transport {
    client: Client,
}

impl S3Transport {
    /// Wrap a configured S3 client (region, credentials, and any test
    /// endpoint already applied).
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// Split an `s3://bucket/key` URL.
fn bucket_and_key(url: &Url) -> Result<(String, String), TransportError> {
    if url.scheme() != "s3" {
        return Err(TransportError::new(
            TransportErrorKind::UnsupportedUrlScheme,
            url.as_str(),
        ));
    }
    let bucket = url
        .host_str()
        .ok_or_else(|| TransportError::new(TransportErrorKind::Other, url.as_str()))?;
    let key = url.path().trim_start_matches('/');
    if key.is_empty() {
        return Err(TransportError::new(TransportErrorKind::Other, url.as_str()));
    }
    Ok((bucket.to_owned(), key.to_owned()))
}

#[async_trait::async_trait]
impl Transport for S3Transport {
    async fn fetch(&self, url: Url) -> Result<TransportStream, TransportError> {
        let (bucket, key) = bucket_and_key(&url)?;
        let response = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .map_err(|err| classify_get_error(err, &url))?;

        // Adapt the SDK body stream to tough's stream type; integrity is
        // tough's job, transport errors are ours.
        let url_for_err = url.to_string();
        let stream =
            futures::stream::unfold((response.body, url_for_err), |(mut body, url)| async move {
                match body.next().await {
                    Some(Ok(bytes)) => Some((Ok(bytes), (body, url))),
                    Some(Err(err)) => Some((
                        Err(TransportError::new_with_cause(
                            TransportErrorKind::Other,
                            url.clone(),
                            err,
                        )),
                        (body, url),
                    )),
                    None => None,
                }
            });
        Ok(Box::pin(stream))
    }
}

/// `NoSuchKey` (and bare 404s from S3-compatible endpoints) mean "absent";
/// everything else is a transport fault.
fn classify_get_error(
    err: SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
    url: &Url,
) -> TransportError {
    let not_found = match &err {
        SdkError::ServiceError(service) => {
            service.err().is_no_such_key() || service.raw().status().as_u16() == 404
        }
        _ => false,
    };
    let kind = if not_found {
        TransportErrorKind::FileNotFound
    } else {
        TransportErrorKind::Other
    };
    TransportError::new_with_cause(kind, url.as_str(), err)
}

/// Upload result for one object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadOutcome {
    /// Object created.
    Created,
    /// Immutable object already present with identical bytes (idempotent
    /// republish).
    AlreadyPresent,
    /// Mutable head replaced.
    Replaced,
}

/// Upload a published repository directory pair to
/// `s3://<bucket>/<prefix>/{metadata,targets}/…` under the write policy.
pub async fn upload_repository(
    client: &Client,
    bucket: &str,
    prefix: &str,
    metadata_dir: &Path,
    targets_dir: &Path,
) -> anyhow::Result<Vec<(String, UploadOutcome)>> {
    let mut outcomes = Vec::new();
    for (dir, class) in [(metadata_dir, "metadata"), (targets_dir, "targets")] {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .with_context(|| format!("reading {}", dir.display()))?
            .collect::<Result<_, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            let key = format!("{prefix}/{class}/{name}");
            let bytes = std::fs::read(entry.path())?;
            // Mutable heads: the freshness statement, and the un-versioned
            // trust-anchor copy of root.json. Everything else is
            // version-/digest-named and immutable.
            let mutable = name == "timestamp.json" || name == "root.json";
            let outcome = if mutable {
                put_unconditional(client, bucket, &key, bytes).await?
            } else {
                put_create_only(client, bucket, &key, bytes).await?
            };
            outcomes.push((key, outcome));
        }
    }
    Ok(outcomes)
}

async fn put_unconditional(
    client: &Client,
    bucket: &str,
    key: &str,
    bytes: Vec<u8>,
) -> anyhow::Result<UploadOutcome> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(bytes))
        .send()
        .await
        .with_context(|| format!("putting {key}"))?;
    Ok(UploadOutcome::Replaced)
}

/// Create-only put: `If-None-Match: *`, and on collision verify the existing
/// object holds the identical bytes — "verify exact bytes on collision", the
/// platform-source-set spec's rule for immutable classes.
async fn put_create_only(
    client: &Client,
    bucket: &str,
    key: &str,
    bytes: Vec<u8>,
) -> anyhow::Result<UploadOutcome> {
    let result = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from(bytes.clone()))
        .send()
        .await;
    match result {
        Ok(_) => Ok(UploadOutcome::Created),
        Err(err) if is_precondition_failed(&err) => {
            let existing = client
                .get_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .with_context(|| format!("verifying existing {key}"))?
                .body
                .collect()
                .await
                .with_context(|| format!("reading existing {key}"))?
                .into_bytes();
            anyhow::ensure!(
                existing.as_ref() == bytes.as_slice(),
                "immutable object {key} exists with different bytes"
            );
            Ok(UploadOutcome::AlreadyPresent)
        }
        Err(err) => Err(err).with_context(|| format!("putting {key}")),
    }
}

fn is_precondition_failed<E>(err: &SdkError<E>) -> bool {
    match err {
        SdkError::ServiceError(service) => {
            let status = service.raw().status().as_u16();
            status == 412 || status == 409
        }
        _ => false,
    }
}

/// Base URLs for loading a repository straight from S3.
pub fn repo_urls(bucket: &str, prefix: &str) -> anyhow::Result<(Url, Url)> {
    let metadata = Url::parse(&format!("s3://{bucket}/{prefix}/metadata/"))?;
    let targets = Url::parse(&format!("s3://{bucket}/{prefix}/targets/"))?;
    Ok((metadata, targets))
}
